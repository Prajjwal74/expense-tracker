#!/usr/bin/env python3
"""
Headless daily email fetch + categorize + notify.

Runs without Streamlit. Designed to be called by cron:
    0 20 * * * cd ~/Desktop/Cursor/expense-tracker && ./venv/bin/python fetch_daily.py

Reuses existing core modules for email parsing, categorization, and database.
"""

import json
import logging
import os
import sys
from datetime import datetime

import requests
from dotenv import load_dotenv

# Ensure the project root is on the path
sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()

from core.database import (
    get_setting,
    init_db,
    insert_transactions,
    get_all_categories,
    get_transactions,
    find_duplicate_transactions,
    flag_cc_payments_visible,
)
from core.email_parser import (
    fetch_transactions_from_email,
    EmailConnectionError,
)
from core.categorizer import categorize_transactions
from core.dedup import detect_cc_payments

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

_EMAIL_CONFIG_KEY = "email_sync_config"

# ntfy.sh configuration
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "")
NTFY_SERVER = os.getenv("NTFY_SERVER", "https://ntfy.sh")


def _get_email_config() -> dict:
    """Load email config from the database settings table."""
    raw = get_setting(_EMAIL_CONFIG_KEY)
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}


def _send_notification(title: str, message: str, url: str = ""):
    """Send a push notification via ntfy.sh."""
    if not NTFY_TOPIC:
        logger.info("NTFY_TOPIC not set -- skipping notification.")
        logger.info("Notification would be: %s -- %s", title, message)
        return

    headers = {"Title": title}
    if url:
        headers["Click"] = url
        headers["Actions"] = f"view, Review Transactions, {url}"

    try:
        resp = requests.post(
            f"{NTFY_SERVER}/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers=headers,
            timeout=10,
        )
        if resp.status_code == 200:
            logger.info("Notification sent successfully.")
        else:
            logger.warning("ntfy returned %d: %s", resp.status_code, resp.text[:200])
    except Exception as e:
        logger.warning("Failed to send notification: %s", e)


def main():
    """Fetch today's transactions from email, categorize, save, and notify."""
    init_db()

    # Load email config
    config = _get_email_config()
    if not config or not config.get("host") or not config.get("email") or not config.get("password"):
        logger.error(
            "Email config not found. Set it up in the Expense Tracker web app first "
            "(Email Sync > Email Configuration)."
        )
        sys.exit(1)

    now = datetime.now()
    month = now.month
    year = now.year
    month_name = now.strftime("%B %Y")
    source_key = "bank"
    uploaded_file = f"email_{config['email']}_{month:02d}_{year}"

    logger.info("Fetching transactions for %s...", month_name)

    # Fetch emails
    try:
        transactions = fetch_transactions_from_email(
            host=config["host"],
            port=config["port"],
            email_address=config["email"],
            password=config["password"],
            month=month,
            year=year,
            folder=config.get("folder", "INBOX"),
            on_progress=lambda step, detail: logger.info("[%s] %s", step, detail),
        )
    except EmailConnectionError as e:
        logger.error("Connection failed: %s", e)
        _send_notification(
            "Expense Tracker: Sync Failed",
            f"Could not connect to email: {e}",
        )
        sys.exit(1)

    if not transactions:
        logger.info("No new transaction emails found for %s.", month_name)
        return

    logger.info("Found %d transaction(s) from email.", len(transactions))

    # Tag with source
    for t in transactions:
        t["_source_file"] = uploaded_file

    # Deduplicate against existing DB
    dupes = find_duplicate_transactions(transactions, email_only=True)
    dupe_indices = {d["new_idx"] for d in dupes}
    new_txns = [t for i, t in enumerate(transactions) if i not in dupe_indices]

    if not new_txns:
        logger.info("All %d transactions already exist in DB. Nothing new.", len(transactions))
        return

    logger.info("%d new transaction(s) after dedup (%d duplicates skipped).",
                len(new_txns), len(dupe_indices))

    # Prepare rows for insert
    rows = []
    for t in new_txns:
        rows.append({
            "date": t["date"],
            "description": t["description"],
            "amount": t["amount"],
            "type": t["type"],
            "source": source_key,
            "category": None,
            "is_cc_payment": 0,
            "is_excluded": 0,
            "month": month,
            "year": year,
            "uploaded_file": uploaded_file,
            "email_body": t.get("email_body"),
        })

    # Save
    count = insert_transactions(rows)
    logger.info("Saved %d transaction(s) to database.", count)

    # CC payment detection
    saved_txns = get_transactions(month=month, year=year, source="bank",
                                  include_excluded=True, email_only=True)
    flagged_ids = detect_cc_payments(saved_txns)
    if flagged_ids:
        flag_cc_payments_visible(flagged_ids)
        logger.info("Flagged %d CC payment(s).", len(flagged_ids))

    # Categorize
    saved_txns = get_transactions(month=month, year=year, email_only=True)
    uncategorized = [t for t in saved_txns if not t.get("category")]

    cat_summary = {}
    if uncategorized:
        logger.info("Categorizing %d uncategorized transaction(s)...", len(uncategorized))
        try:
            categories = get_all_categories()
            results = categorize_transactions(uncategorized, categories)
            if results:
                from core.database import bulk_update_categories
                bulk_update_categories(results)
                logger.info("Categorized %d transaction(s).", len(results))
        except Exception as e:
            logger.warning("Categorization failed: %s", e)

    # Build summary for notification
    # Re-fetch to get final categories
    final_txns = get_transactions(month=month, year=year, email_only=True)
    # Only count today's new ones
    new_ids = set()
    for t in final_txns:
        if t.get("uploaded_file") == uploaded_file:
            new_ids.add(t["id"])

    today_txns = [t for t in final_txns if t["id"] in new_ids or t.get("date") == now.strftime("%Y-%m-%d")]

    # Category summary
    for t in new_txns:
        # Re-read from DB to get assigned category
        pass

    # Simple summary from what we saved
    cat_counts = {}
    total_amount = 0
    uncat_count = 0
    for t in rows:
        total_amount += t["amount"]

    # Re-fetch the just-saved transactions for their categories
    recent = get_transactions(month=month, year=year, email_only=True)
    for t in recent[-len(rows):]:
        cat = t.get("category") or "Uncategorized"
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
        if cat == "Uncategorized":
            uncat_count += 1

    # Format notification
    summary_parts = [f"{cnt}x {cat}" for cat, cnt in sorted(cat_counts.items(), key=lambda x: -x[1])]
    summary_str = ", ".join(summary_parts[:5])

    title = f"Expense Tracker: {len(rows)} new transaction(s)"
    body = f"{summary_str}\nTotal: Rs {total_amount:,.0f}"
    if uncat_count:
        body += f"\n{uncat_count} need review"

    # Use cloud URL if set, otherwise fall back to localhost
    cloud_url = os.getenv("APP_URL", "").strip()
    if cloud_url:
        review_url = f"{cloud_url}/?section=Email&page=Transactions"
    else:
        review_url = "http://localhost:8501/?section=Email&page=Transactions"

    logger.info("Summary: %s", title)
    logger.info("Details: %s", body.replace("\n", " | "))

    _send_notification(title, body, url=review_url)
    logger.info("Done.")


if __name__ == "__main__":
    main()
