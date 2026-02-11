"""Email Sync page -- fetch bank transaction alerts from email via IMAP."""

import json
import streamlit as st
import pandas as pd
from datetime import datetime

from core.email_parser import (
    IMAP_PRESETS,
    EmailConnectionError,
    FetchCancelledError,
    fetch_transactions_from_email,
)
from core.database import (
    insert_transactions,
    get_all_categories,
    get_transactions,
    get_setting,
    set_setting,
    delete_setting,
    get_upload_history,
    delete_transactions_by_file,
    find_duplicate_transactions,
    find_within_file_duplicates,
)
from core.dedup import detect_cc_payments
from core.categorizer import categorize_transactions

_EMAIL_CONFIG_KEY = "email_sync_config"


def render():
    st.header("Email Sync")
    st.caption(
        "Fetch credit & debit transaction alerts directly from your email inbox. "
        "Works with Gmail, Outlook, Yahoo, Zoho, or any IMAP-compatible provider."
    )

    # Restore saved config into session state on first load
    _restore_saved_config()

    # --- Email configuration (persisted in DB) ---
    _render_email_config()

    if not _has_valid_config():
        return

    st.divider()

    # --- Month / Year selection ---
    col1, col2 = st.columns(2)
    now = datetime.now()

    with col1:
        month = st.selectbox(
            "Month to fetch",
            list(range(1, 13)),
            index=now.month - 1,
            format_func=lambda m: datetime(2000, m, 1).strftime("%B"),
            key="email_sync_month",
        )

    with col2:
        year = st.number_input(
            "Year",
            min_value=2020,
            max_value=2030,
            value=now.year,
            key="email_sync_year",
        )

    # Email alerts cover both bank and credit card transactions automatically
    source_key = "bank"

    # --- Fetch button ---
    if st.button("Fetch Transactions from Email", type="primary", use_container_width=True):
        _fetch_and_display(month, year, source_key)

    # --- Show previously fetched transactions (if any in session state) ---
    if "email_transactions" in st.session_state and st.session_state.email_transactions:
        _render_preview_and_save(
            st.session_state.email_transactions,
            source_key,
            month,
            year,
        )

    st.divider()
    _render_sync_history()


# ---------------------------------------------------------------------------
# Config persistence helpers
# ---------------------------------------------------------------------------

def _restore_saved_config():
    """Load saved email config from DB into session state (once per session)."""
    if "email_config_restored" in st.session_state:
        return  # already restored this session

    st.session_state["email_config_restored"] = True

    raw = get_setting(_EMAIL_CONFIG_KEY)
    if not raw:
        return

    try:
        saved = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return

    # Populate the widget default values via session state
    # (Streamlit reads these before rendering the widgets)
    provider_names = list(IMAP_PRESETS.keys())
    saved_host = saved.get("host", "")
    matched_provider = "Custom"
    for name, preset in IMAP_PRESETS.items():
        if preset.get("host") == saved_host and name != "Custom":
            matched_provider = name
            break

    if "email_provider" not in st.session_state:
        st.session_state["email_provider"] = matched_provider
    if "email_address" not in st.session_state:
        st.session_state["email_address"] = saved.get("email", "")
    if "email_password" not in st.session_state:
        st.session_state["email_password"] = saved.get("password", "")
    if "email_folder" not in st.session_state:
        st.session_state["email_folder"] = saved.get("folder", "INBOX")
    if matched_provider == "Custom" and "imap_host" not in st.session_state:
        st.session_state["imap_host"] = saved_host

    # Also set the runtime config dict
    st.session_state["email_config"] = saved


def _save_config_to_db(config: dict) -> None:
    """Persist email config to the database settings table."""
    set_setting(_EMAIL_CONFIG_KEY, json.dumps(config))


def _clear_saved_config() -> None:
    """Remove saved email config from the database."""
    delete_setting(_EMAIL_CONFIG_KEY)
    for key in ["email_config", "email_provider", "email_address",
                 "email_password", "email_folder", "imap_host",
                 "email_config_restored"]:
        st.session_state.pop(key, None)


# ---------------------------------------------------------------------------
# Email configuration UI
# ---------------------------------------------------------------------------

def _render_email_config():
    """Render the email configuration form in an expander."""
    is_configured = _has_valid_config()
    label = "Email Configuration (connected)" if is_configured else "Email Configuration (setup required)"

    with st.expander(label, expanded=not is_configured):
        st.info(
            "**Gmail users:** Use an [App Password](https://myaccount.google.com/apppasswords) "
            "instead of your regular password. You must have 2-Step Verification enabled."
        )

        col1, col2 = st.columns([2, 1])

        with col1:
            provider = st.selectbox(
                "Email Provider",
                list(IMAP_PRESETS.keys()),
                key="email_provider",
            )

        preset = IMAP_PRESETS[provider]

        with col2:
            if provider == "Custom":
                imap_host = st.text_input("IMAP Host", key="imap_host")
            else:
                imap_host = preset["host"]
                st.text_input("IMAP Host", value=imap_host, disabled=True, key="imap_host_display")

        imap_port = preset["port"]

        email_address = st.text_input(
            "Email Address",
            placeholder="you@gmail.com",
            key="email_address",
        )

        password = st.text_input(
            "Password / App Password",
            type="password",
            placeholder="Your app password (not regular password for Gmail)",
            key="email_password",
        )

        folder = st.text_input(
            "IMAP Folder",
            value="INBOX",
            help="Usually INBOX. Gmail labels can be accessed as e.g. '[Gmail]/All Mail'",
            key="email_folder",
        )

        # Store config in session state and persist to DB
        if email_address and password:
            config = {
                "host": imap_host if provider == "Custom" else preset["host"],
                "port": imap_port,
                "email": email_address,
                "password": password,
                "folder": folder or "INBOX",
            }
            st.session_state["email_config"] = config

            btn_col1, btn_col2 = st.columns(2)
            with btn_col1:
                if st.button("Save & Test Connection", key="test_email_conn"):
                    _save_config_to_db(config)
                    _test_connection()
            with btn_col2:
                if is_configured and st.button("Forget Saved Credentials", key="clear_email_config"):
                    _clear_saved_config()
                    st.success("Saved credentials removed.")
                    st.rerun()


def _has_valid_config() -> bool:
    """Check if email configuration is present in session state."""
    config = st.session_state.get("email_config")
    return bool(
        config
        and config.get("host")
        and config.get("email")
        and config.get("password")
    )


def _test_connection():
    """Test the IMAP connection with current settings."""
    config = st.session_state.get("email_config", {})
    with st.spinner("Testing connection..."):
        try:
            from core.email_parser import connect_imap, disconnect_imap
            mail = connect_imap(
                config["host"],
                config["port"],
                config["email"],
                config["password"],
            )
            disconnect_imap(mail)
            st.success("Connection successful! Your email credentials are valid.")
        except EmailConnectionError as e:
            st.error(f"Connection failed: {e}")
        except Exception as e:
            st.error(f"Unexpected error: {e}")


# ---------------------------------------------------------------------------
# Fetch and display transactions (synchronous with live progress)
# ---------------------------------------------------------------------------

def _fetch_and_display(month: int, year: int, source_key: str):
    """Fetch transaction emails with a live progress indicator and cancel support."""
    config = st.session_state.get("email_config", {})
    month_name = datetime(year, month, 1).strftime("%B %Y")

    # Reset cancel flag
    st.session_state["_email_fetch_cancel"] = False

    # Header row: title + cancel button
    hdr_col1, hdr_col2 = st.columns([5, 1])
    with hdr_col1:
        status_container = st.status(
            "Fetching Transaction Details from Email", expanded=True,
        )
    with hdr_col2:
        if st.button("Cancel", key="cancel_fetch", type="secondary"):
            st.session_state["_email_fetch_cancel"] = True

    progress_text = status_container.empty()

    def _on_progress(step: str, detail: str):
        progress_text.text(detail)

    def _is_cancelled() -> bool:
        return st.session_state.get("_email_fetch_cancel", False)

    try:
        transactions = fetch_transactions_from_email(
            host=config["host"],
            port=config["port"],
            email_address=config["email"],
            password=config["password"],
            month=month,
            year=year,
            folder=config.get("folder", "INBOX"),
            on_progress=_on_progress,
            is_cancelled=_is_cancelled,
        )
    except FetchCancelledError:
        status_container.update(label="Fetch cancelled", state="error", expanded=False)
        st.warning("Email fetch was cancelled.")
        st.session_state["email_transactions"] = []
        return
    except EmailConnectionError as e:
        status_container.update(label="Connection failed", state="error", expanded=False)
        st.error(f"Connection error: {e}")
        return
    except Exception as e:
        status_container.update(label="Fetch failed", state="error", expanded=False)
        st.error(f"Failed to fetch emails: {e}")
        return

    if not transactions:
        status_container.update(label="No alerts found", state="complete", expanded=False)
        st.warning(
            f"No transaction alerts found for **{month_name}**. "
            "This could mean:\n"
            "- No bank alert emails exist for this period\n"
            "- Alert emails are in a different folder (try '[Gmail]/All Mail')\n"
            "- The sender addresses aren't recognised (check email filters)"
        )
        st.session_state["email_transactions"] = []
        return

    # Tag each transaction with source info
    for t in transactions:
        t["_source_file"] = f"email_{config['email']}_{month:02d}_{year}"

    status_container.update(
        label=f"Found {len(transactions)} transaction(s)", state="complete", expanded=False,
    )
    st.session_state["email_transactions"] = transactions
    st.success(f"Found **{len(transactions)}** transaction(s) from email alerts for {month_name}!")


# ---------------------------------------------------------------------------
# Preview and save
# ---------------------------------------------------------------------------

def _render_preview_and_save(transactions: list[dict], source_key: str, month: int, year: int):
    """Show fetched transactions with dedup checks and save option."""
    st.divider()
    st.subheader(f"Fetched Transactions ({len(transactions)})")

    # --- Detect issues ---
    out_of_month = _find_out_of_month(transactions, month, year)
    within_dupes = find_within_file_duplicates(transactions)
    db_dupes = find_duplicate_transactions(transactions, email_only=True)

    txns_to_skip = set()

    if out_of_month:
        txns_to_skip |= _show_out_of_month_dialog(transactions, out_of_month, month, year)

    if within_dupes:
        txns_to_skip |= _show_within_dupes_dialog(transactions, within_dupes)

    if db_dupes:
        txns_to_skip |= _show_db_dupes_dialog(db_dupes)

    # --- Filter ---
    filtered = [t for i, t in enumerate(transactions) if i not in txns_to_skip]

    if txns_to_skip:
        st.caption(f"{len(txns_to_skip)} transaction(s) will be skipped (out-of-month or duplicates)")

    # --- Preview table ---
    if filtered:
        preview_df = pd.DataFrame([
            {
                "Date": t["date"],
                "Description": t["description"][:60],
                "Amount": f"₹{t['amount']:,.2f}",
                "Type": t["type"].capitalize(),
            }
            for t in filtered
        ])
        preview_df.index = range(1, len(preview_df) + 1)
        st.dataframe(preview_df, use_container_width=True, height=350)

        # --- Summary metrics ---
        total_debits = sum(t["amount"] for t in filtered if t["type"] == "debit")
        total_credits = sum(t["amount"] for t in filtered if t["type"] == "credit")

        m1, m2, m3 = st.columns(3)
        m1.metric("Total Debits", f"₹{total_debits:,.2f}")
        m2.metric("Total Credits", f"₹{total_credits:,.2f}")
        m3.metric("Transactions", len(filtered))

        # --- Save button ---
        if st.button("Save & Categorize", type="primary", use_container_width=True, key="email_save"):
            _save_and_categorize(filtered, source_key, month, year)
            # Clear fetched transactions after save
            st.session_state["email_transactions"] = []
            st.rerun()
    else:
        st.warning("No transactions to save after filtering.")


# ---------------------------------------------------------------------------
# Issue detection helpers (mirrors upload.py patterns)
# ---------------------------------------------------------------------------

def _find_out_of_month(txns: list[dict], month: int, year: int) -> list[int]:
    """Return indices of transactions outside the selected month."""
    out = []
    for i, t in enumerate(txns):
        try:
            d = datetime.strptime(t["date"], "%Y-%m-%d")
            if d.month != month or d.year != year:
                out.append(i)
        except (ValueError, TypeError):
            pass
    return out


def _show_out_of_month_dialog(txns, indices, month, year):
    month_name = datetime(year, month, 1).strftime("%B %Y")
    skip_set = set()
    with st.expander(f"⚠️ {len(indices)} transaction(s) outside {month_name}", expanded=True):
        st.warning(
            f"These transactions have dates outside **{month_name}**. "
            f"Uncheck to exclude from this sync."
        )
        for idx in indices:
            t = txns[idx]
            keep = st.checkbox(
                f"{t['date']} | {t['description'][:50]} | ₹{t['amount']:,.2f}",
                value=False,
                key=f"email_oom_{idx}",
            )
            if not keep:
                skip_set.add(idx)
    return skip_set


def _show_within_dupes_dialog(txns, dupe_pairs):
    skip_set = set()
    with st.expander(f"⚠️ {len(dupe_pairs)} duplicate(s) in fetched emails", expanded=True):
        st.warning("Duplicate transactions detected. Second occurrence will be skipped by default.")
        for idx_a, idx_b in dupe_pairs:
            t = txns[idx_b]
            keep = st.checkbox(
                f"Duplicate: {t['date']} | {t['description'][:50]} | ₹{t['amount']:,.2f}",
                value=False,
                key=f"email_wfd_{idx_b}",
            )
            if not keep:
                skip_set.add(idx_b)
    return skip_set


def _show_db_dupes_dialog(db_dupes):
    skip_set = set()
    with st.expander(f"⚠️ {len(db_dupes)} transaction(s) already in database", expanded=True):
        st.warning("These match existing records. They'll be skipped to avoid double-counting.")
        for d in db_dupes:
            t = d["new_txn"]
            keep = st.checkbox(
                f"{t['date']} | {t['description'][:45]} | ₹{t['amount']:,.2f} "
                f"(matches: {d['existing_desc'][:30]}...)",
                value=False,
                key=f"email_dbd_{d['new_idx']}",
            )
            if not keep:
                skip_set.add(d["new_idx"])
    return skip_set


# ---------------------------------------------------------------------------
# Save & categorize (mirrors upload.py pattern)
# ---------------------------------------------------------------------------

def _save_and_categorize(transactions: list[dict], source_key: str, month: int, year: int):
    """Save transactions to DB, run dedup, and auto-categorize."""
    rows = []
    for t in transactions:
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
            "uploaded_file": t.get("_source_file", "email_sync"),
            "email_body": t.get("email_body"),
        })

    with st.spinner("Saving transactions..."):
        count = insert_transactions(rows)

    st.success(f"Saved **{count}** transactions from email.")

    # --- CC payment deduplication (bank statements only) ---
    if source_key == "bank":
        with st.spinner("Checking for credit card payment duplicates..."):
            saved_txns = get_transactions(
                month=month, year=year, source="bank",
                include_excluded=True, email_only=True,
            )
            flagged_ids = detect_cc_payments(saved_txns)

            if flagged_ids:
                from core.database import flag_cc_payments_visible
                flag_cc_payments_visible(flagged_ids)
                st.warning(
                    f"Flagged **{len(flagged_ids)}** transaction(s) as likely credit card payments."
                )

    # --- LLM Categorization (email transactions only) ---
    with st.spinner("Categorizing with Ollama..."):
        try:
            saved_txns = get_transactions(month=month, year=year, email_only=True)
            uncategorized = [t for t in saved_txns if not t.get("category")]

            if uncategorized:
                categories = get_all_categories()
                results = categorize_transactions(uncategorized, categories)

                if results:
                    from core.database import bulk_update_categories
                    bulk_update_categories(results)
                    st.success(
                        f"Auto-categorized **{len(results)}** / {len(uncategorized)} transactions."
                    )
                else:
                    st.info("Categorization returned no results. Categorize manually in Email > Transactions.")
            else:
                st.info("All transactions already have categories.")
        except RuntimeError as e:
            st.warning(f"Categorization skipped: {e}")
        except Exception as e:
            st.warning(f"Categorization issue: {e}")


# ---------------------------------------------------------------------------
# Sync history
# ---------------------------------------------------------------------------

def _render_sync_history():
    """Show past email syncs with option to delete."""
    history = get_upload_history(email_only=True)
    if not history:
        return

    st.subheader("Sync History")

    for h in history:
        month_label = datetime(h["year"], h["month"], 1).strftime("%B %Y")

        col1, col2, col3, col4 = st.columns([3, 1.5, 1, 1])
        with col1:
            st.write(f"**{month_label}**")
        with col2:
            st.write(f"{h['txn_count']} transactions")
        with col3:
            synced_at = h["uploaded_at"][:16] if h["uploaded_at"] else "—"
            st.caption(synced_at)
        with col4:
            if st.button("Delete", key=f"edel_{h['uploaded_file']}_{h['month']}_{h['year']}"):
                deleted = delete_transactions_by_file(h["uploaded_file"])
                st.success(f"Deleted {deleted} transaction(s) for {month_label}")
                st.rerun()
