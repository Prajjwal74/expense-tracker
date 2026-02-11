"""Upload Statement page – multi-file, image support, dedup, out-of-month detection."""

import streamlit as st
import pandas as pd
from datetime import datetime

from core.parser import parse_csv, parse_pdf, parse_image
from core.database import (
    insert_transactions,
    get_all_categories,
    get_transactions,
    get_upload_history,
    delete_transactions_by_file,
    find_duplicate_transactions,
    find_within_file_duplicates,
)

# Upload page only shows file-uploaded transactions (not email-synced)
_EMAIL_ONLY = False
from core.dedup import detect_cc_payments
from core.categorizer import categorize_transactions

IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg", ".webp", ".bmp")


def render():
    st.header("Upload Statement")

    # --- Upload controls ---
    col1, col2, col3 = st.columns(3)

    with col1:
        source = st.selectbox("Statement Type", ["Bank Statement", "Credit Card Statement"])
        source_key = "bank" if source == "Bank Statement" else "credit_card"

    with col2:
        now = datetime.now()
        month = st.selectbox(
            "Assign to Month",
            list(range(1, 13)),
            index=now.month - 1,
            format_func=lambda m: datetime(2000, m, 1).strftime("%B"),
        )

    with col3:
        year = st.number_input(
            "Year", min_value=2020, max_value=2030, value=now.year
        )

    uploaded_files = st.file_uploader(
        "Upload your statement(s)",
        type=["csv", "xlsx", "xls", "pdf", "png", "jpg", "jpeg", "webp", "bmp"],
        help="Accepted: CSV, Excel, PDF, or images (PNG/JPEG) of bank statements",
        accept_multiple_files=True,
    )

    if not uploaded_files:
        st.info("Upload one or more bank/credit card statements to get started.")
        _render_upload_history()
        return

    # --- Parse all files ---
    all_transactions = []
    file_results = []

    for uploaded_file in uploaded_files:
        file_bytes = uploaded_file.read()
        filename = uploaded_file.name
        ext = filename.lower()

        with st.spinner(f"Parsing `{filename}`..."):
            try:
                if ext.endswith(IMAGE_EXTENSIONS):
                    txns = parse_image(file_bytes)
                    col_mapping = None
                elif ext.endswith(".pdf"):
                    txns = parse_pdf(file_bytes)
                    col_mapping = None
                else:
                    txns, col_mapping = parse_csv(file_bytes, filename)
            except Exception as e:
                st.error(f"Failed to parse `{filename}`: {e}")
                continue

        if not txns:
            st.warning(f"No transactions extracted from `{filename}`.")
            continue

        # Tag each txn with its source file
        for t in txns:
            t["_source_file"] = filename

        file_results.append({
            "filename": filename,
            "count": len(txns),
            "col_mapping": col_mapping if not ext.endswith(IMAGE_EXTENSIONS) and not ext.endswith(".pdf") else None,
        })
        all_transactions.extend(txns)

    if not all_transactions:
        st.warning("No transactions could be extracted from any file.")
        _render_upload_history()
        return

    # --- File summary ---
    for fr in file_results:
        st.success(f"Parsed **{fr['count']}** transactions from `{fr['filename']}`")
        if fr["col_mapping"]:
            with st.expander(f"Column mapping for {fr['filename']}"):
                for role, col in fr["col_mapping"].items():
                    st.write(f"**{role}**: {col if col else 'Not detected'}")

    # --- Detect issues BEFORE saving ---
    out_of_month = _find_out_of_month(all_transactions, month, year)
    within_dupes = find_within_file_duplicates(all_transactions)
    db_dupes = find_duplicate_transactions(all_transactions, email_only=False)

    # --- Show issues in popups ---
    txns_to_skip = set()  # indices to exclude from saving

    if out_of_month:
        txns_to_skip |= _show_out_of_month_dialog(all_transactions, out_of_month, month, year)

    if within_dupes:
        txns_to_skip |= _show_within_file_dupes_dialog(all_transactions, within_dupes)

    if db_dupes:
        txns_to_skip |= _show_db_dupes_dialog(db_dupes)

    # --- Filter out skipped transactions ---
    filtered_txns = [t for i, t in enumerate(all_transactions) if i not in txns_to_skip]

    # --- Preview ---
    st.subheader(f"Preview: {len(filtered_txns)} transaction(s) to save")
    if txns_to_skip:
        st.caption(f"{len(txns_to_skip)} transaction(s) will be skipped (out-of-month or duplicates)")

    preview_df = pd.DataFrame([
        {"date": t["date"], "description": t["description"][:60],
         "amount": t["amount"], "type": t["type"], "file": t["_source_file"]}
        for t in filtered_txns
    ])
    if not preview_df.empty:
        preview_df.index = range(1, len(preview_df) + 1)
        st.dataframe(preview_df, use_container_width=True, height=300)

    # --- Summary ---
    total_debits = sum(t["amount"] for t in filtered_txns if t["type"] == "debit")
    total_credits = sum(t["amount"] for t in filtered_txns if t["type"] == "credit")

    m1, m2, m3 = st.columns(3)
    m1.metric("Total Debits", f"₹{total_debits:,.2f}")
    m2.metric("Total Credits", f"₹{total_credits:,.2f}")
    m3.metric("Transactions", len(filtered_txns))

    # --- Save & Categorize ---
    if filtered_txns:
        if st.button("Save & Categorize", type="primary", use_container_width=True):
            _save_and_categorize(filtered_txns, source_key, month, year)

    st.divider()
    _render_upload_history()


# ---------------------------------------------------------------------------
# Out-of-month detection
# ---------------------------------------------------------------------------

def _find_out_of_month(txns: list[dict], month: int, year: int) -> list[int]:
    """Return indices of transactions whose date falls outside the selected month."""
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
    """Show out-of-month transactions and let user decide to skip them."""
    month_name = datetime(year, month, 1).strftime("%B %Y")
    skip_set = set()

    with st.expander(f"⚠️ {len(indices)} transaction(s) are outside {month_name}", expanded=True):
        st.warning(
            f"The following transactions have dates outside **{month_name}**. "
            f"Uncheck any you want to **exclude** from this upload."
        )
        for idx in indices:
            t = txns[idx]
            keep = st.checkbox(
                f"{t['date']} | {t['description'][:50]} | ₹{t['amount']:,.2f}",
                value=False,
                key=f"oom_{idx}",
            )
            if not keep:
                skip_set.add(idx)

    return skip_set


# ---------------------------------------------------------------------------
# Within-file duplicate detection
# ---------------------------------------------------------------------------

def _show_within_file_dupes_dialog(txns, dupe_pairs):
    """Show within-file duplicates and let user exclude the second occurrence."""
    skip_set = set()

    with st.expander(f"⚠️ {len(dupe_pairs)} duplicate(s) found within the file(s)", expanded=True):
        st.warning(
            "The following transactions appear more than once in the uploaded file(s). "
            "The duplicate (second occurrence) will be skipped by default."
        )
        for idx_a, idx_b in dupe_pairs:
            t = txns[idx_b]
            keep = st.checkbox(
                f"Duplicate: {t['date']} | {t['description'][:50]} | ₹{t['amount']:,.2f}",
                value=False,
                key=f"wfd_{idx_b}",
            )
            if not keep:
                skip_set.add(idx_b)

    return skip_set


# ---------------------------------------------------------------------------
# Cross-DB duplicate detection
# ---------------------------------------------------------------------------

def _show_db_dupes_dialog(db_dupes):
    """Show transactions that already exist in the database."""
    skip_set = set()

    with st.expander(f"⚠️ {len(db_dupes)} transaction(s) already exist in database", expanded=True):
        st.warning(
            "These transactions match existing records (same date, amount, and similar description). "
            "They will be skipped by default to avoid double-counting."
        )
        for d in db_dupes:
            t = d["new_txn"]
            keep = st.checkbox(
                f"{t['date']} | {t['description'][:45]} | ₹{t['amount']:,.2f} "
                f"(matches: {d['existing_desc'][:30]}... from {d['existing_file']})",
                value=False,
                key=f"dbd_{d['new_idx']}",
            )
            if not keep:
                skip_set.add(d["new_idx"])

    return skip_set


# ---------------------------------------------------------------------------
# Save & categorize
# ---------------------------------------------------------------------------

def _save_and_categorize(transactions, source_key, month, year):
    """Save transactions to DB, run dedup, and run LLM categorization."""
    # Group by source file for proper uploaded_file tagging
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
            "uploaded_file": t.get("_source_file", "unknown"),
        })

    with st.spinner("Saving transactions..."):
        count = insert_transactions(rows)

    st.success(f"Saved **{count}** transactions.")

    # --- CC payment deduplication (bank statements only) ---
    if source_key == "bank":
        with st.spinner("Checking for credit card payment duplicates..."):
            saved_txns = get_transactions(month=month, year=year, source="bank", include_excluded=True)
            flagged_ids = detect_cc_payments(saved_txns)

            if flagged_ids:
                from core.database import flag_cc_payments_visible
                flag_cc_payments_visible(flagged_ids)
                st.warning(
                    f"Flagged **{len(flagged_ids)}** transaction(s) as likely credit card payments "
                    f"(excluded from totals but still visible)."
                )

    # --- LLM Categorization via Ollama ---
    with st.spinner("Categorizing with Ollama... This may take a moment."):
        try:
            saved_txns = get_transactions(month=month, year=year)
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
                    st.info("Categorization returned no results. Categorize manually in Transactions page.")
            else:
                st.info("All transactions already have categories.")
        except RuntimeError as e:
            st.warning(f"Categorization skipped: {e}")
        except Exception as e:
            st.warning(f"Categorization issue: {e}")


# ---------------------------------------------------------------------------
# Upload history
# ---------------------------------------------------------------------------

def _render_upload_history():
    """Show past uploads with option to delete."""
    history = get_upload_history(email_only=_EMAIL_ONLY)
    if not history:
        return

    st.subheader("Upload History")

    for h in history:
        month_label = datetime(h["year"], h["month"], 1).strftime("%B %Y")
        source_label = "Bank" if h["source"] == "bank" else "Credit Card"

        col1, col2, col3, col4, col5 = st.columns([3, 1.5, 1, 1.5, 1])
        with col1:
            st.write(f"**{h['uploaded_file']}**")
        with col2:
            st.write(f"{month_label} / {source_label}")
        with col3:
            st.write(f"{h['txn_count']} txns")
        with col4:
            uploaded_at = h["uploaded_at"][:16] if h["uploaded_at"] else "—"
            st.caption(uploaded_at)
        with col5:
            if st.button("Delete", key=f"del_{h['uploaded_file']}_{h['month']}_{h['year']}"):
                deleted = delete_transactions_by_file(h["uploaded_file"])
                st.success(f"Deleted {deleted} transaction(s) from `{h['uploaded_file']}`")
                st.rerun()
