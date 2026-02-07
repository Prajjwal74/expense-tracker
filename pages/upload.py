"""Upload Statement page – parse, preview, deduplicate, and save."""

import streamlit as st
import pandas as pd
from datetime import datetime

from core.parser import parse_csv, parse_pdf
from core.database import (
    insert_transactions,
    get_all_categories,
    get_transactions,
    flag_cc_payments,
)
from core.dedup import detect_cc_payments
from core.categorizer import categorize_transactions


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

    uploaded_file = st.file_uploader(
        "Upload your statement",
        type=["csv", "xlsx", "xls", "pdf"],
        help="Accepted formats: CSV, Excel (.xlsx/.xls), PDF",
    )

    if uploaded_file is None:
        st.info("Upload a bank or credit card statement to get started.")
        return

    file_bytes = uploaded_file.read()
    filename = uploaded_file.name

    # --- Parse ---
    with st.spinner("Parsing statement..."):
        try:
            if filename.lower().endswith(".pdf"):
                transactions = parse_pdf(file_bytes)
                col_mapping = None
            else:
                transactions, col_mapping = parse_csv(file_bytes, filename)
        except Exception as e:
            st.error(f"Failed to parse file: {e}")
            return

    if not transactions:
        st.warning(
            "No transactions could be extracted. The file format may not be recognised. "
            "Please check that the file contains transaction data with dates and amounts."
        )
        return

    st.success(f"Parsed **{len(transactions)}** transactions from `{filename}`.")

    # Show detected column mapping for CSV/Excel
    if col_mapping:
        with st.expander("Detected column mapping"):
            for role, col in col_mapping.items():
                st.write(f"**{role}**: {col if col else '❌ Not detected'}")

    # --- Preview ---
    preview_df = pd.DataFrame(transactions)
    preview_df.index = range(1, len(preview_df) + 1)
    st.subheader("Preview")
    st.dataframe(preview_df, use_container_width=True, height=350)

    # --- Summary ---
    total_debits = sum(t["amount"] for t in transactions if t["type"] == "debit")
    total_credits = sum(t["amount"] for t in transactions if t["type"] == "credit")

    m1, m2, m3 = st.columns(3)
    m1.metric("Total Debits", f"₹{total_debits:,.2f}")
    m2.metric("Total Credits", f"₹{total_credits:,.2f}")
    m3.metric("Transactions", len(transactions))

    # --- Save & Categorize ---
    if st.button("Save & Categorize", type="primary", use_container_width=True):
        _save_and_categorize(transactions, source_key, month, year, filename)


def _save_and_categorize(transactions, source_key, month, year, filename):
    """Save transactions to DB, run dedup, and run LLM categorization."""
    # Prepare rows for DB insertion
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
            "uploaded_file": filename,
        })

    with st.spinner("Saving transactions..."):
        count = insert_transactions(rows)

    st.success(f"Saved **{count}** transactions.")

    # --- Deduplication (bank statements only) ---
    if source_key == "bank":
        with st.spinner("Checking for credit card payment duplicates..."):
            saved_txns = get_transactions(month=month, year=year, source="bank", include_excluded=True)
            flagged_ids = detect_cc_payments(saved_txns)

            if flagged_ids:
                # Flag as CC payment but keep visible -- only exclude from totals
                from core.database import flag_cc_payments_visible
                flag_cc_payments_visible(flagged_ids)
                st.warning(
                    f"Flagged **{len(flagged_ids)}** transaction(s) as likely credit card payments "
                    f"(excluded from totals but still visible). Review them in the Transactions page."
                )
            else:
                st.info("No credit card payment duplicates detected.")

    # --- LLM Categorization via Ollama ---
    with st.spinner("Categorizing transactions with Ollama... This may take a moment on first run."):
        try:
            # Fetch the saved transactions (they now have IDs)
            saved_txns = get_transactions(month=month, year=year)
            uncategorized = [t for t in saved_txns if not t.get("category")]

            if uncategorized:
                categories = get_all_categories()
                results = categorize_transactions(uncategorized, categories)

                if results:
                    from core.database import bulk_update_categories
                    bulk_update_categories(results)
                    st.success(
                        f"Auto-categorized **{len(results)}** / {len(uncategorized)} transactions. "
                        f"Review and edit categories in the Transactions page."
                    )
                else:
                    st.info("Categorization returned no results. You can categorize manually in the Transactions page.")
            else:
                st.info("All transactions already have categories.")
        except RuntimeError as e:
            st.warning(f"Categorization skipped: {e}")
        except Exception as e:
            st.warning(f"Categorization issue: {e}. You can categorize manually in the Transactions page.")
