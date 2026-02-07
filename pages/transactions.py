"""Transactions page – view, filter, edit categories, smart re-categorize."""

import streamlit as st
import pandas as pd
from datetime import datetime

from core.database import (
    get_transactions,
    get_all_categories,
    get_available_months,
    update_transaction_category,
    update_transaction_exclusion,
    bulk_update_categories,
    add_category,
    find_similar_transactions,
)
from core.categorizer import categorize_transactions


# ---------------------------------------------------------------------------
# Re-categorization modal dialog (true overlay popup)
# ---------------------------------------------------------------------------

@st.dialog("Similar Transactions Found", width="large")
def _recat_dialog(source_txn: dict, old_cat: str, new_cat: str, similar: list[dict]):
    """Modal overlay for bulk re-categorizing similar transactions."""
    st.info(
        f'You changed **"{source_txn["description"][:60]}"** from '
        f'**{old_cat}** to **{new_cat}**. '
        f'Found **{len(similar)}** similar transaction(s) below.'
    )

    st.write("Select which transactions to also re-categorize:")

    selected_ids = []
    for txn in similar:
        current_cat = txn.get("category") or "Uncategorized"
        col1, col2, col3, col4 = st.columns([0.4, 0.8, 4.0, 1.5])
        with col1:
            checked = st.checkbox(
                "sel", value=True,
                key=f"dlg_sel_{txn['id']}",
                label_visibility="collapsed",
            )
            if checked:
                selected_ids.append(txn["id"])
        with col2:
            st.write(txn["date"])
        with col3:
            st.write(txn["description"][:75])
        with col4:
            st.write(f"₹{txn['amount']:,.2f}  ({current_cat})")

    st.divider()
    col_apply, col_skip = st.columns(2)

    with col_apply:
        if st.button(
            f"Apply '{new_cat}' to {len(selected_ids)} selected",
            type="primary", use_container_width=True,
            disabled=len(selected_ids) == 0,
        ):
            if selected_ids:
                bulk_update_categories({tid: new_cat for tid in selected_ids})
                # Queue widget key clearing for the NEXT render cycle.
                # We can't reliably pop keys inside a @st.dialog, so we
                # store the IDs and let render() handle the cleanup.
                st.session_state["_pending_clear_ids"] = selected_ids
            st.session_state["scroll_to_txn"] = source_txn["id"]
            st.rerun()

    with col_skip:
        if st.button("Skip", use_container_width=True):
            st.session_state["scroll_to_txn"] = source_txn["id"]
            st.rerun()


# ---------------------------------------------------------------------------
# Main render
# ---------------------------------------------------------------------------

def render():
    st.header("Transactions")

    # --- Process deferred widget key clearing from dialog actions ---
    # Widget keys can't be reliably cleared inside @st.dialog, so the
    # dialog stores IDs here and we clear them in the main render context.
    pending_ids = st.session_state.pop("_pending_clear_ids", None)
    if pending_ids:
        _clear_widget_keys(pending_ids)

    # Clear the one-shot skip set from the previous cycle
    st.session_state.pop("_skip_recat_ids", None)

    # --- Scroll back to last-edited transaction if needed ---
    scroll_target = st.session_state.pop("scroll_to_txn", None)
    if scroll_target is not None:
        _inject_scroll_js(scroll_target)

    # --- Add custom category (always accessible at top) ---
    with st.expander("Add custom category"):
        col_a, col_b = st.columns([3, 1])
        with col_a:
            new_cat = st.text_input("Category name", key="new_cat_input")
        with col_b:
            st.write("")
            st.write("")
            if st.button("Add Category") and new_cat.strip():
                add_category(new_cat.strip())
                st.success(f"Added category: **{new_cat.strip()}**")
                st.rerun()

    # --- Filters row ---
    available = get_available_months()
    if not available:
        st.info("No transactions yet. Upload a statement first.")
        return

    categories = get_all_categories()

    f1, f2, f3, f4 = st.columns(4)

    with f1:
        options = ["All months"] + [
            f"{datetime(y, m, 1).strftime('%B %Y')}" for y, m in available
        ]
        selected = st.selectbox("Month", options, key="filter_month")
        if selected == "All months":
            sel_month, sel_year = None, None
        else:
            idx = options.index(selected) - 1
            sel_year, sel_month = available[idx]

    with f2:
        source_filter = st.selectbox("Source", ["All", "Bank", "Credit Card"], key="filter_source")
        source_val = {"All": None, "Bank": "bank", "Credit Card": "credit_card"}[source_filter]

    with f3:
        cat_filter_opts = ["All categories", "Uncategorized"] + categories
        cat_filter = st.selectbox("Category", cat_filter_opts, key="filter_category")

    with f4:
        sort_options = ["Date (newest)", "Date (oldest)", "Amount (high to low)", "Amount (low to high)", "Category A-Z"]
        sort_by = st.selectbox("Sort by", sort_options, key="sort_by")

    # --- Fetch and filter ---
    txns = get_transactions(
        month=sel_month, year=sel_year, source=source_val, include_excluded=True
    )

    # Apply category filter
    if cat_filter == "Uncategorized":
        txns = [t for t in txns if not t.get("category")]
    elif cat_filter != "All categories":
        txns = [t for t in txns if t.get("category") == cat_filter]

    # Apply sort
    if sort_by == "Date (newest)":
        txns.sort(key=lambda t: (t["date"], t["id"]), reverse=True)
    elif sort_by == "Date (oldest)":
        txns.sort(key=lambda t: (t["date"], t["id"]))
    elif sort_by == "Amount (high to low)":
        txns.sort(key=lambda t: t["amount"], reverse=True)
    elif sort_by == "Amount (low to high)":
        txns.sort(key=lambda t: t["amount"])
    elif sort_by == "Category A-Z":
        txns.sort(key=lambda t: (t.get("category") or "zzz").lower())

    if not txns:
        st.info("No transactions found for the selected filters.")
        return

    # --- Summary bar (only non-excluded in totals) ---
    total_debit = sum(t["amount"] for t in txns if t["type"] == "debit" and not t["is_excluded"])
    total_credit = sum(t["amount"] for t in txns if t["type"] == "credit" and not t["is_excluded"])
    uncategorized_count = sum(1 for t in txns if not t.get("category"))
    cc_flagged_count = sum(1 for t in txns if t["is_excluded"])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Expenses", f"₹{total_debit:,.2f}")
    c2.metric("Total Earnings", f"₹{total_credit:,.2f}")
    c3.metric("Transactions", f"{len(txns)} ({cc_flagged_count} excl.)")
    c4.metric("Uncategorized", uncategorized_count)

    # --- Bulk actions ---
    action_col1, action_col2 = st.columns(2)

    with action_col1:
        if uncategorized_count > 0:
            if st.button("Auto-categorize uncategorized", type="primary"):
                _run_categorization(txns, categories)
                st.rerun()

    with action_col2:
        if st.button("Re-categorize ALL"):
            _run_categorization(txns, categories, force_all=True)
            st.rerun()

    # --- Transaction list with per-row save ---
    st.subheader(f"Showing {len(txns)} transaction(s)")
    st.caption("Category and Excluded changes save immediately.")

    for i, txn in enumerate(txns):
        _render_transaction_row(txn, categories, i)

    # --- Open recat dialog if triggered (renders as popup over the page) ---
    if st.session_state.get("pending_recat"):
        data = st.session_state.pop("pending_recat")
        _recat_dialog(
            data["source_txn"], data["old_cat"],
            data["new_cat"], data["similar"],
        )


# ---------------------------------------------------------------------------
# Transaction row
# ---------------------------------------------------------------------------

def _render_transaction_row(txn: dict, categories: list[str], idx: int):
    """Render a single transaction row with immediate-save controls."""
    txn_id = txn["id"]
    is_excluded = bool(txn["is_excluded"])
    is_cc = bool(txn["is_cc_payment"])

    # Anchor for scroll-back
    st.html(f'<div id="txn-{txn_id}"></div>')

    with st.container():
        #             Date  Type   Description  Amount  Excl  Category
        cols = st.columns([0.7, 0.7, 4.0, 1.0, 0.5, 2.2])

        with cols[0]:
            st.caption("Date")
            st.write(txn["date"])

        with cols[1]:
            source_label = "Bank" if txn["source"] == "bank" else "CC"
            type_label = "Dr" if txn["type"] == "debit" else "Cr"
            st.caption("Type")
            st.write(f"{type_label}/{source_label}")

        with cols[2]:
            st.caption("Description")
            desc = txn["description"]
            if is_cc:
                st.write(f":orange[CC: {desc[:85]}]")
            elif is_excluded:
                st.write(f":grey[{desc[:85]}]")
            else:
                st.write(desc[:85])

        with cols[3]:
            st.caption("Amount")
            color = "red" if txn["type"] == "debit" else "green"
            st.write(f":{color}[₹{txn['amount']:,.2f}]")

        with cols[4]:
            st.caption("Excl.")
            new_excluded = st.checkbox(
                "x", value=is_excluded, key=f"excl_{txn_id}",
                label_visibility="collapsed",
            )
            if new_excluded != is_excluded:
                update_transaction_exclusion(txn_id, new_excluded)
                st.session_state["scroll_to_txn"] = txn_id
                st.rerun()

        with cols[5]:
            current_cat = txn.get("category") or ""
            cat_options = [""] + categories
            current_idx = cat_options.index(current_cat) if current_cat in cat_options else 0

            new_cat = st.selectbox(
                "Category", options=cat_options, index=current_idx,
                key=f"cat_{txn_id}", label_visibility="collapsed",
            )
            # Only act on genuine user changes, not stale widget state
            if new_cat != current_cat and new_cat and txn_id not in st.session_state.get("_skip_recat_ids", set()):
                update_transaction_category(txn_id, new_cat)
                _clear_widget_keys([txn_id])
                _trigger_smart_recat(txn, current_cat, new_cat)

        st.divider()


# ---------------------------------------------------------------------------
# Widget state management
# ---------------------------------------------------------------------------

def _clear_widget_keys(txn_ids: list[int]):
    """Remove stale selectbox keys from session_state.

    When we save a category to the DB, the selectbox widget key still holds
    the old value. On the next render Streamlit uses the stale widget value
    instead of the `index` parameter, causing a phantom mismatch. Deleting
    the key forces Streamlit to treat it as a fresh widget.
    """
    skip_set: set = st.session_state.get("_skip_recat_ids", set())
    for tid in txn_ids:
        st.session_state.pop(f"cat_{tid}", None)
        skip_set.add(tid)
    st.session_state["_skip_recat_ids"] = skip_set


# ---------------------------------------------------------------------------
# Smart re-categorization trigger
# ---------------------------------------------------------------------------

def _trigger_smart_recat(txn: dict, old_category: str, new_category: str):
    """Find similar transactions; if any, queue the dialog to open."""
    similar = find_similar_transactions(
        txn["description"], txn["id"],
        old_category if old_category else None,
    )
    if similar:
        st.session_state["pending_recat"] = {
            "source_txn": txn,
            "old_cat": old_category or "Uncategorized",
            "new_cat": new_category,
            "similar": similar,
        }
        st.session_state["scroll_to_txn"] = txn["id"]
        st.rerun()
    else:
        st.session_state["scroll_to_txn"] = txn["id"]
        st.rerun()


# ---------------------------------------------------------------------------
# Scroll-to-anchor helper
# ---------------------------------------------------------------------------

def _inject_scroll_js(txn_id: int):
    """Inject a small JS snippet that scrolls the page to the given transaction anchor."""
    st.html(f"""
        <script>
            // Wait for the page to finish rendering, then scroll
            const tryScroll = () => {{
                const el = document.getElementById('txn-{txn_id}');
                if (el) {{
                    el.scrollIntoView({{ behavior: 'smooth', block: 'center' }});
                }} else {{
                    setTimeout(tryScroll, 200);
                }}
            }};
            setTimeout(tryScroll, 300);
        </script>
    """)


# ---------------------------------------------------------------------------
# Bulk LLM categorization
# ---------------------------------------------------------------------------

def _run_categorization(txns, categories, force_all=False):
    """Run LLM categorization on uncategorized (or all) transactions."""
    if force_all:
        to_categorize = [t for t in txns if not t["is_excluded"]]
    else:
        to_categorize = [t for t in txns if not t.get("category") and not t["is_excluded"]]

    if not to_categorize:
        st.info("No transactions to categorize.")
        return

    with st.spinner(f"Categorizing {len(to_categorize)} transactions..."):
        try:
            results = categorize_transactions(to_categorize, categories)
            if results:
                bulk_update_categories(results)
                st.success(f"Categorized {len(results)} transactions.")
            else:
                st.warning("Categorization returned no results.")
        except RuntimeError as e:
            st.error(f"Categorization failed: {e}")
        except Exception as e:
            st.error(f"Error: {e}")
