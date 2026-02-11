"""Dashboard page -- monthly summary, category breakdown, trends."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from typing import Optional

from core.database import (
    get_available_months,
    get_monthly_summary,
    get_category_breakdown,
    get_transactions,
)


def _fmt_inr(amount: float) -> str:
    """Format amount in Indian short form: ₹1.91L, ₹50K, ₹771."""
    if amount >= 1_00_000:
        return f"₹{amount / 1_00_000:.2f}L"
    if amount >= 1_000:
        return f"₹{amount / 1_000:.1f}K"
    return f"₹{amount:,.0f}"


_CATEGORY_ICONS = {
    # Expenditure categories
    "Food": "🍽️", "Groceries": "🛒", "Rent": "🏠", "Utilities": "💡",
    "Shopping": "🛍️", "Travel": "✈️", "Fuel": "⛽", "Entertainment": "🎬",
    "Health": "🏥", "Education": "📚", "Subscriptions": "📱", "Insurance": "🛡️",
    "EMI": "🏦", "Other": "📋",
    # Broad flow categories
    "Earnings": "💰", "Expenditure": "💸", "Invested": "📈",
    "Salary": "💼", "Dividend": "📊", "Investment": "📈", "Transfer": "🔄",
    "Net Transfer In": "📥", "Net Transfer Out": "📤",
    "Credit Card Payment": "💳",
}


def _render_donut_card(
    df: pd.DataFrame,
    name_col: str,
    value_col: str,
    total: float,
    center_label: str,
    center_value: str,
    colors=None,
    color_map: Optional[dict] = None,
):
    """Render a donut chart with center summary and a compact single-column legend."""
    # Build the donut
    if color_map:
        used_colors = [color_map.get(n, "#999") for n in df[name_col]]
    else:
        color_seq = colors or px.colors.qualitative.Vivid
        used_colors = color_seq[:len(df)]

    fig = go.Figure(go.Pie(
        labels=df[name_col],
        values=df[value_col],
        hole=0.6,
        marker=dict(colors=used_colors),
        textinfo="none",
        hovertemplate="%{label}: ₹%{value:,.0f} (%{percent})<extra></extra>",
        direction="clockwise",
        sort=False,
        rotation=90,
    ))

    fig.add_annotation(
        text=f"<b>{center_label}</b><br><span style='font-size:22px'>{center_value}</span>",
        x=0.5, y=0.5, showarrow=False,
        font=dict(size=13),
    )

    fig.update_layout(
        showlegend=False,
        margin=dict(t=10, b=10, l=10, r=10),
        height=320,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )

    # Layout: donut left, single-column legend right
    col_chart, col_legend = st.columns([1, 1.2])

    with col_chart:
        st.plotly_chart(fig, use_container_width=True)

    with col_legend:
        # Build all legend rows as a single HTML block for compact rendering
        legend_html = '<div style="padding-top: 10px; line-height: 2.2;">'
        for i, (_, row) in enumerate(df.iterrows()):
            name = row[name_col]
            val = row[value_col]
            pct = (val / total * 100) if total else 0
            dot_color = used_colors[i] if i < len(used_colors) else "#999"
            icon = _CATEGORY_ICONS.get(name, "•")
            legend_html += (
                f'<div style="white-space: nowrap;">'
                f'<span style="color:{dot_color}; font-size:14px;">&#9679;</span> '
                f'{icon} <b>{name}</b>'
                f'<span style="float:right; font-size:14px;">'
                f'{_fmt_inr(val)} &nbsp;({pct:.1f}%)</span>'
                f'</div>'
            )
        legend_html += '</div>'
        st.markdown(legend_html, unsafe_allow_html=True)


def render(email_only: Optional[bool] = None):
    # Key prefix to avoid widget collisions between sections
    pfx = "ed_" if email_only else "sd_"
    section_label = "Email" if email_only else "Statements"

    st.header(f"Dashboard — {section_label}")

    available = get_available_months(email_only=email_only)
    if not available:
        if email_only:
            st.info("No email data yet. Sync your email in the Email Sync page to see your dashboard.")
        else:
            st.info("No data yet. Upload a statement to see your dashboard.")
        return

    # --- Month selector ---
    month_options = [f"{datetime(y, m, 1).strftime('%B %Y')}" for y, m in available]
    selected = st.selectbox("Select Month", month_options, key=f"{pfx}month_sel")
    idx = month_options.index(selected)
    sel_year, sel_month = available[idx]

    # --- Summary cards ---
    summary = get_monthly_summary(sel_month, sel_year, email_only=email_only)
    all_credits = summary["total_earnings"]
    all_debits = summary["total_expenses"]
    transfer_in = summary["transfer_in"]
    transfer_out = summary["transfer_out"]
    investment = summary["investment"]

    # Earnings = credits minus transfers in (real income only)
    earnings = all_credits - transfer_in

    # Net transfers: if negative (more going out), the deficit is an expense
    net_transfer = transfer_in - transfer_out
    transfer_expense = abs(net_transfer) if net_transfer < 0 else 0

    # Expenses = debits minus investments minus all transfers + net transfer deficit
    expenses = all_debits - investment - transfer_out + transfer_expense
    savings = earnings - expenses

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Earnings", f"₹{_fmt_inr(earnings)}",
              help="Credits excluding transfers in")
    c2.metric("Total Expenses", f"₹{_fmt_inr(expenses)}",
              help="Debits excl. investments & transfers, plus net transfer deficit if any")
    c3.metric("Net Savings", f"₹{_fmt_inr(savings)}",
              delta=f"₹{_fmt_inr(savings)}", delta_color="normal")

    # Second row: breakdown
    c4, c5, c6 = st.columns(3)
    c4.metric("Investments", f"₹{_fmt_inr(investment)}",
              help="Excluded from expenses")
    net_label = f"₹{_fmt_inr(abs(net_transfer))}"
    if net_transfer >= 0:
        c5.metric("Net Transfers", f"+{net_label}",
                  help="Positive: more coming in. Not added to expenses.")
    else:
        c5.metric("Net Transfers", f"-{net_label}",
                  help="Negative: more going out. Deficit added to expenses.",
                  delta=f"-{net_label} added to expenses", delta_color="inverse")
    c6.metric("Transfers", f"₹{_fmt_inr(transfer_in)} in / ₹{_fmt_inr(transfer_out)} out")

    st.divider()

    # --- Two charts stacked vertically ---
    breakdown = get_category_breakdown(sel_month, sel_year, email_only=email_only)

    # ---- Chart 1: Expenditure Breakdown ----
    st.subheader("Expenditure Breakdown")

    if breakdown:
        _EXCLUDE = {"Investment", "Transfer", "Credit Card Payment"}
        spend_rows = [r for r in breakdown if r["category"] not in _EXCLUDE]

        if spend_rows:
            spend_df = pd.DataFrame(spend_rows)
            spend_total = spend_df["total"].sum()
            _render_donut_card(
                spend_df, "category", "total", spend_total,
                "Total Expenditure", _fmt_inr(spend_total),
                px.colors.qualitative.Vivid,
            )
        else:
            st.info("No spending transactions for this month.")
    else:
        st.info("No expense data for this month.")

    st.divider()

    # ---- Chart 2: Money Flow Overview ----
    st.subheader("Money Flow Overview")

    net_transfer_abs = abs(net_transfer)
    net_transfer_label = "Net Transfer In" if net_transfer >= 0 else "Net Transfer Out"

    broad_data = []
    if earnings > 0:
        broad_data.append({"Category": "Earnings", "Amount": earnings})
    if expenses > 0:
        broad_data.append({"Category": "Expenditure", "Amount": expenses})
    if investment > 0:
        broad_data.append({"Category": "Invested", "Amount": investment})
    if net_transfer_abs > 0:
        broad_data.append({"Category": net_transfer_label, "Amount": net_transfer_abs})

    if broad_data:
        broad_df = pd.DataFrame(broad_data)
        broad_total = broad_df["Amount"].sum()
        color_map = {
            "Earnings": "#2ecc71",
            "Expenditure": "#e74c3c",
            "Invested": "#3498db",
            "Net Transfer In": "#27ae60",
            "Net Transfer Out": "#7f8c8d",
        }
        _render_donut_card(
            broad_df, "Category", "Amount", broad_total,
            "Total", _fmt_inr(broad_total),
            color_map=color_map,
        )
    else:
        st.info("No data to display.")

    # Detailed category table
    if breakdown:
        with st.expander("Detailed category breakdown"):
            bd_df = pd.DataFrame(breakdown)
            bd_df["total"] = bd_df["total"].map(lambda x: f"₹{x:,.2f}")
            bd_df.columns = ["Category", "Amount"]
            bd_df.index = range(1, len(bd_df) + 1)
            st.dataframe(bd_df, use_container_width=True)

    st.divider()

    # --- Month-over-month trend ---
    if len(available) > 1:
        st.subheader("Month-over-Month Trend")
        _render_trend(available, email_only=email_only)

    # --- Top expenses ---
    st.subheader("Top 10 Expenses")
    txns = get_transactions(month=sel_month, year=sel_year, email_only=email_only)
    debits = [t for t in txns if t["type"] == "debit"]
    debits.sort(key=lambda t: t["amount"], reverse=True)
    top_10 = debits[:10]

    if top_10:
        top_df = pd.DataFrame(top_10)[["date", "description", "amount", "category", "source"]]
        top_df["amount"] = top_df["amount"].map(lambda x: f"₹{x:,.2f}")
        top_df["source"] = top_df["source"].map({"bank": "Bank", "credit_card": "Credit Card"})
        top_df.columns = ["Date", "Description", "Amount", "Category", "Source"]
        top_df.index = range(1, len(top_df) + 1)
        st.dataframe(top_df, use_container_width=True)
    else:
        st.info("No expenses recorded for this month.")


def _render_trend(
    available: list[tuple[int, int]],
    email_only: Optional[bool] = None,
):
    """Render month-over-month earnings vs expenses trend chart."""
    trend_data = []
    for y, m in reversed(available):  # chronological order
        summary = get_monthly_summary(m, y, email_only=email_only)
        label = datetime(y, m, 1).strftime("%b %Y")
        t_in = summary["transfer_in"]
        t_out = summary["transfer_out"]
        net_t = t_in - t_out
        t_deficit = abs(net_t) if net_t < 0 else 0
        earn = summary["total_earnings"] - t_in
        spend = summary["total_expenses"] - summary["investment"] - t_out + t_deficit
        trend_data.append({
            "Month": label,
            "Earnings": earn,
            "Expenses": spend,
            "Savings": earn - spend,
        })

    trend_df = pd.DataFrame(trend_data)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=trend_df["Month"],
        y=trend_df["Earnings"],
        name="Earnings",
        marker_color="#2ecc71",
    ))
    fig.add_trace(go.Bar(
        x=trend_df["Month"],
        y=trend_df["Expenses"],
        name="Expenses",
        marker_color="#e74c3c",
    ))
    fig.add_trace(go.Scatter(
        x=trend_df["Month"],
        y=trend_df["Savings"],
        name="Net Savings",
        mode="lines+markers",
        line=dict(color="#3498db", width=3),
        marker=dict(size=8),
    ))
    fig.update_layout(
        barmode="group",
        xaxis_title="",
        yaxis_title="Amount (₹)",
        legend=dict(orientation="h", y=1.1),
        margin=dict(t=40, b=20, l=20, r=20),
        height=400,
    )
    st.plotly_chart(fig, use_container_width=True)
