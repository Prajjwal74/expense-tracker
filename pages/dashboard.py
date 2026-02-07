"""Dashboard page – monthly summary, category breakdown, trends."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

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


def render():
    st.header("Dashboard")

    available = get_available_months()
    if not available:
        st.info("No data yet. Upload a statement to see your dashboard.")
        return

    # --- Month selector ---
    month_options = [f"{datetime(y, m, 1).strftime('%B %Y')}" for y, m in available]
    selected = st.selectbox("Select Month", month_options)
    idx = month_options.index(selected)
    sel_year, sel_month = available[idx]

    # --- Summary cards ---
    summary = get_monthly_summary(sel_month, sel_year)
    all_credits = summary["total_earnings"]
    all_debits = summary["total_expenses"]
    transfer_in = summary["transfer_in"]
    transfer_out = summary["transfer_out"]
    investment = summary["investment"]

    # Earnings = credits minus self-transfers
    # Expenses = debits minus self-transfers minus investments
    earnings = all_credits - transfer_in
    expenses = all_debits - transfer_out - investment
    savings = earnings - expenses

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Earnings", f"₹{_fmt_inr(earnings)}",
              help="Credits minus self-transfers")
    c2.metric("Total Expenses", f"₹{_fmt_inr(expenses)}",
              help="Debits minus transfers & investments")
    c3.metric("Net Savings", f"₹{_fmt_inr(savings)}",
              delta=f"₹{_fmt_inr(savings)}", delta_color="normal")

    # Second row: Investment & Transfers
    c4, c5, c6 = st.columns(3)
    c4.metric("Investments", f"₹{_fmt_inr(investment)}")
    c5.metric("Transfers In", f"₹{_fmt_inr(transfer_in)}")
    c6.metric("Transfers Out", f"₹{_fmt_inr(transfer_out)}")

    st.divider()

    # --- Category breakdown ---
    breakdown = get_category_breakdown(sel_month, sel_year)

    if breakdown:
        st.subheader("Spending by Category")

        bd_df = pd.DataFrame(breakdown)

        chart_col1, chart_col2 = st.columns(2)

        with chart_col1:
            fig_pie = px.pie(
                bd_df,
                values="total",
                names="category",
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set3,
            )
            fig_pie.update_traces(textinfo="label+percent", textposition="outside")
            fig_pie.update_layout(
                showlegend=False,
                margin=dict(t=20, b=20, l=20, r=20),
                height=400,
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        with chart_col2:
            bar_df = bd_df.sort_values("total", ascending=True).copy()
            bar_df["label"] = bar_df["total"].apply(_fmt_inr)

            fig_bar = px.bar(
                bar_df,
                x="total",
                y="category",
                orientation="h",
                text="label",
                color="category",
                color_discrete_sequence=px.colors.qualitative.Set3,
            )
            fig_bar.update_traces(textposition="outside")
            fig_bar.update_layout(
                showlegend=False,
                xaxis_title="Amount (₹)",
                yaxis_title="",
                margin=dict(t=20, b=20, l=20, r=80),
                height=400,
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        # Table view of breakdown
        with st.expander("Category breakdown table"):
            bd_display = bd_df.copy()
            bd_display["total"] = bd_display["total"].map(lambda x: f"₹{x:,.2f}")
            bd_display.columns = ["Category", "Amount"]
            bd_display.index = range(1, len(bd_display) + 1)
            st.dataframe(bd_display, use_container_width=True)
    else:
        st.info("No expense data for this month.")

    st.divider()

    # --- Month-over-month trend ---
    if len(available) > 1:
        st.subheader("Month-over-Month Trend")
        _render_trend(available)

    # --- Top expenses ---
    st.subheader("Top 10 Expenses")
    txns = get_transactions(month=sel_month, year=sel_year)
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


def _render_trend(available: list[tuple[int, int]]):
    """Render month-over-month earnings vs expenses trend chart."""
    trend_data = []
    for y, m in reversed(available):  # chronological order
        summary = get_monthly_summary(m, y)
        label = datetime(y, m, 1).strftime("%b %Y")
        earn = summary["total_earnings"] - summary["transfer_in"]
        spend = summary["total_expenses"] - summary["transfer_out"] - summary["investment"]
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
