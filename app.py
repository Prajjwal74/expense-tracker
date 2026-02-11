import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from core.database import init_db

# Initialize database on first run
init_db()

st.set_page_config(
    page_title="Expense Tracker",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.sidebar.title("Expense Tracker")

# --- Two-section navigation ---
section = st.sidebar.radio(
    "Section",
    ["Statements", "Email"],
    key="app_section",
    horizontal=True,
)

if section == "Statements":
    st.sidebar.caption("Upload & manage bank/credit card statements")
    page = st.sidebar.radio(
        "Navigate",
        ["Dashboard", "Upload Statement", "Transactions"],
        key="stmt_page",
    )

    if page == "Dashboard":
        from views.dashboard import render
        render(email_only=False)
    elif page == "Upload Statement":
        from views.upload import render
        render()
    elif page == "Transactions":
        from views.transactions import render
        render(email_only=False)

else:  # Email
    st.sidebar.caption("Fetch transactions from email alerts")
    page = st.sidebar.radio(
        "Navigate",
        ["Email Sync", "Dashboard", "Transactions"],
        key="email_page",
    )

    if page == "Email Sync":
        from views.email_sync import render
        render()
    elif page == "Dashboard":
        from views.dashboard import render
        render(email_only=True)
    elif page == "Transactions":
        from views.transactions import render
        render(email_only=True)
