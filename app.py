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

# Navigation -- persist selected page across refreshes via query params
PAGES = ["Dashboard", "Upload Statement", "Transactions"]

st.sidebar.title("Expense Tracker")

# Read page from URL query param (survives browser refresh)
params = st.query_params
default_page = params.get("page", "Dashboard")
if default_page not in PAGES:
    default_page = "Dashboard"
default_idx = PAGES.index(default_page)

selection = st.sidebar.radio("Navigate", PAGES, index=default_idx, key="nav_page")

# Write selection back to query params so a refresh keeps the same page
if st.query_params.get("page") != selection:
    st.query_params["page"] = selection

if selection == "Dashboard":
    from pages.dashboard import render
    render()
elif selection == "Upload Statement":
    from pages.upload import render
    render()
elif selection == "Transactions":
    from pages.transactions import render
    render()
