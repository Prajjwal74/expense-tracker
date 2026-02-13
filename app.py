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

# --- Password protection (for cloud deployment) ---
def _check_password() -> bool:
    """Return True if password is correct or not configured."""
    try:
        app_password = st.secrets["APP_PASSWORD"]
    except (KeyError, FileNotFoundError):
        return True  # no password configured, allow access

    if not app_password:
        return True

    if st.session_state.get("authenticated"):
        return True

    st.title("Expense Tracker")
    pwd = st.text_input("Enter password to continue", type="password", key="login_pwd")
    if st.button("Login", type="primary"):
        if pwd == app_password:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    return False

if not _check_password():
    st.stop()

st.sidebar.title("Expense Tracker")

# --- Restore navigation from URL on first load ---
_SECTIONS = ["Statements", "Email"]
_STMT_PAGES = ["Dashboard", "Upload Statement", "Transactions"]
_EMAIL_PAGES = ["Email Sync", "Dashboard", "Transactions"]

if "nav_restored" not in st.session_state:
    st.session_state["nav_restored"] = True
    qp = st.query_params
    saved_section = qp.get("section", "Statements")
    saved_page = qp.get("page", "")
    if saved_section in _SECTIONS:
        st.session_state["app_section"] = saved_section
    if saved_section == "Statements" and saved_page in _STMT_PAGES:
        st.session_state["stmt_page"] = saved_page
    elif saved_section == "Email" and saved_page in _EMAIL_PAGES:
        st.session_state["email_page"] = saved_page

# --- Two-section navigation ---
section = st.sidebar.radio(
    "Section",
    _SECTIONS,
    key="app_section",
    horizontal=True,
)

if section == "Statements":
    st.sidebar.caption("Upload & manage bank/credit card statements")
    page = st.sidebar.radio(
        "Navigate",
        _STMT_PAGES,
        key="stmt_page",
    )

    # Persist to URL
    st.query_params.update({"section": section, "page": page})

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
        _EMAIL_PAGES,
        key="email_page",
    )

    # Persist to URL
    st.query_params.update({"section": section, "page": page})

    if page == "Email Sync":
        from views.email_sync import render
        render()
    elif page == "Dashboard":
        from views.dashboard import render
        render(email_only=True)
    elif page == "Transactions":
        from views.transactions import render
        render(email_only=True)
