import sqlite3
import os
from contextlib import contextmanager
from typing import Optional

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "expense_tracker.db")

DEFAULT_CATEGORIES = [
    "Food", "Travel", "Shopping", "Rent", "Utilities",
    "Entertainment", "Health", "EMI", "Salary", "Investment",
    "Transfer", "Groceries", "Fuel", "Insurance", "Subscriptions",
    "Education", "Other",
]


@contextmanager
def get_connection():
    """Yield a SQLite connection with row_factory set."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create tables if they don't exist and seed default categories."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                description TEXT NOT NULL,
                amount REAL NOT NULL,
                type TEXT NOT NULL,
                source TEXT NOT NULL,
                category TEXT,
                is_cc_payment INTEGER DEFAULT 0,
                is_excluded INTEGER DEFAULT 0,
                month INTEGER,
                year INTEGER,
                uploaded_file TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )
        """)

        # Seed default categories
        for cat in DEFAULT_CATEGORIES:
            conn.execute(
                "INSERT OR IGNORE INTO categories (name) VALUES (?)", (cat,)
            )


# ---------------------------------------------------------------------------
# Category helpers
# ---------------------------------------------------------------------------

def get_all_categories() -> list[str]:
    """Return all category names sorted alphabetically."""
    with get_connection() as conn:
        rows = conn.execute("SELECT name FROM categories ORDER BY name").fetchall()
    return [r["name"] for r in rows]


def add_category(name: str) -> None:
    """Add a new category (no-op if it already exists)."""
    with get_connection() as conn:
        conn.execute("INSERT OR IGNORE INTO categories (name) VALUES (?)", (name,))


# ---------------------------------------------------------------------------
# Transaction helpers
# ---------------------------------------------------------------------------

def insert_transactions(rows: list[dict]) -> int:
    """Bulk-insert parsed transactions. Returns number of rows inserted."""
    if not rows:
        return 0
    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO transactions
                (date, description, amount, type, source, category,
                 is_cc_payment, is_excluded, month, year, uploaded_file)
            VALUES
                (:date, :description, :amount, :type, :source, :category,
                 :is_cc_payment, :is_excluded, :month, :year, :uploaded_file)
            """,
            rows,
        )
    return len(rows)


def get_transactions(
    month: Optional[int] = None,
    year: Optional[int] = None,
    source: Optional[str] = None,
    include_excluded: bool = False,
) -> list[dict]:
    """Fetch transactions with optional filters."""
    query = "SELECT * FROM transactions WHERE 1=1"
    params: list = []

    if month is not None:
        query += " AND month = ?"
        params.append(month)
    if year is not None:
        query += " AND year = ?"
        params.append(year)
    if source is not None:
        query += " AND source = ?"
        params.append(source)
    if not include_excluded:
        query += " AND is_excluded = 0"

    query += " ORDER BY date DESC, id DESC"

    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_all_transactions(include_excluded: bool = False) -> list[dict]:
    """Return every transaction (no month/year filter)."""
    query = "SELECT * FROM transactions"
    if not include_excluded:
        query += " WHERE is_excluded = 0"
    query += " ORDER BY date DESC, id DESC"

    with get_connection() as conn:
        rows = conn.execute(query).fetchall()
    return [dict(r) for r in rows]


def update_transaction_category(txn_id: int, category: str) -> None:
    """Update the category for a single transaction."""
    with get_connection() as conn:
        conn.execute(
            "UPDATE transactions SET category = ? WHERE id = ?", (category, txn_id)
        )


def update_transaction_exclusion(txn_id: int, is_excluded: bool) -> None:
    """Mark/unmark a transaction as excluded from totals."""
    with get_connection() as conn:
        conn.execute(
            "UPDATE transactions SET is_excluded = ? WHERE id = ?",
            (int(is_excluded), txn_id),
        )


def bulk_update_categories(updates: dict[int, str]) -> None:
    """Update categories for multiple transactions at once.

    Args:
        updates: mapping of transaction id -> category name
    """
    if not updates:
        return
    with get_connection() as conn:
        conn.executemany(
            "UPDATE transactions SET category = ? WHERE id = ?",
            [(cat, tid) for tid, cat in updates.items()],
        )


def flag_cc_payments(txn_ids: list[int], flag: bool = True) -> None:
    """Flag transactions as credit-card payments and exclude them."""
    if not txn_ids:
        return
    val = 1 if flag else 0
    with get_connection() as conn:
        placeholders = ",".join("?" for _ in txn_ids)
        conn.execute(
            f"UPDATE transactions SET is_cc_payment = ?, is_excluded = ? WHERE id IN ({placeholders})",
            [val, val] + txn_ids,
        )


def flag_cc_payments_visible(txn_ids: list[int]) -> None:
    """Flag transactions as CC payments, exclude from totals, and categorise them."""
    if not txn_ids:
        return
    # Ensure the category exists
    add_category("Credit Card Payment")
    with get_connection() as conn:
        placeholders = ",".join("?" for _ in txn_ids)
        conn.execute(
            f"UPDATE transactions SET is_cc_payment = 1, is_excluded = 1, "
            f"category = 'Credit Card Payment' WHERE id IN ({placeholders})",
            txn_ids,
        )


def get_available_months() -> list[tuple[int, int]]:
    """Return distinct (year, month) pairs that have transactions, sorted desc."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT DISTINCT year, month FROM transactions ORDER BY year DESC, month DESC"
        ).fetchall()
    return [(r["year"], r["month"]) for r in rows]


def get_monthly_summary(month: int, year: int) -> dict:
    """Return aggregated summary for a given month.

    Separates true earnings/expenses from transfers and investments so
    the totals reflect actual income and day-to-day spending.
    """
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END), 0) AS total_earnings,
                COALESCE(SUM(CASE WHEN type = 'debit'  THEN amount ELSE 0 END), 0) AS total_expenses,
                COALESCE(SUM(CASE WHEN type = 'credit' AND category = 'Transfer' THEN amount ELSE 0 END), 0) AS transfer_in,
                COALESCE(SUM(CASE WHEN type = 'debit'  AND category = 'Transfer' THEN amount ELSE 0 END), 0) AS transfer_out,
                COALESCE(SUM(CASE WHEN type = 'debit'  AND category = 'Investment' THEN amount ELSE 0 END), 0) AS investment
            FROM transactions
            WHERE month = ? AND year = ? AND is_excluded = 0
            """,
            (month, year),
        ).fetchone()
    return dict(row)


def get_category_breakdown(month: int, year: int) -> list[dict]:
    """Return spending by category for a given month (debits only)."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                COALESCE(category, 'Uncategorized') AS category,
                SUM(amount) AS total
            FROM transactions
            WHERE month = ? AND year = ? AND type = 'debit' AND is_excluded = 0
            GROUP BY category
            ORDER BY total DESC
            """,
            (month, year),
        ).fetchall()
    return [dict(r) for r in rows]


def find_similar_transactions(
    description: str, current_id: int, old_category: Optional[str] = None,
) -> list[dict]:
    """Find transactions with similar descriptions for bulk re-categorization.

    Extracts key tokens from the description and matches against other
    transactions. Optionally filter to only those with the old category.
    """
    # Extract meaningful keywords (skip short/numeric tokens)
    import re
    tokens = re.split(r"[/\-\s,.|]+", description)
    keywords = [t.strip().upper() for t in tokens if len(t.strip()) >= 4 and not t.strip().isdigit()]

    if not keywords:
        return []

    # Build LIKE clauses for the top keywords (use first 3 meaningful ones)
    search_kw = keywords[:3]
    like_clauses = " AND ".join("UPPER(description) LIKE ?" for _ in search_kw)
    params: list = [f"%{kw}%" for kw in search_kw]
    params.append(current_id)

    query = f"""
        SELECT * FROM transactions
        WHERE {like_clauses} AND id != ?
    """
    if old_category is not None:
        query += " AND (category = ? OR category IS NULL)"
        params.append(old_category)

    query += " ORDER BY date DESC"

    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def delete_transactions_by_file(filename: str) -> int:
    """Delete all transactions from a specific uploaded file. Returns count deleted."""
    with get_connection() as conn:
        cursor = conn.execute(
            "DELETE FROM transactions WHERE uploaded_file = ?", (filename,)
        )
    return cursor.rowcount
