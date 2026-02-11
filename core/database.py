import re
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

        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS category_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT NOT NULL,
                category TEXT NOT NULL,
                source TEXT DEFAULT 'user',
                match_count INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(keyword, category)
            )
        """)

        # Add email_body column if it doesn't exist yet (safe migration)
        try:
            conn.execute("ALTER TABLE transactions ADD COLUMN email_body TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists

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
# Category rules (learned from user corrections)
# ---------------------------------------------------------------------------

def upsert_category_rule(keyword: str, category: str, source: str = "user") -> None:
    """Create or update a keyword-to-category rule.

    If the keyword+category pair already exists, bump match_count.
    If the keyword exists with a DIFFERENT category, update it.
    """
    keyword = keyword.strip()
    if not keyword or len(keyword) < 2:
        return

    with get_connection() as conn:
        existing = conn.execute(
            "SELECT id, category FROM category_rules WHERE UPPER(keyword) = UPPER(?)",
            (keyword,),
        ).fetchone()

        if existing:
            if existing["category"] == category:
                # Same rule exists -- bump confidence
                conn.execute(
                    "UPDATE category_rules SET match_count = match_count + 1 WHERE id = ?",
                    (existing["id"],),
                )
            else:
                # Keyword mapped to different category -- user is overriding
                conn.execute(
                    "UPDATE category_rules SET category = ?, match_count = 1, source = ? WHERE id = ?",
                    (category, source, existing["id"]),
                )
        else:
            conn.execute(
                "INSERT INTO category_rules (keyword, category, source) VALUES (?, ?, ?)",
                (keyword, category, source),
            )


def get_all_rules() -> list[dict]:
    """Return all category rules, ordered by match_count desc."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM category_rules ORDER BY match_count DESC, keyword"
        ).fetchall()
    return [dict(r) for r in rows]


def apply_rules_to_transactions(transactions: list[dict]) -> dict[int, str]:
    """Apply stored keyword rules to a list of transactions.

    Returns a dict mapping transaction id -> category for matches.
    Transactions without a match are not included (left for the LLM).
    """
    rules = get_all_rules()
    if not rules:
        return {}

    results: dict[int, str] = {}
    for txn in transactions:
        desc_upper = txn["description"].upper()
        for rule in rules:
            if rule["keyword"].upper() in desc_upper:
                results[txn["id"]] = rule["category"]
                break  # first matching rule wins (highest confidence first)

    return results


def get_categorized_examples(limit: int = 30) -> list[dict]:
    """Return a diverse set of user-categorized transactions as few-shot examples.

    Picks the most recent user-confirmed categories, grouped by category
    to ensure diversity.
    """
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT description, category, amount, type, date
            FROM transactions
            WHERE category IS NOT NULL AND category != '' AND is_excluded = 0
            GROUP BY category, description
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Transaction helpers
# ---------------------------------------------------------------------------

def insert_transactions(rows: list[dict]) -> int:
    """Bulk-insert parsed transactions. Returns number of rows inserted."""
    if not rows:
        return 0
    # Ensure email_body key exists (None for CSV/PDF uploads)
    for r in rows:
        r.setdefault("email_body", None)

    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO transactions
                (date, description, amount, type, source, category,
                 is_cc_payment, is_excluded, month, year, uploaded_file, email_body)
            VALUES
                (:date, :description, :amount, :type, :source, :category,
                 :is_cc_payment, :is_excluded, :month, :year, :uploaded_file, :email_body)
            """,
            rows,
        )
    return len(rows)


def _apply_email_filter(
    query: str, params: list, email_only: Optional[bool],
) -> tuple[str, list]:
    """Append an uploaded_file filter to separate email vs file-uploaded txns.

    email_only=True  -> only email-synced transactions
    email_only=False -> only file-uploaded transactions
    email_only=None  -> all transactions (no filter)
    """
    if email_only is True:
        query += " AND uploaded_file LIKE 'email_%'"
    elif email_only is False:
        query += " AND (uploaded_file IS NULL OR uploaded_file NOT LIKE 'email_%')"
    return query, params


def get_transactions(
    month: Optional[int] = None,
    year: Optional[int] = None,
    source: Optional[str] = None,
    include_excluded: bool = False,
    email_only: Optional[bool] = None,
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

    query, params = _apply_email_filter(query, params, email_only)
    query += " ORDER BY date DESC, id DESC"

    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_all_transactions(
    include_excluded: bool = False,
    email_only: Optional[bool] = None,
) -> list[dict]:
    """Return every transaction (no month/year filter)."""
    query = "SELECT * FROM transactions WHERE 1=1"
    params: list = []
    if not include_excluded:
        query += " AND is_excluded = 0"

    query, params = _apply_email_filter(query, params, email_only)
    query += " ORDER BY date DESC, id DESC"

    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
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


def get_available_months(email_only: Optional[bool] = None) -> list[tuple[int, int]]:
    """Return distinct (year, month) pairs that have transactions, sorted desc."""
    query = "SELECT DISTINCT year, month FROM transactions WHERE 1=1"
    params: list = []
    query, params = _apply_email_filter(query, params, email_only)
    query += " ORDER BY year DESC, month DESC"

    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
    return [(r["year"], r["month"]) for r in rows]


def get_monthly_summary(
    month: int, year: int, email_only: Optional[bool] = None,
) -> dict:
    """Return aggregated summary for a given month.

    Separates true earnings/expenses from transfers and investments so
    the totals reflect actual income and day-to-day spending.
    """
    email_clause = ""
    if email_only is True:
        email_clause = "AND uploaded_file LIKE 'email_%'"
    elif email_only is False:
        email_clause = "AND (uploaded_file IS NULL OR uploaded_file NOT LIKE 'email_%')"

    with get_connection() as conn:
        row = conn.execute(
            f"""
            SELECT
                COALESCE(SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END), 0) AS total_earnings,
                COALESCE(SUM(CASE WHEN type = 'debit'  THEN amount ELSE 0 END), 0) AS total_expenses,
                COALESCE(SUM(CASE WHEN type = 'credit' AND category = 'Transfer' THEN amount ELSE 0 END), 0) AS transfer_in,
                COALESCE(SUM(CASE WHEN type = 'debit'  AND category = 'Transfer' THEN amount ELSE 0 END), 0) AS transfer_out,
                COALESCE(SUM(CASE WHEN type = 'debit'  AND category = 'Investment' THEN amount ELSE 0 END), 0) AS investment
            FROM transactions
            WHERE month = ? AND year = ? AND is_excluded = 0 {email_clause}
            """,
            (month, year),
        ).fetchone()
    return dict(row)


def get_category_breakdown(
    month: int, year: int, email_only: Optional[bool] = None,
) -> list[dict]:
    """Return spending by category for a given month (debits only)."""
    email_clause = ""
    if email_only is True:
        email_clause = "AND uploaded_file LIKE 'email_%'"
    elif email_only is False:
        email_clause = "AND (uploaded_file IS NULL OR uploaded_file NOT LIKE 'email_%')"

    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT
                COALESCE(category, 'Uncategorized') AS category,
                SUM(amount) AS total
            FROM transactions
            WHERE month = ? AND year = ? AND type = 'debit' AND is_excluded = 0 {email_clause}
            GROUP BY category
            ORDER BY total DESC
            """,
            (month, year),
        ).fetchall()
    return [dict(r) for r in rows]


def find_similar_transactions(
    description: str, current_id: int, old_category: Optional[str] = None,
    email_only: Optional[bool] = None,
) -> list[dict]:
    """Find transactions with similar descriptions for bulk re-categorization.

    Extracts the merchant/payee name from the description and searches for
    other transactions containing that name. Does NOT filter by old category
    so it catches all related transactions regardless of their current state.
    """
    # Extract meaningful keywords, skipping common generic terms
    _GENERIC = {
        "BANK", "HDFC", "ICICI", "AXIS", "YESB", "SBIN", "PAID", "PAYMENT",
        "PAYMEN", "LIMITED", "LTD", "NAVI", "INDIA", "POST", "UPI",
        "P2M", "P2A", "P2V", "NEFT", "RTGS", "IMPS",
    }
    tokens = re.split(r"[/\-\s,.|]+", description)
    keywords = [
        t.strip().upper() for t in tokens
        if len(t.strip()) >= 4
        and not t.strip().isdigit()
        and t.strip().upper() not in _GENERIC
    ]

    if not keywords:
        # Fallback: use any 4+ char tokens
        keywords = [t.strip().upper() for t in tokens if len(t.strip()) >= 4 and not t.strip().isdigit()]

    if not keywords:
        return []

    # Use at most 2 keywords (less strict = more matches)
    search_kw = keywords[:2]
    like_clauses = " AND ".join("UPPER(description) LIKE ?" for _ in search_kw)
    params: list = [f"%{kw}%" for kw in search_kw]
    params.append(current_id)

    # Don't filter by old_category -- find ALL similar transactions
    # regardless of their current category so user can bulk-update them all
    query = f"""
        SELECT * FROM transactions
        WHERE {like_clauses} AND id != ?
    """

    query, params = _apply_email_filter(query, params, email_only)
    query += " ORDER BY date DESC"

    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_upload_history(email_only: Optional[bool] = None) -> list[dict]:
    """Return upload history: file name, upload time, transaction count, month/year."""
    email_clause = ""
    if email_only is True:
        email_clause = "WHERE uploaded_file LIKE 'email_%'"
    elif email_only is False:
        email_clause = "WHERE (uploaded_file IS NULL OR uploaded_file NOT LIKE 'email_%')"

    with get_connection() as conn:
        rows = conn.execute(f"""
            SELECT
                uploaded_file,
                MIN(created_at) AS uploaded_at,
                COUNT(*) AS txn_count,
                month, year, source,
                SUM(CASE WHEN type = 'debit' THEN amount ELSE 0 END) AS total_debits,
                SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END) AS total_credits
            FROM transactions
            {email_clause}
            GROUP BY uploaded_file, month, year, source
            ORDER BY MIN(created_at) DESC
        """).fetchall()
    return [dict(r) for r in rows]


def find_duplicate_transactions(
    txns: list[dict], email_only: Optional[bool] = None,
) -> list[dict]:
    """Find transactions in the DB that match any of the given transactions.

    A duplicate is defined as same date + same amount + similar description.
    Scoped by email_only so Email and Statements sections check independently.
    Returns list of dicts with the incoming txn index and the matching DB row.
    """
    if not txns:
        return []

    email_clause = ""
    if email_only is True:
        email_clause = "AND uploaded_file LIKE 'email_%'"
    elif email_only is False:
        email_clause = "AND (uploaded_file IS NULL OR uploaded_file NOT LIKE 'email_%')"

    duplicates = []
    with get_connection() as conn:
        for i, t in enumerate(txns):
            rows = conn.execute(
                f"""
                SELECT id, date, description, amount, type, source, uploaded_file
                FROM transactions
                WHERE date = ? AND amount = ? AND type = ? {email_clause}
                """,
                (t["date"], t["amount"], t["type"]),
            ).fetchall()

            for row in rows:
                # Fuzzy match on description: check if key words overlap
                db_desc = row["description"].upper()
                new_desc = t["description"].upper()
                # Extract 4+ char tokens
                db_tokens = set(w for w in re.split(r"[/\-\s,.|]+", db_desc) if len(w) >= 4)
                new_tokens = set(w for w in re.split(r"[/\-\s,.|]+", new_desc) if len(w) >= 4)
                overlap = db_tokens & new_tokens
                if len(overlap) >= 2 or db_desc == new_desc:
                    duplicates.append({
                        "new_idx": i,
                        "new_txn": t,
                        "existing_id": row["id"],
                        "existing_desc": row["description"],
                        "existing_file": row["uploaded_file"],
                    })
                    break  # one match is enough per new txn

    return duplicates


def find_within_file_duplicates(txns: list[dict]) -> list[tuple[int, int]]:
    """Find duplicate transactions within the same file (same date + amount + description).

    Returns list of (idx_a, idx_b) pairs.
    """
    dupes = []
    seen: dict[str, int] = {}
    for i, t in enumerate(txns):
        key = f"{t['date']}|{t['amount']:.2f}|{t['type']}|{t['description'][:50].upper()}"
        if key in seen:
            dupes.append((seen[key], i))
        else:
            seen[key] = i
    return dupes


def delete_transactions_by_file(filename: str) -> int:
    """Delete all transactions from a specific uploaded file. Returns count deleted."""
    with get_connection() as conn:
        cursor = conn.execute(
            "DELETE FROM transactions WHERE uploaded_file = ?", (filename,)
        )
    return cursor.rowcount


# ---------------------------------------------------------------------------
# Settings helpers (key-value store for app configuration)
# ---------------------------------------------------------------------------

def get_setting(key: str) -> Optional[str]:
    """Retrieve a setting value by key. Returns None if not found."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = ?", (key,)
        ).fetchone()
    return row["value"] if row else None


def set_setting(key: str, value: str) -> None:
    """Store a setting (insert or update)."""
    with get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (key, value),
        )


def delete_setting(key: str) -> None:
    """Remove a setting by key."""
    with get_connection() as conn:
        conn.execute("DELETE FROM settings WHERE key = ?", (key,))
