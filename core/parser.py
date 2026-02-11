"""
Statement parser for CSV, Excel, PDF, and image (PNG/JPEG) bank statements.

The goal is to normalise every statement into a list of dicts with keys:
    date, description, amount, type ('credit' | 'debit')
"""

import base64
import csv
import io
import json
import logging
import os
import re
from datetime import datetime
from typing import Optional

import pandas as pd
import pdfplumber
import requests

logger = logging.getLogger(__name__)


# ---- Common date formats found in Indian bank statements ----
DATE_FORMATS = [
    "%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%d-%m-%y",
    "%Y-%m-%d", "%m/%d/%Y", "%d %b %Y", "%d-%b-%Y",
    "%d %b %y", "%d-%b-%y", "%d %B %Y",
]


def _parse_date(value: str) -> Optional[str]:
    """Try multiple date formats and return ISO date string or None."""
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _clean_amount(value) -> Optional[float]:
    """Convert an amount value (possibly with commas/currency symbols) to float."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    s = re.sub(r"[₹$,\s]", "", s)
    s = s.replace("(", "-").replace(")", "")  # parentheses = negative
    if not s or s == "-" or s == "":
        return None
    try:
        return abs(float(s))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Column detection heuristics
# ---------------------------------------------------------------------------

_DATE_KEYWORDS = {
    "date", "txn date", "tran date", "transaction date", "trans date",
    "value date", "posting date", "txn dt", "tran dt",
}
_DESC_KEYWORDS = {
    "description", "narration", "particulars", "details",
    "transaction details", "remarks", "narrative",
}
_DEBIT_KEYWORDS = {
    "debit", "withdrawal", "dr", "debit amount", "withdrawal amt",
    "spent", "debit amt",
}
_CREDIT_KEYWORDS = {
    "credit", "deposit", "cr", "credit amount", "deposit amt",
    "earned", "credit amt",
}
_AMOUNT_KEYWORDS = {"amount", "transaction amount", "txn amount", "amt"}


def _match_column(col: str, keywords: set[str]) -> bool:
    """Check if a column name loosely matches any keyword."""
    normalised = col.strip().lower()
    return normalised in keywords or any(k in normalised for k in keywords)


def _detect_columns(df: pd.DataFrame) -> dict:
    """Auto-detect date, description, debit, credit, and amount columns."""
    mapping = {"date": None, "description": None, "debit": None, "credit": None, "amount": None}

    for col in df.columns:
        col_str = str(col)
        if mapping["date"] is None and _match_column(col_str, _DATE_KEYWORDS):
            mapping["date"] = col
        elif mapping["description"] is None and _match_column(col_str, _DESC_KEYWORDS):
            mapping["description"] = col
        elif mapping["debit"] is None and _match_column(col_str, _DEBIT_KEYWORDS):
            mapping["debit"] = col
        elif mapping["credit"] is None and _match_column(col_str, _CREDIT_KEYWORDS):
            mapping["credit"] = col
        elif mapping["amount"] is None and _match_column(col_str, _AMOUNT_KEYWORDS):
            mapping["amount"] = col

    return mapping


# ---------------------------------------------------------------------------
# CSV / Excel parsing
# ---------------------------------------------------------------------------

def parse_csv(file_bytes: bytes, filename: str) -> tuple[list[dict], dict]:
    """Parse a CSV or Excel file.

    Returns (transactions, column_mapping) where column_mapping can be shown
    to the user for confirmation / correction.

    Many Indian bank CSVs have preamble rows (customer info, address, etc.)
    before the actual header row. We detect and skip those automatically.
    They also have unbalanced quotes in narration fields.
    """
    if filename.endswith((".xlsx", ".xls")):
        df = pd.read_excel(io.BytesIO(file_bytes))
    else:
        df = _read_csv_robust(file_bytes)

    # Drop fully empty rows/columns
    df = df.dropna(how="all").dropna(axis=1, how="all")
    df.columns = [str(c).strip() for c in df.columns]

    mapping = _detect_columns(df)
    transactions = _dataframe_to_transactions(df, mapping)
    return transactions, mapping


def _strip_preamble(file_bytes: bytes) -> bytes:
    """Strip preamble/customer-info rows from bank CSV and return only the
    header + data portion as raw bytes.

    Indian bank CSVs (Axis, HDFC, ICICI, SBI, etc.) commonly have 10-20 lines
    of account info before the actual transaction table. We find the header
    line by looking for one that contains both a date keyword AND a
    debit/credit/amount keyword, then return everything from that line onward.
    """
    # Decode just for scanning (latin-1 never fails)
    text = file_bytes.decode("latin-1", errors="replace")
    lines = text.split("\n")

    all_date_kw = _DATE_KEYWORDS | {"date"}

    for i, line in enumerate(lines):
        line_lower = line.strip().lower()
        # Require at least 2 commas (to filter out prose lines that
        # accidentally contain the word "date")
        if line_lower.count(",") < 2:
            continue
        has_date_col = any(kw in line_lower for kw in all_date_kw)
        has_amount_col = (
            any(kw in line_lower for kw in _DEBIT_KEYWORDS)
            or any(kw in line_lower for kw in _CREDIT_KEYWORDS)
            or any(kw in line_lower for kw in _AMOUNT_KEYWORDS)
        )
        if has_date_col and has_amount_col:
            cleaned = "\n".join(lines[i:])
            return cleaned.encode("latin-1", errors="replace")

    # Header not found -- return original bytes
    return file_bytes


def _read_csv_robust(file_bytes: bytes) -> pd.DataFrame:
    """Try progressively more lenient CSV parsing strategies.

    Indian bank CSVs commonly have:
    - Preamble rows (name, address, account info) before the real header
    - Unbalanced quotes in narration/description fields
    - Footer rows with disclaimers and unbalanced quotes
    - Mixed encodings
    """
    # Step 1: strip preamble so pandas sees the header as the first row
    cleaned_bytes = _strip_preamble(file_bytes)

    encodings = ("utf-8", "latin-1", "cp1252")

    # Strategy 1: strict parse of cleaned CSV
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(cleaned_bytes), encoding=enc)
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue

    # Strategy 2: skip bad lines
    for enc in encodings:
        try:
            return pd.read_csv(
                io.BytesIO(cleaned_bytes), encoding=enc,
                on_bad_lines="skip",
            )
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue

    # Strategy 3: disable quoting (fixes "EOF inside string" from
    # footer disclaimer text with unbalanced quotes)
    for enc in encodings:
        try:
            return pd.read_csv(
                io.BytesIO(cleaned_bytes), encoding=enc,
                quoting=csv.QUOTE_NONE, on_bad_lines="skip",
            )
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue

    # Strategy 4: Python engine as final fallback
    return pd.read_csv(
        io.BytesIO(cleaned_bytes), encoding="latin-1",
        quoting=csv.QUOTE_NONE, on_bad_lines="skip",
        engine="python", sep=",",
    )


def _dataframe_to_transactions(df: pd.DataFrame, mapping: dict) -> list[dict]:
    """Convert a DataFrame to a list of transaction dicts using the column mapping."""
    transactions = []

    for _, row in df.iterrows():
        # Date
        date_val = row.get(mapping["date"]) if mapping["date"] else None
        parsed_date = _parse_date(str(date_val)) if date_val is not None else None
        if parsed_date is None:
            continue  # skip rows without a parseable date

        # Description
        desc = str(row.get(mapping["description"], "")).strip() if mapping["description"] else ""
        if not desc or desc == "nan":
            desc = "No description"

        # Amount and type
        if mapping["debit"] and mapping["credit"]:
            debit_amt = _clean_amount(row.get(mapping["debit"]))
            credit_amt = _clean_amount(row.get(mapping["credit"]))
            if debit_amt and debit_amt > 0:
                transactions.append({"date": parsed_date, "description": desc, "amount": debit_amt, "type": "debit"})
            elif credit_amt and credit_amt > 0:
                transactions.append({"date": parsed_date, "description": desc, "amount": credit_amt, "type": "credit"})
        elif mapping["amount"]:
            amt = _clean_amount(row.get(mapping["amount"]))
            if amt is not None and amt > 0:
                # Heuristic: if amount is negative in original or description hints at credit
                raw = str(row.get(mapping["amount"], ""))
                txn_type = "credit" if raw.strip().startswith("-") or "cr" in raw.lower() else "debit"
                transactions.append({"date": parsed_date, "description": desc, "amount": amt, "type": txn_type})

    return transactions


# ---------------------------------------------------------------------------
# PDF parsing
# ---------------------------------------------------------------------------

def parse_pdf(file_bytes: bytes) -> list[dict]:
    """Extract transactions from a PDF bank / credit-card statement.

    Strategy:
    1. Try table extraction via pdfplumber (works for most structured PDFs).
    2. Fall back to line-by-line text parsing if no tables found.
    """
    transactions: list[dict] = []

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        # Attempt 1: table extraction
        all_tables = []
        for page in pdf.pages:
            tables = page.extract_tables()
            if tables:
                all_tables.extend(tables)

        if all_tables:
            transactions = _parse_pdf_tables(all_tables)

        # Attempt 2: line-by-line fallback
        if not transactions:
            full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
            transactions = _parse_pdf_text(full_text)

    return transactions


def _parse_pdf_tables(tables: list[list]) -> list[dict]:
    """Parse transactions from extracted PDF tables."""
    transactions = []

    for table in tables:
        if not table or len(table) < 2:
            continue

        # Use first row as header
        header = [str(c).strip().lower() if c else "" for c in table[0]]

        # Detect column indices
        date_idx = _find_index(header, _DATE_KEYWORDS)
        desc_idx = _find_index(header, _DESC_KEYWORDS)
        debit_idx = _find_index(header, _DEBIT_KEYWORDS)
        credit_idx = _find_index(header, _CREDIT_KEYWORDS)
        amount_idx = _find_index(header, _AMOUNT_KEYWORDS)

        if date_idx is None:
            continue

        for row in table[1:]:
            if not row or len(row) <= date_idx:
                continue

            date_val = _parse_date(str(row[date_idx] or ""))
            if date_val is None:
                continue

            desc = str(row[desc_idx]).strip() if desc_idx is not None and desc_idx < len(row) else "No description"
            if desc == "None" or desc == "nan":
                desc = "No description"

            if debit_idx is not None and credit_idx is not None:
                debit_amt = _clean_amount(row[debit_idx]) if debit_idx < len(row) else None
                credit_amt = _clean_amount(row[credit_idx]) if credit_idx < len(row) else None
                if debit_amt and debit_amt > 0:
                    transactions.append({"date": date_val, "description": desc, "amount": debit_amt, "type": "debit"})
                elif credit_amt and credit_amt > 0:
                    transactions.append({"date": date_val, "description": desc, "amount": credit_amt, "type": "credit"})
            elif amount_idx is not None and amount_idx < len(row):
                amt = _clean_amount(row[amount_idx])
                if amt and amt > 0:
                    raw = str(row[amount_idx] or "")
                    txn_type = "credit" if "-" in raw or "cr" in raw.lower() else "debit"
                    transactions.append({"date": date_val, "description": desc, "amount": amt, "type": txn_type})

    return transactions


def _find_index(header: list[str], keywords: set[str]) -> Optional[int]:
    """Find the first column index that matches any keyword."""
    for i, col in enumerate(header):
        if _match_column(col, keywords):
            return i
    return None


def _parse_pdf_text(text: str) -> list[dict]:
    """Fallback: parse transactions from raw PDF text line by line.

    Looks for lines that start with a date pattern followed by description and amount.
    """
    transactions = []
    # Pattern: date  description  amount (possibly with Cr/Dr suffix)
    line_pattern = re.compile(
        r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s+(.+?)\s+([\d,]+\.?\d*)\s*(Cr|Dr|CR|DR)?$"
    )

    for line in text.split("\n"):
        line = line.strip()
        match = line_pattern.search(line)
        if match:
            date_str, desc, amount_str, dr_cr = match.groups()
            parsed_date = _parse_date(date_str)
            if parsed_date is None:
                continue
            amt = _clean_amount(amount_str)
            if amt is None or amt == 0:
                continue
            txn_type = "credit" if dr_cr and dr_cr.upper() == "CR" else "debit"
            transactions.append({"date": parsed_date, "description": desc.strip(), "amount": amt, "type": txn_type})

    return transactions


# ---------------------------------------------------------------------------
# Image parsing (PNG, JPEG) via Ollama vision model
# ---------------------------------------------------------------------------

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_VISION_MODEL = os.getenv("OLLAMA_VISION_MODEL", "llama3.2-vision")


def _check_vision_model() -> tuple[bool, str]:
    """Check if the Ollama vision model is available. Returns (ok, message)."""
    try:
        resp = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
        if resp.status_code != 200:
            return False, "Ollama is not responding."
        models = [m["name"] for m in resp.json().get("models", [])]
        # Check for the configured vision model or common variants
        for m in models:
            if OLLAMA_VISION_MODEL in m:
                return True, m
        return False, (
            f"Vision model '{OLLAMA_VISION_MODEL}' not found. "
            f"Available models: {models}. "
            f"Run: ollama pull {OLLAMA_VISION_MODEL}"
        )
    except requests.ConnectionError:
        return False, "Ollama is not running. Start it with: ollama serve"


def parse_image(file_bytes: bytes) -> list[dict]:
    """Extract transactions from a bank statement screenshot using Ollama vision.

    Sends the image to a local vision model which reads the table and returns
    structured JSON that we parse into our standard transaction format.

    Raises RuntimeError if the vision model is not available.
    """
    ok, msg = _check_vision_model()
    if not ok:
        raise RuntimeError(f"Image parsing unavailable: {msg}")

    b64_image = base64.b64encode(file_bytes).decode("utf-8")

    prompt = """You are reading a bank or credit card statement screenshot.
Extract ALL transactions visible in the image as a JSON array.

Each transaction must have these fields:
- "date": the transaction date in DD-MM-YYYY or DD/MM/YYYY format
- "description": the narration or description text
- "amount": the numeric amount (no commas or currency symbols)
- "type": "debit" if money was spent/withdrawn, "credit" if money was received/deposited

Rules:
- Return ONLY a valid JSON array. No explanation, no markdown.
- If you can see debit and credit columns, use them to determine the type.
- If there's only one amount column, look for Dr/Cr indicators or use context.
- Include ALL rows you can read, even partially visible ones.

Example output:
[{"date": "01-01-2026", "description": "UPI/SWIGGY", "amount": 500, "type": "debit"}]
"""

    try:
        resp = requests.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={
                "model": OLLAMA_VISION_MODEL,
                "prompt": prompt,
                "images": [b64_image],
                "stream": False,
                "options": {"temperature": 0.1, "num_predict": 4096},
            },
            timeout=180,
        )

        if resp.status_code != 200:
            error_text = resp.text[:300]
            raise RuntimeError(f"Ollama vision error ({resp.status_code}): {error_text}")

        text = resp.json().get("response", "").strip()
        return _parse_vision_response(text)

    except requests.ConnectionError:
        raise RuntimeError(f"Cannot connect to Ollama at {OLLAMA_BASE_URL}. Is it running?")
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Image parsing failed: {e}")


def _parse_vision_response(text: str) -> list[dict]:
    """Parse the JSON array returned by the vision model."""
    # Strip markdown fences
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[-1] if "\n" in cleaned else cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned.rsplit("```", 1)[0]
    cleaned = cleaned.strip()

    # Find the JSON array
    start = cleaned.find("[")
    end = cleaned.rfind("]")
    if start == -1 or end == -1:
        logger.warning("No JSON array in vision response: %s", cleaned[:200])
        return []

    try:
        raw_list = json.loads(cleaned[start : end + 1])
    except json.JSONDecodeError:
        logger.warning("Invalid JSON from vision model: %s", cleaned[:200])
        return []

    transactions = []
    for item in raw_list:
        if not isinstance(item, dict):
            continue
        date_str = str(item.get("date", ""))
        parsed_date = _parse_date(date_str)
        if not parsed_date:
            continue

        desc = str(item.get("description", "")).strip()
        if not desc:
            desc = "No description"

        raw_amt = item.get("amount")
        amt = _clean_amount(raw_amt)
        if not amt or amt <= 0:
            continue

        txn_type = str(item.get("type", "debit")).lower()
        if txn_type not in ("debit", "credit"):
            txn_type = "debit"

        transactions.append({
            "date": parsed_date,
            "description": desc,
            "amount": amt,
            "type": txn_type,
        })

    return transactions
