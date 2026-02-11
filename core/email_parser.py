"""
Email-based transaction parser for Indian bank alert emails.

Connects via IMAP to fetch bank transaction alert emails for a given month,
then extracts credit/debit transaction details using regex patterns tuned
for common Indian banks (HDFC, ICICI, SBI, Axis, Kotak, etc.).

Each transaction is normalised to: {date, description, amount, type}
matching the format used by the statement parser.
"""

import email
import email.message
import imaplib
import logging
import os
import re
from datetime import datetime
from email.header import decode_header
from email.utils import parsedate_to_datetime
from typing import Optional

from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Known bank sender addresses (partial match is fine)
# ---------------------------------------------------------------------------

BANK_SENDERS = [
    "hdfcbank",
    "hdfcbank.bank",   # alerts@hdfcbank.bank.in
    "icicibank",
    "sbi.co.in",
    "axisbank",
    "axis.bank",       # alerts@axis.bank.in
    "kotak",
    "indusind",
    "yesbank",
    "yes.bank",        # alerts@yes.bank
    "idfc",
    "idfcfirst",
    "rbl",
    "federal",
    "canarabank",
    "bankofbaroda",
    "bob.co",          # alerts@bob.co.in
    "pnb",
    "iob",
    "unionbank",
    "citi",
    "sc.com",          # Standard Chartered
    "americanexpress",
    "hsbc",
    "dbs",
    "idbi",
    "bandhan",
    "aubank",
    "au.bank",         # alerts@au.bank
    "paytm",
    "freecharge",
    "phonepe",
    "gpay",
    "amazonpay",
    "slice",
    "onecard",
    "jupiter",
    "fi.money",
    "niyo",
]

# Subject keywords that indicate a transaction alert email
ALERT_SUBJECT_KEYWORDS = [
    "transaction alert",
    "debit alert",
    "credit alert",
    "account alert",
    "a/c alert",
    "alert : update",
    "alert: update",
    "transaction confirmation",
    "purchase alert",
    "payment alert",
    "upi alert",
    "upi txn",             # HDFC: "You have done a UPI txn"
    "upi transaction",
    "neft alert",
    "imps alert",
    "rtgs alert",
    "fund transfer",
    "atm withdrawal",
    "debited",
    "credited",
    "spent on",
    "payment of rs",
    "transaction of rs",
    "you have done",       # HDFC: "You have done a UPI txn. Check details!"
    "imps transaction",    # "Alert: IMPS Transaction"
    "imps transfer",
    "payment received",    # "Payment of Rs X received for your credit card"
]

# IMAP server presets for popular providers
IMAP_PRESETS = {
    "Gmail": {"host": "imap.gmail.com", "port": 993},
    "Outlook/Hotmail": {"host": "imap-mail.outlook.com", "port": 993},
    "Yahoo": {"host": "imap.mail.yahoo.com", "port": 993},
    "Zoho": {"host": "imap.zoho.com", "port": 993},
    "Custom": {"host": "", "port": 993},
}


# ---------------------------------------------------------------------------
# IMAP connection & email fetching
# ---------------------------------------------------------------------------

class EmailConnectionError(Exception):
    """Raised when IMAP connection or authentication fails."""


def connect_imap(
    host: str,
    port: int,
    email_address: str,
    password: str,
) -> imaplib.IMAP4_SSL:
    """Establish an IMAP SSL connection and authenticate."""
    try:
        mail = imaplib.IMAP4_SSL(host, port)
        mail.login(email_address, password)
        return mail
    except imaplib.IMAP4.error as e:
        raise EmailConnectionError(
            f"Authentication failed. If using Gmail, make sure you're using "
            f"an App Password (not your regular password). Error: {e}"
        ) from e
    except Exception as e:
        raise EmailConnectionError(
            f"Cannot connect to {host}:{port}. Check your settings. Error: {e}"
        ) from e


# IMAP search terms: each entry is (field, keyword).
# Subject-only searches are the most precise and fast (~1s each).
# We avoid broad FROM searches like "alerts@" which return 600+ results.
_IMAP_SEARCHES = [
    # Subject searches -- catch the actual transaction alert emails
    ("SUBJECT", "debited"),
    ("SUBJECT", "credited"),
    ("SUBJECT", "spent on"),
    ("SUBJECT", "transaction alert"),
    ("SUBJECT", "debit alert"),
    ("SUBJECT", "credit alert"),
    ("SUBJECT", "payment alert"),
    ("SUBJECT", "debited from your"),
    ("SUBJECT", "credited to your"),
    ("SUBJECT", "withdrawn from"),
    ("SUBJECT", "UPI txn"),          # HDFC: "You have done a UPI txn"
    ("SUBJECT", "UPI transaction"),
    ("SUBJECT", "IMPS"),           # IMPS transfer alerts
    ("SUBJECT", "payment received"),  # Credit card payment confirmations
    ("SUBJECT", "payment of rs"),    # "Payment of Rs X"
    # FROM searches for major banks -- catches emails with unusual subjects
    ("FROM", "hdfcbank"),
    ("FROM", "icicibank"),
    ("FROM", "axis.bank"),
    ("FROM", "axisbank"),           # alerts@axisbank.com
]


class FetchCancelledError(Exception):
    """Raised when the user cancels the email fetch."""


def fetch_transaction_emails(
    mail: imaplib.IMAP4_SSL,
    month: int,
    year: int,
    folder: str = "INBOX",
    on_progress: Optional[callable] = None,
    is_cancelled: Optional[callable] = None,
) -> list[dict]:
    """Fetch and parse all bank transaction alert emails for a given month.

    Strategy (fast server-side search):
      1. Run a small number of targeted IMAP SUBJECT and FROM searches
         (server-side, no downloading) to find candidate email IDs.
      2. Download full bodies only for the candidates.
      3. Parse transaction details from each alert email.

    Args:
        on_progress: optional callback(step: str, detail: str) for live updates.
        is_cancelled: optional callable returning True if user wants to cancel.

    Returns a list of transaction dicts in the standard format:
        {date, description, amount, type}

    Raises FetchCancelledError if cancelled by the user.
    """
    def _progress(step: str, detail: str = ""):
        if on_progress:
            on_progress(step, detail)

    def _check_cancel():
        if is_cancelled and is_cancelled():
            raise FetchCancelledError("Fetch cancelled by user.")

    mail.select(folder, readonly=True)

    # Build date range
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1)
    else:
        end_date = datetime(year, month + 1, 1)

    since_str = start_date.strftime("%d-%b-%Y")
    before_str = end_date.strftime("%d-%b-%Y")
    date_filter = f'SINCE "{since_str}" BEFORE "{before_str}"'

    # --- Pass 1: Server-side searches (fast, ~1s each) ---
    candidate_ids: set[bytes] = set()
    total_searches = len(_IMAP_SEARCHES)

    _progress("search", "Searching inbox for bank transaction alerts...")

    for i, (field, keyword) in enumerate(_IMAP_SEARCHES):
        _check_cancel()
        _progress("search", f"Searching {i + 1}/{total_searches}: {keyword}")
        try:
            criteria = f'({date_filter} {field} "{keyword}")'
            status, message_ids = mail.search(None, criteria)
            if status == "OK" and message_ids[0]:
                candidate_ids.update(message_ids[0].split())
        except imaplib.IMAP4.error:
            continue

    if not candidate_ids:
        _progress("done", "No bank alert emails found.")
        return []

    total_candidates = len(candidate_ids)
    _progress("download", f"Found {total_candidates} candidate emails. Downloading...")

    # --- Pass 2: Download full bodies and parse ---
    transactions = []
    candidate_list = list(candidate_ids)
    skipped_filter = 0
    skipped_no_body = 0
    skipped_no_extract = 0
    skipped_subjects = []  # track what subjects failed extraction

    for i, msg_id in enumerate(candidate_list):
        _check_cancel()
        if (i + 1) % 5 == 0 or i == 0:
            _progress("download",
                f"Processing {i + 1}/{total_candidates} "
                f"({len(transactions)} extracted so far)")

        try:
            status, msg_data = mail.fetch(msg_id, "(RFC822)")
            if status != "OK":
                continue

            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)

            sender = _get_sender(msg)
            subject = _get_subject(msg)
            email_date = _get_email_date(msg)

            # Double-check with local filter (server search can be loose)
            if not _is_transaction_alert(sender, subject):
                skipped_filter += 1
                continue

            body = _get_email_body(msg)
            if not body:
                skipped_no_body += 1
                continue

            txn = _extract_transaction(body, subject, email_date)
            if txn:
                transactions.append(txn)
            else:
                skipped_no_extract += 1
                skipped_subjects.append(subject[:80])
        except Exception as e:
            logger.warning("Error processing email: %s", e)
            continue

    # Log diagnostic summary
    logger.info(
        "Fetch summary: %d candidates, %d extracted, "
        "%d filtered out, %d no body, %d extraction failed",
        total_candidates, len(transactions),
        skipped_filter, skipped_no_body, skipped_no_extract,
    )
    if skipped_subjects:
        logger.info("Failed extraction subjects (sample): %s",
                     skipped_subjects[:10])

    _progress("done",
        f"Done: {len(transactions)} transactions from {total_candidates} emails "
        f"(skipped: {skipped_filter} non-alert, {skipped_no_extract} parse failed)")
    return transactions


def disconnect_imap(mail: imaplib.IMAP4_SSL) -> None:
    """Safely close the IMAP connection."""
    try:
        mail.close()
        mail.logout()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Email header helpers
# ---------------------------------------------------------------------------

def _header_to_str(value) -> str:
    """Safely convert an email header value to a plain string.

    The email module can return str, bytes, or Header objects depending on
    the encoding. This normalises all of them to a plain str.

    Critically, strings containing MIME encoded-words (=?charset?encoding?...?=)
    must be decoded via decode_header, not returned as-is.
    """
    if value is None:
        return ""
    # Always try decode_header for str/Header values that may contain
    # MIME encoded-words like =?UTF-8?B?...?= or =?UTF-8?Q?...?=
    try:
        raw = value
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        raw_str = str(raw)
        decoded_parts = decode_header(raw_str)
        parts = []
        for part, charset in decoded_parts:
            if isinstance(part, bytes):
                parts.append(part.decode(charset or "utf-8", errors="replace"))
            else:
                parts.append(str(part))
        return " ".join(parts)
    except Exception:
        return str(value)


def _get_sender(msg: email.message.Message) -> str:
    """Extract and decode the sender email address."""
    sender = _header_to_str(msg.get("From", ""))
    return sender.lower()


def _get_subject(msg: email.message.Message) -> str:
    """Extract and decode the email subject."""
    raw_subject = msg.get("Subject", "")
    return _header_to_str(raw_subject)


def _get_email_date(msg: email.message.Message) -> Optional[str]:
    """Extract and normalise the email date to ISO format."""
    date_str = _header_to_str(msg.get("Date", ""))
    if not date_str:
        return None
    try:
        dt = parsedate_to_datetime(date_str)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def _is_transaction_alert(sender: str, subject: str) -> bool:
    """Determine if an email is a bank transaction alert based on sender and subject."""
    sender_lower = sender.lower()
    subject_lower = subject.lower()

    # Check if sender is from a known bank
    is_bank_sender = any(bank in sender_lower for bank in BANK_SENDERS)

    # Check if subject contains transaction alert keywords
    is_alert_subject = any(kw in subject_lower for kw in ALERT_SUBJECT_KEYWORDS)

    # Also check for common transaction patterns in subject
    has_amount_in_subject = bool(re.search(r"(?:rs\.?|inr)\s*[\d,]+", subject_lower))

    return is_bank_sender and (is_alert_subject or has_amount_in_subject)


# ---------------------------------------------------------------------------
# Email body extraction
# ---------------------------------------------------------------------------

def _get_email_body(msg: email.message.Message) -> str:
    """Extract the text content from an email message (handles multipart)."""
    body_parts = []

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition", ""))

            # Skip attachments
            if "attachment" in content_disposition:
                continue

            if content_type == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    body_parts.append(payload.decode(charset, errors="replace"))
            elif content_type == "text/html":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    html = payload.decode(charset, errors="replace")
                    body_parts.append(_html_to_text(html))
    else:
        content_type = msg.get_content_type()
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            text = payload.decode(charset, errors="replace")
            if content_type == "text/html":
                body_parts.append(_html_to_text(text))
            else:
                body_parts.append(text)

    return "\n".join(body_parts)


def _html_to_text(html: str) -> str:
    """Convert HTML to plain text, preserving meaningful whitespace."""
    soup = BeautifulSoup(html, "html.parser")

    # Remove script and style elements
    for tag in soup(["script", "style"]):
        tag.decompose()

    text = soup.get_text(separator=" ", strip=True)
    # Collapse multiple whitespace
    text = re.sub(r"\s+", " ", text)
    return text


# ---------------------------------------------------------------------------
# Transaction extraction via regex patterns
# ---------------------------------------------------------------------------

# Amount patterns: Rs.500, Rs 500.00, INR 500, ₹500, Rs.1,23,456.78
_AMOUNT_PATTERN = re.compile(
    r"(?:rs\.?|inr|₹)\s*([\d,]+(?:\.\d{1,2})?)",
    re.IGNORECASE,
)

# Debit indicators
_DEBIT_PATTERNS = [
    re.compile(r"(?:debited|deducted|spent|withdrawn|paid|purchase|payment)", re.IGNORECASE),
    re.compile(r"debit(?:ed)?\s+(?:of|by|with|for)\s+", re.IGNORECASE),
    re.compile(r"(?:rs\.?|inr|₹)\s*[\d,]+(?:\.\d{2})?\s+(?:has been |was )?debited", re.IGNORECASE),
    re.compile(r"debited\s+(?:from|on|with)", re.IGNORECASE),
]

# Credit indicators
_CREDIT_PATTERNS = [
    re.compile(r"(?:credited|received|deposited|refund)", re.IGNORECASE),
    re.compile(r"credit(?:ed)?\s+(?:of|by|with|for|to)\s+", re.IGNORECASE),
    re.compile(r"(?:rs\.?|inr|₹)\s*[\d,]+(?:\.\d{2})?\s+(?:has been |was )?credited", re.IGNORECASE),
    re.compile(r"credited\s+(?:to|into|in)", re.IGNORECASE),
    # "Payment of Rs X received" = credit (card payment confirmation)
    re.compile(r"payment\s+.*?received", re.IGNORECASE),
    re.compile(r"received\s+your\s+payment", re.IGNORECASE),
]

# Description extraction patterns (what was the transaction for)
# Ordered by specificity: most precise patterns first.
_DESCRIPTION_PATTERNS = [
    # "Transaction Info: UPI/P2M/639743533859/CRED Club" (Axis Bank account format)
    # Captures the full UPI/NEFT/IMPS reference string after "Transaction Info:"
    re.compile(
        r"transaction\s+info[:\s]+"
        r"(.{3,100}?)"
        r"(?:\s+(?:if this|feel free|call|regard|always|avl|available|bal|please|for any))",
        re.IGNORECASE,
    ),
    # Axis Burgundy: "debited/credited with INR X on ... by NEFT/.../ACH-CR-..."
    re.compile(
        r"(?:debited|credited)\s+with\s+(?:inr|rs\.?|₹)\s*[\d,]+.*?\s+by\s+([A-Za-z0-9/\-.\s]+?)(?:\s*\.|\s+To check)",
        re.IGNORECASE,
    ),
    # "Merchant Name: SPOTIFY SI" (Axis Bank credit card format)
    re.compile(
        r"merchant\s+name[:\s]+"
        r"([A-Za-z0-9\s&./_'-]{2,60}?)"
        r"(?:\s+(?:axis|hdfc|icici|sbi|card|date|available|total|credit|if this))",
        re.IGNORECASE,
    ),
    # Standalone UPI string: "UPI/P2M/639743533859/CRED Club"
    re.compile(r"(UPI/[A-Za-z0-9]+/\d+/[A-Za-z0-9\s.&_-]+?)(?:\s+(?:if |feel |call |regard|always|$))", re.IGNORECASE),
    # UPI: "to VPA user@bank MERCHANT NAME on DD-MM-YY" or "from VPA user@bank"
    # Captures VPA + trailing merchant name (e.g. "Q394334523@ybl REAL VALUE MART")
    # Stops before "on DD", period, "your", "upi", "if"
    re.compile(
        r"(?:to|from)\s+(?:vpa\s+)?"
        r"([a-zA-Z0-9._]+@[a-zA-Z0-9]+"  # VPA part
        r"(?:\s+[A-Z][A-Za-z0-9\s&.'-]*?)?)"  # optional merchant name (lazy)
        r"\s+on\s+\d",  # terminated by "on DD..."
        re.IGNORECASE,
    ),
    # Fallback: just VPA without merchant name
    re.compile(r"(?:to|from)\s+(?:vpa\s+)?([a-zA-Z0-9._]+@[a-zA-Z0-9]+)", re.IGNORECASE),
    # "Info: DESCRIPTION" pattern (common in HDFC)
    re.compile(r"(?:transaction\s+)?info[:\s]+(.{3,80}?)(?:\s+(?:if this|avl|available|bal|feel free|call))", re.IGNORECASE),
    # "Merchant: NAME" or "Payee: NAME" or "Beneficiary: NAME"
    re.compile(r"(?:merchant|payee|beneficiary)[:\s]+(.{3,60}?)(?:\s*$|\s+(?:ref|on|date|card|account|if this))", re.IGNORECASE),
    # "at MERCHANT NAME" or "to MERCHANT NAME" or "towards MERCHANT NAME"
    re.compile(r"(?:at|towards)\s+([A-Z][A-Za-z0-9\s&.'-]{2,40})(?:\s+on|\s+via|\s+ref|\s*\.)", re.IGNORECASE),
    # IMPS/NEFT/RTGS reference
    re.compile(r"(?:imps|neft|rtgs)[:/\s]+(.{3,50}?)(?:\s+ref|\s+on|\s+if|\s*$)", re.IGNORECASE),
    # Generic "by NEFT-..." / "by IMPS/..." / "by UPI/..." (broad fallback for "debited/credited by X")
    re.compile(
        r"(?:debited|credited)\s+.*?\s+(?:towards|by)\s+"
        r"((?:UPI|NEFT|IMPS|RTGS|ACH)[A-Za-z0-9/\-.\s]*?)(?:\s*\.|\s+(?:Avl|Available|If |if |Your|$))",
        re.IGNORECASE,
    ),
    # "transfer to NAME" or "transfer from NAME"
    re.compile(r"transfer\s+(?:to|from)\s+(.{3,50}?)(?:\s+ref|\s+on|\s+if|\s*$)", re.IGNORECASE),
    # "at MERCHANT on DD-MM-YYYY" (without trailing date)
    re.compile(r"(?:at)\s+([A-Z][A-Za-z0-9\s&.'-]{2,30}?)(?:\s+on\s+\d)", re.IGNORECASE),
    # Refund from MERCHANT
    re.compile(r"refund\s+(?:from|by)\s+(.{3,40}?)(?:\s*\.|\s+(?:Avl|Available|If|$))", re.IGNORECASE),
]

# Date extraction from email body
# Multiple patterns tried in order.
_DATE_BODY_PATTERNS = [
    # "Date & Time: 10-01-2026, 14:07" (Axis Bank format)
    re.compile(r"date\s*[&]\s*time[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", re.IGNORECASE),
    # "on 01-01-2026" or "dated 01/01/2026" or "on 24 Dec 2025"
    re.compile(
        r"(?:on|dated?)\s+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{1,2}\s*(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s*,?\s*\d{2,4})",
        re.IGNORECASE,
    ),
    # US format: "on Dec 24, 2025" (ICICI credit card uses Mon DD, YYYY)
    re.compile(
        r"(?:on|dated?)\s+((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s*\d{2,4})",
        re.IGNORECASE,
    ),
    # Standalone date at start of line/text: "10-01-2026" (Axis often has bare date)
    re.compile(r"(?:^|\s)(\d{2}[/-]\d{2}[/-]\d{4})(?:\s|,)", re.IGNORECASE),
]

# Card / account patterns for description enrichment
_CARD_PATTERN = re.compile(
    r"(?:card|a/c|account|acct?)\s*(?:no\.?\s*)?(?:ending\s+(?:with\s+)?|xx+)?(\d{4})",
    re.IGNORECASE,
)

# Common date formats in email bodies
_EMAIL_DATE_FORMATS = [
    "%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%d-%m-%y",
    "%d %b %Y", "%d-%b-%Y", "%d %b %y", "%d-%b-%y",
    "%d%b%Y", "%d%b%y",  # "01Jan2026"
    "%d %B %Y", "%Y-%m-%d",
    # US format: "Dec 24, 2025" / "Dec 24 2025" (ICICI credit card)
    "%b %d, %Y", "%b %d %Y", "%B %d, %Y", "%B %d %Y",
]


def _parse_email_date(value: str) -> Optional[str]:
    """Try to parse a date from the email body text."""
    value = value.strip()
    for fmt in _EMAIL_DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# Keywords that indicate a promotional/marketing email, not a transaction alert
_PROMO_KEYWORDS = [
    "view the web version",
    "view this message in your mobile",
    "unsubscribe",
    "click here to view",
    "newsletter",
    "offer valid",
    "offers are live",
    "grab incredible",
    "t&c apply",
    "terms and conditions apply",
    "limited period offer",
    "exclusive offer",
    "exciting offer",
    "cashback offer",
    "emi plans",
    "can't see this email properly",
    "aclmails.in",
    "trkp.aclmails",
]

# Words that MUST appear in a real transaction alert body
_REAL_ALERT_MARKERS = [
    "debited",
    "credited",
    "withdrawn",
    "spent",
    "transaction",
    "txn",
    "payment of",
    "received",
    "a/c",
    "card",
    "account",
]


def _is_promotional(body: str) -> bool:
    """Return True if the email body looks like a promotional/marketing email."""
    body_lower = body.lower()

    # Check for promo keywords
    promo_count = sum(1 for kw in _PROMO_KEYWORDS if kw in body_lower)
    if promo_count >= 2:
        return True

    # If body has no real transaction alert markers, it's likely promo
    has_alert_marker = any(m in body_lower for m in _REAL_ALERT_MARKERS)
    if not has_alert_marker:
        return True

    return False


def _extract_transaction(body: str, subject: str, email_date: Optional[str]) -> Optional[dict]:
    """Extract a single transaction from the email body text.

    Combines subject and body for maximum extraction coverage.
    Returns a transaction dict or None if extraction fails.
    """
    # Filter out promotional/marketing emails
    if _is_promotional(body):
        return None

    full_text = f"{subject} {body}"

    # 1. Extract amount
    amount = _extract_amount(full_text)
    if amount is None or amount <= 0:
        return None

    # 2. Determine transaction type (debit vs credit)
    txn_type = _determine_type(full_text)

    # 3. Extract date (prefer body date, fall back to email header date)
    txn_date = _extract_date_from_body(full_text) or email_date
    if not txn_date:
        return None

    # 4. Extract description
    description = _extract_description(full_text, subject)

    return {
        "date": txn_date,
        "description": description,
        "amount": amount,
        "type": txn_type,
        "email_body": body[:2000],  # store full email body (capped for storage)
    }


def _extract_amount(text: str) -> Optional[float]:
    """Extract the transaction amount from text."""
    matches = _AMOUNT_PATTERN.findall(text)
    if not matches:
        return None

    # Take the first amount found (usually the transaction amount)
    # Ignore very small amounts (likely reference numbers) and
    # amounts that look like balance (often the second match)
    for match in matches:
        cleaned = match.replace(",", "")
        try:
            amt = float(cleaned)
            if amt > 0:
                return amt
        except ValueError:
            continue

    return None


def _determine_type(text: str) -> str:
    """Determine if the transaction is a debit or credit."""
    debit_score = sum(1 for p in _DEBIT_PATTERNS if p.search(text))
    credit_score = sum(1 for p in _CREDIT_PATTERNS if p.search(text))

    if credit_score > debit_score:
        return "credit"
    return "debit"  # default to debit if uncertain


def _extract_date_from_body(text: str) -> Optional[str]:
    """Try to extract a transaction date from the email body."""
    for pattern in _DATE_BODY_PATTERNS:
        match = pattern.search(text)
        if match:
            parsed = _parse_email_date(match.group(1))
            if parsed:
                return parsed
    return None


def _extract_description(text: str, subject: str) -> str:
    """Build a meaningful description from the email content."""
    desc_part = ""

    # Try each description pattern (first match wins)
    for pattern in _DESCRIPTION_PATTERNS:
        match = pattern.search(text)
        if match:
            desc = match.group(1).strip()
            desc = re.sub(r"\s+", " ", desc)
            desc = desc.rstrip(".,;")
            # Strip leaked balance / boilerplate from description
            desc = re.sub(
                r"\s*(?:Avl|Available|Your current)\s+(?:Bal|Balance|Credit).*$",
                "", desc, flags=re.IGNORECASE,
            ).strip()
            desc = re.sub(
                r"\s*(?:The Available|If this|If not).*$",
                "", desc, flags=re.IGNORECASE,
            ).strip()
            # Strip trailing incomplete words leaked from terminators
            desc = re.sub(r"[.\s]+(?:The|In|To|For|If|Please|Your)\s*$", "", desc, flags=re.IGNORECASE).strip()
            desc = desc.rstrip(".,;")
            if len(desc) >= 2:
                desc_part = desc
                break

    # Add card/account info if found
    card_match = _CARD_PATTERN.search(text)
    card_part = f"(A/c: xx{card_match.group(1)})" if card_match else ""

    if desc_part and card_part:
        return f"{desc_part} {card_part}"
    if desc_part:
        return desc_part
    if card_part:
        # No description found but have card info; use subject for context
        subject_hint = re.sub(
            r"(?:alert|update|transaction|your|bank|a/c|account|axis|hdfc|icici|sbi)[:\s]*",
            "", subject, flags=re.IGNORECASE,
        ).strip()
        short = subject_hint[:60].strip() if subject_hint else "Transaction"
        return f"{short} {card_part}"

    # Fallback: use cleaned subject
    cleaned_subject = re.sub(
        r"(?:alert|update|transaction|your|bank|a/c|account)[:\s]*",
        "",
        subject,
        flags=re.IGNORECASE,
    ).strip()

    return cleaned_subject[:80] if cleaned_subject else "Email transaction"


# ---------------------------------------------------------------------------
# Public high-level API
# ---------------------------------------------------------------------------

def fetch_transactions_from_email(
    host: str,
    port: int,
    email_address: str,
    password: str,
    month: int,
    year: int,
    folder: str = "INBOX",
    on_progress: Optional[callable] = None,
    is_cancelled: Optional[callable] = None,
) -> list[dict]:
    """Complete pipeline: connect, fetch, parse, and return transactions.

    Raises EmailConnectionError on connection/auth failure.
    Raises FetchCancelledError if user cancels mid-fetch.
    """
    mail = connect_imap(host, port, email_address, password)
    try:
        transactions = fetch_transaction_emails(
            mail, month, year, folder,
            on_progress=on_progress,
            is_cancelled=is_cancelled,
        )
    finally:
        disconnect_imap(mail)

    # Deduplicate by date + amount + type (emails can sometimes repeat)
    seen = set()
    unique = []
    for txn in transactions:
        key = f"{txn['date']}|{txn['amount']:.2f}|{txn['type']}|{txn['description'][:30]}"
        if key not in seen:
            seen.add(key)
            unique.append(txn)

    return unique
