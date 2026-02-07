"""
Credit-card payment deduplication.

When a user pays their credit-card bill from a bank account, the bank statement
records a debit for the full CC bill amount. If the CC statement is also uploaded,
this results in double-counting. This module detects and flags such transactions.
"""

import re

# Keywords commonly found in bank statement descriptions for CC payments
CC_PAYMENT_KEYWORDS = [
    "credit card",
    "cc payment",
    "card payment",
    "credit card payment",
    "cc bill",
    "card bill",
    "credit card bill",
    "cred",
    "visa bill",
    "mastercard bill",
    "amex bill",
    "rupay bill",
    "hdfc card",
    "icici card",
    "sbi card",
    "axis card",
    "kotak card",
    "citi card",
    "rbl card",
    "au card",
    "indusind card",
    "yes card",
    "bob card",
    "onecard",
    "slice",
    "simpl",
    "lazypay",
    "uni card",
    "fi card",
    "jupiter card",
    "navi card",
]

# Regex pattern: 4 or more consecutive digits that could be last digits of a card number
CARD_SUFFIX_PATTERN = re.compile(r"\b\d{4,6}\b")


def detect_cc_payments(transactions: list[dict]) -> list[int]:
    """Identify transactions that look like credit-card bill payments.

    Args:
        transactions: list of transaction dicts (must have 'id', 'description',
                      'type', and 'source' keys).

    Returns:
        List of transaction IDs that are likely CC payments.
    """
    flagged_ids = []

    for txn in transactions:
        # Only flag debits from bank statements
        if txn.get("source") != "bank" or txn.get("type") != "debit":
            continue

        desc = txn.get("description", "").lower()

        if _matches_cc_keywords(desc):
            flagged_ids.append(txn["id"])

    return flagged_ids


def _matches_cc_keywords(description: str) -> bool:
    """Check if a description matches any known CC payment keyword."""
    for keyword in CC_PAYMENT_KEYWORDS:
        if keyword in description:
            return True
    return False


def suggest_cc_payment_matches(
    bank_transactions: list[dict],
    cc_total: float,
    tolerance: float = 10.0,
) -> list[int]:
    """Find bank debits whose amount closely matches a CC statement total.

    Useful when keyword matching misses a CC payment but the amount matches.

    Args:
        bank_transactions: bank-source debit transactions.
        cc_total: total amount of the CC statement being compared.
        tolerance: allowed difference in amount.

    Returns:
        List of transaction IDs within tolerance of the CC total.
    """
    matches = []
    for txn in bank_transactions:
        if txn.get("type") != "debit":
            continue
        diff = abs(txn["amount"] - cc_total)
        if diff <= tolerance:
            matches.append(txn["id"])
    return matches
