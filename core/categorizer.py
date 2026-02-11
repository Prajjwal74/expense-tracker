"""
Three-layer transaction categorisation:

Layer 1: Rule-based matching from user corrections (instant, 100% accurate)
Layer 2: LLM with enriched context (few-shot examples, email body, metadata)

Uses a local Ollama instance. No API key required.
"""

import json
import logging
import os
from typing import Optional

import requests

from core.database import (
    apply_rules_to_transactions,
    bulk_update_categories,
    get_categorized_examples,
    get_all_rules,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 15  # smaller batches = more attention per transaction
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2")


# ---------------------------------------------------------------------------
# Ollama helpers (unchanged)
# ---------------------------------------------------------------------------

def _check_ollama_running() -> bool:
    """Return True if the Ollama server is reachable."""
    try:
        resp = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
        return resp.status_code == 200
    except requests.ConnectionError:
        return False


def _get_available_models() -> list[str]:
    try:
        resp = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
        if resp.status_code == 200:
            return [m["name"] for m in resp.json().get("models", [])]
    except Exception:
        pass
    return []


def _call_ollama(prompt: str, model: Optional[str] = None) -> Optional[str]:
    """Send a prompt to Ollama and return the response text."""
    model = model or OLLAMA_MODEL
    try:
        resp = requests.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.1,
                    "num_predict": 2048,
                },
            },
            timeout=120,
        )
        if resp.status_code != 200:
            logger.warning("Ollama returned status %d: %s", resp.status_code, resp.text[:200])
            return None
        return resp.json().get("response", "").strip()
    except requests.ConnectionError:
        logger.error("Cannot connect to Ollama at %s", OLLAMA_BASE_URL)
        return None
    except requests.Timeout:
        logger.warning("Ollama request timed out")
        return None
    except Exception as e:
        logger.warning("Ollama error: %s", e)
        return None


def _parse_json_response(text: str) -> Optional[dict]:
    """Extract and parse JSON from the model response."""
    if not text:
        return None
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[-1] if "\n" in cleaned else cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned.rsplit("```", 1)[0]
    cleaned = cleaned.strip()

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(cleaned[start : end + 1])
        except json.JSONDecodeError:
            pass
    logger.warning("Could not parse JSON from response: %s", cleaned[:200])
    return None


# ---------------------------------------------------------------------------
# Enriched prompt builder (Layer 2)
# ---------------------------------------------------------------------------

def _build_enriched_prompt(
    batch: list[dict],
    categories: list[str],
    examples: list[dict],
    rules: list[dict],
) -> str:
    """Build a rich categorisation prompt with Indian context and few-shot examples."""
    cat_list = ", ".join(categories)

    # Few-shot examples from user's own history + rules
    example_lines = []
    seen_examples = set()

    # Rules first (highest confidence)
    for rule in rules[:15]:
        key = f"{rule['keyword']}|{rule['category']}"
        if key not in seen_examples:
            example_lines.append(f'  - "{rule["keyword"]}" -> {rule["category"]}')
            seen_examples.add(key)

    # Then past categorized transactions (for diversity)
    for ex in examples:
        key = f"{ex['description'][:40]}|{ex['category']}"
        if key not in seen_examples and len(example_lines) < 25:
            example_lines.append(
                f'  - "{ex["description"][:60]}" -> {ex["category"]}'
            )
            seen_examples.add(key)

    examples_block = "\n".join(example_lines) if example_lines else "  (no examples yet)"

    # Transaction lines with metadata
    txn_lines = []
    for i, t in enumerate(batch):
        amt = t.get("amount", 0)
        txn_type = t.get("type", "debit").upper()
        date = t.get("date", "")
        desc = t["description"]

        line = f'  {i}: [{txn_type} Rs {amt:,.0f}, {date}] "{desc}"'

        # Add email body context if available (truncated for prompt size)
        email_body = t.get("email_body") or ""
        if email_body:
            # Extract the most useful sentence from the email
            clean_body = email_body[:300].replace("\n", " ").strip()
            line += f'\n     Email: "{clean_body}"'

        txn_lines.append(line)

    txn_block = "\n".join(txn_lines)

    return f"""You are a personal finance assistant for an Indian user. Categorise each transaction into exactly ONE category.

Categories: [{cat_list}]

Indian banking context:
- UPI P2M = payment to merchant (shop/restaurant/service)
- UPI P2A = payment to a person (could be rent, food, services, or transfer)
- ACH-DR = automated debit (EMI, insurance, SIP investment, subscription)
- NEFT/RTGS/IMPS = bank transfers (often rent, salary, or self-transfer)
- ECOM PUR = online purchase (shopping)
- CRED/CRED Club = credit card bill payment
- Swiggy/Zomato = Food delivery
- BookMyShow = Entertainment
- Common Indian merchant keywords: Bigbasket/Blinkit/Zepto = Groceries, Ola/Uber/Rapido = Travel

How THIS USER categorises (learn from these):
{examples_block}

Transactions to categorise:
{txn_block}

Rules:
- Return ONLY a valid JSON object mapping index (as string) to category.
- Use the user's past patterns above as strong guidance.
- If a person's name appears (UPI P2A), look at the amount for clues:
  small amounts (Rs 50-500) to the same person = likely Food/Services,
  large amounts (Rs 5000+) = likely Rent/Transfer.
- If unsure, use "Other".
- NO explanation, NO markdown, ONLY JSON.

Example output: {{"0": "Food", "1": "Shopping", "2": "Rent"}}
"""


# ---------------------------------------------------------------------------
# Main categorisation flow (3-layer)
# ---------------------------------------------------------------------------

def categorize_transactions(
    transactions: list[dict],
    categories: list[str],
    model_name: Optional[str] = None,
) -> dict[int, str]:
    """Categorise transactions using a 3-layer approach:

    Layer 1: Apply keyword rules from user corrections (instant, exact).
    Layer 2: LLM with enriched context for remaining transactions.

    Args:
        transactions: list of dicts with 'id', 'description', and optionally
                      'amount', 'type', 'date', 'email_body'.
        categories: allowed category names.

    Returns:
        dict mapping transaction id -> category string.
    """
    if not transactions:
        return {}

    all_results: dict[int, str] = {}

    # --- Layer 1: Rule-based matching ---
    rule_matches = apply_rules_to_transactions(transactions)
    if rule_matches:
        all_results.update(rule_matches)
        logger.info("Layer 1 (rules): matched %d / %d transactions", len(rule_matches), len(transactions))

    # --- Layer 2: LLM for remaining ---
    remaining = [t for t in transactions if t["id"] not in all_results]

    if not remaining:
        return all_results

    if not _check_ollama_running():
        raise RuntimeError(
            f"Ollama is not running at {OLLAMA_BASE_URL}. Start it with: ollama serve"
        )

    model = model_name or OLLAMA_MODEL
    available = _get_available_models()
    if not any(model in m for m in available) and available:
        logger.info("Model '%s' not found. Available: %s", model, available)

    # Fetch few-shot examples and rules for the prompt
    examples = get_categorized_examples(limit=30)
    rules = get_all_rules()

    for start in range(0, len(remaining), BATCH_SIZE):
        batch = remaining[start : start + BATCH_SIZE]
        ids = [t["id"] for t in batch]

        prompt = _build_enriched_prompt(batch, categories, examples, rules)
        response_text = _call_ollama(prompt, model)
        mapping = _parse_json_response(response_text)

        if mapping:
            for idx_str, category in mapping.items():
                try:
                    idx = int(idx_str)
                except (ValueError, TypeError):
                    continue
                if 0 <= idx < len(batch):
                    if category in categories:
                        all_results[ids[idx]] = category
                    else:
                        all_results[ids[idx]] = "Other"
        else:
            logger.warning("No valid mapping for batch starting at %d", start)

    logger.info(
        "Categorization complete: %d rule-matched, %d LLM-categorized, %d total",
        len(rule_matches), len(all_results) - len(rule_matches), len(all_results),
    )
    return all_results


def categorize_single(
    description: str,
    categories: list[str],
    model_name: Optional[str] = None,
) -> Optional[str]:
    """Categorise a single transaction description. Returns category or None."""
    if not _check_ollama_running():
        return None

    examples = get_categorized_examples(limit=15)
    rules = get_all_rules()
    txn = {"id": -1, "description": description}
    prompt = _build_enriched_prompt([txn], categories, examples, rules)
    response_text = _call_ollama(prompt, model_name)
    mapping = _parse_json_response(response_text)

    if mapping:
        category = mapping.get("0")
        if category in categories:
            return category
        return "Other"
    return None
