"""
LLM-based transaction categorisation using a local Ollama instance.

Ollama runs free, locally on your machine. No API key required.
Install: https://ollama.com  then run:  ollama pull llama3.2

Transactions are batched to keep prompt sizes manageable. The prompt asks
the model to return a strict JSON mapping of index -> category.
"""

import json
import logging
import os
from typing import Optional

import requests

logger = logging.getLogger(__name__)

BATCH_SIZE = 20  # slightly smaller batches for local models
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2")


def _check_ollama_running() -> bool:
    """Return True if the Ollama server is reachable."""
    try:
        resp = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
        return resp.status_code == 200
    except requests.ConnectionError:
        return False


def _get_available_models() -> list[str]:
    """Return list of model names pulled locally in Ollama."""
    try:
        resp = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            return [m["name"] for m in data.get("models", [])]
    except Exception:
        pass
    return []


def _build_prompt(descriptions: list[str], categories: list[str]) -> str:
    """Build the categorisation prompt."""
    cat_list = ", ".join(categories)
    txn_lines = "\n".join(f'  {i}: "{desc}"' for i, desc in enumerate(descriptions))

    return f"""You are a personal finance assistant. Categorise each transaction below into exactly ONE of these categories:
[{cat_list}]

Transactions:
{txn_lines}

Rules:
- Return ONLY a valid JSON object mapping the index (as string) to the chosen category.
- If you are unsure, use "Other".
- Do NOT add any explanation, markdown formatting, or extra text.
- Your entire response must be parseable JSON. Nothing else.

Example output:
{{"0": "Food", "1": "Shopping", "2": "Rent"}}
"""


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
                    "temperature": 0.1,  # low temp for consistent categorisation
                    "num_predict": 2048,
                },
            },
            timeout=120,  # local models can be slow on first call
        )

        if resp.status_code != 200:
            logger.warning("Ollama returned status %d: %s", resp.status_code, resp.text[:200])
            return None

        data = resp.json()
        return data.get("response", "").strip()

    except requests.ConnectionError:
        logger.error("Cannot connect to Ollama at %s. Is it running?", OLLAMA_BASE_URL)
        return None
    except requests.Timeout:
        logger.warning("Ollama request timed out")
        return None
    except Exception as e:
        logger.warning("Ollama error: %s", e)
        return None


def _parse_json_response(text: str) -> Optional[dict]:
    """Extract and parse JSON from the model response, handling markdown fences."""
    if not text:
        return None

    # Strip markdown code fences if present
    cleaned = text.strip()
    if cleaned.startswith("```"):
        # Remove opening fence (possibly with language tag)
        cleaned = cleaned.split("\n", 1)[-1] if "\n" in cleaned else cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned.rsplit("```", 1)[0]
    cleaned = cleaned.strip()

    # Try direct parse
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Try to find JSON object in the text
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(cleaned[start : end + 1])
        except json.JSONDecodeError:
            pass

    logger.warning("Could not parse JSON from response: %s", cleaned[:200])
    return None


def categorize_transactions(
    transactions: list[dict],
    categories: list[str],
    model_name: Optional[str] = None,
) -> dict[int, str]:
    """Categorise a list of transactions using a local Ollama model.

    Args:
        transactions: list of dicts with at least a 'description' key and an 'id' key.
        categories: allowed category names.
        model_name: Ollama model to use (defaults to OLLAMA_MODEL env var or llama3.2).

    Returns:
        dict mapping transaction id -> category string.

    Raises:
        RuntimeError: if Ollama is not running or the model is not available.
    """
    if not _check_ollama_running():
        raise RuntimeError(
            f"Ollama is not running at {OLLAMA_BASE_URL}. "
            "Start it with: ollama serve"
        )

    model = model_name or OLLAMA_MODEL
    available = _get_available_models()
    # Check if model (or a variant like "llama3.2:latest") is available
    model_found = any(model in m for m in available)
    if not model_found and available:
        logger.info(
            "Model '%s' not found locally. Available: %s. Attempting anyway...",
            model, available,
        )

    results: dict[int, str] = {}

    for start in range(0, len(transactions), BATCH_SIZE):
        batch = transactions[start : start + BATCH_SIZE]
        descriptions = [t["description"] for t in batch]
        ids = [t["id"] for t in batch]

        prompt = _build_prompt(descriptions, categories)
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
                        results[ids[idx]] = category
                    else:
                        results[ids[idx]] = "Other"
        else:
            logger.warning("No valid mapping for batch starting at %d", start)

    return results


def categorize_single(
    description: str,
    categories: list[str],
    model_name: Optional[str] = None,
) -> Optional[str]:
    """Categorise a single transaction description. Returns category or None."""
    if not _check_ollama_running():
        return None

    prompt = _build_prompt([description], categories)
    response_text = _call_ollama(prompt, model_name)
    mapping = _parse_json_response(response_text)

    if mapping:
        category = mapping.get("0")
        if category in categories:
            return category
        return "Other"
    return None
