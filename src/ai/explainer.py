from __future__ import annotations

import json
import os
from urllib.request import Request, urlopen


def fallback_explanation(metrics: dict) -> str:
    total = metrics.get("total_records", 0)
    invalid = metrics.get("invalid_records", 0)
    quality = metrics.get("quality_percentage")
    if not total:
        return "The dataset contains no records to assess. Upload a non-empty file and run the checks again."
    headline = f"{invalid:,} of {total:,} records have one or more quality issues."
    if quality is not None:
        headline += f" The valid-record rate is {quality:.1f}%."
    counts = metrics.get("issue_counts", {})
    top = sorted(counts.items(), key=lambda item: item[1], reverse=True)[:3]
    if top:
        headline += " The most common findings are " + ", ".join(f"{name.replace('_', ' ')} ({count:,})" for name, count in top) + "."
    return headline + " Review the quarantined rows, correct source-system mappings, and rerun validation before downstream use."


def explain(metrics: dict) -> tuple[str, str]:
    """Use an optional OpenAI-compatible API; on any failure return local guidance."""
    api_key = os.getenv("LLM_API_KEY", "").strip()
    base_url = os.getenv("LLM_BASE_URL", "").strip().rstrip("/")
    model = os.getenv("LLM_MODEL", "").strip()
    fallback = fallback_explanation(metrics)
    if not (api_key and base_url and model):
        return fallback, "rule-based"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "Explain data quality aggregates in concise business language. Give safe, non-destructive remediation advice. Do not infer record-level facts."},
            {"role": "user", "content": json.dumps(metrics, ensure_ascii=False)},
        ],
        "temperature": 0.2,
    }
    request = Request(
        f"{base_url}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=20) as response:
            result = json.loads(response.read().decode("utf-8"))
        text = result["choices"][0]["message"]["content"].strip()
        return (text or fallback), "llm"
    except Exception:
        return fallback, "rule-based fallback"
