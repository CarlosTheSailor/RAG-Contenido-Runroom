from __future__ import annotations

import json
from typing import Any


def parse_json_payload(raw: str) -> Any:
    clean = strip_code_fences(raw)

    parsed = _safe_json_parse(clean)
    if parsed is not None:
        return parsed

    obj = _extract_between(clean, "{", "}")
    if obj:
        parsed = _safe_json_parse(obj)
        if parsed is not None:
            return parsed

    arr = _extract_between(clean, "[", "]")
    if arr:
        parsed = _safe_json_parse(arr)
        if parsed is not None:
            return parsed

    raise ValueError("No se pudo parsear JSON del modelo.")


def strip_code_fences(value: str) -> str:
    text = (value or "").strip()
    if text.startswith("```"):
        text = text.replace("```json", "").replace("```JSON", "").replace("```", "")
    return text.strip()


def normalize_references(rows: Any) -> list[dict[str, str]]:
    entries = rows if isinstance(rows, list) else []
    normalized: list[dict[str, str]] = []
    seen_source: dict[str, dict[str, str]] = {}
    seen_url: set[str] = set()

    for item in entries:
        source = _as_text((item or {}).get("fuente")).strip() if isinstance(item, dict) else ""
        url = _as_text((item or {}).get("url") if isinstance(item, dict) else "").strip()
        newsletter = _as_text((item or {}).get("newsletter_origen") if isinstance(item, dict) else "").strip()

        if not source and not url:
            continue
        if not newsletter:
            newsletter = "Newsletter no identificada"

        payload = {
            "fuente": source,
            "url": url if _is_valid_url(url) else "",
            "newsletter_origen": newsletter,
        }

        source_key = source.lower()
        if source_key:
            prev = seen_source.get(source_key)
            if prev is None or (not prev.get("url") and payload["url"]):
                seen_source[source_key] = payload
        else:
            normalized.append(payload)

    for payload in seen_source.values():
        url_key = payload["url"].lower()
        if url_key:
            if url_key in seen_url:
                continue
            seen_url.add(url_key)
        normalized.append(payload)

    return normalized


def _safe_json_parse(raw: str) -> Any | None:
    try:
        return json.loads(raw)
    except Exception:
        return None


def _extract_between(text: str, left: str, right: str) -> str | None:
    start = text.find(left)
    end = text.rfind(right)
    if start == -1 or end == -1 or end <= start:
        return None
    return text[start : end + 1]


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _is_valid_url(value: str) -> bool:
    sample = value.strip().lower()
    return sample.startswith("http://") or sample.startswith("https://")
