from __future__ import annotations

import re
import unicodedata

_WHITESPACE_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^a-z0-9]+")


STOPWORDS_ES = {
    "de",
    "la",
    "el",
    "en",
    "y",
    "a",
    "que",
    "los",
    "las",
    "con",
    "por",
    "para",
    "del",
    "un",
    "una",
    "al",
    "es",
    "se",
    "lo",
    "le",
    "o",
    "como",
    "su",
    "sus",
    "si",
    "no",
}


def normalize_text(value: str) -> str:
    value = unicodedata.normalize("NFKC", value)
    value = value.replace("\u00a0", " ").replace("\u200b", "")
    value = value.replace("\u2013", "-").replace("\u2014", "-")
    value = value.replace("\u2018", "'").replace("\u2019", "'")
    value = value.replace("\u201c", '"').replace("\u201d", '"')
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    lines = []
    for line in value.split("\n"):
        cleaned = _WHITESPACE_RE.sub(" ", line).strip()
        if cleaned:
            lines.append(cleaned)
    return "\n".join(lines)


def strip_accents(value: str) -> str:
    decomposed = unicodedata.normalize("NFD", value)
    return "".join(ch for ch in decomposed if unicodedata.category(ch) != "Mn")


def normalize_for_match(value: str) -> str:
    value = strip_accents(value.lower())
    value = _PUNCT_RE.sub(" ", value)
    return _WHITESPACE_RE.sub(" ", value).strip()


def slugify(value: str) -> str:
    normalized = normalize_for_match(value)
    return normalized.replace(" ", "-")


def tokenize_for_match(value: str) -> list[str]:
    normalized = normalize_for_match(value)
    if not normalized:
        return []
    return [tok for tok in normalized.split(" ") if tok and tok not in STOPWORDS_ES]


def estimate_tokens(value: str) -> int:
    # Quick approximation: 1 token ~= 4 chars for ES/EN text.
    return max(1, int(round(len(value) / 4.0)))
