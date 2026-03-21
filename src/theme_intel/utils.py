from __future__ import annotations

import re
import unicodedata

_URL_RE = re.compile(r"https?://[^\s)'\">]+", re.IGNORECASE)
_GREETING_RE = re.compile(r"^(hi|hello|hola|buenos dias|buenas)\b", re.IGNORECASE)

_HTML_FALLBACK_LINE_FRAGMENTS = (
    "however, your email software can't display html emails",
    "however, your email software cannot display html emails",
    "sin embargo, tu software de correo no puede desplegar correos en formato html",
    "you can view the newsletter by clicking here",
    "you can view this email here",
    "puedes ver este correo aqui",
    "puedes ver este correo aquí",
    "you have received a newsletter from",
    "has recibido un correo de",
    "you're receiving this newsletter because",
    "you are receiving this newsletter because",
    "estas recibiendo este correo porque",
    "estás recibiendo este correo porque",
    "you have shown interest in",
    "mostraste interes en",
    "mostraste interés en",
)

_LOW_SIGNAL_LINE_FRAGMENTS = (
    "if you've been forwarded",
    "if you have been forwarded",
    "why not subscribe",
    "weekly newsletter",
    "view in browser",
    "view this email in your browser",
    "read this email in your browser",
    "manage your preferences",
    "you are receiving this email",
    *_HTML_FALLBACK_LINE_FRAGMENTS,
)

_LOW_SIGNAL_THEME_FRAGMENTS = (
    "if you've been forwarded",
    "if you have been forwarded",
    "weekly newsletter",
    "hi carlos",
    "hello carlos",
    "newsletter",
    "subscribe",
    "view in browser",
)


def to_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "\n".join(to_text(item) for item in value)
    return str(value)


def clean_newsletter_text(text: str) -> str:
    lines = to_text(text).splitlines()
    blocked = (
        "unsubscribe",
        "update your preferences",
        "copyright",
        "view in browser",
        "mailing address",
        "manage preferences",
        "share this email",
    )

    cleaned: list[str] = []
    for line in lines:
        sample = line.strip()
        if not sample:
            continue
        low = sample.lower()
        if any(token in low for token in blocked):
            continue
        if any(token in low for token in _LOW_SIGNAL_LINE_FRAGMENTS):
            continue
        if _GREETING_RE.match(sample) and len(sample.split()) <= 4:
            continue
        if sample.lower().startswith("subject:"):
            continue
        if sample.lower().startswith("from:"):
            continue
        if sample.lower().startswith("to:"):
            continue
        if sample.lower().startswith("sent:"):
            continue
        if re.fullmatch(r"https?://[^\s]+", sample, flags=re.IGNORECASE):
            continue
        cleaned.append(sample)

    joined = "\n".join(cleaned)
    joined = re.sub(r"[ \t]{2,}", " ", joined)
    joined = re.sub(r"\n{3,}", "\n\n", joined)
    return joined.strip()


def looks_like_html_fallback_text(text: str) -> bool:
    normalized = _normalize_noise_text(text)
    if not normalized:
        return False

    if "display html" in normalized or "formato html" in normalized:
        return True

    markers = sum(1 for fragment in _HTML_FALLBACK_LINE_FRAGMENTS if fragment in normalized)
    if markers >= 2 and len(normalized) < 1200:
        return True
    return False


def extract_links(text: str) -> list[str]:
    seen: set[str] = set()
    links: list[str] = []
    for match in _URL_RE.findall(to_text(text)):
        url = match.strip().rstrip(".,;\"'<>")
        lower = url.lower()
        if lower in seen:
            continue
        seen.add(lower)
        links.append(url)
    return links


def normalize_tag(value: str) -> str:
    text = value.strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text


def pretty_tag(tag_key: str) -> str:
    if not tag_key:
        return ""
    return tag_key.replace("-", " ")


def is_low_signal_theme_text(value: str) -> bool:
    text = to_text(value).strip()
    if not text:
        return True

    normalized = text.lower()
    if len(normalized) < 20:
        return True
    if _GREETING_RE.match(normalized):
        return True
    if any(fragment in normalized for fragment in _LOW_SIGNAL_THEME_FRAGMENTS):
        return True

    alpha_count = sum(1 for ch in normalized if ch.isalpha())
    if alpha_count < 12:
        return True
    return False


def _normalize_noise_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", to_text(text).lower())
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()
