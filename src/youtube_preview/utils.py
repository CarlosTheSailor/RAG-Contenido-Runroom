from __future__ import annotations

import re
from urllib.parse import urlparse
from urllib.parse import parse_qs

TIME_RE = re.compile(r"^(?P<mm>\d{1,2}):(?P<ss>\d{2})(?::(?P<ff>\d{2}))?$")
TS_LINE_RE = re.compile(r"(?P<time>\b\d{1,2}:\d{2}(?::\d{2})?\b)\s*[-–—]\s*(?P<label>.+)")
URL_RE = re.compile(r"https?://[^\s)\]>]+", re.IGNORECASE)
YT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{6,20}$")


def parse_timestamp_to_seconds(value: str) -> int | None:
    raw = value.strip()
    match = TIME_RE.match(raw)
    if not match:
        return None

    part_1 = int(match.group("mm"))
    part_2 = int(match.group("ss"))
    part_3 = match.group("ff")

    if part_3 is None:
        return (part_1 * 60) + part_2
    return (part_1 * 3600) + (part_2 * 60) + int(part_3)


def format_seconds_as_timestamp(seconds: int) -> str:
    total = max(0, int(seconds))
    hh = total // 3600
    mm = (total % 3600) // 60
    ss = total % 60
    if hh > 0:
        return f"{hh:02d}:{mm:02d}:{ss:02d}"
    return f"{mm:02d}:{ss:02d}"


def parse_timestamp_lines(text: str) -> list[tuple[str, int, str]]:
    rows: list[tuple[str, int, str]] = []
    seen_seconds: set[int] = set()
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        match = TS_LINE_RE.search(stripped)
        if not match:
            continue
        ts = match.group("time")
        sec = parse_timestamp_to_seconds(ts)
        if sec is None or sec in seen_seconds:
            continue
        label = match.group("label").strip("- ")
        rows.append((ts, sec, label))
        seen_seconds.add(sec)
    rows.sort(key=lambda item: item[1])
    return rows


def extract_urls(text: str) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()
    for match in URL_RE.finditer(text):
        url = match.group(0).rstrip(".,)")
        key = url.lower()
        if key in seen:
            continue
        seen.add(key)
        found.append(url)
    return found


def normalize_url(url: str) -> str:
    parsed = urlparse(url.strip())
    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    if not path:
        path = "/"
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{scheme}://{netloc}{path}{query}"


def slug_from_url(url: str) -> str | None:
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    path = parsed.path.strip("/")
    if not path:
        return None
    parts = path.split("/")
    return parts[-1].strip() or None


def extract_youtube_video_id(youtube_url: str | None) -> str | None:
    if not youtube_url:
        return None

    raw = youtube_url.strip()
    if not raw:
        return None

    try:
        parsed = urlparse(raw)
    except Exception:
        return None

    host = parsed.netloc.lower()
    path = parsed.path.strip("/")

    if "youtu.be" in host:
        candidate = path.split("/")[0] if path else ""
        return candidate if YT_ID_RE.match(candidate) else None

    if "youtube.com" in host:
        if path == "watch":
            query = parse_qs(parsed.query)
            candidate = (query.get("v") or [""])[0].strip()
            return candidate if YT_ID_RE.match(candidate) else None

        # Best-effort support for common URL variants.
        parts = path.split("/")
        if len(parts) >= 2 and parts[0] in {"shorts", "embed"}:
            candidate = parts[1].strip()
            return candidate if YT_ID_RE.match(candidate) else None

    return None
