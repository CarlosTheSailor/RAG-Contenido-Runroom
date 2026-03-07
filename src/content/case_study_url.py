from __future__ import annotations

import html
import json
import re
import urllib.request
from datetime import datetime, timezone
from urllib.parse import urlparse

from src.content.models import CanonicalContentItem, CanonicalDocument, CanonicalSection
from src.content.taxonomy import canonical_section_key
from src.pipeline.normalization import estimate_tokens, normalize_text, slugify

_META_RE = re.compile(
    r"<meta[^>]+(?:property|name)=['\"](?P<name>[^'\"]+)['\"][^>]*content=['\"](?P<content>[^'\"]+)['\"]",
    re.IGNORECASE,
)
_H1_RE = re.compile(r"<h1\b[^>]*>(?P<content>.*?)</h1>", re.IGNORECASE | re.DOTALL)
_TAG_RE = re.compile(r"<[^>]+>")
_JSON_LD_RE = re.compile(r"<script[^>]*type=['\"]application/ld\+json['\"][^>]*>(?P<body>.*?)</script>", re.IGNORECASE | re.DOTALL)
_TEXT_TOKEN_RE = re.compile(r"(?is)<(?P<tag>h1|h2|h3|p|li|blockquote)\b[^>]*>(?P<body>.*?)</(?P=tag)>")
_CLIENT_RE = re.compile(r"^cliente\s*:?\s*(.+)$", re.IGNORECASE)


def parse_case_study_url(url: str) -> CanonicalDocument:
    html_text = fetch_url_html(url)
    metadata = _extract_meta(html_text)

    title = metadata.get("title") or _extract_first_h1(html_text) or _slug_title(url)
    title = _clean_inline(title)

    slug = _slug_from_url(url) or slugify(title)
    sections = _extract_sections(html_text)

    client = _extract_client_from_sections(sections)
    description = metadata.get("description") or ""
    if description and not sections:
        sections = [
            CanonicalSection(
                section_order=0,
                section_key="description",
                section_title="Descripción",
                text=normalize_text(description),
                token_count=estimate_tokens(description),
                metadata={"section_key": "description", "section_title": "Descripción"},
                source_locator={"source": "meta_description"},
            )
        ]

    if not sections:
        fallback = title
        sections = [
            CanonicalSection(
                section_order=0,
                section_key="description",
                section_title="Descripción",
                text=fallback,
                token_count=estimate_tokens(fallback),
                metadata={"section_key": "description", "section_title": "Descripción"},
                source_locator={"source": "fallback"},
            )
        ]

    raw_text = "\n\n".join(s.text for s in sections)

    item = CanonicalContentItem(
        content_key=f"case_study:runroom:{slug}",
        content_type="case_study",
        title=title,
        slug=slug,
        url=url,
        source="runroom_case_study_url",
        language=metadata.get("language") or "es",
        status="active",
        published_at=metadata.get("published_at"),
        extracted_at=datetime.now(timezone.utc),
        metadata={
            "content_type": "case_study",
            "source": "runroom_case_study_url",
            "client": client,
            "description": description,
            "original_url": url,
            "tags": [],
            "themes": [],
        },
        custom_metadata={
            "technologies": [],
            "areas": [],
        },
        raw_text=raw_text,
    )

    return CanonicalDocument(item=item, sections=sections)


def fetch_url_html(url: str, timeout: int = 30) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "runroom-content-layer/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def _extract_sections(html_text: str) -> list[CanonicalSection]:
    scope = _extract_scope(html_text)
    sections: list[CanonicalSection] = []

    current_title: str | None = None
    current_lines: list[str] = []

    def flush() -> None:
        nonlocal current_lines
        text = normalize_text("\n".join(current_lines)).strip()
        current_lines = []
        if not text:
            return
        key = canonical_section_key(current_title)
        sections.append(
            CanonicalSection(
                section_order=len(sections),
                section_key=key,
                section_title=current_title,
                text=text,
                token_count=estimate_tokens(text),
                metadata={"section_key": key, "section_title": current_title},
                source_locator={"source": "html"},
            )
        )

    for match in _TEXT_TOKEN_RE.finditer(scope):
        tag = match.group("tag").lower()
        text = _clean_inline(match.group("body"))
        if not text:
            continue

        if tag in {"h1", "h2", "h3"}:
            if tag == "h1" and not current_title:
                # avoid duplicating page title as section title
                continue
            flush()
            current_title = text
            continue

        if _CLIENT_RE.match(text):
            continue

        current_lines.append(text)

    flush()

    # If no heading was found but text exists, keep as description.
    if not sections:
        plain_text = _clean_inline(_strip_tags(scope))
        if plain_text:
            sections.append(
                CanonicalSection(
                    section_order=0,
                    section_key="description",
                    section_title="Descripción",
                    text=plain_text,
                    token_count=estimate_tokens(plain_text),
                    metadata={"section_key": "description", "section_title": "Descripción"},
                    source_locator={"source": "html_plain"},
                )
            )

    return sections


def _extract_scope(html_text: str) -> str:
    for tag in ("article", "main", "body"):
        match = re.search(rf"<{tag}\\b[^>]*>(?P<body>.*?)</{tag}>", html_text, re.IGNORECASE | re.DOTALL)
        if match:
            return _remove_non_content_tags(match.group("body"))
    return _remove_non_content_tags(html_text)


def _remove_non_content_tags(html_text: str) -> str:
    cleaned = re.sub(r"<script\b[^>]*>.*?</script>", " ", html_text, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"<style\b[^>]*>.*?</style>", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    return cleaned


def _extract_meta(html_text: str) -> dict[str, object]:
    meta: dict[str, str] = {}
    for match in _META_RE.finditer(html_text):
        key = match.group("name").lower().strip()
        val = _clean_inline(match.group("content"))
        if val:
            meta[key] = val

    data: dict[str, object] = {
        "title": meta.get("og:title") or meta.get("twitter:title"),
        "description": meta.get("og:description") or meta.get("description") or "",
        "language": _infer_lang(meta),
    }

    published = _extract_published_at(html_text)
    if published is not None:
        data["published_at"] = published

    return data


def _extract_published_at(html_text: str) -> datetime | None:
    for match in _JSON_LD_RE.finditer(html_text):
        body = html.unescape(match.group("body")).strip()
        if not body:
            continue

        candidates: list[dict] = []
        try:
            parsed = json.loads(body)
            if isinstance(parsed, dict):
                candidates = [parsed]
            elif isinstance(parsed, list):
                candidates = [item for item in parsed if isinstance(item, dict)]
        except json.JSONDecodeError:
            continue

        for node in candidates:
            for key in ("datePublished", "dateCreated"):
                value = node.get(key)
                if not isinstance(value, str):
                    continue
                dt = _parse_iso_datetime(value)
                if dt:
                    return dt
    return None


def _parse_iso_datetime(value: str) -> datetime | None:
    v = value.strip()
    if not v:
        return None
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _extract_client_from_sections(sections: list[CanonicalSection]) -> str | None:
    for section in sections:
        if section.section_title and section.section_title.lower().strip() == "cliente":
            first = section.text.splitlines()[0].strip() if section.text else ""
            if first:
                return first
        for line in section.text.splitlines():
            m = _CLIENT_RE.match(line.strip())
            if m:
                return _clean_inline(m.group(1))
    return None


def _extract_first_h1(html_text: str) -> str | None:
    m = _H1_RE.search(html_text)
    if not m:
        return None
    return _clean_inline(m.group("content"))


def _slug_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    if not path:
        return ""
    return path.split("/")[-1]


def _slug_title(url: str) -> str:
    slug = _slug_from_url(url)
    return slug.replace("-", " ").strip() or url


def _strip_tags(text: str) -> str:
    return _TAG_RE.sub(" ", text)


def _clean_inline(text: str) -> str:
    payload = html.unescape(_strip_tags(text))
    payload = payload.replace("\\", "")
    payload = re.sub(r"\s+", " ", payload)
    return payload.strip()


def _infer_lang(meta: dict[str, str]) -> str:
    locale = (meta.get("og:locale") or "").lower()
    if locale.startswith("en"):
        return "en"
    return "es"
