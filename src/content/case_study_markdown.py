from __future__ import annotations

import html
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from src.content.models import CanonicalContentItem, CanonicalDocument, CanonicalSection
from src.content.taxonomy import canonical_section_key, extract_bullet_items, normalize_values
from src.pipeline.normalization import estimate_tokens, normalize_text, slugify, strip_accents

_CASE_HEADER_RE = re.compile(r"^##\s+Case Study\s*(?:\\#|#)?\s*(?P<num>\d+)\s*:\s*(?P<title>.+?)\s*$", re.IGNORECASE)
_HEADING_RE = re.compile(r"^(?P<hash>#{1,6})\s+(?P<title>.+?)\s*$")
_LABEL_RE = re.compile(r"^\*\*(?P<label>[^*]+):\*\*\s*(?P<value>.*)$")
_SEPARATOR_RE = re.compile(r"^\s*(?:-{3,}|=+)\s*$")
_LINK_RE = re.compile(r"\[[^\]]+\]\((?P<url>https?://[^)\s]+)\)")
_URL_RE = re.compile(r"https?://[^\s)]+")

_METADATA_LABELS = {
    "cliente",
    "url",
    "url original",
    "fecha de extraccion",
    "fecha de extracción",
    "fuente",
}

@dataclass
class _CaseBlock:
    number: int
    title_hint: str
    start_idx: int
    end_idx: int


def parse_case_studies_markdown(path: Path) -> list[CanonicalDocument]:
    raw = path.read_text(encoding="utf-8", errors="ignore")
    lines = raw.replace("\r\n", "\n").replace("\r", "\n").split("\n")

    extracted_at = _extract_global_date(lines)
    source_url = _extract_global_source(lines)

    blocks = _find_case_blocks(lines)
    docs: list[CanonicalDocument] = []
    for block in blocks:
        docs.append(_parse_case_block(lines, block, extracted_at=extracted_at, source_url=source_url))
    return docs


def _find_case_blocks(lines: list[str]) -> list[_CaseBlock]:
    starts: list[_CaseBlock] = []
    for idx, line in enumerate(lines):
        match = _CASE_HEADER_RE.match(line.strip())
        if not match:
            continue

        starts.append(
            _CaseBlock(
                number=int(match.group("num")),
                title_hint=_clean_inline(match.group("title")),
                start_idx=idx,
                end_idx=len(lines),
            )
        )

    for i in range(len(starts) - 1):
        starts[i].end_idx = starts[i + 1].start_idx
    return starts


def _parse_case_block(
    lines: list[str],
    block: _CaseBlock,
    extracted_at: datetime | None,
    source_url: str | None,
) -> CanonicalDocument:
    block_lines = lines[block.start_idx : block.end_idx]

    labels: dict[str, str] = {}
    for raw_line in block_lines:
        line = raw_line.strip()
        label_match = _LABEL_RE.match(line)
        if not label_match:
            continue
        label = _normalize_key(label_match.group("label"))
        value = _clean_inline(label_match.group("value"))
        if value:
            labels[label] = value

    title = _extract_h1_title(block_lines) or block.title_hint
    title = _clean_inline(title)

    client = _extract_client(block_lines, labels)
    url = _extract_url(labels)
    slug = _slug_from_url(url) or slugify(title)
    content_key = f"case_study:runroom:{slug or f'case-{block.number:03d}'}"

    sections = _extract_sections(block_lines, block.start_idx)
    if not sections:
        sections = [
            CanonicalSection(
                section_order=0,
                section_key="description",
                section_title="Descripción",
                text=title,
                token_count=estimate_tokens(title),
                metadata={},
                source_locator={"line_start": block.start_idx + 1, "line_end": block.end_idx},
            )
        ]

    raw_text = "\n\n".join(section.text for section in sections if section.text)

    technologies = normalize_values(
        item
        for section in sections
        if section.section_key == "technologies"
        for item in extract_bullet_items(section.text)
    )
    areas = normalize_values(
        item
        for section in sections
        if section.section_key == "areas"
        for item in extract_bullet_items(section.text)
    )

    item = CanonicalContentItem(
        content_key=content_key,
        content_type="case_study",
        title=title,
        slug=slug,
        url=url,
        source="runroom_case_studies_markdown",
        language="es",
        status="active",
        extracted_at=extracted_at,
        metadata={
            "client": client,
            "content_type": "case_study",
            "source": "runroom_case_studies_markdown",
            "case_number": block.number,
            "original_url": url,
            "extraction_source": source_url,
            "tags": [],
            "themes": [],
        },
        custom_metadata={
            "technologies": technologies,
            "areas": areas,
        },
        raw_text=raw_text,
    )

    return CanonicalDocument(item=item, sections=sections)


def _extract_sections(block_lines: list[str], start_offset: int) -> list[CanonicalSection]:
    sections: list[CanonicalSection] = []
    current_title: str | None = None
    current_lines: list[str] = []
    current_line_start = start_offset + 1

    def flush(line_end: int) -> None:
        nonlocal current_lines, current_title, current_line_start
        text = _cleanup_text_lines(current_lines)
        current_lines = []
        if not text:
            return

        section_key = canonical_section_key(current_title)
        if current_title and _normalize_key(current_title) in _METADATA_LABELS:
            return

        section_order = len(sections)
        sections.append(
            CanonicalSection(
                section_order=section_order,
                section_key=section_key,
                section_title=current_title,
                text=text,
                token_count=estimate_tokens(text),
                metadata={"section_key": section_key, "section_title": current_title},
                source_locator={
                    "line_start": current_line_start,
                    "line_end": line_end,
                },
            )
        )

    for rel_idx, raw_line in enumerate(block_lines):
        abs_line = start_offset + rel_idx + 1
        stripped = raw_line.strip()

        if not stripped or _SEPARATOR_RE.match(stripped):
            current_lines.append("")
            continue

        heading_match = _HEADING_RE.match(stripped)
        if heading_match:
            level = len(heading_match.group("hash"))
            heading = _clean_inline(heading_match.group("title"))
            if level == 1:
                # H1 is article title, not a section boundary for chunking.
                continue
            if level >= 2:
                flush(abs_line - 1)
                current_title = heading
                current_line_start = abs_line + 1
                continue

        label_match = _LABEL_RE.match(stripped)
        if label_match:
            label = _normalize_key(label_match.group("label"))
            value = _clean_inline(label_match.group("value"))
            if label in _METADATA_LABELS:
                continue
            if value:
                current_lines.append(f"{label_match.group('label').strip()}: {value}")
            continue

        current_lines.append(_clean_inline(raw_line))

    flush(start_offset + len(block_lines))

    if not sections:
        body_lines = [
            _clean_inline(line)
            for line in block_lines
            if line.strip() and not _CASE_HEADER_RE.match(line.strip()) and not _SEPARATOR_RE.match(line.strip())
        ]
        fallback = _cleanup_text_lines(body_lines)
        if fallback:
            sections.append(
                CanonicalSection(
                    section_order=0,
                    section_key="description",
                    section_title="Descripción",
                    text=fallback,
                    token_count=estimate_tokens(fallback),
                    metadata={"section_key": "description", "section_title": "Descripción"},
                    source_locator={"line_start": start_offset + 1, "line_end": start_offset + len(block_lines)},
                )
            )

    return sections


def _extract_h1_title(block_lines: list[str]) -> str | None:
    for line in block_lines:
        stripped = line.strip()
        match = _HEADING_RE.match(stripped)
        if not match:
            continue
        if len(match.group("hash")) == 1:
            return _clean_inline(match.group("title"))
    return None


def _extract_client(block_lines: list[str], labels: dict[str, str]) -> str | None:
    if "cliente" in labels:
        return _clean_inline(labels["cliente"])

    for idx, line in enumerate(block_lines):
        if _normalize_key(line.strip().lstrip("# ")) != "cliente":
            continue
        for candidate in block_lines[idx + 1 : idx + 5]:
            value = _clean_inline(candidate)
            if value:
                return value
    return None


def _extract_url(labels: dict[str, str]) -> str | None:
    for key in ("url original", "url"):
        if key in labels:
            parsed = _extract_first_url(labels[key])
            if parsed:
                return parsed
    return None


def _extract_first_url(value: str) -> str | None:
    m = _LINK_RE.search(value)
    if m:
        return m.group("url")
    m2 = _URL_RE.search(value)
    if m2:
        return m2.group(0)
    return None


def _extract_global_date(lines: list[str]) -> datetime | None:
    month_map = {
        "enero": 1,
        "febrero": 2,
        "marzo": 3,
        "abril": 4,
        "mayo": 5,
        "junio": 6,
        "julio": 7,
        "agosto": 8,
        "septiembre": 9,
        "setiembre": 9,
        "octubre": 10,
        "noviembre": 11,
        "diciembre": 12,
    }
    for line in lines[:40]:
        norm = _normalize_key(line)
        if "fecha de extraccion" not in norm and "fecha de extracción" not in norm:
            continue
        date_match = re.search(r"(\d{1,2})\s+de\s+([a-záéíóúñ]+)\s+de\s+(\d{4})", strip_accents(line.lower()))
        if not date_match:
            continue
        day = int(date_match.group(1))
        month_name = date_match.group(2)
        year = int(date_match.group(3))
        month = month_map.get(month_name)
        if not month:
            continue
        return datetime(year, month, day, tzinfo=timezone.utc)
    return None


def _extract_global_source(lines: list[str]) -> str | None:
    for line in lines[:60]:
        if "fuente" not in _normalize_key(line):
            continue
        parsed = _extract_first_url(line)
        if parsed:
            return parsed
    return None


def _slug_from_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        return None
    return parts[-1] or None


def _normalize_key(text: str) -> str:
    cleaned = strip_accents(text).lower().strip()
    cleaned = cleaned.replace("\\", "")
    cleaned = re.sub(r"[^a-z0-9\s]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _clean_inline(text: str) -> str:
    text = html.unescape(text)
    text = text.replace("\\", "")
    text = text.replace("**", "")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _cleanup_text_lines(lines: list[str]) -> str:
    payload = "\n".join(line for line in lines)
    payload = re.sub(r"\n{3,}", "\n\n", payload)
    payload = payload.strip()
    return normalize_text(payload)
