from __future__ import annotations

import re

from src.content.models import CanonicalChunk, CanonicalSection
from src.pipeline.normalization import estimate_tokens, normalize_text

_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def chunk_sections(
    sections: list[CanonicalSection],
    target_tokens: int = 240,
    overlap_tokens: int = 40,
) -> list[CanonicalChunk]:
    chunks: list[CanonicalChunk] = []
    chunk_order = 0

    for section in sections:
        for local_idx, text in enumerate(_section_chunks(section.text, target_tokens=target_tokens, overlap_tokens=overlap_tokens)):
            token_count = estimate_tokens(text)
            chunks.append(
                CanonicalChunk(
                    chunk_order=chunk_order,
                    section_order=section.section_order,
                    section_key=section.section_key,
                    section_title=section.section_title,
                    text=text,
                    token_count=token_count,
                    metadata={
                        "section_key": section.section_key,
                        "section_title": section.section_title,
                        "chunk_order": chunk_order,
                        "chunk_in_section": local_idx,
                    },
                    source_locator=section.source_locator,
                )
            )
            chunk_order += 1

    return chunks


def _section_chunks(text: str, target_tokens: int, overlap_tokens: int) -> list[str]:
    normalized = normalize_text(text).replace("\n", " ").strip()
    if not normalized:
        return []

    sentences = [s.strip() for s in _SENTENCE_SPLIT_RE.split(normalized) if s.strip()]
    if not sentences:
        return [normalized]

    chunks: list[str] = []
    current: list[str] = []
    current_tokens = 0
    overlap_prefix = ""

    def flush() -> None:
        nonlocal current, current_tokens, overlap_prefix
        if not current:
            return
        payload = normalize_text(" ".join(current)).replace("\n", " ").strip()
        if payload:
            chunks.append(payload)
            overlap_prefix = _tail_overlap(payload, overlap_tokens)
        current = []
        current_tokens = 0

    for sentence in sentences:
        sentence_tokens = estimate_tokens(sentence)
        if not current and overlap_prefix:
            current.append(overlap_prefix)
            current_tokens += estimate_tokens(overlap_prefix)

        projected = current_tokens + sentence_tokens
        if current and projected > target_tokens:
            flush()
            if overlap_prefix:
                current.append(overlap_prefix)
                current_tokens += estimate_tokens(overlap_prefix)

        current.append(sentence)
        current_tokens += sentence_tokens

    flush()

    return chunks or [normalized]


def _tail_overlap(text: str, overlap_tokens: int) -> str:
    if overlap_tokens <= 0:
        return ""

    words = text.split()
    if not words:
        return ""

    take_words = max(1, int(overlap_tokens * 0.75))
    return " ".join(words[-take_words:]).strip()
