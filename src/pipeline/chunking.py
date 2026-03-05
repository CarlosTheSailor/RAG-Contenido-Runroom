from __future__ import annotations

import re
from dataclasses import dataclass

from .models import TranscriptSegment
from .normalization import estimate_tokens, normalize_text

_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


@dataclass
class ChunkDraft:
    chunk_index: int
    start_ts_sec: float
    end_ts_sec: float
    speaker: str | None
    text: str
    token_count: int



def _split_sentences(text: str) -> list[str]:
    text = normalize_text(text).replace("\n", " ")
    if not text:
        return []
    pieces = _SENTENCE_SPLIT_RE.split(text)
    return [piece.strip() for piece in pieces if piece.strip()]



def _tail_overlap(text: str, overlap_tokens: int) -> str:
    if overlap_tokens <= 0:
        return ""
    words = text.split()
    if not words:
        return ""
    take_words = max(1, int(overlap_tokens * 0.75))
    tail = " ".join(words[-take_words:]).strip()
    return tail



def build_chunks(
    segments: list[TranscriptSegment],
    target_tokens: int = 220,
    overlap_tokens: int = 40,
) -> list[ChunkDraft]:
    chunks: list[ChunkDraft] = []
    current_sentences: list[str] = []
    current_speakers: list[str] = []
    current_tokens = 0
    chunk_start = 0.0
    chunk_end = 0.0
    overlap_prefix = ""

    def flush_chunk() -> None:
        nonlocal current_sentences, current_speakers, current_tokens, overlap_prefix
        if not current_sentences:
            return
        text = normalize_text(" ".join(current_sentences))
        token_count = estimate_tokens(text)
        speaker = "; ".join(current_speakers[:4]) if current_speakers else None
        chunks.append(
            ChunkDraft(
                chunk_index=len(chunks),
                start_ts_sec=chunk_start,
                end_ts_sec=chunk_end,
                speaker=speaker,
                text=text,
                token_count=token_count,
            )
        )
        overlap_prefix = _tail_overlap(text, overlap_tokens)
        current_sentences = []
        current_speakers = []
        current_tokens = 0

    for segment in segments:
        sentences = _split_sentences(segment.text)
        if not sentences:
            continue

        for sentence in sentences:
            sentence_tokens = estimate_tokens(sentence)
            if not current_sentences:
                seeded_text = overlap_prefix.strip()
                if seeded_text:
                    current_sentences.append(seeded_text)
                    current_tokens += estimate_tokens(seeded_text)
                chunk_start = segment.start_ts_sec

            projected = current_tokens + sentence_tokens
            if current_sentences and projected > target_tokens:
                flush_chunk()
                chunk_start = segment.start_ts_sec
                seeded_text = overlap_prefix.strip()
                if seeded_text:
                    current_sentences.append(seeded_text)
                    current_tokens += estimate_tokens(seeded_text)

            current_sentences.append(sentence)
            current_tokens += sentence_tokens
            chunk_end = segment.start_ts_sec

            if segment.speaker and segment.speaker not in current_speakers:
                current_speakers.append(segment.speaker)

    flush_chunk()

    if not chunks and segments:
        text = normalize_text(" ".join(seg.text for seg in segments if seg.text))
        if text:
            chunks.append(
                ChunkDraft(
                    chunk_index=0,
                    start_ts_sec=segments[0].start_ts_sec,
                    end_ts_sec=segments[-1].start_ts_sec,
                    speaker=None,
                    text=text,
                    token_count=estimate_tokens(text),
                )
            )

    return chunks
