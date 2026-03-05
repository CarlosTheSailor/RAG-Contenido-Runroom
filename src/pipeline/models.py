from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class TranscriptSegment:
    raw_timestamp: str
    start_ts_sec: float
    speaker: str | None
    text: str


@dataclass
class EpisodeInfo:
    source_filename: str
    transcript_path: str
    episode_code: str | None
    title: str
    guest_names: list[str] = field(default_factory=list)
    language: str = "es"


@dataclass
class Chunk:
    chunk_index: int
    start_ts_sec: float
    end_ts_sec: float
    speaker: str | None
    text: str
    token_count: int
    metadata: dict[str, Any]
    embedding: list[float]


@dataclass
class RunroomArticle:
    url: str
    slug: str
    title: str
    description: str
    lang: str
    episode_code_hint: str | None


@dataclass
class CandidateScore:
    article_id: int
    score: float
    method: str
    lexical_score: float
    semantic_score: float
    review_required: bool
