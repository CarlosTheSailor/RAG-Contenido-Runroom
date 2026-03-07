from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class CanonicalSection:
    section_order: int
    section_key: str
    section_title: str | None
    text: str
    token_count: int
    metadata: dict[str, Any] = field(default_factory=dict)
    source_locator: dict[str, Any] = field(default_factory=dict)


@dataclass
class CanonicalChunk:
    chunk_order: int
    section_order: int
    section_key: str
    section_title: str | None
    text: str
    token_count: int
    metadata: dict[str, Any] = field(default_factory=dict)
    source_locator: dict[str, Any] = field(default_factory=dict)
    embedding: list[float] = field(default_factory=list)


@dataclass
class CanonicalContentItem:
    content_key: str
    content_type: str
    title: str
    slug: str | None
    url: str | None
    source: str
    language: str = "es"
    status: str = "active"
    published_at: datetime | None = None
    extracted_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    custom_metadata: dict[str, Any] = field(default_factory=dict)
    raw_text: str = ""


@dataclass
class CanonicalDocument:
    item: CanonicalContentItem
    sections: list[CanonicalSection]


@dataclass
class RecommendationResult:
    content_item_id: int
    content_type: str
    title: str
    url: str | None
    score: float
    matched_chunks: list[dict[str, Any]]
    metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "content_item_id": self.content_item_id,
            "content_type": self.content_type,
            "title": self.title,
            "url": self.url,
            "score": self.score,
            "matched_chunks": self.matched_chunks,
            "metadata": self.metadata,
        }
