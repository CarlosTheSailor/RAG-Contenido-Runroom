from __future__ import annotations

from typing import Any, Protocol


class EmbeddingClientPort(Protocol):
    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        ...


class LegacyChunkQueryRepositoryPort(Protocol):
    def query_similar_chunks(self, query_embedding: list[float], top_k: int = 8) -> list[dict[str, Any]]:
        ...


class CanonicalChunkQueryRepositoryPort(Protocol):
    def query_similar_content_chunks(
        self,
        query_embedding: list[float],
        top_k: int = 60,
        content_types: list[str] | None = None,
        source: str | None = None,
        language: str | None = None,
        exclude_content_item_id: int | None = None,
    ) -> list[dict[str, Any]]:
        ...
