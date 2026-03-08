from __future__ import annotations

from typing import Any

from src.pipeline.storage import SupabaseStorage


class ContentChunksRepository:
    def __init__(self, storage: SupabaseStorage):
        self._storage = storage

    def query_similar_content_chunks(
        self,
        query_embedding: list[float],
        top_k: int = 60,
        content_types: list[str] | None = None,
        source: str | None = None,
        language: str | None = None,
        exclude_content_item_id: int | None = None,
    ) -> list[dict[str, Any]]:
        return self._storage.query_similar_content_chunks(
            query_embedding,
            top_k=top_k,
            content_types=content_types,
            source=source,
            language=language,
            exclude_content_item_id=exclude_content_item_id,
        )

    def list_content_items(self, content_types: list[str] | None = None, limit: int | None = None) -> list[dict[str, Any]]:
        return self._storage.list_content_items(content_types=content_types, limit=limit)

    def list_content_chunks_for_item(self, content_item_id: int, limit: int | None = None) -> list[dict[str, Any]]:
        return self._storage.list_content_chunks_for_item(content_item_id=content_item_id, limit=limit)

    def parse_vector(self, value: Any) -> list[float]:
        return self._storage.parse_vector(value)

    def upsert_content_relation(
        self,
        from_content_item_id: int,
        to_content_item_id: int,
        relation_type: str,
        method: str,
        score: float,
        status: str,
        rationale: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self._storage.upsert_content_relation(
            from_content_item_id=from_content_item_id,
            to_content_item_id=to_content_item_id,
            relation_type=relation_type,
            method=method,
            score=score,
            status=status,
            rationale=rationale,
            metadata=metadata,
        )
