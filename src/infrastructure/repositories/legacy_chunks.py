from __future__ import annotations

from typing import Any

from src.pipeline.storage import SupabaseStorage


class LegacyChunksRepository:
    def __init__(self, storage: SupabaseStorage):
        self._storage = storage

    def query_similar_chunks(self, query_embedding: list[float], top_k: int = 8) -> list[dict[str, Any]]:
        return self._storage.query_similar_chunks(query_embedding, top_k=top_k)
