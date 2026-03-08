from __future__ import annotations

from pathlib import Path
from typing import Any

from src.application.use_cases.query_similar import QuerySimilarRequest, QuerySimilarUseCase
from src.application.use_cases.recommend_content import RecommendContentRequest, RecommendContentUseCase
from src.config import Settings
from src.infrastructure.ai.openai_embedding_client import OpenAIEmbeddingClient
from src.infrastructure.repositories.content_chunks import ContentChunksRepository
from src.infrastructure.repositories.legacy_chunks import LegacyChunksRepository
from src.pipeline.storage import SupabaseStorage


class QueryApiService:
    def __init__(self, settings: Settings, schema_path: Path):
        self._settings = settings
        self._schema_path = schema_path

    def query_similar(self, text: str, top_k: int, offline_mode: bool = False) -> dict[str, Any]:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            response = QuerySimilarUseCase(
                embedding_client=OpenAIEmbeddingClient(settings=self._settings, force_offline=offline_mode),
                repository=LegacyChunksRepository(storage=storage),
            ).execute(QuerySimilarRequest(text=text, top_k=top_k))
            return response.to_dict()
        finally:
            storage.close()

    def recommend_content(
        self,
        text: str,
        top_k: int,
        fetch_k: int,
        content_types: list[str] | None = None,
        source: str | None = None,
        language: str | None = None,
        group_by_type: bool = False,
        offline_mode: bool = False,
    ) -> dict[str, Any]:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            response = RecommendContentUseCase(
                embedding_client=OpenAIEmbeddingClient(settings=self._settings, force_offline=offline_mode),
                repository=ContentChunksRepository(storage=storage),
            ).execute(
                RecommendContentRequest(
                    text=text,
                    top_k=top_k,
                    fetch_k=fetch_k,
                    content_types=content_types,
                    source=source,
                    language=language,
                    group_by_type=group_by_type,
                )
            )
            return response.to_dict()
        finally:
            storage.close()
