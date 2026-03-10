from __future__ import annotations

from pathlib import Path
from typing import Any

from src.application.newsletter_linkedin_generator import (
    NewsletterLinkedInGenerator,
    NewsletterLinkedInInput,
)
from src.application.use_cases.query_similar import QuerySimilarRequest, QuerySimilarUseCase
from src.application.use_cases.recommend_content import RecommendContentRequest, RecommendContentUseCase
from src.config import Settings
from src.content.ingest import ingest_case_study_url as ingest_case_study_url_pipeline
from src.pipeline.manual_episode_ingest import ingest_uploaded_episode
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

    def generate_newsletter_linkedin(
        self,
        idea: str,
        referencias: str | None = None,
        audiencia: str | None = None,
        objetivo_secundario: str | None = None,
        longitud: str | None = None,
        metafora_visual: str | None = None,
        texto_a_incluir: str | None = None,
        offline_mode: bool = False,
    ) -> dict[str, Any]:
        warnings: list[str] = []
        rag_results: list[dict[str, Any]] = []
        min_score = self._settings.newsletter_rag_min_score

        try:
            rag_summary = self.recommend_content(
                text=idea,
                top_k=3,
                fetch_k=40,
                content_types=["episode", "case_study", "runroom_lab"],
                group_by_type=False,
                offline_mode=offline_mode,
            )
            raw_results = rag_summary.get("results")
            if isinstance(raw_results, list):
                for item in raw_results:
                    if not isinstance(item, dict):
                        continue
                    try:
                        score = float(item.get("score") or 0.0)
                    except (TypeError, ValueError):
                        score = 0.0
                    if score >= min_score:
                        rag_results.append(item)
                if raw_results and not rag_results:
                    warnings.append(
                        f"No se encontraron contenidos relacionados con score >= {min_score:.2f}."
                    )
        except Exception:
            warnings.append(
                "No se pudo consultar el RAG. La newsletter se ha generado sin referencias relacionadas."
            )

        assets_dir = Path(__file__).resolve().parents[3] / "newsletters-linkedin"
        generator = NewsletterLinkedInGenerator(
            settings=self._settings,
            assets_dir=assets_dir,
        )
        result = generator.generate(
            payload=NewsletterLinkedInInput(
                idea=idea,
                referencias=referencias,
                audiencia=audiencia,
                objetivo_secundario=objetivo_secundario,
                longitud=longitud,
                metafora_visual=metafora_visual,
                texto_a_incluir=texto_a_incluir,
            ),
            related_content=rag_results,
            force_offline=offline_mode,
        )

        return {
            "output_text": result.output_text,
            "related_content": result.related_content,
            "warnings": [*warnings, *result.warnings],
            "used_examples": result.used_examples,
        }

    def ingest_case_study_url(self, url: str) -> dict[str, Any]:
        summary = ingest_case_study_url_pipeline(
            settings=self._settings,
            schema_path=self._schema_path,
            url=url,
            target_tokens=240,
            overlap_tokens=40,
            batch_size=32,
            offline_mode=False,
            dry_run=False,
        )
        return {
            "url": url,
            "summary": dict(summary),
        }

    def ingest_episode_upload(
        self,
        transcript_filename: str,
        transcript_bytes: bytes,
        runroom_url: str,
    ) -> dict[str, Any]:
        summary = ingest_uploaded_episode(
            settings=self._settings,
            schema_path=self._schema_path,
            source_filename=transcript_filename,
            transcript_bytes=transcript_bytes,
            runroom_url=runroom_url,
            target_tokens=220,
            overlap_tokens=40,
            batch_size=32,
            offline_mode=False,
        )
        return {
            "runroom_url": runroom_url,
            "summary": dict(summary),
        }
