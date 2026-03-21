from __future__ import annotations

from datetime import datetime
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
from src.content.ingest import ingest_runroom_lab_url as ingest_runroom_lab_url_pipeline
from src.pipeline.manual_episode_ingest import ingest_uploaded_episode
from src.infrastructure.ai.openai_embedding_client import OpenAIEmbeddingClient
from src.infrastructure.repositories.content_chunks import ContentChunksRepository
from src.infrastructure.repositories.legacy_chunks import LegacyChunksRepository
from src.linkedin_draft_publisher.service import LinkedInDraftPublisherService
from src.pipeline.storage import SupabaseStorage
from src.theme_intel.models import ThemeTopicFilters
from src.theme_intel.service import ThemeIntelService


class QueryApiService:
    def __init__(self, settings: Settings, schema_path: Path):
        self._settings = settings
        self._schema_path = schema_path
        self._theme_intel = ThemeIntelService(settings=settings, schema_path=schema_path)
        self._linkedin_draft_publisher = LinkedInDraftPublisherService(settings=settings, schema_path=schema_path)

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

    def list_newsletter_linkedin_ideas(
        self,
        exclude_topic_ids: list[int] | None = None,
        limit: int = 10,
        offline_mode: bool = False,
    ) -> dict[str, Any]:
        excluded = {int(item) for item in (exclude_topic_ids or [])}
        wanted = max(1, int(limit))
        fetch_limit = min(200, max(wanted * 5, wanted + len(excluded)))

        fresh_topics = self._load_newsletter_idea_topics(
            statuses=["new", "in_progress"],
            fetch_limit=fetch_limit,
            offline_mode=offline_mode,
        )
        used_topics = self._load_newsletter_idea_topics(
            statuses=["used"],
            fetch_limit=fetch_limit,
            offline_mode=offline_mode,
        )

        ideas: list[dict[str, Any]] = []
        selected_ids: set[int] = set()

        for topic in [*fresh_topics, *used_topics]:
            topic_id = int(topic.get("id") or 0)
            if not topic_id or topic_id in excluded or topic_id in selected_ids:
                continue
            if str(topic.get("status") or "").strip() == "discarded":
                continue
            try:
                score = float(topic.get("score") or 0.0)
            except (TypeError, ValueError):
                score = 0.0

            ideas.append(
                {
                    "topic_id": topic_id,
                    "title": str(topic.get("title") or "").strip(),
                    "context_preview": _build_context_preview(str(topic.get("context_text") or "")),
                    "canonical_text": str(topic.get("canonical_text") or "").strip(),
                    "score": score,
                    "last_seen_at": topic.get("last_seen_at"),
                    "status": str(topic.get("status") or "").strip() or "unknown",
                }
            )
            selected_ids.add(topic_id)
            if len(ideas) >= wanted:
                break

        return {
            "ideas": ideas,
            "pool_exhausted": len(ideas) < wanted,
        }

    def _load_newsletter_idea_topics(
        self,
        *,
        statuses: list[str],
        fetch_limit: int,
        offline_mode: bool,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for status in statuses:
            result = self.list_theme_intel_topics(
                primary_category="product",
                status=status,
                limit=fetch_limit,
                offset=0,
                offline_mode=offline_mode,
            )
            rows.extend(item for item in result if isinstance(item, dict))
        rows.sort(key=_newsletter_idea_sort_key, reverse=True)
        return rows

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

    def ingest_runroom_lab_url(self, url: str) -> dict[str, Any]:
        summary = ingest_runroom_lab_url_pipeline(
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

    def create_theme_intel_run(
        self,
        gmail_query: str,
        origin_category: str,
        mark_as_read: bool,
        limit_messages: int = 100,
        triggered_by_email: str | None = None,
    ) -> dict[str, Any]:
        return self._theme_intel.create_run(
            gmail_query=gmail_query,
            origin_category=origin_category,
            mark_as_read=mark_as_read,
            limit_messages=limit_messages,
            triggered_by_email=triggered_by_email,
        )

    def execute_theme_intel_run(self, run_id: int, offline_mode: bool = False) -> None:
        self._theme_intel.execute_run(run_id=run_id, force_offline=offline_mode)

    def get_theme_intel_run(self, run_id: int) -> dict[str, Any] | None:
        return self._theme_intel.get_run(run_id=run_id)

    def get_latest_theme_intel_run(self) -> dict[str, Any] | None:
        return self._theme_intel.get_latest_run()

    def list_theme_intel_run_source_documents(self, run_id: int) -> list[dict[str, Any]]:
        return self._theme_intel.list_run_source_documents(run_id=run_id)

    def get_theme_intel_source_document(self, source_document_id: int) -> dict[str, Any] | None:
        return self._theme_intel.get_source_document(source_document_id=source_document_id)

    def list_theme_intel_topics(
        self,
        primary_category: str | None = None,
        status: str | None = None,
        tags_any: list[str] | None = None,
        tags_all: list[str] | None = None,
        min_score: float | None = None,
        semantic_query: str | None = None,
        limit: int = 50,
        offset: int = 0,
        offline_mode: bool = False,
    ) -> list[dict[str, Any]]:
        return self._theme_intel.list_topics(
            filters=ThemeTopicFilters(
                primary_category=primary_category,
                status=status,
                tag_any=tags_any,
                tag_all=tags_all,
                min_score=min_score,
                semantic_query=semantic_query,
                limit=limit,
                offset=offset,
            ),
            force_offline=offline_mode,
        )

    def update_theme_intel_topic_status(self, topic_id: int, status: str) -> dict[str, Any] | None:
        return self._theme_intel.update_topic_status(topic_id=topic_id, status=status)

    def register_theme_intel_topic_usage(
        self,
        topic_id: int,
        client_name: str,
        artifact_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._theme_intel.register_topic_usage(
            topic_id=topic_id,
            client_name=client_name,
            artifact_id=artifact_id,
            metadata=metadata,
        )

    def refresh_theme_intel_related_content(
        self,
        topic_id: int,
        top_k: int | None = 10,
        content_types: list[str] | None = None,
        related_counts_by_type: dict[str, int] | None = None,
        offline_mode: bool = False,
    ) -> dict[str, Any]:
        return self._theme_intel.refresh_related_content(
            topic_id=topic_id,
            top_k=top_k,
            content_types=content_types,
            related_counts_by_type=related_counts_by_type,
            force_offline=offline_mode,
        )

    def get_theme_intel_topic_detail(self, topic_id: int) -> dict[str, Any] | None:
        return self._theme_intel.get_topic_detail(topic_id=topic_id)

    def create_theme_intel_schedule(
        self,
        name: str,
        enabled: bool = True,
        every_n_days: int = 1,
        run_time_local: str = "09:00",
        timezone_name: str = "Europe/Madrid",
    ) -> dict[str, Any]:
        return self._theme_intel.create_schedule(
            name=name,
            enabled=enabled,
            every_n_days=every_n_days,
            run_time_local=run_time_local,
            timezone_name=timezone_name,
        )

    def list_theme_intel_schedules(self) -> list[dict[str, Any]]:
        return self._theme_intel.list_schedules()

    def update_theme_intel_schedule(
        self,
        schedule_id: int,
        name: str | None = None,
        enabled: bool | None = None,
        every_n_days: int | None = None,
        run_time_local: str | None = None,
        timezone_name: str | None = None,
    ) -> dict[str, Any] | None:
        return self._theme_intel.update_schedule(
            schedule_id=schedule_id,
            name=name,
            enabled=enabled,
            every_n_days=every_n_days,
            run_time_local=run_time_local,
            timezone_name=timezone_name,
        )

    def create_theme_intel_schedule_config(
        self,
        schedule_id: int,
        execution_order: int,
        gmail_query: str,
        origin_category: str,
        mark_as_read: bool,
        limit_messages: int,
        enabled: bool = True,
    ) -> dict[str, Any]:
        return self._theme_intel.create_schedule_config(
            schedule_id=schedule_id,
            execution_order=execution_order,
            gmail_query=gmail_query,
            origin_category=origin_category,
            mark_as_read=mark_as_read,
            limit_messages=limit_messages,
            enabled=enabled,
        )

    def update_theme_intel_schedule_config(
        self,
        schedule_id: int,
        config_id: int,
        execution_order: int | None = None,
        gmail_query: str | None = None,
        origin_category: str | None = None,
        mark_as_read: bool | None = None,
        limit_messages: int | None = None,
        enabled: bool | None = None,
    ) -> dict[str, Any] | None:
        return self._theme_intel.update_schedule_config(
            schedule_id=schedule_id,
            config_id=config_id,
            execution_order=execution_order,
            gmail_query=gmail_query,
            origin_category=origin_category,
            mark_as_read=mark_as_read,
            limit_messages=limit_messages,
            enabled=enabled,
        )

    def run_theme_intel_schedule_now(self, schedule_id: int, offline_mode: bool = False) -> dict[str, Any]:
        return self._theme_intel.run_schedule_now(schedule_id=schedule_id, force_offline=offline_mode)

    def list_theme_intel_schedule_executions(self, schedule_id: int, limit: int = 20) -> list[dict[str, Any]]:
        return self._theme_intel.list_schedule_executions(schedule_id=schedule_id, limit=limit)

    def tick_theme_intel_scheduler(self, offline_mode: bool = False) -> dict[str, Any]:
        return self._theme_intel.scheduler_tick(force_offline=offline_mode)

    def create_linkedin_draft_publisher_schedule(
        self,
        name: str,
        enabled: bool = True,
        every_n_days: int = 1,
        run_time_local: str = "09:00",
        timezone_name: str = "Europe/Madrid",
    ) -> dict[str, Any]:
        return self._linkedin_draft_publisher.create_schedule(
            name=name,
            enabled=enabled,
            every_n_days=every_n_days,
            run_time_local=run_time_local,
            timezone_name=timezone_name,
        )

    def list_linkedin_draft_publisher_schedules(self) -> list[dict[str, Any]]:
        return self._linkedin_draft_publisher.list_schedules()

    def update_linkedin_draft_publisher_schedule(
        self,
        schedule_id: int,
        name: str | None = None,
        enabled: bool | None = None,
        every_n_days: int | None = None,
        run_time_local: str | None = None,
        timezone_name: str | None = None,
    ) -> dict[str, Any] | None:
        return self._linkedin_draft_publisher.update_schedule(
            schedule_id=schedule_id,
            name=name,
            enabled=enabled,
            every_n_days=every_n_days,
            run_time_local=run_time_local,
            timezone_name=timezone_name,
        )

    def create_linkedin_draft_publisher_schedule_config(
        self,
        schedule_id: int,
        execution_order: int,
        origin_category: str,
        slack_channel: str,
        buyer_persona_objetivo: str,
        enabled: bool = True,
    ) -> dict[str, Any]:
        return self._linkedin_draft_publisher.create_schedule_config(
            schedule_id=schedule_id,
            execution_order=execution_order,
            origin_category=origin_category,
            slack_channel=slack_channel,
            buyer_persona_objetivo=buyer_persona_objetivo,
            enabled=enabled,
        )

    def update_linkedin_draft_publisher_schedule_config(
        self,
        schedule_id: int,
        config_id: int,
        execution_order: int | None = None,
        origin_category: str | None = None,
        slack_channel: str | None = None,
        buyer_persona_objetivo: str | None = None,
        enabled: bool | None = None,
    ) -> dict[str, Any] | None:
        return self._linkedin_draft_publisher.update_schedule_config(
            schedule_id=schedule_id,
            config_id=config_id,
            execution_order=execution_order,
            origin_category=origin_category,
            slack_channel=slack_channel,
            buyer_persona_objetivo=buyer_persona_objetivo,
            enabled=enabled,
        )

    def run_linkedin_draft_publisher_schedule_now(self, schedule_id: int, offline_mode: bool = False) -> dict[str, Any]:
        return self._linkedin_draft_publisher.run_schedule_now(schedule_id=schedule_id, force_offline=offline_mode)

    def list_linkedin_draft_publisher_schedule_executions(self, schedule_id: int, limit: int = 20) -> list[dict[str, Any]]:
        return self._linkedin_draft_publisher.list_schedule_executions(schedule_id=schedule_id, limit=limit)

    def tick_linkedin_draft_publisher_scheduler(self, offline_mode: bool = False) -> dict[str, Any]:
        return self._linkedin_draft_publisher.scheduler_tick(force_offline=offline_mode)

    def create_linkedin_draft_publisher_run(
        self,
        origin_category: str,
        slack_channel: str,
        buyer_persona_objetivo: str,
        triggered_by_email: str | None = None,
        offline_mode: bool = False,
    ) -> dict[str, Any]:
        return self._linkedin_draft_publisher.create_run(
            origin_category=origin_category,
            slack_channel=slack_channel,
            buyer_persona_objetivo=buyer_persona_objetivo,
            triggered_by_email=triggered_by_email,
            offline_mode=offline_mode,
        )

    def execute_linkedin_draft_publisher_run(self, run_id: int, offline_mode: bool = False) -> None:
        self._linkedin_draft_publisher.execute_run(run_id=run_id, force_offline=offline_mode)

    def get_linkedin_draft_publisher_run(self, run_id: int) -> dict[str, Any] | None:
        return self._linkedin_draft_publisher.get_run(run_id=run_id)

    def get_latest_linkedin_draft_publisher_run(self) -> dict[str, Any] | None:
        return self._linkedin_draft_publisher.get_latest_run()

    def get_linkedin_draft_publisher_run_result(self, run_id: int) -> dict[str, Any] | None:
        return self._linkedin_draft_publisher.get_run_result(run_id=run_id)


def _newsletter_idea_sort_key(row: dict[str, Any]) -> tuple[datetime, float, int]:
    last_seen = row.get("last_seen_at")
    if isinstance(last_seen, datetime):
        last_seen_dt = last_seen
    else:
        try:
            last_seen_dt = datetime.fromisoformat(str(last_seen).replace("Z", "+00:00"))
        except ValueError:
            last_seen_dt = datetime.min
    try:
        score = float(row.get("score") or 0.0)
    except (TypeError, ValueError):
        score = 0.0
    try:
        topic_id = int(row.get("id") or 0)
    except (TypeError, ValueError):
        topic_id = 0
    return (last_seen_dt, score, topic_id)


def _build_context_preview(text: str, max_chars: int = 180) -> str:
    compact = " ".join(text.split()).strip()
    if len(compact) <= max_chars:
        return compact
    return compact[: max_chars - 3].rstrip() + "..."
