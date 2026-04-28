from __future__ import annotations

from datetime import datetime, time, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, Callable

from src.application.use_cases.recommend_content import RecommendContentRequest, RecommendContentUseCase
from src.config import Settings
from src.infrastructure.ai.openai_embedding_client import OpenAIEmbeddingClient
from src.infrastructure.repositories.content_chunks import ContentChunksRepository
from src.pipeline.storage import SimilarContentQueryError, SupabaseStorage

from .extractor import ThemeExtractor
from .gmail import GmailClient
from .models import ThemeRunConfig, ThemeScheduleConfigCreate, ThemeScheduleCreate, ThemeTopicFilters
from .repository import ThemeIntelRepository
from .scheduling import compute_next_run_at_utc, parse_run_time_local, validate_timezone_name
from .utils import is_low_signal_theme_text, looks_like_html_fallback_text, normalize_tag, pretty_tag

SCHEDULER_LOCK_KEY = 2026031101
DEFAULT_SCHEDULE_TIMEZONE = "Europe/Madrid"
RUN_DEBUG_EVENT_LIMIT = 200
RELATED_CONTENT_STATEMENT_TIMEOUT_MS = 15000
RELATED_CONTENT_LOCK_TIMEOUT_MS = 2000


class ThemeIntelService:
    def __init__(self, settings: Settings, schema_path: Path):
        self._settings = settings
        self._schema_path = schema_path
        self._assets_dir = Path(__file__).resolve().parents[2] / "theme-intel"

    def create_run(
        self,
        gmail_query: str,
        origin_category: str,
        mark_as_read: bool,
        limit_messages: int = 100,
        triggered_by_email: str | None = None,
    ) -> dict[str, Any]:
        config = ThemeRunConfig(
            gmail_query=gmail_query.strip(),
            origin_category=origin_category.strip(),
            mark_as_read=mark_as_read,
            limit_messages=limit_messages,
            source_type="gmail",
            source_account=self._settings.gmail_source_account,
        )
        if not config.gmail_query:
            raise ValueError("gmailQuery es obligatorio.")
        if not config.origin_category:
            raise ValueError("originCategory es obligatorio.")

        category_key = normalize_tag(config.origin_category)
        if not category_key:
            raise ValueError("originCategory invalido.")

        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            repo.ensure_category(key=category_key, label=config.origin_category, source="origin")
            run_id = repo.create_run(config=config, triggered_by_email=triggered_by_email)
            return {"run_id": run_id, "status": "queued"}
        finally:
            storage.close()

    def execute_run(self, run_id: int, force_offline: bool = False) -> None:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            run = repo.get_run(run_id=run_id)
            if run is None:
                return

            repo.set_run_running(run_id=run_id)
            errors: list[dict[str, Any]] = []
            stats: dict[str, Any] = {
                "source_documents_discovered": 0,
                "source_documents_fetched": 0,
                "source_documents_saved": 0,
                "source_links_written": 0,
                "themes_extracted": 0,
                "themes_processed": 0,
                "themes_created": 0,
                "themes_merged": 0,
                "themes_skipped_low_signal": 0,
                "topic_tags_written": 0,
                "evidences_written": 0,
                "related_content_links_written": 0,
                "emails_marked_read": 0,
                "warnings": [],
                "debug_events": [],
            }
            repo.update_run_progress(run_id=run_id, stats=stats, errors=errors)

            def log_progress(stage: str, message: str, **payload: Any) -> None:
                _append_run_debug_event(stats=stats, stage=stage, message=message, **payload)
                repo.update_run_progress(run_id=run_id, stats=stats, errors=errors)

            log_progress(
                "run_started",
                "Run en ejecucion.",
                run_id=run_id,
                force_offline=force_offline,
                origin_category=str(run["origin_category"]),
                limit_messages=int(run["limit_messages"]),
            )

            try:
                def _on_message_fetched(fetched: int, discovered: int, parsed: int) -> None:
                    stats["source_documents_discovered"] = discovered
                    stats["source_documents_fetched"] = fetched
                    stats["source_documents_parsed"] = parsed
                    log_progress(
                        "source_fetch_progress",
                        "Avance cargando newsletters.",
                        fetched=fetched,
                        discovered=discovered,
                        parsed=parsed,
                    )

                log_progress("source_fetch_start", "Iniciando lectura de Gmail.")
                documents, message_ids = self._load_source_documents(
                    run=run,
                    on_message_fetched=_on_message_fetched,
                )
                stats["source_documents_discovered"] = max(
                    int(stats.get("source_documents_discovered") or 0),
                    len(documents),
                )
                stats["source_documents_fetched"] = max(
                    int(stats.get("source_documents_fetched") or 0),
                    len(documents),
                )
                stats["source_documents_parsed"] = len(documents)
                log_progress(
                    "source_fetch_done",
                    "Lectura de Gmail completada.",
                    documents=len(documents),
                    message_ids=len(message_ids),
                )
            except Exception as exc:
                _append_run_debug_event(
                    stats=stats,
                    stage="source_fetch_failed",
                    message="Error cargando newsletters.",
                    error=str(exc),
                )
                repo.finalize_run(
                    run_id=run_id,
                    status="failed",
                    stats=stats,
                    errors=[{"stage": "source_fetch", "message": str(exc)}],
                )
                return

            source_doc_id_by_external_id: dict[str, int] = {}
            persisted_source_doc_ids: list[int] = []
            for doc in documents:
                try:
                    log_progress(
                        "source_persist_start",
                        "Persistiendo newsletter.",
                        source_external_id=doc.source_external_id,
                        subject=_short_debug_text(doc.subject, 120),
                    )
                    source_doc_id = repo.upsert_source_document(
                        run_id=run_id,
                        doc=doc,
                        source_type=str(run["source_type"]),
                        source_account=str(run["source_account"]),
                    )
                    source_doc_id_by_external_id[doc.source_external_id] = source_doc_id
                    persisted_source_doc_ids.append(source_doc_id)
                    stats["source_documents_saved"] += 1
                    log_progress(
                        "source_persist_done",
                        "Newsletter persistida.",
                        source_external_id=doc.source_external_id,
                        source_document_id=source_doc_id,
                        saved=stats["source_documents_saved"],
                    )
                except Exception as exc:
                    errors.append(
                        {
                            "stage": "source_persist",
                            "message": str(exc),
                            "source_external_id": doc.source_external_id,
                        }
                    )
                    log_progress(
                        "source_persist_failed",
                        "Error persistiendo newsletter.",
                        source_external_id=doc.source_external_id,
                        error=str(exc),
                    )

            newsletters_clean = "\n\n".join(doc.cleaned_text for doc in documents if doc.cleaned_text.strip())
            extractor = ThemeExtractor(settings=self._settings, assets_dir=self._assets_dir)
            log_progress(
                "theme_extraction_start",
                "Extrayendo temas desde newsletters.",
                newsletters_with_text=sum(1 for doc in documents if doc.cleaned_text.strip()),
            )
            extraction = extractor.extract(
                newsletters_clean=newsletters_clean,
                origin_category=str(run["origin_category"]),
                gmail_query=str(run["gmail_query"]),
                force_offline=force_offline,
            )
            stats["themes_extracted"] = len(extraction.temas)
            stats["warnings"] = extraction.warnings
            log_progress(
                "theme_extraction_done",
                "Extraccion de temas completada.",
                themes_extracted=stats["themes_extracted"],
                warnings_count=len(extraction.warnings),
            )

            base_origin_tags = self._origin_tags(
                origin_category=str(run["origin_category"]),
                gmail_query=str(run["gmail_query"]),
                documents=documents,
            )
            total_themes = len(extraction.temas)
            for theme_index, theme in enumerate(extraction.temas, start=1):
                try:
                    stats["themes_processed"] += 1
                    theme_title = _short_debug_text(theme.tema, 140)
                    log_progress(
                        "theme_processing_start",
                        "Procesando tema.",
                        theme_index=theme_index,
                        themes_total=total_themes,
                        theme_title=theme_title,
                    )
                    if is_low_signal_theme_text(theme.tema):
                        stats["themes_skipped_low_signal"] += 1
                        log_progress(
                            "theme_processing_skipped",
                            "Tema omitido por baja senal.",
                            theme_index=theme_index,
                            themes_total=total_themes,
                            theme_title=theme_title,
                            skipped_low_signal=stats["themes_skipped_low_signal"],
                        )
                        continue

                    canonical_text = _canonical_topic_text(theme.tema, theme.contexto_newsletters, theme.keywords)
                    log_progress(
                        "theme_embedding_start",
                        "Generando embedding del tema.",
                        theme_index=theme_index,
                        themes_total=total_themes,
                        theme_title=theme_title,
                    )
                    embedding = OpenAIEmbeddingClient(
                        settings=self._settings,
                        force_offline=force_offline,
                    ).embed_texts([canonical_text])[0]
                    log_progress(
                        "theme_embedding_done",
                        "Embedding del tema generado.",
                        theme_index=theme_index,
                        themes_total=total_themes,
                        theme_title=theme_title,
                    )
                    similar = repo.find_similar_topic(
                        embedding=embedding,
                        primary_category_key=normalize_tag(str(run["origin_category"])),
                        similarity_threshold=self._settings.theme_intel_dedupe_threshold,
                        window_days=self._settings.theme_intel_dedupe_window_days,
                    )
                    log_progress(
                        "theme_similarity_done",
                        "Busqueda de tema similar completada.",
                        theme_index=theme_index,
                        themes_total=total_themes,
                        theme_title=theme_title,
                        matched_existing=similar is not None,
                    )

                    metadata = {
                        "keywords": theme.keywords,
                        "run_id": run_id,
                        "origin_category": run["origin_category"],
                        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
                    }

                    if similar is None:
                        topic_id = repo.create_topic(
                            run_id=run_id,
                            title=theme.tema,
                            context_text=theme.contexto_newsletters,
                            canonical_text=canonical_text,
                            primary_category_key=normalize_tag(str(run["origin_category"])),
                            score=float(len(theme.keywords)),
                            origin_source_type=str(run["source_type"]),
                            origin_source_account=str(run["source_account"]),
                            origin_query=str(run["gmail_query"]),
                            metadata=metadata,
                        )
                        stats["themes_created"] += 1
                        log_progress(
                            "theme_topic_created",
                            "Tema creado.",
                            theme_index=theme_index,
                            themes_total=total_themes,
                            theme_title=theme_title,
                            topic_id=topic_id,
                        )
                    else:
                        topic_id = int(similar["id"])
                        repo.touch_topic(
                            topic_id=topic_id,
                            run_id=run_id,
                            score=float(similar.get("similarity") or 0.0),
                            metadata=metadata,
                        )
                        stats["themes_merged"] += 1
                        log_progress(
                            "theme_topic_merged",
                            "Tema fusionado con uno existente.",
                            theme_index=theme_index,
                            themes_total=total_themes,
                            theme_title=theme_title,
                            topic_id=topic_id,
                        )

                    for doc_index, source_doc_id in enumerate(persisted_source_doc_ids):
                        repo.upsert_topic_source_document(
                            topic_id=topic_id,
                            source_document_id=source_doc_id,
                            link_type="primary" if doc_index == 0 else "run_scope",
                            metadata={"run_id": run_id, "source": "run"},
                        )
                        stats["source_links_written"] += 1

                    repo.upsert_topic_embedding(
                        topic_id=topic_id,
                        embedding=embedding,
                        model=self._settings.openai_embedding_model,
                    )
                    log_progress(
                        "theme_topic_embedding_saved",
                        "Embedding persistido para el tema.",
                        theme_index=theme_index,
                        themes_total=total_themes,
                        theme_title=theme_title,
                        topic_id=topic_id,
                    )

                    for tag in base_origin_tags:
                        repo.upsert_topic_tag(
                            topic_id=topic_id,
                            tag_key=tag,
                            tag_label=pretty_tag(tag),
                            provenance="origin",
                            confidence=1.0,
                        )
                        stats["topic_tags_written"] += 1

                    for keyword in theme.keywords:
                        tag_key = normalize_tag(keyword)
                        if not tag_key:
                            continue
                        repo.upsert_topic_tag(
                            topic_id=topic_id,
                            tag_key=tag_key,
                            tag_label=keyword.strip(),
                            provenance="ai",
                            confidence=0.8,
                        )
                        stats["topic_tags_written"] += 1

                    source_doc_id = None
                    if documents:
                        source_doc_id = source_doc_id_by_external_id.get(documents[0].source_external_id)
                    for evidence in theme.datos_cuantitativos_relacionados:
                        repo.insert_evidence(
                            topic_id=topic_id,
                            source_document_id=source_doc_id,
                            dato=evidence.dato,
                            fuente=evidence.fuente,
                            texto_fuente_breve=evidence.texto_fuente_breve,
                            url_referencia=evidence.url_referencia,
                            newsletter_origen=evidence.newsletter_origen,
                            metadata={"run_id": run_id},
                        )
                        stats["evidences_written"] += 1

                    log_progress(
                        "theme_related_start",
                        "Calculando contenido relacionado.",
                        theme_index=theme_index,
                        themes_total=total_themes,
                        theme_title=theme_title,
                        topic_id=topic_id,
                    )
                    related_warnings: list[dict[str, Any]] = []
                    related = self._recommend_related_content(
                        storage=storage,
                        text=canonical_text,
                        top_k=10,
                        force_offline=force_offline,
                        progress_logger=log_progress,
                        progress_context={
                            "theme_index": theme_index,
                            "themes_total": total_themes,
                            "theme_title": theme_title,
                            "topic_id": topic_id,
                        },
                        warning_collector=related_warnings,
                    )
                    for warning in related_warnings:
                        errors.append(dict(warning))
                        warning_message = str(warning.get("message") or "").strip()
                        if warning_message:
                            stats["warnings"].append(warning_message)
                    repo.replace_related_content(topic_id=topic_id, related_items=related)
                    stats["related_content_links_written"] += len(related)
                    log_progress(
                        "theme_processing_done",
                        "Tema procesado.",
                        theme_index=theme_index,
                        themes_total=total_themes,
                        theme_title=theme_title,
                        topic_id=topic_id,
                        related_links=len(related),
                        related_warning_count=len(related_warnings),
                    )

                except Exception as exc:
                    errors.append(
                        {
                            "stage": "topic_processing",
                            "theme": theme.tema,
                            "message": str(exc),
                        }
                    )
                    log_progress(
                        "theme_processing_failed",
                        "Error procesando tema.",
                        theme_index=theme_index,
                        themes_total=total_themes,
                        theme_title=_short_debug_text(theme.tema, 140),
                        error=str(exc),
                    )

            if bool(run["mark_as_read"]) and stats["themes_extracted"] > 0 and message_ids:
                try:
                    log_progress(
                        "mark_as_read_start",
                        "Marcando mensajes como leidos.",
                        message_ids=len(message_ids),
                    )
                    GmailClient(settings=self._settings).mark_as_read(message_ids=message_ids)
                    stats["emails_marked_read"] = len(message_ids)
                    log_progress(
                        "mark_as_read_done",
                        "Mensajes marcados como leidos.",
                        message_ids=len(message_ids),
                    )
                except Exception as exc:
                    errors.append({"stage": "mark_as_read", "message": str(exc)})
                    log_progress(
                        "mark_as_read_failed",
                        "Error marcando mensajes como leidos.",
                        error=str(exc),
                    )

            status = _resolve_run_status(stats=stats, errors=errors)

            _append_run_debug_event(
                stats=stats,
                stage="run_finalize",
                message="Finalizando run.",
                final_status=status,
                errors_count=len(errors),
            )
            repo.finalize_run(run_id=run_id, status=status, stats=stats, errors=errors)
        finally:
            storage.close()

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            run = repo.get_run(run_id=run_id)
            return dict(run) if run else None
        finally:
            storage.close()

    def get_latest_run(self) -> dict[str, Any] | None:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            run = repo.get_latest_run()
            return dict(run) if run else None
        finally:
            storage.close()

    def list_run_source_documents(self, run_id: int) -> list[dict[str, Any]]:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            return repo.list_source_documents_for_run(run_id=run_id)
        finally:
            storage.close()

    def get_source_document(self, source_document_id: int) -> dict[str, Any] | None:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            return repo.get_source_document(source_document_id=source_document_id)
        finally:
            storage.close()

    def backfill_html_fallback_source_documents(
        self,
        limit: int | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            candidates = repo.list_html_fallback_source_documents(limit=limit)
            summary: dict[str, Any] = {
                "dry_run": bool(dry_run),
                "matched": len(candidates),
                "processed": 0,
                "updated": 0,
                "unchanged": 0,
                "skipped_missing": 0,
                "skipped_non_gmail": 0,
                "still_flagged": 0,
                "errors": [],
                "documents": [
                    {
                        "id": int(row["id"]),
                        "run_id": int(row["run_id"]) if row.get("run_id") is not None else None,
                        "source_external_id": str(row.get("source_external_id") or ""),
                        "subject": str(row.get("subject") or ""),
                    }
                    for row in candidates
                ],
            }
            if dry_run or not candidates:
                return summary

            gmail = GmailClient(settings=self._settings)
            refreshed_by_external_id = gmail.get_messages(
                [str(row.get("source_external_id") or "") for row in candidates],
                ignore_missing=True,
            )
            for row in candidates:
                summary["processed"] += 1
                if str(row.get("source_type") or "") != "gmail":
                    summary["skipped_non_gmail"] += 1
                    continue

                source_external_id = str(row.get("source_external_id") or "").strip()
                if not source_external_id:
                    summary["errors"].append({"id": int(row["id"]), "message": "source_external_id vacio."})
                    continue

                refreshed = refreshed_by_external_id.get(source_external_id)
                if refreshed is None:
                    summary["skipped_missing"] += 1
                    continue

                was_changed = any(
                    [
                        str(row.get("raw_text") or "") != refreshed.raw_text,
                        str(row.get("cleaned_text") or "") != refreshed.cleaned_text,
                        row.get("links_json") != refreshed.links,
                        row.get("metadata_json") != refreshed.metadata,
                    ]
                )
                if looks_like_html_fallback_text(refreshed.cleaned_text):
                    summary["still_flagged"] += 1

                if not was_changed:
                    summary["unchanged"] += 1
                    continue

                repo.upsert_source_document(
                    run_id=int(row["run_id"]),
                    doc=refreshed,
                    source_type=str(row["source_type"]),
                    source_account=str(row["source_account"]),
                )
                summary["updated"] += 1
            return summary
        finally:
            storage.close()

    def list_topics(
        self,
        filters: ThemeTopicFilters,
        force_offline: bool = False,
    ) -> list[dict[str, Any]]:
        semantic_vector: list[float] | None = None
        if filters.semantic_query and filters.semantic_query.strip():
            semantic_vector = OpenAIEmbeddingClient(
                settings=self._settings,
                force_offline=force_offline,
            ).embed_texts([filters.semantic_query.strip()])[0]

        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            return repo.list_topics(filters=filters, semantic_vector=semantic_vector)
        finally:
            storage.close()

    def update_topic_status(self, topic_id: int, status: str) -> dict[str, Any] | None:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            row = repo.update_topic_status(topic_id=topic_id, status=status)
            return dict(row) if row else None
        finally:
            storage.close()

    def register_topic_usage(
        self,
        topic_id: int,
        client_name: str,
        artifact_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            usage_id = repo.insert_topic_usage(
                topic_id=topic_id,
                client_name=client_name,
                artifact_id=artifact_id,
                metadata=metadata,
            )
            return {
                "usage_id": usage_id,
                "topic_id": topic_id,
                "client_name": client_name,
                "artifact_id": artifact_id,
            }
        finally:
            storage.close()

    def refresh_related_content(
        self,
        topic_id: int,
        top_k: int | None = 10,
        content_types: list[str] | None = None,
        related_counts_by_type: dict[str, int] | None = None,
        force_offline: bool = False,
    ) -> dict[str, Any]:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            topic = repo.get_topic(topic_id=topic_id)
            if topic is None:
                raise ValueError("Topic no encontrado.")

            canonical_text = str(topic.get("canonical_text") or "").strip()
            if not canonical_text:
                canonical_text = _canonical_topic_text(
                    str(topic.get("title") or ""),
                    str(topic.get("context_text") or ""),
                    [],
                )
            related_warnings: list[dict[str, Any]] = []
            related = self._recommend_related_content(
                storage=storage,
                text=canonical_text,
                top_k=top_k,
                content_types=content_types,
                related_counts_by_type=related_counts_by_type,
                force_offline=force_offline,
                warning_collector=related_warnings,
            )
            repo.replace_related_content(topic_id=topic_id, related_items=related)
            return {
                "topic_id": topic_id,
                "related_items": len(related),
            }
        finally:
            storage.close()

    def get_topic_detail(self, topic_id: int) -> dict[str, Any] | None:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            return repo.get_topic_detail(topic_id=topic_id)
        finally:
            storage.close()

    def backfill_related_content(
        self,
        *,
        origin_category: str,
        days: int = 7,
        top_k: int = 10,
        force_offline: bool = False,
    ) -> dict[str, Any]:
        clean_category = origin_category.strip()
        if not clean_category:
            raise ValueError("origin_category es obligatorio.")
        if days < 1 or days > 3650:
            raise ValueError("days debe estar entre 1 y 3650.")
        if top_k < 1 or top_k > 100:
            raise ValueError("top_k debe estar entre 1 y 100.")
        process_all = clean_category.lower() == "all"
        if not process_all:
            category_key = normalize_tag(clean_category)
            if not category_key:
                raise ValueError("origin_category invalido.")

        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            topics_by_category: dict[str, list[dict[str, Any]]] = {}
            if process_all:
                categories = repo.list_recent_origin_categories(days=days)
                for category in categories:
                    topics_by_category[category] = repo.list_topics_for_recent_origin_runs(
                        origin_category=category,
                        days=days,
                    )
            else:
                topics_by_category[clean_category] = repo.list_topics_for_recent_origin_runs(
                    origin_category=clean_category,
                    days=days,
                )
            processed_total = sum(len(items) for items in topics_by_category.values())
            summary: dict[str, Any] = {
                "origin_category": clean_category,
                "categories": list(topics_by_category.keys()),
                "days": days,
                "top_k": top_k,
                "processed": processed_total,
                "succeeded": 0,
                "failed": 0,
                "per_category": {
                    category: {
                        "processed": len(items),
                        "succeeded": 0,
                        "failed": 0,
                        "warnings": 0,
                    }
                    for category, items in topics_by_category.items()
                },
                "failures": [],
                "warnings": [],
            }

            for category, topics in topics_by_category.items():
                for topic in topics:
                    topic_id = int(topic["id"])
                    canonical_text = str(topic.get("canonical_text") or "").strip()
                    if not canonical_text:
                        canonical_text = _canonical_topic_text(
                            str(topic.get("title") or ""),
                            str(topic.get("context_text") or ""),
                            [],
                        )
                    try:
                        related_warnings: list[dict[str, Any]] = []
                        related = self._recommend_related_content(
                            storage=storage,
                            text=canonical_text,
                            top_k=top_k,
                            force_offline=force_offline,
                            warning_collector=related_warnings,
                        )
                        repo.replace_related_content(topic_id=topic_id, related_items=related)
                        summary["succeeded"] += 1
                        summary["per_category"][category]["succeeded"] += 1
                        if related_warnings:
                            summary["per_category"][category]["warnings"] += len(related_warnings)
                            summary["warnings"].extend(
                                {
                                    "origin_category": category,
                                    "topic_id": topic_id,
                                    **warning,
                                }
                                for warning in related_warnings
                            )
                    except Exception as exc:
                        summary["failed"] += 1
                        summary["per_category"][category]["failed"] += 1
                        summary["failures"].append(
                            {
                                "origin_category": category,
                                "topic_id": topic_id,
                                "message": str(exc),
                            }
                        )
            return summary
        finally:
            storage.close()

    def create_schedule(
        self,
        *,
        name: str,
        enabled: bool = True,
        every_n_days: int = 1,
        run_time_local: str = "09:00",
        timezone_name: str = DEFAULT_SCHEDULE_TIMEZONE,
    ) -> dict[str, Any]:
        clean_name = name.strip()
        if not clean_name:
            raise ValueError("name es obligatorio.")
        if every_n_days < 1 or every_n_days > 365:
            raise ValueError("every_n_days debe estar entre 1 y 365.")

        validated_timezone = validate_timezone_name(timezone_name)
        run_time = parse_run_time_local(run_time_local)
        now_utc = datetime.now(tz=timezone.utc)
        next_run_at_utc = None
        if enabled:
            next_run_at_utc = compute_next_run_at_utc(
                every_n_days=every_n_days,
                run_time_local=run_time,
                timezone_name=validated_timezone,
                now_utc=now_utc,
                last_run_at_utc=None,
            )

        payload = ThemeScheduleCreate(
            name=clean_name,
            enabled=enabled,
            every_n_days=every_n_days,
            run_time_local=run_time.isoformat(timespec="seconds"),
            timezone=validated_timezone,
        )
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            schedule = repo.create_schedule(
                name=payload.name,
                enabled=payload.enabled,
                every_n_days=payload.every_n_days,
                run_time_local=payload.run_time_local,
                timezone=payload.timezone,
                next_run_at_utc=next_run_at_utc,
                metadata={},
            )
            schedule["configs"] = []
            return _normalize_schedule_payload(schedule)
        finally:
            storage.close()

    def list_schedules(self) -> list[dict[str, Any]]:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            schedules = repo.list_schedules()
            output: list[dict[str, Any]] = []
            for schedule in schedules:
                configs = repo.list_schedule_configs(schedule_id=int(schedule["id"]), enabled_only=False)
                payload = dict(schedule)
                payload["configs"] = [_normalize_schedule_config_payload(item) for item in configs]
                output.append(_normalize_schedule_payload(payload))
            return output
        finally:
            storage.close()

    def update_schedule(
        self,
        *,
        schedule_id: int,
        name: str | None = None,
        enabled: bool | None = None,
        every_n_days: int | None = None,
        run_time_local: str | None = None,
        timezone_name: str | None = None,
    ) -> dict[str, Any] | None:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            current = repo.get_schedule(schedule_id=schedule_id)
            if current is None:
                return None

            patch: dict[str, Any] = {}
            if name is not None:
                clean_name = name.strip()
                if not clean_name:
                    raise ValueError("name no puede estar vacio.")
                patch["name"] = clean_name
            if enabled is not None:
                patch["enabled"] = bool(enabled)
            if every_n_days is not None:
                if every_n_days < 1 or every_n_days > 365:
                    raise ValueError("every_n_days debe estar entre 1 y 365.")
                patch["every_n_days"] = int(every_n_days)
            if run_time_local is not None:
                parsed_time = parse_run_time_local(run_time_local)
                patch["run_time_local"] = parsed_time.isoformat(timespec="seconds")
            if timezone_name is not None:
                patch["timezone"] = validate_timezone_name(timezone_name)

            cadence_changed = any(
                key in patch for key in ("enabled", "every_n_days", "run_time_local", "timezone")
            )
            if cadence_changed:
                final_enabled = bool(patch.get("enabled", current["enabled"]))
                final_every_n_days = int(patch.get("every_n_days", current["every_n_days"]))
                final_run_time_raw = patch.get("run_time_local", current["run_time_local"])
                final_run_time = _to_time(final_run_time_raw)
                final_timezone = str(patch.get("timezone", current["timezone"]))
                if final_enabled:
                    patch["next_run_at_utc"] = compute_next_run_at_utc(
                        every_n_days=final_every_n_days,
                        run_time_local=final_run_time,
                        timezone_name=final_timezone,
                        now_utc=datetime.now(tz=timezone.utc),
                        last_run_at_utc=current.get("last_run_at_utc"),
                    )
                else:
                    patch["next_run_at_utc"] = None

            updated = repo.update_schedule(schedule_id=schedule_id, patch=patch)
            if updated is None:
                return None
            configs = repo.list_schedule_configs(schedule_id=schedule_id, enabled_only=False)
            updated["configs"] = [_normalize_schedule_config_payload(item) for item in configs]
            return _normalize_schedule_payload(updated)
        finally:
            storage.close()

    def create_schedule_config(
        self,
        *,
        schedule_id: int,
        execution_order: int,
        gmail_query: str,
        origin_category: str,
        mark_as_read: bool,
        limit_messages: int,
        enabled: bool = True,
    ) -> dict[str, Any]:
        if execution_order < 1:
            raise ValueError("execution_order debe ser >= 1.")
        clean_query = gmail_query.strip()
        if not clean_query:
            raise ValueError("gmail_query es obligatorio.")
        clean_category = origin_category.strip()
        if not clean_category:
            raise ValueError("origin_category es obligatorio.")
        if limit_messages < 1 or limit_messages > 200:
            raise ValueError("limit_messages debe estar entre 1 y 200.")

        category_key = normalize_tag(clean_category)
        if not category_key:
            raise ValueError("origin_category invalido.")

        payload = ThemeScheduleConfigCreate(
            execution_order=execution_order,
            gmail_query=clean_query,
            origin_category=clean_category,
            mark_as_read=mark_as_read,
            limit_messages=limit_messages,
            enabled=enabled,
        )
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            schedule = repo.get_schedule(schedule_id=schedule_id)
            if schedule is None:
                raise ValueError("Schedule no encontrado.")
            repo.ensure_category(key=category_key, label=clean_category, source="origin")
            config = repo.create_schedule_config(
                schedule_id=schedule_id,
                execution_order=payload.execution_order,
                gmail_query=payload.gmail_query,
                origin_category=payload.origin_category,
                mark_as_read=payload.mark_as_read,
                limit_messages=payload.limit_messages,
                enabled=payload.enabled,
                metadata={},
            )
            return _normalize_schedule_config_payload(config)
        finally:
            storage.close()

    def update_schedule_config(
        self,
        *,
        schedule_id: int,
        config_id: int,
        execution_order: int | None = None,
        gmail_query: str | None = None,
        origin_category: str | None = None,
        mark_as_read: bool | None = None,
        limit_messages: int | None = None,
        enabled: bool | None = None,
    ) -> dict[str, Any] | None:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            current = repo.get_schedule_config(schedule_id=schedule_id, config_id=config_id)
            if current is None:
                return None

            patch: dict[str, Any] = {}
            if execution_order is not None:
                if execution_order < 1:
                    raise ValueError("execution_order debe ser >= 1.")
                patch["execution_order"] = int(execution_order)
            if gmail_query is not None:
                clean_query = gmail_query.strip()
                if not clean_query:
                    raise ValueError("gmail_query no puede estar vacio.")
                patch["gmail_query"] = clean_query
            if origin_category is not None:
                clean_category = origin_category.strip()
                if not clean_category:
                    raise ValueError("origin_category no puede estar vacio.")
                category_key = normalize_tag(clean_category)
                if not category_key:
                    raise ValueError("origin_category invalido.")
                patch["origin_category"] = clean_category
                repo.ensure_category(key=category_key, label=clean_category, source="origin")
            if mark_as_read is not None:
                patch["mark_as_read"] = bool(mark_as_read)
            if limit_messages is not None:
                if limit_messages < 1 or limit_messages > 200:
                    raise ValueError("limit_messages debe estar entre 1 y 200.")
                patch["limit_messages"] = int(limit_messages)
            if enabled is not None:
                patch["enabled"] = bool(enabled)

            updated = repo.update_schedule_config(
                schedule_id=schedule_id,
                config_id=config_id,
                patch=patch,
            )
            return _normalize_schedule_config_payload(updated) if updated else None
        finally:
            storage.close()

    def run_schedule_now(self, schedule_id: int, force_offline: bool = False) -> dict[str, Any]:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            schedule = repo.get_schedule(schedule_id=schedule_id)
            if schedule is None:
                raise ValueError("Schedule no encontrado.")

            lock_acquired = repo.try_acquire_scheduler_lock(lock_key=SCHEDULER_LOCK_KEY)
            if not lock_acquired:
                return {
                    "status": "locked",
                    "executed": 0,
                    "message": "Scheduler lock ocupado.",
                }

            try:
                execution = self._execute_schedule(
                    repo=repo,
                    schedule=schedule,
                    trigger_type="manual_run_now",
                    force_offline=force_offline,
                )
                return {
                    "status": "ok",
                    "executed": 1,
                    "execution": execution,
                }
            finally:
                repo.release_scheduler_lock(lock_key=SCHEDULER_LOCK_KEY)
        finally:
            storage.close()

    def list_schedule_executions(self, schedule_id: int, limit: int = 20) -> list[dict[str, Any]]:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            if repo.get_schedule(schedule_id=schedule_id) is None:
                raise ValueError("Schedule no encontrado.")
            executions = repo.list_schedule_executions(schedule_id=schedule_id, limit=max(1, min(limit, 100)))
            output: list[dict[str, Any]] = []
            for execution in executions:
                normalized_execution = dict(execution)
                normalized_execution["items"] = [
                    _normalize_schedule_execution_item_payload(item)
                    for item in execution.get("items", [])
                ]
                output.append(_normalize_schedule_execution_payload(normalized_execution))
            return output
        finally:
            storage.close()

    def scheduler_tick(self, force_offline: bool = False) -> dict[str, Any]:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        now_utc = datetime.now(tz=timezone.utc)
        try:
            storage.ensure_schema(self._schema_path)
            repo = ThemeIntelRepository(storage)
            lock_acquired = repo.try_acquire_scheduler_lock(lock_key=SCHEDULER_LOCK_KEY)
            if not lock_acquired:
                return {
                    "status": "locked",
                    "due_schedules": 0,
                    "executed_schedules": 0,
                    "executions": [],
                }

            try:
                due_schedules = repo.list_due_schedules(now_utc=now_utc)
                executions: list[dict[str, Any]] = []
                for schedule in due_schedules:
                    executions.append(
                        self._execute_schedule(
                            repo=repo,
                            schedule=schedule,
                            trigger_type="cron_tick",
                            force_offline=force_offline,
                        )
                    )
                return {
                    "status": "ok",
                    "due_schedules": len(due_schedules),
                    "executed_schedules": len(executions),
                    "executions": executions,
                }
            finally:
                repo.release_scheduler_lock(lock_key=SCHEDULER_LOCK_KEY)
        finally:
            storage.close()

    def _execute_schedule(
        self,
        *,
        repo: ThemeIntelRepository,
        schedule: dict[str, Any],
        trigger_type: str,
        force_offline: bool,
    ) -> dict[str, Any]:
        schedule_id = int(schedule["id"])
        execution = repo.create_schedule_execution(schedule_id=schedule_id, trigger_type=trigger_type)
        execution_id = int(execution["id"])

        stats: dict[str, Any] = {
            "schedule_id": schedule_id,
            "configs_total": 0,
            "runs_created": 0,
            "items_succeeded": 0,
            "items_failed": 0,
        }
        errors: list[dict[str, Any]] = []

        configs = repo.list_schedule_configs(schedule_id=schedule_id, enabled_only=True)
        stats["configs_total"] = len(configs)

        if not configs:
            errors.append(
                {
                    "stage": "configs",
                    "message": "No hay configuraciones activas para este schedule.",
                }
            )
        for config in configs:
            item = repo.create_schedule_execution_item(
                execution_id=execution_id,
                schedule_config_id=int(config["id"]),
                execution_order=int(config["execution_order"]),
            )
            item_id = int(item["id"])
            item_errors: list[dict[str, Any]] = []
            item_stats: dict[str, Any] = {}
            run_id: int | None = None
            item_status = "failed"
            try:
                created = self.create_run(
                    gmail_query=str(config["gmail_query"]),
                    origin_category=str(config["origin_category"]),
                    mark_as_read=bool(config["mark_as_read"]),
                    limit_messages=int(config["limit_messages"]),
                    triggered_by_email=f"scheduler:{schedule_id}",
                )
                run_id = int(created["run_id"])
                stats["runs_created"] += 1

                self.execute_run(run_id=run_id, force_offline=force_offline)
                run = self.get_run(run_id=run_id) or {}
                item_stats = dict(run.get("stats_json") or {})
                raw_errors = run.get("errors_json")
                if isinstance(raw_errors, list):
                    for entry in raw_errors:
                        if isinstance(entry, dict):
                            item_errors.append(dict(entry))
                        else:
                            item_errors.append({"message": str(entry)})

                run_status = str(run.get("status") or "")
                if run_status == "succeeded":
                    item_status = "succeeded"
                    stats["items_succeeded"] += 1
                else:
                    item_status = "failed"
                    stats["items_failed"] += 1
                    if not item_errors:
                        item_errors.append(
                            {
                                "stage": "theme_run",
                                "message": f"Run {run_id} finalizo con estado {run_status or 'unknown'}",
                            }
                        )
                    errors.append(
                        {
                            "stage": "schedule_item",
                            "config_id": int(config["id"]),
                            "run_id": run_id,
                            "message": f"Run finalizo con estado {run_status or 'unknown'}",
                        }
                    )
            except Exception as exc:
                item_status = "failed"
                stats["items_failed"] += 1
                item_errors.append({"stage": "schedule_item", "message": str(exc)})
                errors.append(
                    {
                        "stage": "schedule_item",
                        "config_id": int(config["id"]),
                        "run_id": run_id,
                        "message": str(exc),
                    }
                )
            finally:
                repo.finalize_schedule_execution_item(
                    item_id=item_id,
                    status=item_status,
                    theme_run_id=run_id,
                    stats=item_stats,
                    errors=item_errors,
                )

        status = "succeeded"
        if stats["items_failed"] > 0 and stats["items_succeeded"] > 0:
            status = "partial_failed"
        elif stats["items_failed"] > 0:
            status = "failed"

        finalized = repo.finalize_schedule_execution(
            execution_id=execution_id,
            status=status,
            stats=stats,
            errors=errors,
        )

        finished_at = datetime.now(tz=timezone.utc)
        if finalized and finalized.get("finished_at") is not None:
            finished_at = finalized["finished_at"]
            if finished_at.tzinfo is None:
                finished_at = finished_at.replace(tzinfo=timezone.utc)

        run_time_local = _to_time(schedule["run_time_local"])
        next_run_at_utc: datetime | None = None
        if bool(schedule["enabled"]):
            next_run_at_utc = compute_next_run_at_utc(
                every_n_days=int(schedule["every_n_days"]),
                run_time_local=run_time_local,
                timezone_name=str(schedule["timezone"]),
                now_utc=datetime.now(tz=timezone.utc),
                last_run_at_utc=finished_at,
            )
        repo.update_schedule(
            schedule_id=schedule_id,
            patch={
                "last_run_at_utc": finished_at,
                "next_run_at_utc": next_run_at_utc,
            },
        )

        execution_payload = finalized or {"id": execution_id, "status": status, "stats_json": stats, "errors_json": errors}
        latest_executions = repo.list_schedule_executions(schedule_id=schedule_id, limit=1)
        execution_payload["items"] = latest_executions[0].get("items", []) if latest_executions else []
        return _normalize_schedule_execution_payload(execution_payload)

    def _load_source_documents(
        self,
        run: dict[str, Any],
        on_message_fetched: Callable[[int, int, int], None] | None = None,
    ) -> tuple[list[Any], list[str]]:
        source_type = str(run.get("source_type") or "")
        if source_type != "gmail":
            raise ValueError(f"source_type no soportado en fase 1: {source_type}")

        gmail = GmailClient(settings=self._settings)
        documents = gmail.list_messages(
            query=str(run["gmail_query"]),
            max_results=int(run["limit_messages"]),
            on_message_fetched=on_message_fetched,
        )
        message_ids = [doc.source_external_id for doc in documents if doc.source_external_id]
        return documents, message_ids

    def _recommend_related_content(
        self,
        storage: SupabaseStorage,
        text: str,
        top_k: int | None,
        content_types: list[str] | None = None,
        related_counts_by_type: dict[str, int] | None = None,
        force_offline: bool = False,
        progress_logger: Callable[..., None] | None = None,
        progress_context: dict[str, Any] | None = None,
        warning_collector: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        context = dict(progress_context or {})
        allowed_types = _normalize_related_types(content_types or [])
        explicit_min_by_type = _normalize_related_count_map(related_counts_by_type or {})
        forced_min_by_type = explicit_min_by_type
        if allowed_types:
            forced_min_by_type = {k: v for k, v in forced_min_by_type.items() if k in allowed_types}

        fallback_top_k = int(top_k or 10)
        forced_total = sum(forced_min_by_type.values())
        target_top_k = max(fallback_top_k, forced_total) if forced_total > 0 else fallback_top_k
        if target_top_k <= 0:
            return []

        if progress_logger is not None:
            progress_logger(
                "theme_related_query_plan",
                "Preparando queries de related content.",
                allowed_types=allowed_types,
                forced_min_by_type=forced_min_by_type,
                target_top_k=target_top_k,
                **context,
            )

        use_case = RecommendContentUseCase(
            embedding_client=OpenAIEmbeddingClient(settings=self._settings, force_offline=force_offline),
            repository=ContentChunksRepository(storage=storage),
        )
        base_top_k = max(24, target_top_k * 3)
        base_fetch_k = max(80, target_top_k * 12)

        base: list[dict[str, Any]] = []
        if progress_logger is not None:
            progress_logger(
                "theme_related_base_query_start",
                "Lanzando query base de related content.",
                fetch_k=base_fetch_k,
                top_k=base_top_k,
                **context,
            )
        base_started = perf_counter()
        try:
            base = use_case.execute(
                RecommendContentRequest(
                    text=text,
                    top_k=base_top_k,
                    fetch_k=base_fetch_k,
                    content_types=allowed_types or None,
                    group_by_type=False,
                    statement_timeout_ms=RELATED_CONTENT_STATEMENT_TIMEOUT_MS,
                    lock_timeout_ms=RELATED_CONTENT_LOCK_TIMEOUT_MS,
                    prefer_type_diversity=False,
                    apply_runroom_lab_lexical_boost=False,
                )
            ).to_dict().get("results", [])
            if progress_logger is not None:
                progress_logger(
                    "theme_related_base_query_done",
                    "Query base de related content completada.",
                    base_candidates=len(base) if isinstance(base, list) else 0,
                    duration_ms=int((perf_counter() - base_started) * 1000),
                    **context,
                )
        except Exception as exc:
            warning = _build_related_warning_entry(
                stage="theme_related_base_query_failed",
                message="Query base de related content fallida.",
                exc=exc,
                duration_ms=int((perf_counter() - base_started) * 1000),
                forced_type=None,
            )
            if warning_collector is not None:
                warning_collector.append(warning)
            if progress_logger is not None:
                progress_logger(
                    "theme_related_base_query_failed",
                    "Query base de related content fallida.",
                    duration_ms=warning["duration_ms"],
                    error=warning["error"],
                    sql_timeout=warning["sql_timeout"],
                    lock_timeout=warning["lock_timeout"],
                    sqlstate=warning["sqlstate"],
                    **context,
                )

        guaranteed_by_type: list[dict[str, Any]] = []
        for forced_type in forced_min_by_type.keys():
            required_candidates = int(forced_min_by_type.get(forced_type) or 0)
            base_coverage = _count_related_candidates_for_type(
                candidates=base if isinstance(base, list) else [],
                forced_type=forced_type,
            )
            if required_candidates > 0 and base_coverage >= required_candidates:
                if progress_logger is not None:
                    progress_logger(
                        "theme_related_typed_query_skipped",
                        "Query forzada por tipo omitida por cobertura suficiente en base.",
                        forced_type=forced_type,
                        required_candidates=required_candidates,
                        base_coverage=base_coverage,
                        **context,
                    )
                continue
            typed_top_k = max(6, target_top_k)
            typed_fetch_k = max(40, target_top_k * 8)
            if progress_logger is not None:
                progress_logger(
                    "theme_related_typed_query_start",
                    "Lanzando query forzada por tipo.",
                    forced_type=forced_type,
                    fetch_k=typed_fetch_k,
                    top_k=typed_top_k,
                    **context,
                )
            typed_started = perf_counter()
            try:
                typed_rows = use_case.execute(
                    RecommendContentRequest(
                        text=text,
                        top_k=typed_top_k,
                        fetch_k=typed_fetch_k,
                        content_types=[forced_type],
                        group_by_type=False,
                        statement_timeout_ms=RELATED_CONTENT_STATEMENT_TIMEOUT_MS,
                        lock_timeout_ms=RELATED_CONTENT_LOCK_TIMEOUT_MS,
                        prefer_type_diversity=False,
                        apply_runroom_lab_lexical_boost=False,
                    )
                ).to_dict().get("results", [])
                guaranteed_by_type.extend(typed_rows if isinstance(typed_rows, list) else [])
                if progress_logger is not None:
                    progress_logger(
                        "theme_related_typed_query_done",
                        "Query forzada por tipo completada.",
                        forced_type=forced_type,
                        typed_candidates=len(typed_rows) if isinstance(typed_rows, list) else 0,
                        duration_ms=int((perf_counter() - typed_started) * 1000),
                        **context,
                    )
            except Exception as exc:
                warning = _build_related_warning_entry(
                    stage="theme_related_typed_query_failed",
                    message="Query forzada por tipo fallida.",
                    exc=exc,
                    duration_ms=int((perf_counter() - typed_started) * 1000),
                    forced_type=forced_type,
                )
                if warning_collector is not None:
                    warning_collector.append(warning)
                if progress_logger is not None:
                    progress_logger(
                        "theme_related_typed_query_failed",
                        "Query forzada por tipo fallida.",
                        forced_type=forced_type,
                        duration_ms=warning["duration_ms"],
                        error=warning["error"],
                        sql_timeout=warning["sql_timeout"],
                        lock_timeout=warning["lock_timeout"],
                        sqlstate=warning["sqlstate"],
                        **context,
                    )

        merge_started = perf_counter()
        merged = _merge_related_candidates(
            candidates=(base if isinstance(base, list) else []) + guaranteed_by_type,
        )
        if progress_logger is not None:
            progress_logger(
                "theme_related_merge_done",
                "Candidatos related fusionados.",
                merged_candidates=len(merged),
                duration_ms=int((perf_counter() - merge_started) * 1000),
                **context,
            )
        selection_started = perf_counter()
        selected = _select_mixed_related_candidates(
            candidates=merged,
            top_k=target_top_k,
            forced_min_by_type=forced_min_by_type,
            allowed_types=allowed_types,
        )
        if progress_logger is not None:
            progress_logger(
                "theme_related_selection_done",
                "Seleccion final de related content completada.",
                selected_candidates=len(selected),
                duration_ms=int((perf_counter() - selection_started) * 1000),
                **context,
            )
        return selected

    def _origin_tags(self, origin_category: str, gmail_query: str, documents: list[Any]) -> list[str]:
        tags: set[str] = set()
        ignored = {
            "inbox",
            "unread",
            "important",
            "sent",
            "draft",
            "spam",
            "trash",
            "starred",
            "category-promotions",
            "category-social",
            "category-updates",
            "category-forums",
            "category-personal",
        }
        cat = normalize_tag(origin_category)
        if cat:
            tags.add(cat)

        # Derive tag hints from query labels, e.g. "label:cx is:unread".
        for token in gmail_query.split():
            candidate = token.strip()
            if candidate.lower().startswith("label:"):
                tag = normalize_tag(candidate.split(":", 1)[1])
                if tag:
                    tags.add(tag)

        for doc in documents:
            labels = getattr(doc, "labels", [])
            if isinstance(labels, list):
                for label in labels:
                    tag = normalize_tag(str(label))
                    if tag and tag not in ignored and not tag.startswith("label-"):
                        tags.add(tag)
        return sorted(tags)

def _canonical_topic_text(title: str, context_text: str, keywords: list[str]) -> str:
    parts = [title.strip(), context_text.strip(), ", ".join(keyword.strip() for keyword in keywords if keyword.strip())]
    return " | ".join(part for part in parts if part)


def _append_run_debug_event(stats: dict[str, Any], stage: str, message: str, **payload: Any) -> None:
    now_iso = datetime.now(tz=timezone.utc).isoformat()
    events_raw = stats.get("debug_events")
    events = list(events_raw) if isinstance(events_raw, list) else []
    event = {
        "ts": now_iso,
        "stage": stage,
        "message": message,
    }
    for key, value in payload.items():
        if value is None:
            continue
        event[key] = value
    events.append(event)
    stats["debug_events"] = events[-RUN_DEBUG_EVENT_LIMIT:]
    stats["current_stage"] = stage
    stats["current_stage_message"] = message
    stats["last_progress_at"] = now_iso


def _short_debug_text(value: str, max_len: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= max_len:
        return text
    return text[: max_len - 3].rstrip() + "..."


def _build_related_warning_entry(
    *,
    stage: str,
    message: str,
    exc: Exception,
    duration_ms: int,
    forced_type: str | None,
) -> dict[str, Any]:
    sql_timeout = isinstance(exc, SimilarContentQueryError) and exc.is_statement_timeout
    lock_timeout = isinstance(exc, SimilarContentQueryError) and exc.is_lock_timeout
    sqlstate = exc.sqlstate if isinstance(exc, SimilarContentQueryError) else None
    error_text = str(exc).strip() or exc.__class__.__name__

    warning: dict[str, Any] = {
        "stage": stage,
        "message": message if forced_type is None else f"{message} ({forced_type}).",
        "error": error_text,
        "duration_ms": int(duration_ms),
        "sql_timeout": bool(sql_timeout),
        "lock_timeout": bool(lock_timeout),
        "sqlstate": sqlstate,
    }
    if forced_type is not None:
        warning["forced_type"] = forced_type
    return warning


def _resolve_run_status(*, stats: dict[str, Any], errors: list[dict[str, Any]]) -> str:
    if not errors:
        return "succeeded"
    if int(stats.get("themes_created") or 0) > 0 or int(stats.get("themes_merged") or 0) > 0:
        return "partial_failed"
    return "failed"


def _to_time(value: Any) -> time:
    if isinstance(value, time):
        return value.replace(microsecond=0)
    if isinstance(value, str):
        return parse_run_time_local(value)
    raise ValueError("run_time_local invalido.")


def _normalize_schedule_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    run_time_local = normalized.get("run_time_local")
    if isinstance(run_time_local, time):
        normalized["run_time_local"] = run_time_local.isoformat(timespec="seconds")
    configs = normalized.get("configs")
    if isinstance(configs, list):
        normalized["configs"] = [_normalize_schedule_config_payload(item) for item in configs]
    return normalized


def _normalize_schedule_config_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    return normalized


def _normalize_schedule_execution_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    items = normalized.get("items")
    if isinstance(items, list):
        normalized["items"] = [_normalize_schedule_execution_item_payload(item) for item in items]
    return normalized


def _normalize_schedule_execution_item_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return dict(payload)


def _merge_related_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[int, dict[str, Any]] = {}
    for raw in candidates:
        if not isinstance(raw, dict):
            continue
        item_id_raw = raw.get("content_item_id")
        if not isinstance(item_id_raw, int):
            continue
        score = float(raw.get("score") or 0.0)
        current = deduped.get(item_id_raw)
        if current is None or score > float(current.get("score") or 0.0):
            deduped[item_id_raw] = dict(raw)
    merged = list(deduped.values())
    merged.sort(key=lambda row: float(row.get("score") or 0.0), reverse=True)
    return merged


def _count_related_candidates_for_type(*, candidates: list[dict[str, Any]], forced_type: str) -> int:
    normalized_forced_type = _normalize_content_type_key(forced_type)
    if not normalized_forced_type:
        return 0
    seen_ids: set[int] = set()
    count = 0
    for item in candidates:
        if not isinstance(item, dict):
            continue
        content_type = _normalize_content_type_key(str(item.get("content_type") or ""))
        if content_type != normalized_forced_type:
            continue
        content_item_id = int(item.get("content_item_id") or 0)
        if content_item_id <= 0 or content_item_id in seen_ids:
            continue
        seen_ids.add(content_item_id)
        count += 1
    return count


def _select_mixed_related_candidates(
    *,
    candidates: list[dict[str, Any]],
    top_k: int,
    forced_min_by_type: dict[str, int],
    allowed_types: list[str] | None = None,
) -> list[dict[str, Any]]:
    if top_k <= 0 or not candidates:
        return []

    allowed_set = set(allowed_types or [])
    normalized_candidates: list[dict[str, Any]] = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        ctype = _normalize_content_type_key(str(row.get("content_type") or ""))
        if not ctype:
            ctype = "other"
        row["content_type"] = ctype
        if allowed_set and ctype not in allowed_set:
            continue
        normalized_candidates.append(row)
    normalized_candidates.sort(key=lambda row: float(row.get("score") or 0.0), reverse=True)

    by_type: dict[str, list[dict[str, Any]]] = {}
    for row in normalized_candidates:
        content_type = _normalize_content_type_key(str(row.get("content_type") or ""))
        if not content_type:
            content_type = "other"
        by_type.setdefault(content_type, []).append(row)

    selected: list[dict[str, Any]] = []
    selected_ids: set[int] = set()

    forced_types = [ctype for ctype, min_count in forced_min_by_type.items() if min_count > 0 and by_type.get(ctype)]
    for idx, ctype in enumerate(forced_types):
        min_count = int(forced_min_by_type.get(ctype) or 0)
        if min_count <= 0:
            continue
        available = [row for row in by_type.get(ctype, []) if int(row.get("content_item_id") or 0) not in selected_ids]
        if not available:
            continue

        future_required = 0
        for future_type in forced_types[idx + 1 :]:
            if forced_min_by_type.get(future_type, 0) > 0 and by_type.get(future_type):
                future_required += 1
        max_take = max(0, top_k - len(selected) - future_required)
        if max_take <= 0:
            continue

        take_count = min(min_count, len(available), max_take)
        for row in available[:take_count]:
            content_item_id = int(row.get("content_item_id") or 0)
            if content_item_id <= 0 or content_item_id in selected_ids:
                continue
            selected.append(row)
            selected_ids.add(content_item_id)

    for row in normalized_candidates:
        if len(selected) >= top_k:
            break
        content_item_id = int(row.get("content_item_id") or 0)
        if content_item_id <= 0 or content_item_id in selected_ids:
            continue
        selected.append(row)
        selected_ids.add(content_item_id)

    selected.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    return selected[:top_k]


def _normalize_related_types(content_types: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for raw in content_types:
        key = _normalize_content_type_key(str(raw))
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(key)
    return output


def _normalize_related_count_map(raw_map: dict[str, int] | None) -> dict[str, int]:
    if not raw_map:
        return {}
    output: dict[str, int] = {}
    for raw_key, raw_value in raw_map.items():
        key = _normalize_content_type_key(str(raw_key))
        if not key:
            continue
        try:
            value = int(raw_value)
        except (TypeError, ValueError):
            continue
        if value <= 0:
            continue
        output[key] = value
    return output


def _normalize_content_type_key(raw_value: str) -> str:
    value = raw_value.strip().lower()
    if not value:
        return ""
    value = value.replace("-", "_")
    value = value.replace(" ", "_")
    while "__" in value:
        value = value.replace("__", "_")
    return value.strip("_")
