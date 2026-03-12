from __future__ import annotations

import asyncio
import hashlib
import json
import math
import random
import re
import queue
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from datetime import date, datetime, time as dt_time, timezone
from pathlib import Path
from typing import Any, Sequence

from src.application.use_cases.recommend_content import RecommendContentRequest, RecommendContentUseCase
from src.config import Settings
from src.infrastructure.ai.openai_embedding_client import OpenAIEmbeddingClient
from src.infrastructure.repositories.content_chunks import ContentChunksRepository
from src.pipeline.normalization import normalize_for_match
from src.pipeline.storage import SupabaseStorage
from src.theme_intel.repository import ThemeIntelRepository
from src.theme_intel.scheduling import compute_next_run_at_utc, parse_run_time_local, validate_timezone_name
from src.theme_intel.utils import normalize_tag

from .models import DraftStage1Output, DraftStage2Output, LinkedInDraftRunConfig, TopicCandidate
from .parsing import normalize_references, parse_json_payload
from .prompts import LinkedInDraftPromptLoader
from .repository import LinkedInDraftPublisherRepository

SCHEDULER_LOCK_KEY = 2026031201
DEFAULT_SCHEDULE_TIMEZONE = "Europe/Madrid"


class LinkedInDraftPublisherService:
    def __init__(self, settings: Settings, schema_path: Path):
        self._settings = settings
        self._schema_path = schema_path
        self._assets_dir = Path(__file__).resolve().parents[2] / "linkedin-draft-publisher"
        self._prompts = LinkedInDraftPromptLoader(assets_dir=self._assets_dir)
        self._thread_context = threading.local()

    def create_run(
        self,
        *,
        origin_category: str,
        slack_channel: str,
        buyer_persona_objetivo: str,
        triggered_by_email: str | None = None,
        offline_mode: bool = False,
    ) -> dict[str, Any]:
        clean_category = normalize_tag(origin_category)
        if not clean_category:
            raise ValueError("originCategory es obligatorio.")
        if not slack_channel.strip():
            raise ValueError("slackChannel es obligatorio.")
        if not buyer_persona_objetivo.strip():
            raise ValueError("buyerPersonaObjetivo es obligatorio.")

        config = LinkedInDraftRunConfig(
            origin_category=clean_category,
            slack_channel=slack_channel.strip(),
            buyer_persona_objetivo=buyer_persona_objetivo.strip(),
            offline_mode=bool(offline_mode),
            client_name=self._settings.linkedin_draft_publisher_client_name,
            topics_target_count=self._settings.linkedin_draft_publisher_topics_target_count,
            topics_fetch_limit=self._settings.linkedin_draft_publisher_topics_fetch_limit,
            related_top_k=self._settings.linkedin_draft_publisher_related_top_k,
            related_counts_by_type=_parse_counts_by_type(self._settings.linkedin_draft_publisher_related_counts_by_type),
            triggered_by_email=triggered_by_email,
        )

        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = LinkedInDraftPublisherRepository(storage)
            self._recover_stale_running_runs(repo=repo)
            run_id = repo.create_run(
                origin_category=config.origin_category,
                slack_channel=config.slack_channel,
                buyer_persona_objetivo=config.buyer_persona_objetivo,
                offline_mode=config.offline_mode,
                client_name=config.client_name,
                target_count=config.topics_target_count,
                topics_fetch_limit=config.topics_fetch_limit,
                related_top_k=config.related_top_k,
                related_counts_by_type=config.related_counts_by_type,
                triggered_by_email=config.triggered_by_email,
            )
            return {"run_id": run_id, "status": "queued"}
        finally:
            storage.close()

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = LinkedInDraftPublisherRepository(storage)
            self._recover_stale_running_runs(repo=repo)
            return repo.get_run_with_items(run_id=run_id)
        finally:
            storage.close()

    def get_latest_run(self) -> dict[str, Any] | None:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = LinkedInDraftPublisherRepository(storage)
            self._recover_stale_running_runs(repo=repo)
            run = repo.get_latest_run()
            if not run:
                return None
            return repo.get_run_with_items(run_id=int(run["id"]))
        finally:
            storage.close()

    def get_run_result(self, run_id: int) -> dict[str, Any] | None:
        payload = self.get_run(run_id=run_id)
        if payload is None:
            return None
        items = payload.get("items") if isinstance(payload.get("items"), list) else []
        finals: list[dict[str, Any]] = []
        for item in items:
            if str(item.get("status") or "") not in {"succeeded", "failed"}:
                continue
            finals.append(
                {
                    "item_index": item.get("item_index"),
                    "topic_id": item.get("topic_id"),
                    "status": item.get("status"),
                    "title": item.get("title"),
                    "draft_final_text": item.get("draft_final_text"),
                    "related_selected": item.get("related_selected_json"),
                    "references": item.get("references_json") or [],
                    "draft_publish": item.get("draft_publish_json") or {},
                    "slack_publish": item.get("slack_publish_json") or {},
                    "debug": item.get("debug_json") or {},
                    "warnings": item.get("warnings_json") or [],
                    "errors": item.get("errors_json") or [],
                }
            )
        return {
            "run": {key: value for key, value in payload.items() if key != "items"},
            "items": finals,
            "total": len(finals),
        }

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

        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = LinkedInDraftPublisherRepository(storage)
            schedule = repo.create_schedule(
                name=clean_name,
                enabled=enabled,
                every_n_days=every_n_days,
                run_time_local=run_time.isoformat(timespec="seconds"),
                timezone=validated_timezone,
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
            repo = LinkedInDraftPublisherRepository(storage)
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
            repo = LinkedInDraftPublisherRepository(storage)
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
        origin_category: str,
        slack_channel: str,
        buyer_persona_objetivo: str,
        enabled: bool = True,
    ) -> dict[str, Any]:
        if execution_order < 1:
            raise ValueError("execution_order debe ser >= 1.")
        clean_category = normalize_tag(origin_category)
        if not clean_category:
            raise ValueError("originCategory es obligatorio.")
        clean_slack_channel = slack_channel.strip()
        if not clean_slack_channel:
            raise ValueError("slackChannel es obligatorio.")
        clean_buyer_persona = buyer_persona_objetivo.strip()
        if not clean_buyer_persona:
            raise ValueError("buyerPersonaObjetivo es obligatorio.")

        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = LinkedInDraftPublisherRepository(storage)
            schedule = repo.get_schedule(schedule_id=schedule_id)
            if schedule is None:
                raise ValueError("Schedule no encontrado.")
            config = repo.create_schedule_config(
                schedule_id=schedule_id,
                execution_order=execution_order,
                origin_category=clean_category,
                slack_channel=clean_slack_channel,
                buyer_persona_objetivo=clean_buyer_persona,
                enabled=enabled,
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
        origin_category: str | None = None,
        slack_channel: str | None = None,
        buyer_persona_objetivo: str | None = None,
        enabled: bool | None = None,
    ) -> dict[str, Any] | None:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            repo = LinkedInDraftPublisherRepository(storage)
            current = repo.get_schedule_config(schedule_id=schedule_id, config_id=config_id)
            if current is None:
                return None

            patch: dict[str, Any] = {}
            if execution_order is not None:
                if execution_order < 1:
                    raise ValueError("execution_order debe ser >= 1.")
                patch["execution_order"] = int(execution_order)
            if origin_category is not None:
                clean_category = normalize_tag(origin_category)
                if not clean_category:
                    raise ValueError("originCategory es obligatorio.")
                patch["origin_category"] = clean_category
            if slack_channel is not None:
                clean_slack_channel = slack_channel.strip()
                if not clean_slack_channel:
                    raise ValueError("slackChannel es obligatorio.")
                patch["slack_channel"] = clean_slack_channel
            if buyer_persona_objetivo is not None:
                clean_buyer_persona = buyer_persona_objetivo.strip()
                if not clean_buyer_persona:
                    raise ValueError("buyerPersonaObjetivo es obligatorio.")
                patch["buyer_persona_objetivo"] = clean_buyer_persona
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
            repo = LinkedInDraftPublisherRepository(storage)
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
            repo = LinkedInDraftPublisherRepository(storage)
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
            repo = LinkedInDraftPublisherRepository(storage)
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
        repo: LinkedInDraftPublisherRepository,
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
                    origin_category=str(config["origin_category"]),
                    slack_channel=str(config["slack_channel"]),
                    buyer_persona_objetivo=str(config["buyer_persona_objetivo"]),
                    triggered_by_email=f"scheduler:{schedule_id}",
                    offline_mode=force_offline,
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
                                "stage": "linkedin_draft_run",
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
                    linkedin_draft_run_id=run_id,
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

    def _set_thread_item_context(self, payload: dict[str, Any] | None) -> None:
        self._thread_context.item_ctx = payload or {}

    def _clear_thread_item_context(self) -> None:
        self._thread_context.item_ctx = {}

    def _get_thread_item_context(self) -> dict[str, Any]:
        ctx = getattr(self._thread_context, "item_ctx", None)
        if isinstance(ctx, dict):
            return ctx
        return {}

    def _track_llm_call(self) -> None:
        ctx = self._get_thread_item_context()
        if not ctx:
            return
        ctx["llm_calls_count"] = int(ctx.get("llm_calls_count") or 0) + 1

    def _track_http_retry(self) -> None:
        ctx = self._get_thread_item_context()
        if not ctx:
            return
        ctx["http_retries_count"] = int(ctx.get("http_retries_count") or 0) + 1

    def _validate_llm_configuration(self, *, offline_mode: bool) -> None:
        if offline_mode:
            return
        if not self._settings.openai_api_key:
            raise ValueError("OPENAI_API_KEY is required when offline_mode=false.")
        missing: list[str] = []
        if not self._settings.linkedin_draft_publisher_topic_selection_model.strip():
            missing.append("LINKEDIN_DRAFT_PUBLISHER_TOPIC_SELECTION_MODEL")
        if not self._settings.linkedin_draft_publisher_stage1_model.strip():
            missing.append("LINKEDIN_DRAFT_PUBLISHER_STAGE1_MODEL")
        if not self._settings.linkedin_draft_publisher_stage2_model.strip():
            missing.append("LINKEDIN_DRAFT_PUBLISHER_STAGE2_MODEL")
        if missing:
            raise ValueError(f"Missing required model configuration: {', '.join(missing)}")

    def _resolve_model_for_stage(self, stage: str) -> str:
        stage_key = stage.strip().lower()
        if stage_key == "topic_selection":
            return self._settings.linkedin_draft_publisher_topic_selection_model.strip()
        if stage_key == "draft_stage1":
            return self._settings.linkedin_draft_publisher_stage1_model.strip()
        if stage_key == "draft_stage2":
            return self._settings.linkedin_draft_publisher_stage2_model.strip()
        return (self._settings.linkedin_draft_publisher_openai_model or "").strip()

    def _prompt_versions(self) -> dict[str, str]:
        names = (
            "topic_selection_system.txt",
            "topic_selection_user.txt",
            "draft_stage1_system.txt",
            "draft_stage1_user.txt",
            "draft_stage2_refine_system.txt",
            "draft_stage2_refine_user.txt",
            "draft_stage2_repair_system.txt",
            "draft_stage2_repair_user.txt",
            "draft_stage2_quality_repair_system.txt",
            "draft_stage2_quality_repair_user.txt",
        )
        versions: dict[str, str] = {}
        for name in names:
            try:
                text = self._prompts.load(name)
            except Exception:
                versions[name] = "missing"
                continue
            versions[name] = _prompt_text_version(text)
        return versions

    def _recover_stale_running_runs(
        self,
        *,
        repo: LinkedInDraftPublisherRepository,
        exclude_run_id: int | None = None,
    ) -> int:
        if not hasattr(repo, "list_stale_running_runs"):
            return 0
        stale_runs = repo.list_stale_running_runs(
            stale_minutes=self._settings.linkedin_draft_publisher_stale_run_minutes,
            exclude_run_id=exclude_run_id,
        )
        recovered = 0
        now = datetime.utcnow()
        for run in stale_runs:
            run_id = int(run["id"])
            run_items = repo.list_run_items(run_id=run_id)
            forced_failed = 0
            for item in run_items:
                item_status = str(item.get("status") or "")
                if item_status not in {"queued", "new", "running"}:
                    continue
                item_errors = item.get("errors_json")
                if not isinstance(item_errors, list):
                    item_errors = []
                item_errors.append(
                    "Run marcado como failed por recuperacion automatica tras inactividad prolongada."
                )
                repo.update_run_item(
                    item_id=int(item["id"]),
                    patch={
                        "status": "failed",
                        "warnings_json": item.get("warnings_json") if isinstance(item.get("warnings_json"), list) else [],
                        "errors_json": item_errors,
                        "finished_at": now,
                    },
                )
                forced_failed += 1

            stats = run.get("stats_json") if isinstance(run.get("stats_json"), dict) else {}
            stats = dict(stats)
            stats.update(
                {
                    "stage": "recovered_stale_run",
                    "recovered_items_failed": forced_failed,
                    "recovered_at": now.isoformat() + "Z",
                }
            )
            errors = run.get("errors_json") if isinstance(run.get("errors_json"), list) else []
            errors = list(errors)
            errors.append(
                {
                    "stage": "recovery",
                    "message": (
                        "Run recuperado automaticamente tras quedar en running sin actividad "
                        f"durante >= {self._settings.linkedin_draft_publisher_stale_run_minutes} minutos."
                    ),
                }
            )
            repo.mark_run_finished(
                run_id=run_id,
                status="failed",
                stats=stats,
                errors=errors,
            )
            recovered += 1
        return recovered

    def execute_run(self, run_id: int, force_offline: bool | None = None) -> None:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        repo: LinkedInDraftPublisherRepository | None = None
        run: dict[str, Any] | None = None
        candidates: list[TopicCandidate] = []
        selected_topics: list[TopicCandidate] = []
        global_errors: list[dict[str, Any]] = []
        stage_samples: dict[str, list[float]] = defaultdict(list)
        total_llm_calls = 0
        total_http_retries = 0
        stage = "init"
        try:
            storage.ensure_schema(self._schema_path)
            repo = LinkedInDraftPublisherRepository(storage)
            content_repo = ContentChunksRepository(storage)
            self._recover_stale_running_runs(repo=repo, exclude_run_id=run_id)

            stage = "load_run"
            run = repo.get_run(run_id=run_id)
            if run is None:
                return

            stage = "mark_run_started"
            repo.mark_run_started(run_id=run_id)

            stage = "load_run_config"
            offline_mode = bool(force_offline) if force_offline is not None else bool(run.get("offline_mode"))
            self._validate_llm_configuration(offline_mode=offline_mode)
            topic_selection_model = self._resolve_model_for_stage("topic_selection")
            stage1_model = self._resolve_model_for_stage("draft_stage1")
            stage2_model = self._resolve_model_for_stage("draft_stage2")
            enforce_related_integration = bool(self._settings.linkedin_draft_publisher_enforce_related_integration)
            max_concurrency = max(1, int(self._settings.linkedin_draft_publisher_max_concurrency))
            stage_timeout_seconds = int(self._settings.linkedin_draft_publisher_stage_timeout_seconds)
            min_chars = max(800, int(self._settings.linkedin_draft_publisher_min_chars))
            max_chars = max(min_chars, int(self._settings.linkedin_draft_publisher_max_chars))
            target_count = int(run.get("target_count") or 5)
            fetch_limit = int(run.get("topics_fetch_limit") or 40)
            related_top_k = int(run.get("related_top_k") or 10)
            related_counts_by_type = run.get("related_counts_by_type_json")
            if not isinstance(related_counts_by_type, dict):
                related_counts_by_type = {}
            prompt_versions = self._prompt_versions()
            run_warnings: list[str] = []

            stage = "list_topic_candidates"
            candidates_raw = repo.list_topic_candidates_unused_by_client(
                primary_category_key=str(run.get("origin_category") or "").strip(),
                client_name=str(run.get("client_name") or self._settings.linkedin_draft_publisher_client_name),
                limit=fetch_limit,
            )
            if not candidates_raw:
                candidates_raw = repo.list_topic_candidates_by_category(
                    primary_category_key=str(run.get("origin_category") or "").strip(),
                    limit=fetch_limit,
                )
                if candidates_raw:
                    run_warnings.append(
                        "No hay topics ineditos para esta categoria; se reutilizan topics ya usados."
                    )
            candidates = [_topic_candidate_from_row(row) for row in candidates_raw]

            stage = "pick_topics"
            selected_ids = self._pick_topics(
                candidates=candidates,
                buyer_persona_objetivo=str(run.get("buyer_persona_objetivo") or ""),
                target_count=target_count,
                force_offline=offline_mode,
                model=topic_selection_model,
            )
            if not selected_ids:
                repo.mark_run_finished(
                    run_id=run_id,
                    status="succeeded",
                    stats={
                        "topics_candidates": len(candidates),
                        "topics_selected": 0,
                        "items_succeeded": 0,
                        "items_failed": 0,
                        "warnings": [
                            *run_warnings,
                            "No hay topics elegibles para esta categoria y cliente.",
                        ],
                    },
                    errors=[],
                )
                return

            stage = "materialize_selected_topics"
            selected_by_id: dict[int, TopicCandidate] = {item.topic_id: item for item in candidates}
            selected_topics = [selected_by_id[item_id] for item_id in selected_ids if item_id in selected_by_id]
            selected_topics = selected_topics[:target_count]

            stage = "create_run_items"
            item_ids_by_topic: dict[int, int] = {}
            for idx, topic in enumerate(selected_topics, start=1):
                item_id = repo.create_run_item(
                    run_id=run_id,
                    item_index=idx,
                    topic_id=topic.topic_id,
                    topic_payload={
                        "topic_id": topic.topic_id,
                        "title": topic.title,
                        "context_text": topic.context_text,
                        "canonical_text": topic.canonical_text,
                        "score": topic.score,
                        "last_seen_at": topic.last_seen_at,
                    },
                )
                item_ids_by_topic[topic.topic_id] = item_id

            stage = "publish_run_intro"
            intro_publish = self._publish_run_intro_to_slack(
                channel=str(run.get("slack_channel") or ""),
                total_items=len(selected_topics),
            )
            if intro_publish.get("skipped"):
                run_warnings.append("Aviso inicial en Slack omitido por configuracion.")
            elif not intro_publish.get("ok"):
                run_warnings.append(
                    f"No se pudo publicar el aviso inicial en Slack: {intro_publish.get('error') or 'error desconocido'}"
                )

            repo.update_run(
                run_id=run_id,
                patch={
                    "stats_json": {
                        "stage": "process_items",
                        "topics_candidates": len(candidates),
                        "topics_selected": len(selected_topics),
                        "items_total": len(selected_topics),
                        "items_succeeded": 0,
                        "items_failed": 0,
                        "items_running": 0,
                        "items_pending": len(selected_topics),
                        "max_concurrency": max_concurrency,
                    }
                },
            )

            stage = "load_content_types"
            available_types = content_repo.list_content_types()

            stage = "process_items"
            related_cache: dict[str, list[dict[str, Any]]] = {}
            related_cache_lock = threading.Lock()
            future_payload: dict[Any, int] = {}
            with ThreadPoolExecutor(max_workers=max_concurrency) as executor:
                for idx, topic in enumerate(selected_topics, start=1):
                    item_id = item_ids_by_topic[topic.topic_id]
                    future = executor.submit(
                        self._process_run_item,
                        run_id=run_id,
                        item_id=item_id,
                        item_index=idx,
                        topic=topic,
                        run_payload=run,
                        offline_mode=offline_mode,
                        stage1_model=stage1_model,
                        stage2_model=stage2_model,
                        topic_selection_model=topic_selection_model,
                        prompt_versions=prompt_versions,
                        enforce_related_integration=enforce_related_integration,
                        available_types=available_types,
                        related_top_k=related_top_k,
                        related_counts_by_type=related_counts_by_type,
                        candidates_count=len(candidates),
                        selected_count=len(selected_topics),
                        stage_timeout_seconds=stage_timeout_seconds,
                        min_chars=min_chars,
                        max_chars=max_chars,
                        related_cache=related_cache,
                        related_cache_lock=related_cache_lock,
                    )
                    future_payload[future] = idx

                for future in as_completed(future_payload):
                    result = future.result()
                    if not isinstance(result, dict):
                        continue
                    if isinstance(result.get("global_error"), dict):
                        global_errors.append(result["global_error"])

                    durations = result.get("durations_ms")
                    if isinstance(durations, dict):
                        for stage_name, value in durations.items():
                            try:
                                stage_samples[str(stage_name)].append(float(value))
                            except Exception:
                                continue
                    total_llm_calls += int(result.get("llm_calls_count") or 0)
                    total_http_retries += int(result.get("http_retries_count") or 0)

                    current_items = repo.list_run_items(run_id=run_id)
                    current_succeeded = sum(1 for item in current_items if str(item.get("status") or "") == "succeeded")
                    current_failed = sum(1 for item in current_items if str(item.get("status") or "") == "failed")
                    current_running = sum(1 for item in current_items if str(item.get("status") or "") == "running")
                    current_pending = sum(1 for item in current_items if str(item.get("status") or "") in {"queued", "new"})
                    repo.update_run(
                        run_id=run_id,
                        patch={
                            "stats_json": {
                                "stage": "process_items",
                                "topics_candidates": len(candidates),
                                "topics_selected": len(selected_topics),
                                "items_total": len(current_items),
                                "items_succeeded": current_succeeded,
                                "items_failed": current_failed,
                                "items_running": current_running,
                                "items_pending": current_pending,
                                "current_item_index": int(result.get("item_index") or 0),
                                "current_item_step": str(result.get("last_step") or "processing"),
                                "max_concurrency": max_concurrency,
                            }
                        },
                    )

            stage = "finalize_run"
            final_items = repo.list_run_items(run_id=run_id)
            succeeded = sum(1 for item in final_items if str(item.get("status") or "") == "succeeded")
            failed = sum(1 for item in final_items if str(item.get("status") or "") == "failed")
            if succeeded and failed:
                final_status = "partial_failed"
            elif succeeded:
                final_status = "succeeded"
            else:
                final_status = "failed"

            repo.mark_run_finished(
                run_id=run_id,
                status=final_status,
                stats={
                    "topics_candidates": len(candidates),
                    "topics_selected": len(selected_topics),
                    "items_succeeded": succeeded,
                    "items_failed": failed,
                    "items_total": len(final_items),
                    "warnings": run_warnings,
                    "stage_p50_ms": _compute_stage_percentiles(stage_samples, 0.50),
                    "stage_p95_ms": _compute_stage_percentiles(stage_samples, 0.95),
                    "slowest_stage": _select_slowest_stage(stage_samples),
                    "total_llm_calls": total_llm_calls,
                    "http_retries_count": total_http_retries,
                    "max_concurrency": max_concurrency,
                },
                errors=global_errors,
            )
        except BaseException as exc:
            if repo is not None and run is not None:
                try:
                    current_items = repo.list_run_items(run_id=run_id)
                except Exception:
                    current_items = []
                succeeded = sum(1 for item in current_items if str(item.get("status") or "") == "succeeded")
                failed = sum(1 for item in current_items if str(item.get("status") or "") == "failed")
                error_payload = [
                    *global_errors,
                    {
                        "stage": stage,
                        "message": str(exc),
                    },
                ]
                repo.mark_run_finished(
                    run_id=run_id,
                    status="failed",
                    stats={
                        "stage": stage,
                        "topics_candidates": len(candidates),
                        "topics_selected": len(selected_topics),
                        "items_succeeded": succeeded,
                        "items_failed": failed,
                        "items_total": len(current_items),
                    },
                    errors=error_payload,
                )
            if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                raise
        finally:
            storage.close()

    def _process_run_item(
        self,
        *,
        run_id: int,
        item_id: int,
        item_index: int,
        topic: TopicCandidate,
        run_payload: dict[str, Any],
        offline_mode: bool,
        stage1_model: str,
        stage2_model: str,
        topic_selection_model: str,
        prompt_versions: dict[str, str],
        enforce_related_integration: bool,
        available_types: list[str],
        related_top_k: int,
        related_counts_by_type: dict[str, int],
        candidates_count: int,
        selected_count: int,
        stage_timeout_seconds: int,
        min_chars: int,
        max_chars: int,
        related_cache: dict[str, list[dict[str, Any]]],
        related_cache_lock: threading.Lock,
    ) -> dict[str, Any]:
        storage = SupabaseStorage(self._settings.supabase_db_url)
        durations_ms: dict[str, float] = {
            "load_topic_bundle": 0.0,
            "draft_stage1": 0.0,
            "lookup_related": 0.0,
            "refine_stage2": 0.0,
            "repair_selection": 0.0,
            "repair_integration": 0.0,
            "repair_quality": 0.0,
            "publish_drafts": 0.0,
            "publish_slack": 0.0,
        }
        item_warnings: list[str] = []
        item_errors: list[str] = []
        repair_reasons: list[str] = []
        last_step = "starting"
        integration_contract_passed: bool | None = None
        validation_flags: dict[str, bool] = {}
        related_forced = False
        stage1_source = "unknown"
        stage2_source = "unknown"
        global_error: dict[str, Any] | None = None
        self._set_thread_item_context({"llm_calls_count": 0, "http_retries_count": 0})

        def _time_stage(stage_name: str, fn: Any) -> Any:
            start = time.perf_counter()
            try:
                value = _run_callable_with_timeout(
                    fn=fn,
                    timeout_seconds=float(stage_timeout_seconds),
                )
            except TimeoutError as exc:
                elapsed_ms = (time.perf_counter() - start) * 1000.0
                durations_ms[stage_name] = float(elapsed_ms)
                raise TimeoutError(
                    f"Stage timeout: '{stage_name}' excedio {stage_timeout_seconds}s ({elapsed_ms:.0f}ms)."
                ) from exc
            except Exception:
                elapsed_ms = (time.perf_counter() - start) * 1000.0
                durations_ms[stage_name] = float(elapsed_ms)
                raise
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            durations_ms[stage_name] = float(elapsed_ms)
            return value

        def _heartbeat(repo: LinkedInDraftPublisherRepository, step: str) -> None:
            nonlocal last_step
            last_step = step
            try:
                items = repo.list_run_items(run_id=run_id)
                succeeded = sum(1 for row in items if str(row.get("status") or "") == "succeeded")
                failed = sum(1 for row in items if str(row.get("status") or "") == "failed")
                running = sum(1 for row in items if str(row.get("status") or "") == "running")
                pending = sum(1 for row in items if str(row.get("status") or "") in {"queued", "new"})
                repo.update_run(
                    run_id=run_id,
                    patch={
                        "stats_json": {
                            "stage": "process_items",
                            "topics_candidates": candidates_count,
                            "topics_selected": selected_count,
                            "items_total": len(items),
                            "items_succeeded": succeeded,
                            "items_failed": failed,
                            "items_running": running,
                            "items_pending": pending,
                            "current_item_index": item_index,
                            "current_item_step": step,
                            "heartbeat_at": datetime.utcnow(),
                        }
                    },
                )
            except Exception:
                return

        try:
            storage.ensure_schema(self._schema_path)
            repo = LinkedInDraftPublisherRepository(storage)
            theme_repo = ThemeIntelRepository(storage)

            repo.update_run_item(item_id=item_id, patch={"status": "running", "started_at": datetime.utcnow()})
            _heartbeat(repo, "starting")

            _heartbeat(repo, "loading_topic_bundle")
            topic_bundle = _time_stage("load_topic_bundle", lambda: repo.get_topic_bundle(topic_id=topic.topic_id))
            if topic_bundle is None:
                topic_bundle = {
                    "topic": {
                        "id": topic.topic_id,
                        "title": topic.title,
                        "context_text": topic.context_text,
                        "canonical_text": topic.canonical_text,
                    },
                    "evidences": [],
                    "source_documents": [],
                }

            _heartbeat(repo, "draft_stage1")
            stage1 = _time_stage(
                "draft_stage1",
                lambda: self._generate_stage1_draft(
                    topic=topic,
                    topic_bundle=topic_bundle,
                    buyer_persona_objetivo=str(run_payload.get("buyer_persona_objetivo") or ""),
                    force_offline=offline_mode,
                    model=stage1_model,
                ),
            )
            stage1_source = stage1.stage_source
            if stage1.stage_source != "llm":
                item_warnings.append("Draft stage1 generado en fallback (offline).")

            related_query = _build_related_query(stage1=stage1, topic=topic)
            _heartbeat(repo, "lookup_related")
            related_candidates = _time_stage(
                "lookup_related",
                lambda: self._lookup_related_content(
                    text=related_query,
                    available_types=available_types,
                    top_k=related_top_k,
                    counts_by_type=related_counts_by_type,
                    force_offline=offline_mode,
                    lookup_cache=related_cache,
                    lookup_cache_lock=related_cache_lock,
                ),
            )
            references = _collect_topic_references(topic_bundle=topic_bundle)

            _heartbeat(repo, "refine_stage2")
            stage2 = _time_stage(
                "refine_stage2",
                lambda: self._refine_draft_stage2(
                    stage1=stage1,
                    topic_bundle=topic_bundle,
                    related_candidates=related_candidates,
                    references=references,
                    buyer_persona_objetivo=str(run_payload.get("buyer_persona_objetivo") or ""),
                    force_offline=offline_mode,
                    model=stage2_model,
                ),
            )
            stage2_source = stage2.stage_source
            if stage2.stage_source != "llm":
                item_warnings.append("Draft stage2 generado en fallback (offline).")

            related_selected = _find_related_candidate_by_id(
                related_candidates=related_candidates,
                content_item_id=stage2.selected_related_content_item_id,
            )

            contract_errors: list[str] = []
            if enforce_related_integration and related_candidates:
                if stage2.selected_related_content_item_id is None or related_selected is None:
                    related_forced = True
                    repair_reasons.append("force_top1_related_selection")
                    top_candidate = related_candidates[0]
                    forced_id = int(top_candidate.get("content_item_id") or 0)
                    if forced_id <= 0:
                        contract_errors.append("No se pudo forzar related: top-1 sin content_item_id valido.")
                    else:
                        stage2 = DraftStage2Output(
                            titulo=stage2.titulo,
                            por_que_importa_ahora=stage2.por_que_importa_ahora,
                            borrador_post=stage2.borrador_post,
                            referencias_abstract=stage2.referencias_abstract,
                            selected_related_content_item_id=forced_id,
                            selected_related_rationale=(
                                stage2.selected_related_rationale
                                or "Seleccion forzada top-1 por politica de integracion obligatoria."
                            ),
                            stage_source=stage2.stage_source,
                        )
                        related_selected = top_candidate
                related_url = str((related_selected or {}).get("url") or "").strip()
                if not related_url:
                    contract_errors.append("Contenido relacionado elegido sin URL.")
                    integration_contract_passed = False
                elif not _text_contains_url(stage2.borrador_post, related_url):
                    contract_errors.append("No se integra la URL exacta del contenido relacionado elegido.")
                    integration_contract_passed = False
                else:
                    integration_contract_passed = True
            elif enforce_related_integration and not related_candidates:
                integration_contract_passed = None
                item_warnings.append("No hay candidatos de contenido relacionado para este topic.")

            validation_flags, validation_errors = _validate_editorial_output(
                stage2=stage2,
                selected_related=related_selected,
                enforce_related_with_candidates=bool(enforce_related_integration and related_candidates),
                min_chars=min_chars,
                max_chars=max_chars,
            )
            all_repair_errors = [*contract_errors, *validation_errors]

            if all_repair_errors:
                _heartbeat(repo, "repair_composite")
                repair_reasons.append("repair_composite")
                repaired = _time_stage(
                    "repair_quality",
                    lambda: self._repair_stage2_composite(
                        stage2=stage2,
                        selected_related=related_selected,
                        buyer_persona_objetivo=str(run_payload.get("buyer_persona_objetivo") or ""),
                        force_offline=offline_mode,
                        model=stage2_model,
                        validation_errors=all_repair_errors,
                    ),
                )
                if repaired is not None:
                    stage2 = repaired
                    related_selected = _find_related_candidate_by_id(
                        related_candidates=related_candidates,
                        content_item_id=stage2.selected_related_content_item_id,
                    ) or related_selected

            final_contract_errors: list[str] = []
            if enforce_related_integration and related_candidates:
                related_url = str((related_selected or {}).get("url") or "").strip()
                if not related_url:
                    final_contract_errors.append("Contrato editorial incumplido: related sin URL.")
                    integration_contract_passed = False
                elif not _text_contains_url(stage2.borrador_post, related_url):
                    final_contract_errors.append(
                        "Contrato editorial incumplido: el draft final no integra la URL del contenido relacionado elegido."
                    )
                    integration_contract_passed = False
                else:
                    integration_contract_passed = True

            validation_flags, validation_errors = _validate_editorial_output(
                stage2=stage2,
                selected_related=related_selected,
                enforce_related_with_candidates=bool(enforce_related_integration and related_candidates),
                min_chars=min_chars,
                max_chars=max_chars,
            )
            if validation_errors and _only_length_contract_error(validation_errors):
                repair_reasons.append("repair_length_contract")
                _heartbeat(repo, "repair_quality")
                repaired_length = _time_stage(
                    "repair_quality",
                    lambda: self._repair_stage2_length_contract(
                        stage2=stage2,
                        selected_related=related_selected,
                        buyer_persona_objetivo=str(run_payload.get("buyer_persona_objetivo") or ""),
                        force_offline=offline_mode,
                        model=stage2_model,
                        min_chars=min_chars,
                        max_chars=max_chars,
                    ),
                )
                if repaired_length is not None:
                    stage2 = repaired_length
                    validation_flags, validation_errors = _validate_editorial_output(
                        stage2=stage2,
                        selected_related=related_selected,
                        enforce_related_with_candidates=bool(enforce_related_integration and related_candidates),
                        min_chars=min_chars,
                        max_chars=max_chars,
                    )
            item_errors.extend([f"Validacion editorial: {msg}" for msg in [*final_contract_errors, *validation_errors]])

            draft_publish: dict[str, Any] = {}
            slack_publish: dict[str, Any] = {}
            if not item_errors:
                _heartbeat(repo, "publish_drafts")
                draft_publish = _time_stage(
                    "publish_drafts",
                    lambda: self._publish_to_drafts_app(
                        title=stage2.titulo,
                        content=stage2.borrador_post,
                    ),
                )
                if draft_publish.get("skipped"):
                    item_warnings.append("Publicacion en app drafts omitida por configuracion.")
                elif not draft_publish.get("ok"):
                    item_errors.append(str(draft_publish.get("error") or "Error publicando en app drafts."))

                summary_text = _build_slack_summary_text(
                    title=stage2.titulo,
                    why_now=stage2.por_que_importa_ahora,
                    topic_title=topic.title,
                    edit_url=str(draft_publish.get("edit_url") or ""),
                    selected_related=related_selected or {},
                )
                thread_text = _build_slack_thread_text(stage2.borrador_post)
                _heartbeat(repo, "publish_slack")
                slack_publish = _time_stage(
                    "publish_slack",
                    lambda: self._publish_to_slack(
                        channel=str(run_payload.get("slack_channel") or ""),
                        summary_text=summary_text,
                        thread_text=thread_text,
                    ),
                )
                if slack_publish.get("skipped"):
                    item_warnings.append("Publicacion en Slack omitida por configuracion.")
                elif not slack_publish.get("ok"):
                    item_errors.append(str(slack_publish.get("error") or "Error publicando en Slack."))

            final_status = "succeeded" if not item_errors else "failed"
            if final_status == "succeeded":
                theme_repo.insert_topic_usage(
                    topic_id=topic.topic_id,
                    client_name=str(run_payload.get("client_name") or self._settings.linkedin_draft_publisher_client_name),
                    artifact_id=str(draft_publish.get("draft_id") or "") or None,
                    metadata={
                        "linkedin_draft_run_id": run_id,
                        "linkedin_draft_item_id": item_id,
                        "origin_category": run_payload.get("origin_category"),
                        "slack_channel": run_payload.get("slack_channel"),
                        "edit_url": draft_publish.get("edit_url"),
                    },
                )

            thread_ctx = self._get_thread_item_context()
            repo.update_run_item(
                item_id=item_id,
                patch={
                    "status": final_status,
                    "title": stage2.titulo,
                    "draft_stage1_text": stage1.borrador_post,
                    "draft_final_text": stage2.borrador_post,
                    "related_candidates_json": related_candidates,
                    "related_selected_json": {
                        **(related_selected if isinstance(related_selected, dict) else {}),
                        "rationale": stage2.selected_related_rationale,
                    },
                    "references_json": stage2.referencias_abstract,
                    "draft_publish_json": draft_publish,
                    "slack_publish_json": slack_publish,
                    "debug_json": {
                        "model_used": {
                            "topic_selection": topic_selection_model,
                            "draft_stage1": stage1_model,
                            "draft_stage2": stage2_model,
                        },
                        "stage_source": {
                            "draft_stage1": stage1_source,
                            "draft_stage2": stage2_source,
                        },
                        "prompt_version": prompt_versions,
                        "repair_applied": bool(repair_reasons),
                        "repair_reason": ",".join(repair_reasons),
                        "integration_contract_passed": integration_contract_passed,
                        "selected_related_content_item_id": stage2.selected_related_content_item_id,
                        "related_forced": related_forced,
                        "validation_flags": validation_flags,
                        "validation_contract": {"min_chars": min_chars, "max_chars": max_chars},
                        "durations_ms": durations_ms,
                        "llm_calls_count": int(thread_ctx.get("llm_calls_count") or 0),
                        "http_retries_count": int(thread_ctx.get("http_retries_count") or 0),
                    },
                    "warnings_json": item_warnings,
                    "errors_json": item_errors,
                    "finished_at": datetime.utcnow(),
                },
            )
            if final_status == "failed":
                global_error = {
                    "topic_id": topic.topic_id,
                    "item_id": item_id,
                    "message": " | ".join(item_errors),
                }
        except BaseException as exc:
            message = str(exc)
            global_error = {
                "topic_id": topic.topic_id,
                "item_id": item_id,
                "message": message,
            }
            try:
                storage.ensure_schema(self._schema_path)
                repo = LinkedInDraftPublisherRepository(storage)
                thread_ctx = self._get_thread_item_context()
                repo.update_run_item(
                    item_id=item_id,
                    patch={
                        "status": "failed",
                        "errors_json": [message],
                        "warnings_json": item_warnings,
                        "debug_json": {
                            "model_used": {
                                "topic_selection": topic_selection_model,
                                "draft_stage1": stage1_model,
                                "draft_stage2": stage2_model,
                            },
                            "stage_source": {
                                "draft_stage1": stage1_source,
                                "draft_stage2": stage2_source,
                            },
                            "prompt_version": prompt_versions,
                            "repair_applied": bool(repair_reasons),
                            "repair_reason": ",".join(repair_reasons),
                            "integration_contract_passed": integration_contract_passed,
                            "related_forced": related_forced,
                            "validation_flags": validation_flags,
                            "durations_ms": durations_ms,
                            "llm_calls_count": int(thread_ctx.get("llm_calls_count") or 0),
                            "http_retries_count": int(thread_ctx.get("http_retries_count") or 0),
                        },
                        "finished_at": datetime.utcnow(),
                    },
                )
            except Exception:
                pass
            if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                raise
        finally:
            thread_ctx = self._get_thread_item_context()
            self._clear_thread_item_context()
            storage.close()

        return {
            "item_index": item_index,
            "last_step": last_step,
            "durations_ms": durations_ms,
            "llm_calls_count": int(thread_ctx.get("llm_calls_count") or 0),
            "http_retries_count": int(thread_ctx.get("http_retries_count") or 0),
            "global_error": global_error,
        }

    def _pick_topics(
        self,
        *,
        candidates: Sequence[TopicCandidate],
        buyer_persona_objetivo: str,
        target_count: int,
        force_offline: bool,
        model: str,
    ) -> list[int]:
        if not candidates:
            return []
        fallback = [candidate.topic_id for candidate in candidates[:target_count]]
        if force_offline or not self._settings.openai_api_key:
            return fallback

        system_prompt = self._prompts.load_topic_selection_system()
        user_prompt = self._prompts.load_topic_selection_user()
        compact_candidates = [
            {
                "topic_id": item.topic_id,
                "title": item.title,
                "context_text": item.context_text,
                "score": item.score,
                "last_seen_at": item.last_seen_at,
            }
            for item in candidates
        ]
        user_payload = (
            user_prompt.replace("{{buyer_persona_objetivo}}", buyer_persona_objetivo)
            .replace("{{target_count}}", str(target_count))
            .replace("{{candidates_json}}", _json_dumps(compact_candidates))
        )
        raw = self._openai_chat(
            model=model,
            system_prompt=system_prompt,
            user_prompt=user_payload,
            force_offline=force_offline,
            temperature=0.2,
        )
        if raw is None:
            return fallback

        try:
            parsed = parse_json_payload(raw)
            ids_raw = parsed.get("selected_topic_ids") if isinstance(parsed, dict) else None
            if not isinstance(ids_raw, list):
                return fallback
            allowed = {item.topic_id for item in candidates}
            selected: list[int] = []
            for value in ids_raw:
                try:
                    topic_id = int(value)
                except Exception:
                    continue
                if topic_id not in allowed or topic_id in selected:
                    continue
                selected.append(topic_id)
                if len(selected) >= target_count:
                    break
            return selected or fallback
        except Exception:
            return fallback

    def _generate_stage1_draft(
        self,
        *,
        topic: TopicCandidate,
        topic_bundle: dict[str, Any],
        buyer_persona_objetivo: str,
        force_offline: bool,
        model: str,
    ) -> DraftStage1Output:
        fallback = _fallback_stage1(topic)
        if force_offline or not self._settings.openai_api_key:
            return fallback

        system_prompt = self._prompts.load_draft_stage1_system()
        user_prompt = self._prompts.load_draft_stage1_user()
        topic_payload = {
            "topic_id": topic.topic_id,
            "title": topic.title,
            "context_text": topic.context_text,
            "canonical_text": topic.canonical_text,
            "score": topic.score,
        }
        topic_bundle_payload = _curate_topic_bundle_for_prompt(
            topic_bundle=topic_bundle,
            anchor_text=f"{topic.title}\n{topic.context_text}\n{topic.canonical_text}",
            evidence_limit=self._settings.linkedin_draft_publisher_context_evidence_limit,
            doc_limit=self._settings.linkedin_draft_publisher_context_doc_limit,
        )
        user_payload = (
            user_prompt.replace("{{buyer_persona_objetivo}}", buyer_persona_objetivo)
            .replace("{{topic_json}}", _json_dumps(topic_payload))
            .replace("{{topic_bundle_json}}", _json_dumps(topic_bundle_payload))
        )
        raw, openai_error = self._openai_chat_with_error(
            model=model,
            system_prompt=system_prompt,
            user_prompt=user_payload,
            force_offline=force_offline,
            temperature=0.4,
        )
        if raw is None:
            reason = openai_error or "sin detalle"
            raise RuntimeError(f"Stage1 OpenAI error: {reason}")

        parsed = parse_json_payload(raw)
        if not isinstance(parsed, dict):
            raise RuntimeError("Stage1 parse error: respuesta JSON no es objeto.")
        titulo = str(parsed.get("titulo") or "").strip()
        por_que_importa_ahora = str(parsed.get("por_que_importa_ahora") or "").strip()
        borrador_post = str(parsed.get("borrador_post") or "").strip()
        if not titulo or not por_que_importa_ahora or not borrador_post:
            raise RuntimeError("Stage1 parse error: faltan campos requeridos (titulo/por_que_importa_ahora/borrador_post).")
        return DraftStage1Output(
            topic_id=topic.topic_id,
            titulo=titulo,
            por_que_importa_ahora=por_que_importa_ahora,
            borrador_post=borrador_post,
            referencias_abstract=normalize_references(parsed.get("referencias_abstract")),
            stage_source="llm",
        )

    def _lookup_related_content(
        self,
        *,
        text: str,
        available_types: list[str],
        top_k: int,
        counts_by_type: dict[str, int],
        force_offline: bool,
        lookup_cache: dict[str, list[dict[str, Any]]] | None = None,
        lookup_cache_lock: threading.Lock | None = None,
    ) -> list[dict[str, Any]]:
        normalized_types = [item.strip() for item in available_types if item.strip()]
        multiplier = max(2, int(self._settings.linkedin_draft_publisher_related_fetch_multiplier))
        fetch_k = max(top_k * multiplier, top_k + 8, 24)
        if fetch_k > 220:
            fetch_k = 220

        cache_key = None
        if lookup_cache is not None:
            cache_key = _json_dumps(
                {
                    "text": text[:1200],
                    "types": normalized_types,
                    "top_k": int(top_k),
                    "fetch_k": int(fetch_k),
                    "counts_by_type": counts_by_type,
                    "offline": bool(force_offline),
                }
            )
            if lookup_cache_lock is not None:
                with lookup_cache_lock:
                    cached = lookup_cache.get(cache_key)
            else:
                cached = lookup_cache.get(cache_key)
            if isinstance(cached, list):
                return [dict(row) for row in cached]

        storage = SupabaseStorage(self._settings.supabase_db_url)
        try:
            storage.ensure_schema(self._schema_path)
            response = RecommendContentUseCase(
                embedding_client=OpenAIEmbeddingClient(
                    settings=self._settings,
                    force_offline=force_offline,
                    allow_fallback=bool(force_offline),
                ),
                repository=ContentChunksRepository(storage),
            ).execute(
                RecommendContentRequest(
                    text=text,
                    top_k=fetch_k,
                    fetch_k=fetch_k,
                    content_types=normalized_types,
                )
            )
        finally:
            storage.close()

        candidates = response.results or []
        mixed = _select_related_candidates(
            candidates=candidates,
            top_k=top_k,
            forced_counts=counts_by_type,
            available_types=normalized_types,
        )
        if lookup_cache is not None and cache_key is not None:
            cache_rows = [dict(row) for row in mixed]
            if lookup_cache_lock is not None:
                with lookup_cache_lock:
                    lookup_cache[cache_key] = cache_rows
            else:
                lookup_cache[cache_key] = cache_rows
        return mixed

    def _refine_draft_stage2(
        self,
        *,
        stage1: DraftStage1Output,
        topic_bundle: dict[str, Any],
        related_candidates: list[dict[str, Any]],
        references: list[dict[str, str]],
        buyer_persona_objetivo: str,
        force_offline: bool,
        model: str,
    ) -> DraftStage2Output:
        fallback = _fallback_stage2(stage1=stage1, references=references, related_candidates=related_candidates)

        if force_offline or not self._settings.openai_api_key:
            return fallback

        system_prompt = self._prompts.load_draft_stage2_refine_system()
        user_prompt = self._prompts.load_draft_stage2_refine_user()

        topic_payload = _curate_topic_bundle_for_prompt(
            topic_bundle=topic_bundle,
            anchor_text=f"{stage1.titulo}\n{stage1.por_que_importa_ahora}\n{stage1.borrador_post[:900]}",
            evidence_limit=self._settings.linkedin_draft_publisher_context_evidence_limit,
            doc_limit=self._settings.linkedin_draft_publisher_context_doc_limit,
        )
        related_prompt_payload = _compact_related_candidates_for_prompt(
            related_candidates=related_candidates,
            limit=min(6, max(3, len(related_candidates))),
        )
        stage1_payload = {
            "topic_id": stage1.topic_id,
            "titulo": stage1.titulo,
            "por_que_importa_ahora": stage1.por_que_importa_ahora,
            "borrador_post": stage1.borrador_post,
            "referencias_abstract": stage1.referencias_abstract,
        }
        user_payload = (
            user_prompt.replace("{{buyer_persona_objetivo}}", buyer_persona_objetivo)
            .replace("{{stage1_json}}", _json_dumps(stage1_payload))
            .replace("{{topic_bundle_json}}", _json_dumps(topic_payload))
            .replace("{{related_candidates_json}}", _json_dumps(related_prompt_payload))
            .replace("{{references_json}}", _json_dumps(normalize_references(references)))
        )

        raw, openai_error = self._openai_chat_with_error(
            model=model,
            system_prompt=system_prompt,
            user_prompt=user_payload,
            force_offline=force_offline,
            temperature=0.35,
        )
        if raw is None:
            reason = openai_error or "sin detalle"
            raise RuntimeError(f"Stage2 OpenAI error: {reason}")

        parsed = parse_json_payload(raw)
        if not isinstance(parsed, dict):
            raise RuntimeError("Stage2 parse error: respuesta JSON no es objeto.")
        titulo = str(parsed.get("titulo") or "").strip()
        por_que_importa_ahora = str(parsed.get("por_que_importa_ahora") or "").strip()
        borrador_post = str(parsed.get("borrador_post") or "").strip()
        if not titulo or not por_que_importa_ahora or not borrador_post:
            raise RuntimeError("Stage2 parse error: faltan campos requeridos (titulo/por_que_importa_ahora/borrador_post).")
        selected_id_raw = parsed.get("selected_related_content_item_id")
        selected_id: int | None = None
        if selected_id_raw is not None and str(selected_id_raw).strip() != "":
            try:
                selected_id = int(selected_id_raw)
            except Exception:
                selected_id = None
        return DraftStage2Output(
            titulo=titulo,
            por_que_importa_ahora=por_que_importa_ahora,
            borrador_post=borrador_post,
            referencias_abstract=normalize_references(parsed.get("referencias_abstract")) or fallback.referencias_abstract,
            selected_related_content_item_id=selected_id,
            selected_related_rationale=str(parsed.get("selected_related_rationale") or "").strip(),
            stage_source="llm",
        )

    def _repair_stage2_related_selection(
        self,
        *,
        stage1: DraftStage1Output,
        stage2: DraftStage2Output,
        topic_bundle: dict[str, Any],
        related_candidates: list[dict[str, Any]],
        references: list[dict[str, str]],
        buyer_persona_objetivo: str,
        force_offline: bool,
        model: str,
        reason: str,
    ) -> DraftStage2Output | None:
        if force_offline or not self._settings.openai_api_key:
            return None
        if not related_candidates:
            return None

        system_prompt = self._prompts.load("draft_stage2_refine_system.txt")
        user_prompt = self._prompts.load("draft_stage2_refine_user.txt")
        topic_payload = {
            "topic": topic_bundle.get("topic") or {},
            "evidences": _compact_evidences_for_prompt(topic_bundle.get("evidences")),
            "source_documents": _compact_source_docs_for_prompt(topic_bundle.get("source_documents")),
        }
        stage1_payload = {
            "topic_id": stage1.topic_id,
            "titulo": stage1.titulo,
            "por_que_importa_ahora": stage1.por_que_importa_ahora,
            "borrador_post": stage1.borrador_post,
            "referencias_abstract": stage1.referencias_abstract,
        }
        user_payload = (
            user_prompt.replace("{{buyer_persona_objetivo}}", buyer_persona_objetivo)
            .replace("{{stage1_json}}", _json_dumps(stage1_payload))
            .replace("{{topic_bundle_json}}", _json_dumps(topic_payload))
            .replace("{{related_candidates_json}}", _json_dumps(related_candidates))
            .replace("{{references_json}}", _json_dumps(references))
        )
        user_payload = (
            f"{user_payload}\n\n"
            f"[REPAIR_REASON]\n{reason}\n\n"
            "Regla adicional obligatoria de esta iteracion:\n"
            "- Debes devolver selected_related_content_item_id con un valor valido de related_candidates_json.\n"
            "- No devuelvas null si hay candidatos.\n"
            "- Integra la URL exacta del contenido seleccionado en borrador_post.\n"
        )
        raw, _ = self._openai_chat_with_error(
            model=model,
            system_prompt=system_prompt,
            user_prompt=user_payload,
            force_offline=force_offline,
            temperature=0.2,
        )
        if raw is None:
            return None
        try:
            parsed = parse_json_payload(raw)
            if not isinstance(parsed, dict):
                return None
            selected_id_raw = parsed.get("selected_related_content_item_id")
            selected_id: int | None = None
            if selected_id_raw is not None and str(selected_id_raw).strip() != "":
                try:
                    selected_id = int(selected_id_raw)
                except Exception:
                    selected_id = None
            return DraftStage2Output(
                titulo=str(parsed.get("titulo") or stage2.titulo).strip() or stage2.titulo,
                por_que_importa_ahora=(
                    str(parsed.get("por_que_importa_ahora") or stage2.por_que_importa_ahora).strip()
                    or stage2.por_que_importa_ahora
                ),
                borrador_post=str(parsed.get("borrador_post") or stage2.borrador_post).strip() or stage2.borrador_post,
                referencias_abstract=normalize_references(parsed.get("referencias_abstract")) or stage2.referencias_abstract,
                selected_related_content_item_id=selected_id,
                selected_related_rationale=str(parsed.get("selected_related_rationale") or stage2.selected_related_rationale).strip(),
                stage_source="llm",
            )
        except Exception:
            return None

    def _repair_stage2_editorial_quality(
        self,
        *,
        stage2: DraftStage2Output,
        selected_related: dict[str, Any] | None,
        buyer_persona_objetivo: str,
        force_offline: bool,
        model: str,
        validation_errors: list[str],
    ) -> DraftStage2Output | None:
        if force_offline or not self._settings.openai_api_key:
            return None
        system_prompt = self._prompts.load("draft_stage2_quality_repair_system.txt")
        user_prompt = self._prompts.load("draft_stage2_quality_repair_user.txt")
        payload = (
            user_prompt.replace("{{buyer_persona_objetivo}}", buyer_persona_objetivo)
            .replace(
                "{{stage2_json}}",
                _json_dumps(
                    {
                        "titulo": stage2.titulo,
                        "por_que_importa_ahora": stage2.por_que_importa_ahora,
                        "borrador_post": stage2.borrador_post,
                        "referencias_abstract": stage2.referencias_abstract,
                        "selected_related_content_item_id": stage2.selected_related_content_item_id,
                        "selected_related_rationale": stage2.selected_related_rationale,
                    }
                ),
            )
            .replace("{{selected_related_json}}", _json_dumps(selected_related or {}))
            .replace("{{validation_errors_json}}", _json_dumps(validation_errors))
        )
        raw, _ = self._openai_chat_with_error(
            model=model,
            system_prompt=system_prompt,
            user_prompt=payload,
            force_offline=force_offline,
            temperature=0.2,
        )
        if raw is None:
            return None
        try:
            parsed = parse_json_payload(raw)
            if not isinstance(parsed, dict):
                return None
            return DraftStage2Output(
                titulo=str(parsed.get("titulo") or stage2.titulo).strip() or stage2.titulo,
                por_que_importa_ahora=(
                    str(parsed.get("por_que_importa_ahora") or stage2.por_que_importa_ahora).strip()
                    or stage2.por_que_importa_ahora
                ),
                borrador_post=str(parsed.get("borrador_post") or stage2.borrador_post).strip() or stage2.borrador_post,
                referencias_abstract=normalize_references(parsed.get("referencias_abstract")) or stage2.referencias_abstract,
                selected_related_content_item_id=stage2.selected_related_content_item_id,
                selected_related_rationale=stage2.selected_related_rationale,
                stage_source="llm",
            )
        except Exception:
            return None

    def _repair_stage2_composite(
        self,
        *,
        stage2: DraftStage2Output,
        selected_related: dict[str, Any] | None,
        buyer_persona_objetivo: str,
        force_offline: bool,
        model: str,
        validation_errors: list[str],
    ) -> DraftStage2Output | None:
        if force_offline or not self._settings.openai_api_key:
            return None
        system_prompt = self._prompts.load("draft_stage2_quality_repair_system.txt")
        user_prompt = self._prompts.load("draft_stage2_quality_repair_user.txt")
        payload = (
            user_prompt.replace("{{buyer_persona_objetivo}}", buyer_persona_objetivo)
            .replace(
                "{{stage2_json}}",
                _json_dumps(
                    {
                        "titulo": stage2.titulo,
                        "por_que_importa_ahora": stage2.por_que_importa_ahora,
                        "borrador_post": stage2.borrador_post,
                        "referencias_abstract": stage2.referencias_abstract,
                        "selected_related_content_item_id": stage2.selected_related_content_item_id,
                        "selected_related_rationale": stage2.selected_related_rationale,
                    }
                ),
            )
            .replace("{{selected_related_json}}", _json_dumps(selected_related or {}))
            .replace("{{validation_errors_json}}", _json_dumps(validation_errors))
        )
        payload = (
            f"{payload}\n\n"
            "Reglas adicionales obligatorias:\n"
            "- Si selected_related_json tiene URL, debe aparecer literal en borrador_post.\n"
            "- Mantener selected_related_content_item_id sin cambiar salvo que venga vacio y exista selected_related_json.content_item_id.\n"
            "- Devuelve JSON valido con todos los campos de stage2.\n"
        )
        raw, _ = self._openai_chat_with_error(
            model=model,
            system_prompt=system_prompt,
            user_prompt=payload,
            force_offline=force_offline,
            temperature=0.2,
        )
        if raw is None:
            return None
        try:
            parsed = parse_json_payload(raw)
            if not isinstance(parsed, dict):
                return None
            selected_id_raw = parsed.get("selected_related_content_item_id")
            selected_id = stage2.selected_related_content_item_id
            if selected_id_raw is not None and str(selected_id_raw).strip() != "":
                try:
                    selected_id = int(selected_id_raw)
                except Exception:
                    selected_id = stage2.selected_related_content_item_id
            return DraftStage2Output(
                titulo=str(parsed.get("titulo") or stage2.titulo).strip() or stage2.titulo,
                por_que_importa_ahora=(
                    str(parsed.get("por_que_importa_ahora") or stage2.por_que_importa_ahora).strip()
                    or stage2.por_que_importa_ahora
                ),
                borrador_post=str(parsed.get("borrador_post") or stage2.borrador_post).strip() or stage2.borrador_post,
                referencias_abstract=normalize_references(parsed.get("referencias_abstract")) or stage2.referencias_abstract,
                selected_related_content_item_id=selected_id,
                selected_related_rationale=(
                    str(parsed.get("selected_related_rationale") or stage2.selected_related_rationale).strip()
                    or stage2.selected_related_rationale
                ),
                stage_source="llm",
            )
        except Exception:
            return None

    def _repair_stage2_length_contract(
        self,
        *,
        stage2: DraftStage2Output,
        selected_related: dict[str, Any] | None,
        buyer_persona_objetivo: str,
        force_offline: bool,
        model: str,
        min_chars: int,
        max_chars: int,
    ) -> DraftStage2Output | None:
        if force_offline or not self._settings.openai_api_key:
            return None
        system_prompt = self._prompts.load("draft_stage2_quality_repair_system.txt")
        user_prompt = self._prompts.load("draft_stage2_quality_repair_user.txt")
        target_min = max(800, int(min_chars))
        target_max = max(target_min, int(max_chars))
        current_stage2 = stage2

        for attempt in range(2):
            payload = (
                user_prompt.replace("{{buyer_persona_objetivo}}", buyer_persona_objetivo)
                .replace(
                    "{{stage2_json}}",
                    _json_dumps(
                        {
                            "titulo": current_stage2.titulo,
                            "por_que_importa_ahora": current_stage2.por_que_importa_ahora,
                            "borrador_post": current_stage2.borrador_post,
                            "referencias_abstract": current_stage2.referencias_abstract,
                            "selected_related_content_item_id": current_stage2.selected_related_content_item_id,
                            "selected_related_rationale": current_stage2.selected_related_rationale,
                        }
                    ),
                )
                .replace("{{selected_related_json}}", _json_dumps(selected_related or {}))
                .replace(
                    "{{validation_errors_json}}",
                    _json_dumps([f"borrador_post debe estar entre {target_min} y {target_max} caracteres."]),
                )
            )
            payload = (
                f"{payload}\n\n"
                "[REGLAS ADICIONALES DE LONGITUD]\n"
                f"- Longitud actual de borrador_post: {len(current_stage2.borrador_post or '')} caracteres.\n"
                f"- Debe quedar entre {target_min} y {target_max} caracteres.\n"
                "- Objetivo recomendado: entre 2000 y 2300 caracteres.\n"
                "- Si falta longitud, amplia con detalle accionable sin repetir ideas.\n"
                "- Si sobra longitud, elimina redundancias sin perder URLs ya integradas.\n"
                "- Si selected_related_json contiene URL, mantenla literal en borrador_post.\n"
                "- Devuelve SOLO JSON valido.\n"
            )
            raw, _ = self._openai_chat_with_error(
                model=model,
                system_prompt=system_prompt,
                user_prompt=payload,
                force_offline=force_offline,
                temperature=0.1,
            )
            if raw is None:
                return None
            try:
                parsed = parse_json_payload(raw)
                if not isinstance(parsed, dict):
                    return None
                selected_id_raw = parsed.get("selected_related_content_item_id")
                selected_id = current_stage2.selected_related_content_item_id
                if selected_id_raw is not None and str(selected_id_raw).strip() != "":
                    try:
                        selected_id = int(selected_id_raw)
                    except Exception:
                        selected_id = current_stage2.selected_related_content_item_id
                repaired = DraftStage2Output(
                    titulo=str(parsed.get("titulo") or current_stage2.titulo).strip() or current_stage2.titulo,
                    por_que_importa_ahora=(
                        str(parsed.get("por_que_importa_ahora") or current_stage2.por_que_importa_ahora).strip()
                        or current_stage2.por_que_importa_ahora
                    ),
                    borrador_post=str(parsed.get("borrador_post") or current_stage2.borrador_post).strip()
                    or current_stage2.borrador_post,
                    referencias_abstract=normalize_references(parsed.get("referencias_abstract"))
                    or current_stage2.referencias_abstract,
                    selected_related_content_item_id=selected_id,
                    selected_related_rationale=(
                        str(parsed.get("selected_related_rationale") or current_stage2.selected_related_rationale).strip()
                        or current_stage2.selected_related_rationale
                    ),
                    stage_source="llm",
                )
                if target_min <= len(repaired.borrador_post) <= target_max:
                    return repaired
                current_stage2 = repaired
            except Exception:
                return None
        return current_stage2

    def _repair_draft_related_integration(
        self,
        *,
        stage2: DraftStage2Output,
        selected_related: dict[str, Any],
        buyer_persona_objetivo: str,
        force_offline: bool,
        model: str,
    ) -> DraftStage2Output | None:
        if force_offline or not self._settings.openai_api_key:
            return None

        system_prompt = self._prompts.load("draft_stage2_repair_system.txt")
        user_prompt = self._prompts.load("draft_stage2_repair_user.txt")
        payload = (
            user_prompt.replace("{{buyer_persona_objetivo}}", buyer_persona_objetivo)
            .replace(
                "{{stage2_json}}",
                _json_dumps(
                    {
                        "titulo": stage2.titulo,
                        "por_que_importa_ahora": stage2.por_que_importa_ahora,
                        "borrador_post": stage2.borrador_post,
                        "referencias_abstract": stage2.referencias_abstract,
                        "selected_related_content_item_id": stage2.selected_related_content_item_id,
                    }
                ),
            )
            .replace("{{selected_related_json}}", _json_dumps(selected_related))
        )
        raw = self._openai_chat(
            model=model,
            system_prompt=system_prompt,
            user_prompt=payload,
            force_offline=force_offline,
            temperature=0.1,
        )
        if raw is None:
            return None

        try:
            parsed = parse_json_payload(raw)
            if not isinstance(parsed, dict):
                return None
            return DraftStage2Output(
                titulo=stage2.titulo,
                por_que_importa_ahora=stage2.por_que_importa_ahora,
                borrador_post=str(parsed.get("borrador_post") or stage2.borrador_post).strip() or stage2.borrador_post,
                referencias_abstract=normalize_references(parsed.get("referencias_abstract")) or stage2.referencias_abstract,
                selected_related_content_item_id=stage2.selected_related_content_item_id,
                selected_related_rationale=stage2.selected_related_rationale,
                stage_source=stage2.stage_source,
            )
        except Exception:
            return None

    def _publish_to_drafts_app(self, *, title: str, content: str) -> dict[str, Any]:
        if not self._settings.linkedin_draft_publisher_drafts_api_url.strip():
            return {"skipped": True, "ok": False}

        payload = {
            "title": title,
            "content": content,
        }
        headers = {
            "Content-Type": "application/json",
        }
        secret = self._settings.linkedin_draft_publisher_drafts_api_secret.strip()
        if secret:
            headers["X-INGEST-SECRET"] = secret

        try:
            response = self._post_json_with_retry(
                url=self._settings.linkedin_draft_publisher_drafts_api_url.strip(),
                payload=payload,
                headers=headers,
                timeout=min(45, int(self._settings.linkedin_draft_publisher_stage_timeout_seconds)),
            )
            edit_url = _resolve_editor_url(
                editor_base_url=self._settings.linkedin_draft_publisher_drafts_editor_base_url,
                payload=response,
            )
            return {
                "ok": True,
                "skipped": False,
                "draft_id": response.get("id") or response.get("draftId"),
                "edit_url": edit_url,
                "raw": response,
            }
        except Exception as exc:
            return {"ok": False, "skipped": False, "error": str(exc)}

    def _publish_to_slack(self, *, channel: str, summary_text: str, thread_text: str) -> dict[str, Any]:
        token = self._settings.linkedin_draft_publisher_slack_bot_token.strip()
        post_url = self._settings.linkedin_draft_publisher_slack_post_url.strip()
        if not channel.strip() or not token or not post_url:
            return {"skipped": True, "ok": False}

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        }

        try:
            summary = self._post_json_with_retry(
                url=post_url,
                payload={
                    "channel": channel,
                    "text": summary_text,
                    "unfurl_links": False,
                    "unfurl_media": False,
                    "mrkdwn": True,
                    "link_names": True,
                },
                headers=headers,
                timeout=min(30, int(self._settings.linkedin_draft_publisher_stage_timeout_seconds)),
            )
            if not bool(summary.get("ok")):
                raise RuntimeError(f"Slack summary error: {summary}")

            thread_ts = str(summary.get("ts") or "").strip()
            thread_resp: dict[str, Any] = {}
            if thread_ts:
                thread_resp = self._post_json_with_retry(
                    url=post_url,
                    payload={
                        "channel": channel,
                        "text": thread_text,
                        "thread_ts": thread_ts,
                        "unfurl_links": False,
                        "unfurl_media": False,
                        "mrkdwn": True,
                        "link_names": True,
                    },
                    headers=headers,
                    timeout=min(30, int(self._settings.linkedin_draft_publisher_stage_timeout_seconds)),
                )
                if not bool(thread_resp.get("ok")):
                    raise RuntimeError(f"Slack thread error: {thread_resp}")

            return {
                "ok": True,
                "skipped": False,
                "summary": summary,
                "thread": thread_resp,
            }
        except Exception as exc:
            return {
                "ok": False,
                "skipped": False,
                "error": str(exc),
            }

    def _publish_run_intro_to_slack(self, *, channel: str, total_items: int) -> dict[str, Any]:
        token = self._settings.linkedin_draft_publisher_slack_bot_token.strip()
        post_url = self._settings.linkedin_draft_publisher_slack_post_url.strip()
        if not channel.strip() or not token or not post_url:
            return {"skipped": True, "ok": False}
        if total_items <= 0:
            return {"skipped": True, "ok": False}

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        }
        text = _build_slack_run_intro_text(total_items=total_items)

        try:
            raw = self._post_json_with_retry(
                url=post_url,
                payload={
                    "channel": channel,
                    "text": text,
                    "unfurl_links": False,
                    "unfurl_media": False,
                    "mrkdwn": True,
                    "link_names": True,
                },
                headers=headers,
                timeout=min(30, int(self._settings.linkedin_draft_publisher_stage_timeout_seconds)),
            )
            if not bool(raw.get("ok")):
                raise RuntimeError(f"Slack intro error: {raw}")
            return {"ok": True, "skipped": False, "raw": raw}
        except Exception as exc:
            return {"ok": False, "skipped": False, "error": str(exc)}

    def _post_json_with_retry(
        self,
        *,
        url: str,
        payload: dict[str, Any],
        headers: dict[str, str],
        timeout: int,
    ) -> dict[str, Any]:
        retry_max = max(0, int(self._settings.linkedin_draft_publisher_http_retry_max))
        attempt = 0
        while True:
            try:
                return _post_json(
                    url=url,
                    payload=payload,
                    headers=headers,
                    timeout=timeout,
                )
            except Exception as exc:
                if attempt >= retry_max or not _is_transient_http_error(exc):
                    raise
                self._track_http_retry()
                wait_seconds = min(4.0, (0.6 * (2**attempt)) + random.uniform(0.05, 0.4))
                time.sleep(wait_seconds)
                attempt += 1

    def _openai_chat(
        self,
        *,
        model: str,
        system_prompt: str,
        user_prompt: str,
        force_offline: bool,
        temperature: float,
    ) -> str | None:
        content, _ = self._openai_chat_with_error(
            model=model,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            force_offline=force_offline,
            temperature=temperature,
        )
        return content

    def _openai_chat_with_error(
        self,
        *,
        model: str,
        system_prompt: str,
        user_prompt: str,
        force_offline: bool,
        temperature: float,
    ) -> tuple[str | None, str | None]:
        if force_offline:
            return None, "offline_mode=true"
        if not self._settings.openai_api_key:
            raise RuntimeError("OPENAI_API_KEY is required when offline_mode=false.")
        if not model.strip():
            raise RuntimeError("OpenAI model is required for linkedin_draft_publisher.")
        self._track_llm_call()
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        if not model.strip().lower().startswith("gpt-5"):
            payload["temperature"] = temperature

        try:
            raw = self._post_json_with_retry(
                url=f"{self._settings.openai_base_url.rstrip('/')}/chat/completions",
                payload=payload,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self._settings.openai_api_key}",
                },
                timeout=min(
                    max(15, int(self._settings.linkedin_draft_publisher_openai_timeout_seconds)),
                    max(15, int(self._settings.linkedin_draft_publisher_stage_timeout_seconds)),
                ),
            )
            content = str(raw["choices"][0]["message"]["content"]).strip()
            if not content:
                return None, "OpenAI response vacia"
            return content, None
        except Exception as exc:
            return None, str(exc)


def _fallback_stage1(topic: TopicCandidate) -> DraftStage1Output:
    title = topic.title.strip() or f"Topic {topic.topic_id}"
    context = topic.context_text.strip() or "Tema detectado desde Theme Intel."
    why_now = f"Este tema aparece de forma recurrente en señales recientes de la categoría y tiene impacto práctico inmediato. {context[:220]}"
    draft = (
        f"{title}\n\n"
        f"{context}\n\n"
        "Cuando un equipo de producto ignora estas señales, reacciona tarde y pierde opciones estratégicas. "
        "La oportunidad es traducir la señal en decisión operativa hoy: qué priorizar, qué dejar de hacer y qué hipótesis validar esta semana.\n\n"
        "¿Qué decisión concreta cambiarías esta semana para capturar esta oportunidad antes que tu competencia?"
    )
    return DraftStage1Output(
        topic_id=topic.topic_id,
        titulo=title,
        por_que_importa_ahora=why_now,
        borrador_post=draft,
        referencias_abstract=[],
        stage_source="fallback",
    )


def _fallback_stage2(
    *,
    stage1: DraftStage1Output,
    references: list[dict[str, str]],
    related_candidates: list[dict[str, Any]],
) -> DraftStage2Output:
    final = stage1.borrador_post

    refs = normalize_references([*stage1.referencias_abstract, *references])
    return DraftStage2Output(
        titulo=stage1.titulo,
        por_que_importa_ahora=stage1.por_que_importa_ahora,
        borrador_post=final,
        referencias_abstract=refs,
        selected_related_content_item_id=None,
        selected_related_rationale="Refinado fallback sin contenido relacionado forzado.",
        stage_source="fallback",
    )


def _topic_candidate_from_row(row: dict[str, Any]) -> TopicCandidate:
    return TopicCandidate(
        topic_id=int(row["id"]),
        title=str(row.get("title") or ""),
        context_text=str(row.get("context_text") or ""),
        canonical_text=str(row.get("canonical_text") or ""),
        score=float(row.get("score") or 0.0),
        last_seen_at=row.get("last_seen_at"),
    )


def _collect_topic_references(topic_bundle: dict[str, Any]) -> list[dict[str, str]]:
    output: list[dict[str, str]] = []
    evidences = topic_bundle.get("evidences") if isinstance(topic_bundle.get("evidences"), list) else []
    source_docs = topic_bundle.get("source_documents") if isinstance(topic_bundle.get("source_documents"), list) else []

    for evidence in evidences:
        if not isinstance(evidence, dict):
            continue
        output.append(
            {
                "fuente": str(evidence.get("fuente") or evidence.get("newsletter_origen") or "").strip(),
                "url": str(evidence.get("url_referencia") or "").strip(),
                "newsletter_origen": str(evidence.get("newsletter_origen") or "Newsletter no identificada").strip(),
            }
        )

    for doc in source_docs:
        if not isinstance(doc, dict):
            continue
        links = doc.get("links_json")
        if not isinstance(links, list):
            continue
        subject = str(doc.get("subject") or "Newsletter").strip()
        for link in links:
            link_text = str(link or "").strip()
            if not link_text:
                continue
            output.append(
                {
                    "fuente": subject,
                    "url": link_text,
                    "newsletter_origen": subject,
                }
            )

    return normalize_references(output)


def _build_related_query(*, stage1: DraftStage1Output, topic: TopicCandidate) -> str:
    fragments = [
        stage1.titulo,
        stage1.por_que_importa_ahora,
        topic.title,
        topic.context_text,
    ]
    merged = " | ".join(item.strip() for item in fragments if str(item or "").strip())
    return merged[:1800]


def _curate_topic_bundle_for_prompt(
    *,
    topic_bundle: dict[str, Any],
    anchor_text: str,
    evidence_limit: int,
    doc_limit: int,
) -> dict[str, Any]:
    topic_data = topic_bundle.get("topic") or {}
    evidences = topic_bundle.get("evidences") if isinstance(topic_bundle.get("evidences"), list) else []
    source_docs = topic_bundle.get("source_documents") if isinstance(topic_bundle.get("source_documents"), list) else []
    anchor_tokens = set(normalize_for_match(anchor_text).split())

    def evidence_score(row: dict[str, Any]) -> float:
        text = " ".join(
            [
                str(row.get("dato") or ""),
                str(row.get("fuente") or ""),
                str(row.get("texto_fuente_breve") or ""),
                str(row.get("newsletter_origen") or ""),
            ]
        )
        return _token_overlap_ratio(anchor_tokens=anchor_tokens, text=text)

    def doc_score(row: dict[str, Any]) -> float:
        parts = [
            str(row.get("subject") or ""),
            str(row.get("sender") or ""),
        ]
        links = row.get("links_json")
        if isinstance(links, list):
            parts.extend([str(link or "") for link in links[:8]])
        return _token_overlap_ratio(anchor_tokens=anchor_tokens, text=" ".join(parts))

    sorted_evidences = sorted(
        [row for row in evidences if isinstance(row, dict)],
        key=evidence_score,
        reverse=True,
    )[: max(1, evidence_limit)]
    sorted_docs = sorted(
        [row for row in source_docs if isinstance(row, dict)],
        key=doc_score,
        reverse=True,
    )[: max(1, doc_limit)]

    curated_docs = _compact_source_docs_for_prompt(sorted_docs, limit=max(1, doc_limit), links_limit=8)
    for row in curated_docs:
        links = row.get("links_json")
        if not isinstance(links, list):
            continue
        deduped: list[str] = []
        seen: set[str] = set()
        for raw in links:
            value = str(raw or "").strip()
            if not value:
                continue
            key = value.lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(value[:260])
            if len(deduped) >= 6:
                break
        row["links_json"] = deduped

    return {
        "topic": topic_data,
        "evidences": _compact_evidences_for_prompt(sorted_evidences, limit=max(1, evidence_limit)),
        "source_documents": curated_docs,
    }


def _compact_related_candidates_for_prompt(
    *,
    related_candidates: list[dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in related_candidates[: max(1, limit)]:
        if not isinstance(row, dict):
            continue
        chunks = row.get("matched_chunks") if isinstance(row.get("matched_chunks"), list) else []
        compact_chunks: list[dict[str, Any]] = []
        for chunk in chunks[:2]:
            if not isinstance(chunk, dict):
                continue
            compact_chunks.append(
                {
                    "chunk_id": chunk.get("chunk_id"),
                    "section_key": chunk.get("section_key"),
                    "similarity": chunk.get("similarity"),
                    "text": str(chunk.get("text") or "")[:220],
                }
            )
        output.append(
            {
                "content_item_id": row.get("content_item_id"),
                "content_type": row.get("content_type"),
                "title": row.get("title"),
                "url": row.get("url"),
                "score": row.get("score"),
                "matched_chunks": compact_chunks,
            }
        )
    return output


def _compact_evidences_for_prompt(raw: Any, limit: int = 8) -> list[dict[str, Any]]:
    evidences = raw if isinstance(raw, list) else []
    compact: list[dict[str, Any]] = []
    for item in evidences[:limit]:
        if not isinstance(item, dict):
            continue
        compact.append(
            {
                "fuente": str(item.get("fuente") or "").strip()[:120],
                "dato": str(item.get("dato") or "").strip()[:500],
                "url_referencia": str(item.get("url_referencia") or "").strip()[:300],
                "newsletter_origen": str(item.get("newsletter_origen") or "").strip()[:160],
            }
        )
    return compact


def _compact_source_docs_for_prompt(raw: Any, limit: int = 8, links_limit: int = 6) -> list[dict[str, Any]]:
    docs = raw if isinstance(raw, list) else []
    compact: list[dict[str, Any]] = []
    for item in docs[:limit]:
        if not isinstance(item, dict):
            continue
        links = item.get("links_json")
        safe_links: list[str] = []
        if isinstance(links, list):
            for value in links[:links_limit]:
                link = str(value or "").strip()
                if link:
                    safe_links.append(link[:300])
        compact.append(
            {
                "source_document_id": item.get("source_document_id"),
                "subject": str(item.get("subject") or "").strip()[:200],
                "sender": str(item.get("sender") or "").strip()[:140],
                "received_at": item.get("received_at"),
                "links_json": safe_links,
                "link_type": str(item.get("link_type") or "").strip()[:40],
            }
        )
    return compact


def _to_time(value: Any) -> dt_time:
    if isinstance(value, dt_time):
        return value.replace(microsecond=0)
    if isinstance(value, str):
        return parse_run_time_local(value)
    raise ValueError("run_time_local invalido.")


def _normalize_schedule_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    run_time_local = normalized.get("run_time_local")
    if isinstance(run_time_local, dt_time):
        normalized["run_time_local"] = run_time_local.isoformat(timespec="seconds")
    configs = normalized.get("configs")
    if isinstance(configs, list):
        normalized["configs"] = [_normalize_schedule_config_payload(item) for item in configs]
    return normalized


def _normalize_schedule_config_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return dict(payload)


def _normalize_schedule_execution_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    items = normalized.get("items")
    if isinstance(items, list):
        normalized["items"] = [_normalize_schedule_execution_item_payload(item) for item in items]
    return normalized


def _normalize_schedule_execution_item_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return dict(payload)


def _normalize_content_type(value: str) -> str:
    sample = normalize_for_match(value)
    sample = sample.replace("-", "_").replace(" ", "_")
    while "__" in sample:
        sample = sample.replace("__", "_")
    return sample.strip("_")


def _parse_counts_by_type(raw: str | None) -> dict[str, int]:
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}

    output: dict[str, int] = {}
    for key, value in payload.items():
        ctype = _normalize_content_type(str(key))
        if not ctype:
            continue
        try:
            count = int(value)
        except Exception:
            continue
        if count > 0:
            output[ctype] = count
    return output


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=_json_default)


def _prompt_text_version(text: str) -> str:
    digest = hashlib.sha1(text.encode("utf-8")).hexdigest()
    return digest[:12]


def _find_related_candidate_by_id(
    *,
    related_candidates: list[dict[str, Any]],
    content_item_id: int | None,
) -> dict[str, Any] | None:
    if content_item_id is None:
        return None
    target = int(content_item_id)
    for candidate in related_candidates:
        if int(candidate.get("content_item_id") or 0) == target:
            return candidate
    return None


def _text_contains_url(text: str, url: str) -> bool:
    body = str(text or "")
    target = str(url or "").strip()
    if not target:
        return False
    return target in body


_PROHIBITED_TEMPLATE_PATTERNS = [
    "la pregunta estrategica no es",
    "la cuestion no es",
    "el debate no es",
    "la competencia real no es",
    "la ecuacion es clara",
    "la clave esta en",
    "para un cpo esto implica",
]


def _validate_editorial_output(
    *,
    stage2: DraftStage2Output,
    selected_related: dict[str, Any] | None,
    enforce_related_with_candidates: bool,
    min_chars: int = 1600,
    max_chars: int = 3200,
) -> tuple[dict[str, bool], list[str]]:
    text = str(stage2.borrador_post or "")
    why_now = str(stage2.por_que_importa_ahora or "")
    refs = normalize_references(stage2.referencias_abstract)
    refs_urls = [str(item.get("url") or "").strip() for item in refs if str(item.get("url") or "").strip()]
    min_allowed = max(800, int(min_chars))
    max_allowed = max(min_allowed, int(max_chars))

    flags: dict[str, bool] = {
        "length_1800_2600": min_allowed <= len(text) <= max_allowed,
        "length_in_range": min_allowed <= len(text) <= max_allowed,
        "no_prohibited_templates": _has_no_prohibited_templates(text),
        "why_now_no_urls_or_attribution": _why_now_is_clean(why_now),
        "references_deduped": len(refs) == len(stage2.referencias_abstract),
        "references_urls_present_in_text": all(url in text for url in refs_urls),
        "no_hashtags": "#" not in text,
        "no_semicolons": ";" not in text,
        "no_emojis": not _contains_emoji(text),
    }

    selected_url = str((selected_related or {}).get("url") or "").strip()
    if enforce_related_with_candidates and selected_url:
        flags["related_url_integrated"] = selected_url in text
    elif enforce_related_with_candidates:
        flags["related_url_integrated"] = False
    else:
        flags["related_url_integrated"] = True

    errors: list[str] = []
    if not flags["length_1800_2600"]:
        errors.append(f"borrador_post debe estar entre {min_allowed} y {max_allowed} caracteres.")
    if not flags["no_prohibited_templates"]:
        errors.append("borrador_post contiene una plantilla prohibida de Posts2.")
    if not flags["why_now_no_urls_or_attribution"]:
        errors.append("por_que_importa_ahora contiene URL o atribuciones/fuentes.")
    if not flags["references_deduped"]:
        errors.append("referencias_abstract no esta deduplicado.")
    if not flags["references_urls_present_in_text"]:
        errors.append("Hay referencias con URL que no aparecen en borrador_post.")
    if not flags["related_url_integrated"]:
        errors.append("No se integra la URL exacta del contenido relacionado seleccionado.")
    if not flags["no_hashtags"]:
        errors.append("borrador_post contiene hashtags y no esta permitido.")
    if not flags["no_semicolons"]:
        errors.append("borrador_post contiene punto y coma y no esta permitido.")
    if not flags["no_emojis"]:
        errors.append("borrador_post contiene emojis y no esta permitido.")

    return flags, errors


def _only_length_contract_error(errors: list[str]) -> bool:
    normalized = [str(item).strip().lower() for item in errors if str(item).strip()]
    if not normalized:
        return False
    return all("borrador_post debe estar entre" in item and "caracteres" in item for item in normalized)


def _has_no_prohibited_templates(text: str) -> bool:
    sample = normalize_for_match(text)
    for pattern in _PROHIBITED_TEMPLATE_PATTERNS:
        if pattern in sample:
            return False
    return True


def _why_now_is_clean(text: str) -> bool:
    sample = str(text or "").strip()
    if not sample:
        return False
    lower = sample.lower()
    if "http://" in lower or "https://" in lower or "www." in lower:
        return False
    if "fuente" in lower or "newsletter" in lower:
        return False
    if re.search(r"\([^)]*(fuente|http|www|newsletter)[^)]*\)", lower):
        return False
    return True


def _contains_emoji(text: str) -> bool:
    if not text:
        return False
    return bool(
        re.search(
            r"[\U0001F300-\U0001FAFF\U00002700-\U000027BF\U0001F1E6-\U0001F1FF]",
            text,
        )
    )


def _token_overlap_ratio(*, anchor_tokens: set[str], text: str) -> float:
    if not anchor_tokens:
        return 0.0
    candidate_tokens = set(normalize_for_match(text).split())
    if not candidate_tokens:
        return 0.0
    overlap = len(anchor_tokens & candidate_tokens)
    if overlap <= 0:
        return 0.0
    return overlap / float(max(1, len(anchor_tokens)))


def _compute_stage_percentiles(
    stage_samples: dict[str, list[float]],
    percentile: float,
) -> dict[str, float]:
    output: dict[str, float] = {}
    for stage, samples in stage_samples.items():
        if not samples:
            continue
        sorted_samples = sorted(float(value) for value in samples)
        if len(sorted_samples) == 1:
            output[stage] = round(sorted_samples[0], 2)
            continue
        position = (len(sorted_samples) - 1) * float(percentile)
        low = int(math.floor(position))
        high = int(math.ceil(position))
        if low == high:
            value = sorted_samples[low]
        else:
            ratio = position - low
            value = sorted_samples[low] + (sorted_samples[high] - sorted_samples[low]) * ratio
        output[stage] = round(value, 2)
    return output


def _run_callable_with_timeout(*, fn: Any, timeout_seconds: float) -> Any:
    timeout = max(0.1, float(timeout_seconds))
    result_q: queue.Queue[tuple[str, Any]] = queue.Queue(maxsize=1)

    def _runner() -> None:
        try:
            value = fn()
        except Exception as exc:  # pragma: no cover - passthrough
            result_q.put(("error", exc))
            return
        result_q.put(("ok", value))

    worker = threading.Thread(target=_runner, daemon=True)
    worker.start()
    worker.join(timeout=timeout)
    if worker.is_alive():
        raise TimeoutError(f"Stage timeout excedido ({timeout:.1f}s).")
    if result_q.empty():
        raise RuntimeError("Ejecucion de etapa sin resultado.")
    status, payload = result_q.get_nowait()
    if status == "error":
        raise payload
    return payload


def _select_slowest_stage(stage_samples: dict[str, list[float]]) -> str:
    p95 = _compute_stage_percentiles(stage_samples, 0.95)
    if not p95:
        return ""
    return max(p95.items(), key=lambda item: float(item[1]))[0]


def _is_transient_http_error(exc: Exception) -> bool:
    message = str(exc).lower()
    if "timed out" in message or "timeout" in message:
        return True
    if "temporary failure" in message:
        return True
    if "connection reset" in message or "connection refused" in message:
        return True
    for code in ("http 429", "http 500", "http 502", "http 503", "http 504"):
        if code in message:
            return True
    return False


def _select_related_candidates(
    *,
    candidates: list[dict[str, Any]],
    top_k: int,
    forced_counts: dict[str, int],
    available_types: list[str],
) -> list[dict[str, Any]]:
    if top_k <= 0 or not candidates:
        return []

    normalized: list[dict[str, Any]] = []
    allowed = {_normalize_content_type(item) for item in available_types if _normalize_content_type(item)}

    for row in candidates:
        if not isinstance(row, dict):
            continue
        content_type = _normalize_content_type(str(row.get("content_type") or "")) or "other"
        if allowed and content_type not in allowed:
            continue
        item = dict(row)
        item["content_type"] = content_type
        item.setdefault("matched_chunks", row.get("matched_chunks") if isinstance(row.get("matched_chunks"), list) else [])
        normalized.append(item)

    if not normalized:
        return []

    by_type: dict[str, list[dict[str, Any]]] = {}
    for item in normalized:
        by_type.setdefault(str(item.get("content_type") or "other"), []).append(item)
    for rows in by_type.values():
        rows.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)

    selected: list[dict[str, Any]] = []
    selected_ids: set[int] = set()

    normalized_forced: dict[str, int] = {}
    for key, value in forced_counts.items():
        norm = _normalize_content_type(str(key))
        try:
            count = int(value)
        except Exception:
            continue
        if norm and count > 0:
            normalized_forced[norm] = count

    if not normalized_forced:
        for ctype in by_type.keys():
            normalized_forced[ctype] = 1

    for ctype, min_count in normalized_forced.items():
        rows = by_type.get(ctype) or []
        taken = 0
        for row in rows:
            cid = int(row.get("content_item_id") or 0)
            if cid <= 0 or cid in selected_ids:
                continue
            selected.append(row)
            selected_ids.add(cid)
            taken += 1
            if taken >= min_count or len(selected) >= top_k:
                break
        if len(selected) >= top_k:
            break

    ranked = sorted(normalized, key=lambda x: float(x.get("score") or 0.0), reverse=True)
    for row in ranked:
        cid = int(row.get("content_item_id") or 0)
        if cid <= 0 or cid in selected_ids:
            continue
        selected.append(row)
        selected_ids.add(cid)
        if len(selected) >= top_k:
            break

    selected.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
    return selected[:top_k]


def _build_slack_summary_text(
    *,
    title: str,
    why_now: str,
    topic_title: str,
    edit_url: str,
    selected_related: dict[str, Any],
) -> str:
    related_title = str(selected_related.get("title") or "").strip()
    related_url = str(selected_related.get("url") or "").strip()
    related_line = "Sin contenido relacionado integrado."
    if related_title:
        related_line = f"{related_title}"
        if related_url:
            related_line = f"{related_title} ({related_url})"

    lines = [
        "────────────────────────",
        f"*{title.strip() or topic_title.strip() or 'Draft LinkedIn'}*",
        f"*Topic origen:* {topic_title.strip()}",
        "*Por qué importa ahora:*",
        why_now.strip(),
        "*Contenido relacionado integrado:*",
        related_line,
    ]
    if edit_url.strip():
        lines.append(f"*Editar draft:* {edit_url.strip()}")
    lines.append("_💬 Borrador final en hilo_")
    return "\n".join(lines)


def _build_slack_thread_text(final_draft: str) -> str:
    return "\n".join([
        "*Borrador final:*",
        "```",
        final_draft.strip(),
        "```",
    ])


def _build_slack_run_intro_text(*, total_items: int) -> str:
    separators = "\n".join(["---"] * max(1, total_items))
    return (
        f"<!channel>\n"
        f"Nuevas {total_items} propuestas de LinkedIn generadas. Las publico debajo para revision.\n"
        f"{separators}"
    )


def _resolve_editor_url(editor_base_url: str, payload: dict[str, Any]) -> str:
    edit = str(payload.get("editUrl") or payload.get("edit_url") or "").strip()
    if edit.startswith("http://") or edit.startswith("https://"):
        return edit
    base = editor_base_url.strip().rstrip("/")
    if not base:
        return edit
    if not edit:
        return ""
    if edit.startswith("/"):
        return f"{base}{edit}"
    return f"{base}/{edit}"


def _post_json(
    *,
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str],
    timeout: int,
) -> dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            text = resp.read().decode("utf-8")
            if not text.strip():
                return {}
            return json.loads(text)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"HTTP {exc.code}: {raw}") from exc
