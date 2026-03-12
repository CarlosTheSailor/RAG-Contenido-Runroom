from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from src.pipeline.storage import SupabaseStorage

from .models import SourceDocumentInput, ThemeRunConfig, ThemeTopicFilters


class ThemeIntelRepository:
    def __init__(self, storage: SupabaseStorage):
        self._storage = storage
        self._conn = storage._conn  # Internal reuse to avoid opening extra connections.

    def create_run(self, config: ThemeRunConfig, triggered_by_email: str | None) -> int:
        query = """
        INSERT INTO theme_runs (
            status,
            source_type,
            source_account,
            gmail_query,
            origin_category,
            mark_as_read,
            limit_messages,
            triggered_by_email
        ) VALUES (
            'queued',
            %(source_type)s,
            %(source_account)s,
            %(gmail_query)s,
            %(origin_category)s,
            %(mark_as_read)s,
            %(limit_messages)s,
            %(triggered_by_email)s
        )
        RETURNING id
        """
        with self._conn.cursor() as cur:
            cur.execute(
                query,
                {
                    "source_type": config.source_type,
                    "source_account": config.source_account,
                    "gmail_query": config.gmail_query,
                    "origin_category": config.origin_category,
                    "mark_as_read": config.mark_as_read,
                    "limit_messages": config.limit_messages,
                    "triggered_by_email": triggered_by_email,
                },
            )
            row = cur.fetchone()
        self._conn.commit()
        assert row is not None
        return int(row["id"])

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT * FROM theme_runs WHERE id = %s LIMIT 1", (run_id,))
            row = cur.fetchone()
        return row

    def get_latest_run(self) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM theme_runs
                ORDER BY id DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
        return row

    def try_acquire_scheduler_lock(self, lock_key: int) -> bool:
        with self._conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s) AS locked", (int(lock_key),))
            row = cur.fetchone()
        return bool(row and row.get("locked"))

    def release_scheduler_lock(self, lock_key: int) -> None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (int(lock_key),))

    def create_schedule(
        self,
        *,
        name: str,
        enabled: bool,
        every_n_days: int,
        run_time_local: str,
        timezone: str,
        next_run_at_utc: datetime | None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO theme_schedules (
                    name,
                    enabled,
                    every_n_days,
                    run_time_local,
                    timezone,
                    next_run_at_utc,
                    metadata_json
                ) VALUES (
                    %(name)s,
                    %(enabled)s,
                    %(every_n_days)s,
                    %(run_time_local)s,
                    %(timezone)s,
                    %(next_run_at_utc)s,
                    %(metadata_json)s::jsonb
                )
                RETURNING *
                """,
                {
                    "name": name,
                    "enabled": enabled,
                    "every_n_days": int(every_n_days),
                    "run_time_local": run_time_local,
                    "timezone": timezone,
                    "next_run_at_utc": next_run_at_utc,
                    "metadata_json": json.dumps(metadata or {}, ensure_ascii=False),
                },
            )
            row = cur.fetchone()
        self._conn.commit()
        assert row is not None
        return dict(row)

    def list_schedules(self) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM theme_schedules
                ORDER BY id DESC
                """
            )
            rows = cur.fetchall()
        return [dict(row) for row in rows]

    def get_schedule(self, schedule_id: int) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM theme_schedules
                WHERE id = %s
                LIMIT 1
                """,
                (schedule_id,),
            )
            row = cur.fetchone()
        return dict(row) if row else None

    def list_due_schedules(self, now_utc: datetime) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM theme_schedules
                WHERE enabled = true
                  AND next_run_at_utc IS NOT NULL
                  AND next_run_at_utc <= %s
                ORDER BY next_run_at_utc, id
                """,
                (now_utc,),
            )
            rows = cur.fetchall()
        return [dict(row) for row in rows]

    def update_schedule(self, schedule_id: int, patch: dict[str, Any]) -> dict[str, Any] | None:
        allowed = {
            "name",
            "enabled",
            "every_n_days",
            "run_time_local",
            "timezone",
            "next_run_at_utc",
            "last_run_at_utc",
            "metadata_json",
        }
        sets: list[str] = []
        params: dict[str, Any] = {"schedule_id": schedule_id}
        for key, value in patch.items():
            if key not in allowed:
                continue
            param_key = f"value_{key}"
            if key == "metadata_json":
                sets.append(f"{key} = %({param_key})s::jsonb")
                params[param_key] = json.dumps(value or {}, ensure_ascii=False)
            else:
                sets.append(f"{key} = %({param_key})s")
                params[param_key] = value

        if not sets:
            return self.get_schedule(schedule_id=schedule_id)

        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE theme_schedules
                SET {", ".join(sets)}
                WHERE id = %(schedule_id)s
                RETURNING *
                """,
                params,
            )
            row = cur.fetchone()
        self._conn.commit()
        return dict(row) if row else None

    def create_schedule_config(
        self,
        *,
        schedule_id: int,
        execution_order: int,
        gmail_query: str,
        origin_category: str,
        mark_as_read: bool,
        limit_messages: int,
        enabled: bool,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO theme_schedule_configs (
                    schedule_id,
                    execution_order,
                    gmail_query,
                    origin_category,
                    mark_as_read,
                    limit_messages,
                    enabled,
                    metadata_json
                ) VALUES (
                    %(schedule_id)s,
                    %(execution_order)s,
                    %(gmail_query)s,
                    %(origin_category)s,
                    %(mark_as_read)s,
                    %(limit_messages)s,
                    %(enabled)s,
                    %(metadata_json)s::jsonb
                )
                RETURNING *
                """,
                {
                    "schedule_id": schedule_id,
                    "execution_order": execution_order,
                    "gmail_query": gmail_query,
                    "origin_category": origin_category,
                    "mark_as_read": mark_as_read,
                    "limit_messages": limit_messages,
                    "enabled": enabled,
                    "metadata_json": json.dumps(metadata or {}, ensure_ascii=False),
                },
            )
            row = cur.fetchone()
        self._conn.commit()
        assert row is not None
        return dict(row)

    def get_schedule_config(self, schedule_id: int, config_id: int) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM theme_schedule_configs
                WHERE schedule_id = %s
                  AND id = %s
                LIMIT 1
                """,
                (schedule_id, config_id),
            )
            row = cur.fetchone()
        return dict(row) if row else None

    def list_schedule_configs(self, schedule_id: int, enabled_only: bool = False) -> list[dict[str, Any]]:
        query = """
        SELECT *
        FROM theme_schedule_configs
        WHERE schedule_id = %(schedule_id)s
        """
        if enabled_only:
            query += " AND enabled = true"
        query += " ORDER BY execution_order, id"

        with self._conn.cursor() as cur:
            cur.execute(query, {"schedule_id": schedule_id})
            rows = cur.fetchall()
        return [dict(row) for row in rows]

    def update_schedule_config(self, schedule_id: int, config_id: int, patch: dict[str, Any]) -> dict[str, Any] | None:
        allowed = {
            "execution_order",
            "gmail_query",
            "origin_category",
            "mark_as_read",
            "limit_messages",
            "enabled",
            "metadata_json",
        }
        sets: list[str] = []
        params: dict[str, Any] = {"schedule_id": schedule_id, "config_id": config_id}
        for key, value in patch.items():
            if key not in allowed:
                continue
            param_key = f"value_{key}"
            if key == "metadata_json":
                sets.append(f"{key} = %({param_key})s::jsonb")
                params[param_key] = json.dumps(value or {}, ensure_ascii=False)
            else:
                sets.append(f"{key} = %({param_key})s")
                params[param_key] = value

        if not sets:
            return self.get_schedule_config(schedule_id=schedule_id, config_id=config_id)

        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE theme_schedule_configs
                SET {", ".join(sets)}
                WHERE schedule_id = %(schedule_id)s
                  AND id = %(config_id)s
                RETURNING *
                """,
                params,
            )
            row = cur.fetchone()
        self._conn.commit()
        return dict(row) if row else None

    def create_schedule_execution(self, schedule_id: int, trigger_type: str) -> dict[str, Any]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO theme_schedule_executions (
                    schedule_id,
                    trigger_type,
                    status
                ) VALUES (
                    %s,
                    %s,
                    'running'
                )
                RETURNING *
                """,
                (schedule_id, trigger_type),
            )
            row = cur.fetchone()
        self._conn.commit()
        assert row is not None
        return dict(row)

    def finalize_schedule_execution(
        self,
        execution_id: int,
        *,
        status: str,
        stats: dict[str, Any],
        errors: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE theme_schedule_executions
                SET
                    status = %(status)s,
                    finished_at = now(),
                    stats_json = %(stats_json)s::jsonb,
                    errors_json = %(errors_json)s::jsonb
                WHERE id = %(execution_id)s
                RETURNING *
                """,
                {
                    "execution_id": execution_id,
                    "status": status,
                    "stats_json": json.dumps(stats, ensure_ascii=False),
                    "errors_json": json.dumps(errors, ensure_ascii=False),
                },
            )
            row = cur.fetchone()
        self._conn.commit()
        return dict(row) if row else None

    def create_schedule_execution_item(
        self,
        *,
        execution_id: int,
        schedule_config_id: int,
        execution_order: int,
    ) -> dict[str, Any]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO theme_schedule_execution_items (
                    execution_id,
                    schedule_config_id,
                    execution_order,
                    status
                ) VALUES (
                    %s,
                    %s,
                    %s,
                    'running'
                )
                RETURNING *
                """,
                (execution_id, schedule_config_id, execution_order),
            )
            row = cur.fetchone()
        self._conn.commit()
        assert row is not None
        return dict(row)

    def finalize_schedule_execution_item(
        self,
        item_id: int,
        *,
        status: str,
        theme_run_id: int | None,
        stats: dict[str, Any],
        errors: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE theme_schedule_execution_items
                SET
                    status = %(status)s,
                    theme_run_id = %(theme_run_id)s,
                    finished_at = now(),
                    stats_json = %(stats_json)s::jsonb,
                    errors_json = %(errors_json)s::jsonb
                WHERE id = %(item_id)s
                RETURNING *
                """,
                {
                    "item_id": item_id,
                    "status": status,
                    "theme_run_id": theme_run_id,
                    "stats_json": json.dumps(stats, ensure_ascii=False),
                    "errors_json": json.dumps(errors, ensure_ascii=False),
                },
            )
            row = cur.fetchone()
        self._conn.commit()
        return dict(row) if row else None

    def list_schedule_executions(self, schedule_id: int, limit: int = 20) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM theme_schedule_executions
                WHERE schedule_id = %s
                ORDER BY started_at DESC, id DESC
                LIMIT %s
                """,
                (schedule_id, limit),
            )
            rows = cur.fetchall()

        if not rows:
            return []

        execution_ids = [int(row["id"]) for row in rows]
        items_by_execution = self._list_schedule_execution_items(execution_ids=execution_ids)
        output: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            payload["items"] = items_by_execution.get(int(row["id"]), [])
            output.append(payload)
        return output

    def _list_schedule_execution_items(self, execution_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    esi.*,
                    sc.gmail_query,
                    sc.origin_category,
                    sc.mark_as_read,
                    sc.limit_messages
                FROM theme_schedule_execution_items esi
                LEFT JOIN theme_schedule_configs sc ON sc.id = esi.schedule_config_id
                WHERE esi.execution_id = ANY(%s)
                ORDER BY esi.execution_id, esi.execution_order, esi.id
                """,
                (execution_ids,),
            )
            rows = cur.fetchall()
        output: dict[int, list[dict[str, Any]]] = {}
        for row in rows:
            execution_id = int(row["execution_id"])
            output.setdefault(execution_id, []).append(dict(row))
        return output

    def set_run_running(self, run_id: int) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE theme_runs
                SET status = 'running', started_at = now()
                WHERE id = %s
                """,
                (run_id,),
            )
        self._conn.commit()

    def finalize_run(self, run_id: int, status: str, stats: dict[str, Any], errors: list[dict[str, Any]]) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE theme_runs
                SET
                    status = %(status)s,
                    finished_at = now(),
                    stats_json = %(stats_json)s::jsonb,
                    errors_json = %(errors_json)s::jsonb
                WHERE id = %(run_id)s
                """,
                {
                    "status": status,
                    "stats_json": json.dumps(stats, ensure_ascii=False),
                    "errors_json": json.dumps(errors, ensure_ascii=False),
                    "run_id": run_id,
                },
            )
        self._conn.commit()

    def update_run_progress(
        self,
        run_id: int,
        stats: dict[str, Any],
        errors: list[dict[str, Any]] | None = None,
    ) -> None:
        with self._conn.cursor() as cur:
            if errors is None:
                cur.execute(
                    """
                    UPDATE theme_runs
                    SET
                        status = 'running',
                        stats_json = %(stats_json)s::jsonb
                    WHERE id = %(run_id)s
                    """,
                    {
                        "run_id": run_id,
                        "stats_json": json.dumps(stats, ensure_ascii=False),
                    },
                )
            else:
                cur.execute(
                    """
                    UPDATE theme_runs
                    SET
                        status = 'running',
                        stats_json = %(stats_json)s::jsonb,
                        errors_json = %(errors_json)s::jsonb
                    WHERE id = %(run_id)s
                    """,
                    {
                        "run_id": run_id,
                        "stats_json": json.dumps(stats, ensure_ascii=False),
                        "errors_json": json.dumps(errors, ensure_ascii=False),
                    },
                )
        self._conn.commit()

    def ensure_category(self, key: str, label: str, source: str = "origin") -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO theme_categories (key, label, source, status)
                VALUES (%s, %s, %s, 'active')
                ON CONFLICT (key)
                DO UPDATE SET
                    label = EXCLUDED.label,
                    source = COALESCE(theme_categories.source, EXCLUDED.source),
                    status = 'active'
                """,
                (key, label, source),
            )
        self._conn.commit()

    def upsert_source_document(self, run_id: int, doc: SourceDocumentInput, source_type: str, source_account: str) -> int:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO source_documents (
                    run_id,
                    source_type,
                    source_account,
                    source_external_id,
                    source_thread_id,
                    subject,
                    sender,
                    received_at,
                    labels_json,
                    links_json,
                    raw_text,
                    cleaned_text,
                    metadata_json
                ) VALUES (
                    %(run_id)s,
                    %(source_type)s,
                    %(source_account)s,
                    %(source_external_id)s,
                    %(source_thread_id)s,
                    %(subject)s,
                    %(sender)s,
                    %(received_at)s,
                    %(labels_json)s::jsonb,
                    %(links_json)s::jsonb,
                    %(raw_text)s,
                    %(cleaned_text)s,
                    %(metadata_json)s::jsonb
                )
                ON CONFLICT (source_type, source_account, source_external_id)
                DO UPDATE SET
                    run_id = EXCLUDED.run_id,
                    source_thread_id = EXCLUDED.source_thread_id,
                    subject = EXCLUDED.subject,
                    sender = EXCLUDED.sender,
                    received_at = EXCLUDED.received_at,
                    labels_json = EXCLUDED.labels_json,
                    links_json = EXCLUDED.links_json,
                    raw_text = EXCLUDED.raw_text,
                    cleaned_text = EXCLUDED.cleaned_text,
                    metadata_json = EXCLUDED.metadata_json
                RETURNING id
                """,
                {
                    "run_id": run_id,
                    "source_type": source_type,
                    "source_account": source_account,
                    "source_external_id": doc.source_external_id,
                    "source_thread_id": doc.source_thread_id,
                    "subject": doc.subject,
                    "sender": doc.sender,
                    "received_at": doc.received_at,
                    "labels_json": json.dumps(doc.labels, ensure_ascii=False),
                    "links_json": json.dumps(doc.links, ensure_ascii=False),
                    "raw_text": doc.raw_text,
                    "cleaned_text": doc.cleaned_text,
                    "metadata_json": json.dumps(doc.metadata, ensure_ascii=False),
                },
            )
            row = cur.fetchone()
        self._conn.commit()
        assert row is not None
        return int(row["id"])

    def find_similar_topic(
        self,
        embedding: list[float],
        primary_category_key: str,
        similarity_threshold: float,
        window_days: int,
    ) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    tt.id,
                    tt.title,
                    tt.score,
                    (1 - (te.embedding <=> %(vec)s::vector)) AS similarity
                FROM theme_topic_embeddings te
                JOIN theme_topics tt ON tt.id = te.topic_id
                WHERE tt.primary_category_key = %(category)s
                  AND tt.last_seen_at >= (now() - make_interval(days => %(window_days)s))
                  AND (1 - (te.embedding <=> %(vec)s::vector)) >= %(threshold)s
                ORDER BY te.embedding <=> %(vec)s::vector
                LIMIT 1
                """,
                {
                    "vec": _vector_literal(embedding),
                    "category": primary_category_key,
                    "window_days": int(window_days),
                    "threshold": similarity_threshold,
                },
            )
            row = cur.fetchone()
        return row

    def create_topic(
        self,
        run_id: int,
        title: str,
        context_text: str,
        canonical_text: str,
        primary_category_key: str,
        score: float,
        origin_source_type: str,
        origin_source_account: str,
        origin_query: str,
        metadata: dict[str, Any],
    ) -> int:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO theme_topics (
                    run_id,
                    title,
                    context_text,
                    canonical_text,
                    primary_category_key,
                    score,
                    origin_source_type,
                    origin_source_account,
                    origin_query,
                    metadata_json
                ) VALUES (
                    %(run_id)s,
                    %(title)s,
                    %(context_text)s,
                    %(canonical_text)s,
                    %(primary_category_key)s,
                    %(score)s,
                    %(origin_source_type)s,
                    %(origin_source_account)s,
                    %(origin_query)s,
                    %(metadata_json)s::jsonb
                )
                RETURNING id
                """,
                {
                    "run_id": run_id,
                    "title": title,
                    "context_text": context_text,
                    "canonical_text": canonical_text,
                    "primary_category_key": primary_category_key,
                    "score": score,
                    "origin_source_type": origin_source_type,
                    "origin_source_account": origin_source_account,
                    "origin_query": origin_query,
                    "metadata_json": json.dumps(metadata, ensure_ascii=False),
                },
            )
            row = cur.fetchone()
        self._conn.commit()
        assert row is not None
        return int(row["id"])

    def touch_topic(self, topic_id: int, run_id: int, score: float, metadata: dict[str, Any]) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE theme_topics
                SET
                    run_id = %(run_id)s,
                    score = GREATEST(score, %(score)s),
                    metadata_json = metadata_json || %(metadata_json)s::jsonb,
                    times_seen = times_seen + 1,
                    last_seen_at = now()
                WHERE id = %(topic_id)s
                """,
                {
                    "run_id": run_id,
                    "score": score,
                    "metadata_json": json.dumps(metadata, ensure_ascii=False),
                    "topic_id": topic_id,
                },
            )
        self._conn.commit()

    def upsert_topic_embedding(self, topic_id: int, embedding: list[float], model: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO theme_topic_embeddings (topic_id, embedding, model, updated_at)
                VALUES (%(topic_id)s, %(embedding)s::vector, %(model)s, now())
                ON CONFLICT (topic_id)
                DO UPDATE SET
                    embedding = EXCLUDED.embedding,
                    model = EXCLUDED.model,
                    updated_at = now()
                """,
                {
                    "topic_id": topic_id,
                    "embedding": _vector_literal(embedding),
                    "model": model,
                },
            )
        self._conn.commit()

    def upsert_topic_tag(self, topic_id: int, tag_key: str, tag_label: str, provenance: str, confidence: float | None) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO theme_topic_tags (topic_id, tag_key, tag_label, provenance, confidence)
                VALUES (%(topic_id)s, %(tag_key)s, %(tag_label)s, %(provenance)s, %(confidence)s)
                ON CONFLICT (topic_id, tag_key)
                DO UPDATE SET
                    tag_label = EXCLUDED.tag_label,
                    provenance = EXCLUDED.provenance,
                    confidence = EXCLUDED.confidence
                """,
                {
                    "topic_id": topic_id,
                    "tag_key": tag_key,
                    "tag_label": tag_label,
                    "provenance": provenance,
                    "confidence": confidence,
                },
            )
        self._conn.commit()

    def insert_evidence(
        self,
        topic_id: int,
        source_document_id: int | None,
        dato: str,
        fuente: str,
        texto_fuente_breve: str,
        url_referencia: str,
        newsletter_origen: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO theme_evidences (
                    topic_id,
                    source_document_id,
                    dato,
                    fuente,
                    texto_fuente_breve,
                    url_referencia,
                    newsletter_origen,
                    metadata_json
                ) VALUES (
                    %(topic_id)s,
                    %(source_document_id)s,
                    %(dato)s,
                    %(fuente)s,
                    %(texto_fuente_breve)s,
                    %(url_referencia)s,
                    %(newsletter_origen)s,
                    %(metadata_json)s::jsonb
                )
                """,
                {
                    "topic_id": topic_id,
                    "source_document_id": source_document_id,
                    "dato": dato,
                    "fuente": fuente,
                    "texto_fuente_breve": texto_fuente_breve,
                    "url_referencia": url_referencia,
                    "newsletter_origen": newsletter_origen,
                    "metadata_json": json.dumps(metadata or {}, ensure_ascii=False),
                },
            )
        if source_document_id is not None:
            self.upsert_topic_source_document(
                topic_id=topic_id,
                source_document_id=source_document_id,
                link_type="evidence",
                metadata={"source": "evidence"},
            )
        self._conn.commit()

    def upsert_topic_source_document(
        self,
        topic_id: int,
        source_document_id: int,
        link_type: str = "run_scope",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO theme_topic_source_documents (
                    topic_id,
                    source_document_id,
                    link_type,
                    metadata_json
                ) VALUES (
                    %(topic_id)s,
                    %(source_document_id)s,
                    %(link_type)s,
                    %(metadata_json)s::jsonb
                )
                ON CONFLICT (topic_id, source_document_id)
                DO UPDATE SET
                    link_type = CASE
                        WHEN theme_topic_source_documents.link_type = 'evidence' THEN 'evidence'
                        WHEN EXCLUDED.link_type = 'evidence' THEN 'evidence'
                        WHEN theme_topic_source_documents.link_type = 'primary' THEN 'primary'
                        WHEN EXCLUDED.link_type = 'primary' THEN 'primary'
                        ELSE theme_topic_source_documents.link_type
                    END,
                    metadata_json = theme_topic_source_documents.metadata_json || EXCLUDED.metadata_json,
                    updated_at = now()
                """,
                {
                    "topic_id": topic_id,
                    "source_document_id": source_document_id,
                    "link_type": link_type,
                    "metadata_json": json.dumps(metadata or {}, ensure_ascii=False),
                },
            )
        self._conn.commit()

    def replace_related_content(self, topic_id: int, related_items: list[dict[str, Any]]) -> None:
        with self._conn.cursor() as cur:
            cur.execute("DELETE FROM theme_related_content WHERE topic_id = %s", (topic_id,))
            for rank, item in enumerate(related_items, start=1):
                content_item_id = item.get("content_item_id")
                if not isinstance(content_item_id, int):
                    continue
                score = float(item.get("score") or 0.0)
                cur.execute(
                    """
                    INSERT INTO theme_related_content (
                        topic_id,
                        content_item_id,
                        relation_rank,
                        score,
                        rationale,
                        metadata_json,
                        computed_at
                    ) VALUES (
                        %(topic_id)s,
                        %(content_item_id)s,
                        %(relation_rank)s,
                        %(score)s,
                        %(rationale)s,
                        %(metadata_json)s::jsonb,
                        now()
                    )
                    ON CONFLICT (topic_id, content_item_id)
                    DO UPDATE SET
                        relation_rank = EXCLUDED.relation_rank,
                        score = EXCLUDED.score,
                        rationale = EXCLUDED.rationale,
                        metadata_json = EXCLUDED.metadata_json,
                        computed_at = now()
                    """,
                    {
                        "topic_id": topic_id,
                        "content_item_id": content_item_id,
                        "relation_rank": rank,
                        "score": score,
                        "rationale": str(item.get("rationale") or ""),
                        "metadata_json": json.dumps(item, ensure_ascii=False),
                    },
                )
        self._conn.commit()

    def get_topic(self, topic_id: int) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT * FROM theme_topics WHERE id = %s LIMIT 1", (topic_id,))
            row = cur.fetchone()
        return row

    def update_topic_status(self, topic_id: int, status: str) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE theme_topics
                SET status = %s
                WHERE id = %s
                RETURNING *
                """,
                (status, topic_id),
            )
            row = cur.fetchone()
        self._conn.commit()
        return row

    def insert_topic_usage(
        self,
        topic_id: int,
        client_name: str,
        artifact_id: str | None,
        metadata: dict[str, Any] | None = None,
    ) -> int:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO theme_topic_usage (topic_id, client_name, artifact_id, metadata_json)
                VALUES (%(topic_id)s, %(client_name)s, %(artifact_id)s, %(metadata_json)s::jsonb)
                RETURNING id
                """,
                {
                    "topic_id": topic_id,
                    "client_name": client_name,
                    "artifact_id": artifact_id,
                    "metadata_json": json.dumps(metadata or {}, ensure_ascii=False),
                },
            )
            row = cur.fetchone()
        self._conn.commit()
        assert row is not None
        return int(row["id"])

    def list_topics(self, filters: ThemeTopicFilters, semantic_vector: list[float] | None = None) -> list[dict[str, Any]]:
        select_score = "NULL::double precision AS semantic_score"
        joins = "LEFT JOIN theme_categories tc ON tc.key = tt.primary_category_key"
        params: dict[str, Any] = {"limit": filters.limit, "offset": filters.offset}
        if semantic_vector is not None:
            joins += " LEFT JOIN theme_topic_embeddings te ON te.topic_id = tt.id"
            select_score = "(1 - (te.embedding <=> %(semantic_vec)s::vector)) AS semantic_score"
            params["semantic_vec"] = _vector_literal(semantic_vector)

        query = f"""
        SELECT
            tt.*,
            tc.label AS primary_category_label,
            {select_score}
        FROM theme_topics tt
        {joins}
        WHERE 1=1
        """

        if filters.primary_category:
            query += " AND tt.primary_category_key = %(primary_category)s"
            params["primary_category"] = filters.primary_category
        if filters.status:
            query += " AND tt.status = %(status)s"
            params["status"] = filters.status
        if filters.min_score is not None:
            query += " AND tt.score >= %(min_score)s"
            params["min_score"] = filters.min_score
        if filters.created_from is not None:
            query += " AND tt.created_at >= %(created_from)s"
            params["created_from"] = filters.created_from
        if filters.created_to is not None:
            query += " AND tt.created_at <= %(created_to)s"
            params["created_to"] = filters.created_to
        if filters.tag_any:
            query += """
            AND EXISTS (
                SELECT 1
                FROM theme_topic_tags ttag
                WHERE ttag.topic_id = tt.id
                  AND ttag.tag_key = ANY(%(tag_any)s)
            )
            """
            params["tag_any"] = filters.tag_any
        if filters.tag_all:
            query += """
            AND NOT EXISTS (
                SELECT 1
                FROM unnest(%(tag_all)s::text[]) AS req(tag)
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM theme_topic_tags ttag_all
                    WHERE ttag_all.topic_id = tt.id
                      AND ttag_all.tag_key = req.tag
                )
            )
            """
            params["tag_all"] = filters.tag_all

        if semantic_vector is not None:
            query += " ORDER BY te.embedding <=> %(semantic_vec)s::vector, tt.last_seen_at DESC"
        else:
            query += " ORDER BY tt.last_seen_at DESC"
        query += " LIMIT %(limit)s OFFSET %(offset)s"

        with self._conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        if not rows:
            return []

        topic_ids = [int(row["id"]) for row in rows]
        tags_by_topic = self._list_tags_by_topic(topic_ids)
        related_by_topic = self._list_related_by_topic(topic_ids)

        output: list[dict[str, Any]] = []
        for row in rows:
            topic_id = int(row["id"])
            payload = dict(row)
            payload["tags"] = tags_by_topic.get(topic_id, [])
            payload["related_content"] = related_by_topic.get(topic_id, [])
            output.append(payload)
        return output

    def list_topics_for_recent_origin_runs(self, origin_category: str, days: int) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT
                    tt.id,
                    tt.run_id,
                    tt.title,
                    tt.context_text,
                    tt.canonical_text,
                    tt.primary_category_key,
                    tr.origin_category,
                    tt.last_seen_at
                FROM theme_topics tt
                JOIN theme_runs tr ON tr.id = tt.run_id
                WHERE tr.origin_category = %(origin_category)s
                  AND COALESCE(tr.started_at, tr.created_at) >= (now() - make_interval(days => %(days)s))
                ORDER BY tt.last_seen_at DESC NULLS LAST, tt.id DESC
                """,
                {
                    "origin_category": origin_category,
                    "days": int(days),
                },
            )
            rows = cur.fetchall()
        return [dict(row) for row in rows]

    def list_recent_origin_categories(self, days: int) -> list[str]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT tr.origin_category
                FROM theme_runs tr
                WHERE COALESCE(tr.started_at, tr.created_at) >= (now() - make_interval(days => %(days)s))
                  AND tr.origin_category IS NOT NULL
                  AND btrim(tr.origin_category) <> ''
                ORDER BY tr.origin_category
                """,
                {"days": int(days)},
            )
            rows = cur.fetchall()
        output: list[str] = []
        for row in rows:
            category = str(row.get("origin_category") or "").strip()
            if category:
                output.append(category)
        return output

    def list_source_documents_for_run(self, run_id: int) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    run_id,
                    source_type,
                    source_account,
                    source_external_id,
                    source_thread_id,
                    subject,
                    sender,
                    received_at,
                    labels_json,
                    links_json,
                    left(raw_text, 2000) AS raw_text_preview,
                    left(cleaned_text, 2000) AS cleaned_text_preview,
                    metadata_json,
                    created_at
                FROM source_documents
                WHERE run_id = %s
                ORDER BY received_at DESC NULLS LAST, id DESC
                """,
                (run_id,),
            )
            rows = cur.fetchall()
        return [dict(row) for row in rows]

    def get_topic_detail(self, topic_id: int) -> dict[str, Any] | None:
        topic = self.get_topic(topic_id=topic_id)
        if topic is None:
            return None

        tags = self._list_tags_by_topic([topic_id]).get(topic_id, [])
        related = self._list_related_by_topic([topic_id]).get(topic_id, [])
        evidences = self._list_evidences_by_topic(topic_id=topic_id)
        usage = self._list_usage_by_topic(topic_id=topic_id)
        source_documents = self._list_source_documents_by_topic(topic_id=topic_id)

        payload = dict(topic)
        payload["tags"] = tags
        payload["related_content"] = related
        payload["evidences"] = evidences
        payload["usage"] = usage
        payload["source_documents"] = source_documents
        return payload

    def _list_tags_by_topic(self, topic_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT topic_id, tag_key, tag_label, provenance, confidence
                FROM theme_topic_tags
                WHERE topic_id = ANY(%s)
                ORDER BY topic_id, tag_key
                """,
                (topic_ids,),
            )
            rows = cur.fetchall()
        output: dict[int, list[dict[str, Any]]] = {}
        for row in rows:
            topic_id = int(row["topic_id"])
            output.setdefault(topic_id, []).append(dict(row))
        return output

    def _list_related_by_topic(self, topic_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    rc.topic_id,
                    rc.content_item_id,
                    rc.relation_rank,
                    rc.score,
                    rc.rationale,
                    ci.content_type,
                    ci.title,
                    ci.url
                FROM theme_related_content rc
                JOIN content_items ci ON ci.id = rc.content_item_id
                WHERE rc.topic_id = ANY(%s)
                ORDER BY rc.topic_id, rc.relation_rank
                """,
                (topic_ids,),
            )
            rows = cur.fetchall()
        output: dict[int, list[dict[str, Any]]] = {}
        for row in rows:
            topic_id = int(row["topic_id"])
            output.setdefault(topic_id, []).append(dict(row))
        return output

    def _list_evidences_by_topic(self, topic_id: int) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    topic_id,
                    source_document_id,
                    dato,
                    fuente,
                    texto_fuente_breve,
                    url_referencia,
                    newsletter_origen,
                    metadata_json,
                    created_at
                FROM theme_evidences
                WHERE topic_id = %s
                ORDER BY id
                """,
                (topic_id,),
            )
            rows = cur.fetchall()
        return [dict(row) for row in rows]

    def _list_usage_by_topic(self, topic_id: int) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    topic_id,
                    client_name,
                    artifact_id,
                    used_at,
                    metadata_json
                FROM theme_topic_usage
                WHERE topic_id = %s
                ORDER BY used_at DESC, id DESC
                """,
                (topic_id,),
            )
            rows = cur.fetchall()
        return [dict(row) for row in rows]

    def _list_source_documents_by_topic(self, topic_id: int) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    tsd.id AS link_id,
                    tsd.topic_id,
                    tsd.source_document_id,
                    tsd.link_type,
                    tsd.metadata_json AS link_metadata_json,
                    tsd.created_at AS linked_at,
                    sd.source_external_id,
                    sd.source_thread_id,
                    sd.subject,
                    sd.sender,
                    sd.received_at,
                    sd.labels_json,
                    sd.links_json,
                    left(sd.raw_text, 1500) AS raw_text_preview,
                    left(sd.cleaned_text, 1500) AS cleaned_text_preview
                FROM theme_topic_source_documents tsd
                JOIN source_documents sd ON sd.id = tsd.source_document_id
                WHERE tsd.topic_id = %s
                ORDER BY
                    CASE tsd.link_type
                        WHEN 'evidence' THEN 1
                        WHEN 'primary' THEN 2
                        ELSE 3
                    END,
                    sd.received_at DESC NULLS LAST,
                    tsd.id DESC
                """,
                (topic_id,),
            )
            rows = cur.fetchall()
        return [dict(row) for row in rows]


def _vector_literal(vector: list[float]) -> str:
    return "[" + ",".join(f"{float(value):.8f}" for value in vector) + "]"
