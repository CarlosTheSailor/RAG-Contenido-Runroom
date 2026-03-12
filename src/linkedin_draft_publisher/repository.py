from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

from src.pipeline.storage import SupabaseStorage


class LinkedInDraftPublisherRepository:
    def __init__(self, storage: SupabaseStorage):
        self._storage = storage
        self._conn = storage._conn

    def create_run(
        self,
        *,
        origin_category: str,
        slack_channel: str,
        buyer_persona_objetivo: str,
        offline_mode: bool,
        client_name: str,
        target_count: int,
        topics_fetch_limit: int,
        related_top_k: int,
        related_counts_by_type: dict[str, int],
        triggered_by_email: str | None,
    ) -> int:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO linkedin_draft_runs (
                    status,
                    origin_category,
                    slack_channel,
                    buyer_persona_objetivo,
                    offline_mode,
                    client_name,
                    target_count,
                    topics_fetch_limit,
                    related_top_k,
                    related_counts_by_type_json,
                    triggered_by_email
                ) VALUES (
                    'queued',
                    %(origin_category)s,
                    %(slack_channel)s,
                    %(buyer_persona_objetivo)s,
                    %(offline_mode)s,
                    %(client_name)s,
                    %(target_count)s,
                    %(topics_fetch_limit)s,
                    %(related_top_k)s,
                    %(related_counts_by_type_json)s::jsonb,
                    %(triggered_by_email)s
                )
                RETURNING id
                """,
                {
                    "origin_category": origin_category,
                    "slack_channel": slack_channel,
                    "buyer_persona_objetivo": buyer_persona_objetivo,
                    "offline_mode": bool(offline_mode),
                    "client_name": client_name,
                    "target_count": int(target_count),
                    "topics_fetch_limit": int(topics_fetch_limit),
                    "related_top_k": int(related_top_k),
                    "related_counts_by_type_json": _json_dumps(related_counts_by_type or {}),
                    "triggered_by_email": triggered_by_email,
                },
            )
            row = cur.fetchone()
        self._conn.commit()
        assert row is not None
        return int(row["id"])

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT * FROM linkedin_draft_runs WHERE id = %s LIMIT 1", (run_id,))
            row = cur.fetchone()
        return dict(row) if row else None

    def get_latest_run(self) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT * FROM linkedin_draft_runs ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
        return dict(row) if row else None

    def list_stale_running_runs(self, *, stale_minutes: int, exclude_run_id: int | None = None) -> list[dict[str, Any]]:
        stale_minutes = max(1, int(stale_minutes))
        with self._conn.cursor() as cur:
            if exclude_run_id is None:
                cur.execute(
                    """
                    SELECT *
                    FROM linkedin_draft_runs
                    WHERE status = 'running'
                      AND finished_at IS NULL
                      AND updated_at <= (now() - make_interval(mins => %(stale_minutes)s))
                    ORDER BY id ASC
                    """,
                    {"stale_minutes": stale_minutes},
                )
            else:
                cur.execute(
                    """
                    SELECT *
                    FROM linkedin_draft_runs
                    WHERE status = 'running'
                      AND finished_at IS NULL
                      AND id <> %(exclude_run_id)s
                      AND updated_at <= (now() - make_interval(mins => %(stale_minutes)s))
                    ORDER BY id ASC
                    """,
                    {
                        "stale_minutes": stale_minutes,
                        "exclude_run_id": int(exclude_run_id),
                    },
                )
            rows = cur.fetchall()
        return [dict(row) for row in rows]

    def update_run(self, run_id: int, patch: dict[str, Any]) -> dict[str, Any] | None:
        allowed = {
            "status",
            "started_at",
            "finished_at",
            "stats_json",
            "errors_json",
        }
        sets: list[str] = []
        params: dict[str, Any] = {"run_id": int(run_id)}
        for key, value in patch.items():
            if key not in allowed:
                continue
            param_key = f"value_{key}"
            if key in {"stats_json", "errors_json"}:
                sets.append(f"{key} = %({param_key})s::jsonb")
                params[param_key] = _json_dumps(value or ({} if key == "stats_json" else []))
            else:
                sets.append(f"{key} = %({param_key})s")
                params[param_key] = value

        if not sets:
            return self.get_run(run_id)

        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE linkedin_draft_runs
                SET {", ".join(sets)}
                WHERE id = %(run_id)s
                RETURNING *
                """,
                params,
            )
            row = cur.fetchone()
        self._conn.commit()
        return dict(row) if row else None

    def create_run_item(
        self,
        *,
        run_id: int,
        item_index: int,
        topic_id: int,
        topic_payload: dict[str, Any],
    ) -> int:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO linkedin_draft_run_items (
                    run_id,
                    topic_id,
                    item_index,
                    status,
                    topic_payload_json
                ) VALUES (
                    %(run_id)s,
                    %(topic_id)s,
                    %(item_index)s,
                    'queued',
                    %(topic_payload_json)s::jsonb
                )
                RETURNING id
                """,
                {
                    "run_id": int(run_id),
                    "topic_id": int(topic_id),
                    "item_index": int(item_index),
                    "topic_payload_json": _json_dumps(topic_payload or {}),
                },
            )
            row = cur.fetchone()
        self._conn.commit()
        assert row is not None
        return int(row["id"])

    def update_run_item(self, item_id: int, patch: dict[str, Any]) -> dict[str, Any] | None:
        allowed = {
            "status",
            "title",
            "draft_stage1_text",
            "draft_final_text",
            "topic_payload_json",
            "related_candidates_json",
            "related_selected_json",
            "references_json",
            "draft_publish_json",
            "slack_publish_json",
            "debug_json",
            "warnings_json",
            "errors_json",
            "started_at",
            "finished_at",
        }
        sets: list[str] = []
        params: dict[str, Any] = {"item_id": int(item_id)}
        json_fields = {
            "topic_payload_json",
            "related_candidates_json",
            "related_selected_json",
            "references_json",
            "draft_publish_json",
            "slack_publish_json",
            "debug_json",
            "warnings_json",
            "errors_json",
        }
        for key, value in patch.items():
            if key not in allowed:
                continue
            param_key = f"value_{key}"
            if key in json_fields:
                sets.append(f"{key} = %({param_key})s::jsonb")
                default: Any = {}
                if key in {"warnings_json", "errors_json", "related_candidates_json", "references_json"}:
                    default = []
                params[param_key] = _json_dumps(value if value is not None else default)
            else:
                sets.append(f"{key} = %({param_key})s")
                params[param_key] = value

        if not sets:
            return self.get_run_item(item_id)

        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE linkedin_draft_run_items
                SET {", ".join(sets)}
                WHERE id = %(item_id)s
                RETURNING *
                """,
                params,
            )
            row = cur.fetchone()
        self._conn.commit()
        return dict(row) if row else None

    def get_run_item(self, item_id: int) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT * FROM linkedin_draft_run_items WHERE id = %s LIMIT 1", (int(item_id),))
            row = cur.fetchone()
        return dict(row) if row else None

    def list_run_items(self, run_id: int) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM linkedin_draft_run_items
                WHERE run_id = %s
                ORDER BY item_index ASC, id ASC
                """,
                (int(run_id),),
            )
            rows = cur.fetchall()
        return [dict(row) for row in rows]

    def list_topic_candidates_unused_by_client(
        self,
        *,
        primary_category_key: str,
        client_name: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    tt.id,
                    tt.title,
                    tt.context_text,
                    tt.canonical_text,
                    tt.score,
                    tt.last_seen_at,
                    tt.first_seen_at,
                    tt.times_seen,
                    tt.primary_category_key,
                    tt.origin_query,
                    tt.origin_source_account
                FROM theme_topics tt
                WHERE tt.primary_category_key = %(primary_category_key)s
                  AND NOT EXISTS (
                    SELECT 1
                    FROM theme_topic_usage tu
                    WHERE tu.topic_id = tt.id
                      AND tu.client_name = %(client_name)s
                  )
                ORDER BY tt.last_seen_at DESC NULLS LAST, tt.score DESC, tt.id DESC
                LIMIT %(limit)s
                """,
                {
                    "primary_category_key": primary_category_key,
                    "client_name": client_name,
                    "limit": int(limit),
                },
            )
            rows = cur.fetchall()
        return [dict(row) for row in rows]

    def list_topic_candidates_by_category(
        self,
        *,
        primary_category_key: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    tt.id,
                    tt.title,
                    tt.context_text,
                    tt.canonical_text,
                    tt.score,
                    tt.last_seen_at,
                    tt.first_seen_at,
                    tt.times_seen,
                    tt.primary_category_key,
                    tt.origin_query,
                    tt.origin_source_account
                FROM theme_topics tt
                WHERE tt.primary_category_key = %(primary_category_key)s
                ORDER BY tt.last_seen_at DESC NULLS LAST, tt.score DESC, tt.id DESC
                LIMIT %(limit)s
                """,
                {
                    "primary_category_key": primary_category_key,
                    "limit": int(limit),
                },
            )
            rows = cur.fetchall()
        return [dict(row) for row in rows]

    def get_topic_bundle(self, topic_id: int) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '5000ms'")
            cur.execute("SELECT * FROM theme_topics WHERE id = %s LIMIT 1", (int(topic_id),))
            topic = cur.fetchone()
            if topic is None:
                return None

            cur.execute(
                """
                SELECT tag_key, tag_label, provenance, confidence
                FROM theme_topic_tags
                WHERE topic_id = %s
                ORDER BY tag_key
                """,
                (int(topic_id),),
            )
            tags = cur.fetchall()

            cur.execute(
                """
                SELECT
                    id,
                    dato,
                    fuente,
                    texto_fuente_breve,
                    url_referencia,
                    newsletter_origen,
                    metadata_json,
                    created_at
                FROM theme_evidences
                WHERE topic_id = %s
                ORDER BY id ASC
                """,
                (int(topic_id),),
            )
            evidences = cur.fetchall()

            cur.execute(
                """
                SELECT
                    tsd.id AS link_id,
                    tsd.link_type,
                    tsd.metadata_json AS link_metadata_json,
                    sd.id AS source_document_id,
                    sd.subject,
                    sd.sender,
                    sd.received_at,
                    sd.links_json,
                    sd.labels_json
                FROM theme_topic_source_documents tsd
                JOIN source_documents sd ON sd.id = tsd.source_document_id
                WHERE tsd.topic_id = %s
                ORDER BY
                    CASE tsd.link_type WHEN 'evidence' THEN 1 WHEN 'primary' THEN 2 ELSE 3 END,
                    sd.received_at DESC NULLS LAST,
                    tsd.id DESC
                LIMIT 30
                """,
                (int(topic_id),),
            )
            source_docs = cur.fetchall()

        return {
            "topic": dict(topic),
            "tags": [dict(row) for row in tags],
            "evidences": [dict(row) for row in evidences],
            "source_documents": [dict(row) for row in source_docs],
        }

    def list_content_types(self) -> list[str]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT content_type
                FROM content_items
                WHERE content_type IS NOT NULL
                  AND btrim(content_type) <> ''
                ORDER BY content_type
                """
            )
            rows = cur.fetchall()
        return [str(row["content_type"]).strip() for row in rows if str(row["content_type"]).strip()]

    def get_run_with_items(self, run_id: int) -> dict[str, Any] | None:
        run = self.get_run(run_id=run_id)
        if run is None:
            return None
        items = self.list_run_items(run_id=run_id)
        payload = dict(run)
        payload["items"] = items
        return payload

    def mark_run_started(self, run_id: int) -> dict[str, Any] | None:
        return self.update_run(
            run_id=run_id,
            patch={
                "status": "running",
                "started_at": datetime.utcnow(),
            },
        )

    def mark_run_finished(
        self,
        *,
        run_id: int,
        status: str,
        stats: dict[str, Any],
        errors: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        return self.update_run(
            run_id=run_id,
            patch={
                "status": status,
                "finished_at": datetime.utcnow(),
                "stats_json": stats,
                "errors_json": errors,
            },
        )


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=_json_default)
