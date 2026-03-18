from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from time import perf_counter
from typing import Any

import psycopg
from psycopg.rows import dict_row

from src.content.models import CanonicalChunk, CanonicalContentItem, CanonicalSection
from src.pipeline.models import Chunk, EpisodeInfo, RunroomArticle
from src.pipeline.normalization import slugify
from src.pipeline.schema import apply_migrations

logger = logging.getLogger(__name__)


class SimilarContentQueryError(RuntimeError):
    def __init__(
        self,
        *,
        message: str,
        content_types: list[str] | None,
        duration_ms: int,
        sqlstate: str | None,
        statement_timeout_ms: int | None,
        lock_timeout_ms: int | None,
    ) -> None:
        super().__init__(message)
        self.content_types = list(content_types or [])
        self.duration_ms = int(duration_ms)
        self.sqlstate = sqlstate
        self.statement_timeout_ms = statement_timeout_ms
        self.lock_timeout_ms = lock_timeout_ms

    @property
    def is_statement_timeout(self) -> bool:
        return self.sqlstate == "57014"

    @property
    def is_lock_timeout(self) -> bool:
        return self.sqlstate == "55P03"

THEME_INTEL_TABLES: tuple[str, ...] = (
    "theme_schedule_execution_items",
    "theme_schedule_executions",
    "theme_schedule_configs",
    "theme_schedules",
    "theme_topic_usage",
    "theme_topic_embeddings",
    "theme_related_content",
    "theme_evidences",
    "theme_topic_tags",
    "theme_topic_source_documents",
    "theme_topics",
    "source_documents",
    "theme_categories",
    "theme_runs",
)


class SupabaseStorage:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._conn = psycopg.connect(dsn, row_factory=dict_row)
        self._query_conn: psycopg.Connection[Any] | None = None

    def close(self) -> None:
        if self._query_conn is not None:
            self._query_conn.close()
        self._conn.close()

    def _get_query_conn(self) -> psycopg.Connection[Any]:
        if self._query_conn is None or self._query_conn.closed:
            self._query_conn = psycopg.connect(self._dsn, row_factory=dict_row)
        return self._query_conn

    def ensure_schema(self, schema_path: Path) -> None:
        apply_migrations(self._conn, schema_path)

    def count_theme_intel_rows(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        with self._conn.cursor() as cur:
            for table_name in THEME_INTEL_TABLES:
                cur.execute(f"SELECT COUNT(*) AS total FROM {table_name}")
                row = cur.fetchone()
                counts[table_name] = int(row["total"]) if row is not None else 0
        return counts

    def reset_theme_intel_data(self, dry_run: bool = False) -> dict[str, Any]:
        before = self.count_theme_intel_rows()
        if dry_run:
            return {
                "dry_run": True,
                "tables": list(THEME_INTEL_TABLES),
                "rows_before": before,
                "rows_after": before,
                "rows_deleted_total": 0,
            }

        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                TRUNCATE TABLE
                    {", ".join(THEME_INTEL_TABLES)}
                RESTART IDENTITY
                """
            )
        self._conn.commit()
        after = self.count_theme_intel_rows()
        return {
            "dry_run": False,
            "tables": list(THEME_INTEL_TABLES),
            "rows_before": before,
            "rows_after": after,
            "rows_deleted_total": max(0, sum(before.values()) - sum(after.values())),
        }

    # ---------------------------
    # Legacy episode/chunk methods
    # ---------------------------

    def upsert_episode(self, episode: EpisodeInfo) -> int:
        query = """
        INSERT INTO episodes (
            source_filename, episode_code, title, guest_names, language, transcript_path
        ) VALUES (
            %(source_filename)s, %(episode_code)s, %(title)s, %(guest_names)s, %(language)s, %(transcript_path)s
        )
        ON CONFLICT (source_filename)
        DO UPDATE SET
            episode_code = EXCLUDED.episode_code,
            title = EXCLUDED.title,
            guest_names = EXCLUDED.guest_names,
            language = EXCLUDED.language,
            transcript_path = EXCLUDED.transcript_path
        RETURNING id
        """
        payload = {
            "source_filename": episode.source_filename,
            "episode_code": episode.episode_code,
            "title": episode.title,
            "guest_names": episode.guest_names,
            "language": episode.language,
            "transcript_path": episode.transcript_path,
        }
        with self._conn.cursor() as cur:
            cur.execute(query, payload)
            row = cur.fetchone()
        self._conn.commit()
        assert row is not None
        return int(row["id"])

    def replace_chunks(self, episode_id: int, chunks: list[Chunk]) -> None:
        with self._conn.cursor() as cur:
            cur.execute("DELETE FROM chunks WHERE episode_id = %s", (episode_id,))
            insert_query = """
            INSERT INTO chunks (
                episode_id, chunk_index, start_ts_sec, end_ts_sec, speaker,
                text, metadata_json, embedding, token_count
            ) VALUES (
                %(episode_id)s, %(chunk_index)s, %(start_ts_sec)s, %(end_ts_sec)s, %(speaker)s,
                %(text)s, %(metadata_json)s::jsonb, %(embedding)s::vector, %(token_count)s
            )
            """
            for chunk in chunks:
                cur.execute(
                    insert_query,
                    {
                        "episode_id": episode_id,
                        "chunk_index": chunk.chunk_index,
                        "start_ts_sec": chunk.start_ts_sec,
                        "end_ts_sec": chunk.end_ts_sec,
                        "speaker": chunk.speaker,
                        "text": chunk.text,
                        "metadata_json": json.dumps(chunk.metadata, ensure_ascii=False),
                        "embedding": _vector_literal(chunk.embedding),
                        "token_count": chunk.token_count,
                    },
                )
        self._conn.commit()

    def list_chunks_for_episode(self, episode_id: int) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    episode_id,
                    chunk_index,
                    start_ts_sec,
                    end_ts_sec,
                    speaker,
                    text,
                    metadata_json,
                    embedding,
                    token_count
                FROM chunks
                WHERE episode_id = %s
                ORDER BY chunk_index
                """,
                (episode_id,),
            )
            rows = cur.fetchall()
        return list(rows)

    def list_episodes(self) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute("SELECT * FROM episodes ORDER BY id")
            rows = cur.fetchall()
        return list(rows)

    def get_episode_by_id(self, episode_id: int) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT * FROM episodes WHERE id = %s", (episode_id,))
            row = cur.fetchone()
        return row

    def get_episode_by_source_filename(self, source_filename: str) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT * FROM episodes WHERE source_filename = %s LIMIT 1", (source_filename,))
            row = cur.fetchone()
        return row

    def list_episodes_for_title_sync(self, statuses: list[str], limit: int | None = None) -> list[dict[str, Any]]:
        statuses = [s.strip() for s in statuses if s.strip()]
        if not statuses:
            statuses = ["auto_matched", "manual_matched"]

        query = """
        SELECT
            id,
            title,
            runroom_article_url,
            match_status
        FROM episodes
        WHERE runroom_article_url IS NOT NULL
          AND match_status = ANY(%(statuses)s)
        ORDER BY id
        """
        params: dict[str, Any] = {"statuses": statuses}
        if limit is not None:
            query += " LIMIT %(limit)s"
            params["limit"] = int(limit)

        with self._conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        return list(rows)

    def episode_exists(self, episode_id: int) -> bool:
        with self._conn.cursor() as cur:
            cur.execute("SELECT 1 FROM episodes WHERE id = %s", (episode_id,))
            row = cur.fetchone()
        return row is not None

    def upsert_runroom_articles(self, articles: list[RunroomArticle]) -> int:
        if not articles:
            return 0
        query = """
        INSERT INTO runroom_articles (url, slug, title, description, lang, episode_code_hint)
        VALUES (%(url)s, %(slug)s, %(title)s, %(description)s, %(lang)s, %(episode_code_hint)s)
        ON CONFLICT (url)
        DO UPDATE SET
            slug = EXCLUDED.slug,
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            lang = EXCLUDED.lang,
            episode_code_hint = EXCLUDED.episode_code_hint,
            fetched_at = now()
        """
        with self._conn.cursor() as cur:
            for article in articles:
                cur.execute(
                    query,
                    {
                        "url": article.url,
                        "slug": article.slug,
                        "title": article.title,
                        "description": article.description,
                        "lang": article.lang,
                        "episode_code_hint": article.episode_code_hint,
                    },
                )
        self._conn.commit()
        return len(articles)

    def upsert_runroom_article(self, article: RunroomArticle) -> int:
        query = """
        INSERT INTO runroom_articles (url, slug, title, description, lang, episode_code_hint)
        VALUES (%(url)s, %(slug)s, %(title)s, %(description)s, %(lang)s, %(episode_code_hint)s)
        ON CONFLICT (url)
        DO UPDATE SET
            slug = EXCLUDED.slug,
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            lang = EXCLUDED.lang,
            episode_code_hint = EXCLUDED.episode_code_hint,
            fetched_at = now()
        RETURNING id
        """
        with self._conn.cursor() as cur:
            cur.execute(
                query,
                {
                    "url": article.url,
                    "slug": article.slug,
                    "title": article.title,
                    "description": article.description,
                    "lang": article.lang,
                    "episode_code_hint": article.episode_code_hint,
                },
            )
            row = cur.fetchone()
        self._conn.commit()
        assert row is not None
        return int(row["id"])

    def list_runroom_articles(self) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute("SELECT * FROM runroom_articles ORDER BY id")
            rows = cur.fetchall()
        return list(rows)

    def get_runroom_article_by_url(self, url: str) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT * FROM runroom_articles WHERE url = %s LIMIT 1", (url,))
            row = cur.fetchone()
        return row

    def clear_candidates_for_episode(self, episode_id: int) -> None:
        with self._conn.cursor() as cur:
            cur.execute("DELETE FROM episode_article_candidates WHERE episode_id = %s", (episode_id,))
        self._conn.commit()

    def insert_candidate(
        self,
        episode_id: int,
        article_id: int,
        score: float,
        method: str,
        is_selected: bool,
        review_required: bool,
    ) -> None:
        query = """
        INSERT INTO episode_article_candidates (
            episode_id, article_id, score, method, is_selected, review_required
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (episode_id, article_id, method)
        DO UPDATE SET
            score = EXCLUDED.score,
            is_selected = EXCLUDED.is_selected,
            review_required = EXCLUDED.review_required,
            created_at = now()
        """
        with self._conn.cursor() as cur:
            cur.execute(query, (episode_id, article_id, score, method, is_selected, review_required))
        self._conn.commit()

    def set_episode_match(
        self,
        episode_id: int,
        url: str | None,
        status: str,
        confidence: float | None,
    ) -> None:
        query = """
        UPDATE episodes
        SET runroom_article_url = %s,
            match_status = %s,
            match_confidence = %s,
            matched_at = now()
        WHERE id = %s
        """
        with self._conn.cursor() as cur:
            cur.execute(query, (url, status, confidence, episode_id))
        self._conn.commit()

    def update_episode_and_article_title(
        self,
        episode_id: int,
        runroom_article_url: str,
        new_title: str,
    ) -> dict[str, int]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE episodes
                SET title = %s
                WHERE id = %s
                """,
                (new_title, episode_id),
            )
            episode_rows = cur.rowcount

            cur.execute(
                """
                UPDATE runroom_articles
                SET title = %s,
                    fetched_at = now()
                WHERE url = %s
                """,
                (new_title, runroom_article_url),
            )
            article_rows = cur.rowcount
        self._conn.commit()
        return {
            "episodes_updated": int(episode_rows),
            "runroom_articles_updated": int(article_rows),
        }

    def query_similar_chunks(self, query_embedding: list[float], top_k: int = 8) -> list[dict[str, Any]]:
        vec = _vector_literal(query_embedding)
        query = """
        SELECT
            e.id AS episode_id,
            e.episode_code,
            e.title AS episode_title,
            e.runroom_article_url,
            c.chunk_index,
            c.start_ts_sec,
            c.end_ts_sec,
            c.speaker,
            c.text,
            c.metadata_json,
            (1 - (c.embedding <=> %(vec)s::vector)) AS similarity
        FROM chunks c
        JOIN episodes e ON e.id = c.episode_id
        ORDER BY c.embedding <=> %(vec)s::vector
        LIMIT %(top_k)s
        """
        with self._conn.cursor() as cur:
            cur.execute(query, {"vec": vec, "top_k": top_k})
            rows = cur.fetchall()
        return list(rows)

    def export_review_report(self, output_path: Path) -> int:
        query = """
        SELECT
            e.id AS episode_id,
            e.source_filename,
            e.episode_code,
            e.title AS episode_title,
            a.url AS candidate_url,
            a.title AS candidate_title,
            c.score,
            c.method,
            c.review_required
        FROM episodes e
        LEFT JOIN episode_article_candidates c ON c.episode_id = e.id
        LEFT JOIN runroom_articles a ON a.id = c.article_id
        WHERE e.match_status = 'review_required'
        ORDER BY e.id, c.score DESC NULLS LAST
        """
        with self._conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "episode_id",
                    "source_filename",
                    "episode_code",
                    "episode_title",
                    "candidate_url",
                    "candidate_title",
                    "score",
                    "method",
                    "review_required",
                ],
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

        return len(rows)

    def list_review_candidates(self, limit_episodes: int | None = None) -> list[dict[str, Any]]:
        query = """
        WITH ranked AS (
            SELECT
                e.id AS episode_id,
                e.source_filename,
                e.episode_code,
                e.title AS episode_title,
                e.match_confidence,
                a.id AS article_id,
                a.url AS candidate_url,
                a.title AS candidate_title,
                c.score,
                c.method,
                row_number() OVER (PARTITION BY e.id ORDER BY c.score DESC NULLS LAST) AS rn
            FROM episodes e
            JOIN episode_article_candidates c ON c.episode_id = e.id
            JOIN runroom_articles a ON a.id = c.article_id
            WHERE e.match_status = 'review_required'
        )
        SELECT *
        FROM ranked
        WHERE rn <= 5
        ORDER BY episode_id, score DESC NULLS LAST
        """
        with self._conn.cursor() as cur:
            cur.execute(query)
            rows = list(cur.fetchall())

        if limit_episodes is None:
            return rows

        allowed_ids: set[int] = set()
        for row in rows:
            episode_id = int(row["episode_id"])
            if len(allowed_ids) >= limit_episodes and episode_id not in allowed_ids:
                continue
            allowed_ids.add(episode_id)
        return [row for row in rows if int(row["episode_id"]) in allowed_ids]

    def set_manual_match(self, episode_id: int, article_id: int, confidence: float | None = None) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE episode_article_candidates
                SET is_selected = FALSE,
                    review_required = FALSE
                WHERE episode_id = %s
                """,
                (episode_id,),
            )
            cur.execute(
                """
                UPDATE episode_article_candidates
                SET is_selected = TRUE,
                    review_required = FALSE
                WHERE episode_id = %s AND article_id = %s
                """,
                (episode_id, article_id),
            )
            cur.execute(
                """
                UPDATE episodes e
                SET runroom_article_url = a.url,
                    match_status = 'manual_matched',
                    match_confidence = %s,
                    matched_at = now()
                FROM runroom_articles a
                WHERE e.id = %s
                  AND a.id = %s
                """,
                (confidence, episode_id, article_id),
            )
        self._conn.commit()

    # ---------------------------
    # Canonical multi-content layer
    # ---------------------------

    def upsert_content_item(self, item: CanonicalContentItem, legacy_episode_id: int | None = None) -> int:
        query = """
        INSERT INTO content_items (
            content_key,
            content_type,
            title,
            slug,
            url,
            source,
            language,
            status,
            published_at,
            extracted_at,
            metadata_json,
            custom_metadata_json,
            raw_text,
            legacy_episode_id
        ) VALUES (
            %(content_key)s,
            %(content_type)s,
            %(title)s,
            %(slug)s,
            %(url)s,
            %(source)s,
            %(language)s,
            %(status)s,
            %(published_at)s,
            %(extracted_at)s,
            %(metadata_json)s::jsonb,
            %(custom_metadata_json)s::jsonb,
            %(raw_text)s,
            %(legacy_episode_id)s
        )
        ON CONFLICT (content_key)
        DO UPDATE SET
            content_type = EXCLUDED.content_type,
            title = EXCLUDED.title,
            slug = EXCLUDED.slug,
            url = EXCLUDED.url,
            source = EXCLUDED.source,
            language = EXCLUDED.language,
            status = EXCLUDED.status,
            published_at = EXCLUDED.published_at,
            extracted_at = EXCLUDED.extracted_at,
            metadata_json = EXCLUDED.metadata_json,
            custom_metadata_json = EXCLUDED.custom_metadata_json,
            raw_text = EXCLUDED.raw_text,
            legacy_episode_id = EXCLUDED.legacy_episode_id,
            updated_at = now()
        RETURNING id
        """
        payload = {
            "content_key": item.content_key,
            "content_type": item.content_type,
            "title": item.title,
            "slug": item.slug,
            "url": item.url,
            "source": item.source,
            "language": item.language,
            "status": item.status,
            "published_at": item.published_at,
            "extracted_at": item.extracted_at,
            "metadata_json": json.dumps(item.metadata, ensure_ascii=False),
            "custom_metadata_json": json.dumps(item.custom_metadata, ensure_ascii=False),
            "raw_text": item.raw_text,
            "legacy_episode_id": legacy_episode_id,
        }
        with self._conn.cursor() as cur:
            cur.execute(query, payload)
            row = cur.fetchone()
        self._conn.commit()
        assert row is not None
        return int(row["id"])

    def replace_content_sections(self, item_id: int, sections: list[CanonicalSection]) -> dict[int, int]:
        section_map: dict[int, int] = {}
        with self._conn.cursor() as cur:
            cur.execute("DELETE FROM content_sections WHERE content_item_id = %s", (item_id,))
            query = """
            INSERT INTO content_sections (
                content_item_id,
                section_order,
                section_key,
                section_title,
                text,
                token_count,
                metadata_json,
                source_locator
            ) VALUES (
                %(content_item_id)s,
                %(section_order)s,
                %(section_key)s,
                %(section_title)s,
                %(text)s,
                %(token_count)s,
                %(metadata_json)s::jsonb,
                %(source_locator)s::jsonb
            )
            RETURNING id
            """
            for section in sections:
                cur.execute(
                    query,
                    {
                        "content_item_id": item_id,
                        "section_order": section.section_order,
                        "section_key": section.section_key,
                        "section_title": section.section_title,
                        "text": section.text,
                        "token_count": section.token_count,
                        "metadata_json": json.dumps(section.metadata, ensure_ascii=False),
                        "source_locator": json.dumps(section.source_locator, ensure_ascii=False),
                    },
                )
                row = cur.fetchone()
                if row is not None:
                    section_map[section.section_order] = int(row["id"])
        self._conn.commit()
        return section_map

    def replace_content_chunks(self, content_item_id: int, chunks: list[CanonicalChunk]) -> None:
        with self._conn.cursor() as cur:
            cur.execute("DELETE FROM content_chunks WHERE content_item_id = %s", (content_item_id,))
            query = """
            INSERT INTO content_chunks (
                content_item_id,
                section_id,
                chunk_order,
                section_key,
                section_title,
                text,
                token_count,
                metadata_json,
                source_locator,
                embedding
            ) VALUES (
                %(content_item_id)s,
                %(section_id)s,
                %(chunk_order)s,
                %(section_key)s,
                %(section_title)s,
                %(text)s,
                %(token_count)s,
                %(metadata_json)s::jsonb,
                %(source_locator)s::jsonb,
                %(embedding)s::vector
            )
            """
            for chunk in chunks:
                section_id_val = chunk.metadata.get("section_id")
                section_id = int(section_id_val) if isinstance(section_id_val, int) or (isinstance(section_id_val, str) and section_id_val.isdigit()) else None
                cur.execute(
                    query,
                    {
                        "content_item_id": content_item_id,
                        "section_id": section_id,
                        "chunk_order": chunk.chunk_order,
                        "section_key": chunk.section_key,
                        "section_title": chunk.section_title,
                        "text": chunk.text,
                        "token_count": chunk.token_count,
                        "metadata_json": json.dumps(chunk.metadata, ensure_ascii=False),
                        "source_locator": json.dumps(chunk.source_locator, ensure_ascii=False),
                        "embedding": _vector_literal(chunk.embedding),
                    },
                )
        self._conn.commit()

    def list_content_items(
        self,
        content_types: list[str] | None = None,
        source: str | None = None,
        language: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT
            id,
            content_key,
            content_type,
            title,
            slug,
            url,
            source,
            language,
            status,
            published_at,
            extracted_at,
            metadata_json,
            custom_metadata_json,
            raw_text,
            legacy_episode_id
        FROM content_items
        WHERE 1=1
        """
        params: dict[str, Any] = {}

        if content_types:
            query += " AND content_type = ANY(%(content_types)s)"
            params["content_types"] = content_types
        if source:
            query += " AND source = %(source)s"
            params["source"] = source
        if language:
            query += " AND language = %(language)s"
            params["language"] = language

        query += " ORDER BY id"
        if limit is not None:
            query += " LIMIT %(limit)s"
            params["limit"] = int(limit)

        with self._conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        return list(rows)

    def list_content_types(self) -> list[str]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT content_type
                FROM content_items
                WHERE content_type IS NOT NULL AND btrim(content_type) <> ''
                ORDER BY content_type
                """
            )
            rows = cur.fetchall()
        output: list[str] = []
        for row in rows:
            content_type = str(row.get("content_type") or "").strip()
            if content_type:
                output.append(content_type)
        return output

    def get_content_item_by_legacy_episode_id(self, legacy_episode_id: int) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    content_key,
                    content_type,
                    title,
                    slug,
                    url,
                    source,
                    language,
                    status,
                    published_at,
                    extracted_at,
                    metadata_json,
                    custom_metadata_json,
                    raw_text,
                    legacy_episode_id
                FROM content_items
                WHERE legacy_episode_id = %s
                ORDER BY id
                LIMIT 1
                """,
                (legacy_episode_id,),
            )
            row = cur.fetchone()
        return row

    def list_content_chunks_for_item(self, content_item_id: int, limit: int | None = None) -> list[dict[str, Any]]:
        query = """
        SELECT
            id,
            content_item_id,
            section_id,
            chunk_order,
            section_key,
            section_title,
            text,
            token_count,
            metadata_json,
            source_locator,
            embedding
        FROM content_chunks
        WHERE content_item_id = %(content_item_id)s
        ORDER BY chunk_order
        """
        params: dict[str, Any] = {"content_item_id": content_item_id}
        if limit is not None:
            query += " LIMIT %(limit)s"
            params["limit"] = int(limit)

        with self._conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        return list(rows)

    def query_similar_content_chunks(
        self,
        query_embedding: list[float],
        top_k: int = 60,
        content_types: list[str] | None = None,
        source: str | None = None,
        language: str | None = None,
        exclude_content_item_id: int | None = None,
        statement_timeout_ms: int | None = None,
        lock_timeout_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        query, params = self._build_query_similar_content_chunks_sql(
            query_embedding=query_embedding,
            top_k=top_k,
            content_types=content_types,
            source=source,
            language=language,
            exclude_content_item_id=exclude_content_item_id,
        )
        conn = self._get_query_conn() if (statement_timeout_ms or lock_timeout_ms) else self._conn
        started = perf_counter()
        try:
            with conn.cursor() as cur:
                _apply_local_query_timeouts(
                    cur,
                    statement_timeout_ms=statement_timeout_ms,
                    lock_timeout_ms=lock_timeout_ms,
                )
                cur.execute(query, params)
                rows = cur.fetchall()
            if conn is not self._conn:
                conn.commit()
            return list(rows)
        except psycopg.Error as exc:
            if conn is not self._conn:
                conn.rollback()
            duration_ms = int((perf_counter() - started) * 1000)
            raise SimilarContentQueryError(
                message=str(exc),
                content_types=content_types,
                duration_ms=duration_ms,
                sqlstate=_extract_sqlstate(exc),
                statement_timeout_ms=statement_timeout_ms,
                lock_timeout_ms=lock_timeout_ms,
            ) from exc

    def explain_query_similar_content_chunks(
        self,
        query_embedding: list[float],
        top_k: int = 60,
        content_types: list[str] | None = None,
        source: str | None = None,
        language: str | None = None,
        exclude_content_item_id: int | None = None,
        statement_timeout_ms: int | None = None,
        lock_timeout_ms: int | None = None,
    ) -> list[str]:
        base_query, params = self._build_query_similar_content_chunks_sql(
            query_embedding=query_embedding,
            top_k=top_k,
            content_types=content_types,
            source=source,
            language=language,
            exclude_content_item_id=exclude_content_item_id,
        )
        query = "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) " + base_query
        conn = self._get_query_conn()
        started = perf_counter()
        try:
            with conn.cursor() as cur:
                _apply_local_query_timeouts(
                    cur,
                    statement_timeout_ms=statement_timeout_ms,
                    lock_timeout_ms=lock_timeout_ms,
                )
                cur.execute(query, params)
                rows = cur.fetchall()
            conn.commit()
            output: list[str] = []
            for row in rows:
                for value in row.values():
                    if value is not None:
                        output.append(str(value))
                        break
            return output
        except psycopg.Error as exc:
            conn.rollback()
            duration_ms = int((perf_counter() - started) * 1000)
            raise SimilarContentQueryError(
                message=str(exc),
                content_types=content_types,
                duration_ms=duration_ms,
                sqlstate=_extract_sqlstate(exc),
                statement_timeout_ms=statement_timeout_ms,
                lock_timeout_ms=lock_timeout_ms,
            ) from exc

    def count_content_inventory_by_type(self) -> list[dict[str, Any]]:
        query = """
        WITH item_counts AS (
            SELECT content_type, COUNT(*) AS item_count
            FROM content_items
            GROUP BY content_type
        ),
        chunk_counts AS (
            SELECT ci.content_type, COUNT(*) AS chunk_count
            FROM content_chunks cc
            JOIN content_items ci ON ci.id = cc.content_item_id
            GROUP BY ci.content_type
        )
        SELECT
            COALESCE(ic.content_type, cc.content_type) AS content_type,
            COALESCE(ic.item_count, 0) AS item_count,
            COALESCE(cc.chunk_count, 0) AS chunk_count
        FROM item_counts ic
        FULL OUTER JOIN chunk_counts cc ON cc.content_type = ic.content_type
        ORDER BY COALESCE(cc.chunk_count, 0) DESC, COALESCE(ic.item_count, 0) DESC, COALESCE(ic.content_type, cc.content_type)
        """
        with self._get_query_conn().cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        self._get_query_conn().commit()
        return [dict(row) for row in rows]

    def list_content_query_activity(self) -> list[dict[str, Any]]:
        query = """
        SELECT
            pid,
            state,
            wait_event_type,
            wait_event,
            now() - query_start AS running_for,
            query_start,
            state_change,
            query
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND pid <> pg_backend_pid()
          AND (
            query ILIKE '%%content_chunks%%'
            OR query ILIKE '%%content_items%%'
            OR query ILIKE '%%theme_topic_embeddings%%'
          )
        ORDER BY query_start DESC
        LIMIT 20
        """
        with self._get_query_conn().cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        self._get_query_conn().commit()
        return [dict(row) for row in rows]

    def list_content_query_locks(self) -> list[dict[str, Any]]:
        query = """
        SELECT
            l.pid,
            c.relname AS relation_name,
            l.mode,
            l.granted,
            a.state,
            a.wait_event_type,
            a.wait_event,
            a.query
        FROM pg_locks l
        LEFT JOIN pg_class c ON c.oid = l.relation
        LEFT JOIN pg_stat_activity a ON a.pid = l.pid
        WHERE c.relname IN ('content_chunks', 'content_items', 'theme_topic_embeddings')
        ORDER BY l.granted ASC, l.pid ASC
        """
        with self._get_query_conn().cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        self._get_query_conn().commit()
        return [dict(row) for row in rows]

    def get_theme_topic_embedding(self, topic_id: int) -> list[float]:
        with self._get_query_conn().cursor() as cur:
            cur.execute(
                """
                SELECT embedding
                FROM theme_topic_embeddings
                WHERE topic_id = %s
                LIMIT 1
                """,
                (topic_id,),
            )
            row = cur.fetchone()
        self._get_query_conn().commit()
        if row is None:
            return []
        return self.parse_vector(row.get("embedding"))

    def _build_query_similar_content_chunks_sql(
        self,
        *,
        query_embedding: list[float],
        top_k: int,
        content_types: list[str] | None,
        source: str | None,
        language: str | None,
        exclude_content_item_id: int | None,
    ) -> tuple[str, dict[str, Any]]:
        vec = _vector_literal(query_embedding)
        query = """
        SELECT
            ci.id AS content_item_id,
            ci.content_type,
            ci.title,
            ci.url,
            ci.source,
            ci.language,
            ci.metadata_json,
            cc.id AS chunk_id,
            cc.chunk_order,
            cc.section_key,
            cc.section_title,
            cc.text AS chunk_text,
            cc.metadata_json AS chunk_metadata_json,
            (1 - (cc.embedding <=> %(vec)s::vector)) AS similarity
        FROM content_chunks cc
        JOIN content_items ci ON ci.id = cc.content_item_id
        WHERE 1=1
        """
        params: dict[str, Any] = {"vec": vec, "top_k": top_k}

        if content_types:
            query += " AND ci.content_type = ANY(%(content_types)s)"
            params["content_types"] = content_types
        if source:
            query += " AND ci.source = %(source)s"
            params["source"] = source
        if language:
            query += " AND ci.language = %(language)s"
            params["language"] = language
        if exclude_content_item_id is not None:
            query += " AND ci.id <> %(exclude_content_item_id)s"
            params["exclude_content_item_id"] = exclude_content_item_id

        query += " ORDER BY cc.embedding <=> %(vec)s::vector LIMIT %(top_k)s"
        return query, params

    def upsert_content_relation(
        self,
        from_content_item_id: int,
        to_content_item_id: int,
        relation_type: str,
        method: str,
        score: float,
        status: str,
        rationale: str | None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        query = """
        INSERT INTO content_relations (
            from_content_item_id,
            to_content_item_id,
            relation_type,
            method,
            score,
            status,
            rationale,
            metadata_json,
            computed_at
        ) VALUES (
            %(from_content_item_id)s,
            %(to_content_item_id)s,
            %(relation_type)s,
            %(method)s,
            %(score)s,
            %(status)s,
            %(rationale)s,
            %(metadata_json)s::jsonb,
            now()
        )
        ON CONFLICT (from_content_item_id, to_content_item_id, relation_type, method)
        DO UPDATE SET
            score = EXCLUDED.score,
            status = EXCLUDED.status,
            rationale = EXCLUDED.rationale,
            metadata_json = EXCLUDED.metadata_json,
            computed_at = now()
        """
        with self._conn.cursor() as cur:
            cur.execute(
                query,
                {
                    "from_content_item_id": from_content_item_id,
                    "to_content_item_id": to_content_item_id,
                    "relation_type": relation_type,
                    "method": method,
                    "score": score,
                    "status": status,
                    "rationale": rationale,
                    "metadata_json": json.dumps(metadata or {}, ensure_ascii=False),
                },
            )
        self._conn.commit()

    def list_content_chunks_for_reembed(
        self,
        content_type: str | None = None,
        item_id: int | None = None,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT
            cc.id,
            cc.content_item_id,
            cc.text,
            ci.content_type
        FROM content_chunks cc
        JOIN content_items ci ON ci.id = cc.content_item_id
        WHERE 1=1
        """
        params: dict[str, Any] = {}

        if content_type:
            query += " AND ci.content_type = %(content_type)s"
            params["content_type"] = content_type
        if item_id is not None:
            query += " AND ci.id = %(item_id)s"
            params["item_id"] = item_id

        query += " ORDER BY cc.id"

        with self._conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        return list(rows)

    def update_content_chunk_embedding(self, chunk_id: int, embedding: list[float]) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE content_chunks
                SET embedding = %(embedding)s::vector
                WHERE id = %(chunk_id)s
                """,
                {"embedding": _vector_literal(embedding), "chunk_id": chunk_id},
            )
        self._conn.commit()

    def sync_episode_to_canonical(self, episode_id: int) -> int | None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT * FROM episodes WHERE id = %s", (episode_id,))
            episode = cur.fetchone()
        if episode is None:
            return None

        chunks = self.list_chunks_for_episode(episode_id)
        raw_text = "\n\n".join(str(row.get("text") or "") for row in chunks if row.get("text"))
        title = str(episode.get("title") or f"Episode {episode_id}")
        slug = str(episode.get("episode_code") or "").strip().lower() or slugify(title)

        item = CanonicalContentItem(
            content_key=f"episode:{episode_id}",
            content_type="episode",
            title=title,
            slug=slug,
            url=str(episode.get("runroom_article_url") or "").strip() or None,
            source="realworld_transcript",
            language=str(episode.get("language") or "es"),
            status="active",
            metadata={
                "content_type": "episode",
                "source": "realworld_transcript",
                "legacy_episode_id": episode_id,
                "episode_code": episode.get("episode_code"),
                "guest_names": episode.get("guest_names") or [],
            },
            custom_metadata={
                "source_filename": episode.get("source_filename"),
                "transcript_path": episode.get("transcript_path"),
            },
            raw_text=raw_text,
        )

        section = CanonicalSection(
            section_order=0,
            section_key="other",
            section_title="Transcript",
            text=raw_text,
            token_count=max(1, len(raw_text) // 4),
            metadata={"section_key": "other", "section_title": "Transcript", "legacy_episode_id": episode_id},
            source_locator={"legacy_episode_id": episode_id},
        )

        canonical_chunks: list[CanonicalChunk] = []
        for row in chunks:
            embedding = self.parse_vector(row.get("embedding"))
            canonical_chunks.append(
                CanonicalChunk(
                    chunk_order=int(row.get("chunk_index") or 0),
                    section_order=0,
                    section_key="other",
                    section_title="Transcript",
                    text=str(row.get("text") or ""),
                    token_count=int(row.get("token_count") or 1),
                    metadata={
                        "legacy_chunk_id": row.get("id"),
                        "legacy_episode_id": episode_id,
                        "speaker": row.get("speaker"),
                        "metadata_json": row.get("metadata_json") or {},
                        "start_ts_sec": row.get("start_ts_sec"),
                        "end_ts_sec": row.get("end_ts_sec"),
                    },
                    source_locator={
                        "start_ts_sec": row.get("start_ts_sec"),
                        "end_ts_sec": row.get("end_ts_sec"),
                    },
                    embedding=embedding,
                )
            )

        item_id = self.upsert_content_item(item, legacy_episode_id=episode_id)
        section_map = self.replace_content_sections(item_id, [section])
        section_id = section_map.get(0)
        if section_id is not None:
            for chunk in canonical_chunks:
                chunk.metadata["section_id"] = section_id
        self.replace_content_chunks(item_id, canonical_chunks)
        return item_id

    @staticmethod
    def parse_vector(value: Any) -> list[float]:
        if value is None:
            return []
        if isinstance(value, list):
            return [float(v) for v in value]
        if isinstance(value, tuple):
            return [float(v) for v in value]

        raw = str(value).strip()
        if not raw:
            return []
        if raw.startswith("[") and raw.endswith("]"):
            raw = raw[1:-1]
        parts = [part.strip() for part in raw.split(",") if part.strip()]
        out: list[float] = []
        for part in parts:
            try:
                out.append(float(part))
            except ValueError:
                continue
        return out


def _vector_literal(values: list[float]) -> str:
    return "[" + ",".join(f"{v:.8f}" for v in values) + "]"


def _apply_local_query_timeouts(
    cur: psycopg.Cursor[Any],
    *,
    statement_timeout_ms: int | None,
    lock_timeout_ms: int | None,
) -> None:
    if lock_timeout_ms is not None:
        cur.execute("SELECT set_config('lock_timeout', %s, true)", (f"{int(lock_timeout_ms)}ms",))
    if statement_timeout_ms is not None:
        cur.execute("SELECT set_config('statement_timeout', %s, true)", (f"{int(statement_timeout_ms)}ms",))


def _extract_sqlstate(exc: BaseException) -> str | None:
    current: BaseException | None = exc
    while current is not None:
        sqlstate = getattr(current, "sqlstate", None)
        if isinstance(sqlstate, str) and sqlstate.strip():
            return sqlstate.strip()
        current = current.__cause__
    return None
