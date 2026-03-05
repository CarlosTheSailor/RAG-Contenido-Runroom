from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from src.pipeline.models import Chunk, EpisodeInfo, RunroomArticle

logger = logging.getLogger(__name__)


class SupabaseStorage:
    def __init__(self, dsn: str):
        self._conn = psycopg.connect(dsn, row_factory=dict_row)

    def close(self) -> None:
        self._conn.close()

    def ensure_schema(self, schema_path: Path) -> None:
        sql = schema_path.read_text(encoding="utf-8")
        with self._conn.cursor() as cur:
            cur.execute(sql)
        self._conn.commit()

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

    def list_episodes(self) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute("SELECT * FROM episodes ORDER BY id")
            rows = cur.fetchall()
        return list(rows)

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

    def list_runroom_articles(self) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute("SELECT * FROM runroom_articles ORDER BY id")
            rows = cur.fetchall()
        return list(rows)

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


def _vector_literal(values: list[float]) -> str:
    return "[" + ",".join(f"{v:.8f}" for v in values) + "]"
