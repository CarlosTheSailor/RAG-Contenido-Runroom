from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import TYPE_CHECKING, Any

from src.config import Settings
from src.content.models import RecommendationResult
from src.pipeline.ai_client import AIClient

if TYPE_CHECKING:
    from src.pipeline.storage import SupabaseStorage


class RecommendationSummary(dict):
    pass


def recommend_content(
    settings: Settings,
    schema_path: Path,
    text: str,
    top_k: int = 8,
    fetch_k: int = 60,
    content_types: list[str] | None = None,
    source: str | None = None,
    language: str | None = None,
    group_by_type: bool = False,
    offline_mode: bool = False,
) -> RecommendationSummary:
    from src.pipeline.storage import SupabaseStorage

    storage = SupabaseStorage(settings.supabase_db_url)
    ai = AIClient(settings=settings, force_offline=offline_mode)

    try:
        storage.ensure_schema(schema_path)
        vector = ai.embed_texts([text])[0]
        rows = storage.query_similar_content_chunks(
            vector,
            top_k=fetch_k,
            content_types=content_types,
            source=source,
            language=language,
        )

        ranked = _aggregate_and_rerank(rows, top_k=top_k)

        if group_by_type:
            grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for item in ranked:
                grouped[item.content_type].append(item.to_dict())
            return RecommendationSummary(
                query=text,
                top_k=top_k,
                total_candidates=len(rows),
                grouped=True,
                results_by_type=dict(grouped),
            )

        return RecommendationSummary(
            query=text,
            top_k=top_k,
            total_candidates=len(rows),
            grouped=False,
            results=[item.to_dict() for item in ranked],
        )
    finally:
        storage.close()


def materialize_relations(
    settings: Settings,
    schema_path: Path,
    top_k_per_item: int = 5,
    limit_items: int | None = None,
    content_types: list[str] | None = None,
    min_score: float = 0.55,
) -> dict[str, int]:
    from src.pipeline.storage import SupabaseStorage

    storage = SupabaseStorage(settings.supabase_db_url)
    summary = {
        "items_processed": 0,
        "relations_upserted": 0,
        "skipped_items_without_chunks": 0,
    }

    try:
        storage.ensure_schema(schema_path)
        items = storage.list_content_items(content_types=content_types, limit=limit_items)

        for item in items:
            summary["items_processed"] += 1
            source_id = int(item["id"])
            source_chunks = storage.list_content_chunks_for_item(source_id, limit=3)
            if not source_chunks:
                summary["skipped_items_without_chunks"] += 1
                continue

            target_scores: dict[int, float] = {}
            for chunk in source_chunks:
                embedding = storage.parse_vector(chunk.get("embedding"))
                if not embedding:
                    continue
                rows = storage.query_similar_content_chunks(
                    embedding,
                    top_k=top_k_per_item * 8,
                    content_types=content_types,
                    exclude_content_item_id=source_id,
                )
                for row in rows:
                    target_id = int(row["content_item_id"])
                    sim = float(row.get("similarity") or 0.0)
                    prev = target_scores.get(target_id, 0.0)
                    if sim > prev:
                        target_scores[target_id] = sim

            if not target_scores:
                continue

            ranked = sorted(target_scores.items(), key=lambda item: item[1], reverse=True)
            for target_id, score in ranked[:top_k_per_item]:
                if score < min_score:
                    continue
                storage.upsert_content_relation(
                    from_content_item_id=source_id,
                    to_content_item_id=target_id,
                    relation_type="related",
                    method="semantic_chunk",
                    score=score,
                    status="suggested",
                    rationale="similarity from representative chunks",
                    metadata={"min_score": min_score},
                )
                summary["relations_upserted"] += 1

    finally:
        storage.close()

    return summary


def _aggregate_and_rerank(rows: list[dict[str, Any]], top_k: int) -> list[RecommendationResult]:
    if not rows:
        return []

    grouped: dict[int, dict[str, Any]] = {}
    for row in rows:
        item_id = int(row["content_item_id"])
        bucket = grouped.setdefault(
            item_id,
            {
                "content_item_id": item_id,
                "content_type": row["content_type"],
                "title": row["title"],
                "url": row.get("url"),
                "metadata": row.get("metadata_json") or {},
                "sims": [],
                "chunks": [],
            },
        )
        sim = float(row.get("similarity") or 0.0)
        bucket["sims"].append(sim)
        if len(bucket["chunks"]) < 3:
            bucket["chunks"].append(
                {
                    "chunk_id": row.get("chunk_id"),
                    "section_key": row.get("section_key"),
                    "text": str(row.get("chunk_text") or "")[:240],
                    "similarity": sim,
                }
            )

    scored: list[dict[str, Any]] = []
    for bucket in grouped.values():
        sims = sorted(bucket["sims"], reverse=True)
        top_mean = mean(sims[:3])
        score = (0.75 * sims[0]) + (0.25 * top_mean)
        scored.append({**bucket, "score": score})

    scored.sort(key=lambda item: item["score"], reverse=True)

    # Type diversity penalty.
    type_counts: dict[str, int] = defaultdict(int)
    diversified: list[RecommendationResult] = []
    for row in scored:
        ctype = str(row["content_type"])
        penalty = 1.0 - (0.08 * type_counts[ctype])
        final_score = max(0.0, float(row["score"]) * max(0.6, penalty))
        type_counts[ctype] += 1

        diversified.append(
            RecommendationResult(
                content_item_id=int(row["content_item_id"]),
                content_type=ctype,
                title=str(row["title"]),
                url=row.get("url"),
                score=final_score,
                matched_chunks=list(row["chunks"]),
                metadata=row.get("metadata") or {},
            )
        )

    diversified.sort(key=lambda item: item.score, reverse=True)
    return diversified[:top_k]
