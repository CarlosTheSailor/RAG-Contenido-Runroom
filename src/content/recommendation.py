from __future__ import annotations

from pathlib import Path
from typing import Any

from src.application.reranking import aggregate_and_rerank
from src.application.use_cases.recommend_content import RecommendContentRequest, RecommendContentUseCase
from src.config import Settings
from src.infrastructure.ai.openai_embedding_client import OpenAIEmbeddingClient
from src.infrastructure.repositories.content_chunks import ContentChunksRepository


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
    try:
        storage.ensure_schema(schema_path)
        use_case = RecommendContentUseCase(
            embedding_client=OpenAIEmbeddingClient(settings=settings, force_offline=offline_mode),
            repository=ContentChunksRepository(storage),
        )
        summary = use_case.execute(
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
        return RecommendationSummary(summary.to_dict())
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
        repository = ContentChunksRepository(storage)
        items = repository.list_content_items(content_types=content_types, limit=limit_items)

        for item in items:
            summary["items_processed"] += 1
            source_id = int(item["id"])
            source_chunks = repository.list_content_chunks_for_item(source_id, limit=3)
            if not source_chunks:
                summary["skipped_items_without_chunks"] += 1
                continue

            target_scores: dict[int, float] = {}
            for chunk in source_chunks:
                embedding = repository.parse_vector(chunk.get("embedding"))
                if not embedding:
                    continue
                rows = repository.query_similar_content_chunks(
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
                repository.upsert_content_relation(
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


def _aggregate_and_rerank(rows: list[dict[str, Any]], top_k: int) -> list[Any]:
    return aggregate_and_rerank(rows=rows, top_k=top_k)
