from __future__ import annotations

from collections import defaultdict
from statistics import mean
from typing import Any

from src.content.models import RecommendationResult


def aggregate_and_rerank(rows: list[dict[str, Any]], top_k: int) -> list[RecommendationResult]:
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
