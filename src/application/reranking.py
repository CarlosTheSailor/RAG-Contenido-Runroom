from __future__ import annotations

from collections import defaultdict
from statistics import mean
from typing import Any

from src.content.models import RecommendationResult
from src.pipeline.normalization import normalize_for_match, tokenize_for_match

_LOW_SIGNAL_CHUNK_EXACT = {
    "casos servicios nosotros academy realworld",
}

_LOW_SIGNAL_CHUNK_CONTAINS = (
    "saltar al contenido principal",
    "saltar al pie de pagina",
    "completa el formulario y nos pondremos en contacto",
    "siguenos instagram linkedin bluesky youtube",
    "tienes que inscribirte para reservar tu plaza",
    "consulta los proximos eventos en runroom com lab",
    "uso de imagenes en el evento",
    "el evento es gratuito y las cervezas tambien",
    "compartir en linkedin",
    "compartir en bluesky",
    "accesibilidad hemos de informarte que lamentablemente",
)

_LOW_SIGNAL_TOKENS = {
    "casos",
    "servicios",
    "nosotros",
    "academy",
    "realworld",
    "instagram",
    "linkedin",
    "bluesky",
    "youtube",
    "formulario",
    "inscribirte",
    "plaza",
    "saltar",
    "contenido",
    "principal",
    "pie",
    "pagina",
    "compartir",
    "accesibilidad",
    "evento",
    "imagenes",
}


def aggregate_and_rerank(
    rows: list[dict[str, Any]],
    top_k: int,
    query_text: str | None = None,
) -> list[RecommendationResult]:
    if not rows:
        return []

    filtered_rows = [row for row in rows if not _is_low_signal_chunk(str(row.get("chunk_text") or ""))]
    if not filtered_rows:
        return []

    grouped: dict[int, dict[str, Any]] = {}
    for row in filtered_rows:
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
        if query_text and str(bucket.get("content_type") or "") == "runroom_lab":
            score = score * (1.0 + _runroom_lab_lexical_boost(query_text=query_text, bucket=bucket))
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


def _is_low_signal_chunk(text: str) -> bool:
    normalized = normalize_for_match(text)
    if not normalized:
        return True
    if normalized in _LOW_SIGNAL_CHUNK_EXACT:
        return True
    if any(fragment in normalized for fragment in _LOW_SIGNAL_CHUNK_CONTAINS):
        return True

    tokens = normalized.split()
    if not tokens:
        return True
    low_signal_hits = sum(1 for token in tokens if token in _LOW_SIGNAL_TOKENS)
    if len(tokens) <= 6 and low_signal_hits >= max(2, len(tokens) - 1):
        return True
    if len(tokens) >= 8 and low_signal_hits >= 5 and (low_signal_hits / len(tokens)) >= 0.55:
        return True
    return False


def _runroom_lab_lexical_boost(query_text: str, bucket: dict[str, Any]) -> float:
    query_tokens = set(tokenize_for_match(query_text))
    if not query_tokens:
        return 0.0

    title_tokens = set(tokenize_for_match(str(bucket.get("title") or "")))
    title_overlap = len(query_tokens & title_tokens) / max(1, len(query_tokens))

    matched_text = " ".join(str(chunk.get("text") or "") for chunk in bucket.get("chunks", []))
    body_tokens = set(tokenize_for_match(matched_text))
    body_overlap = len(query_tokens & body_tokens) / max(1, len(query_tokens))

    # Moderate bounded boost that preserves semantic ranking as primary signal.
    return min(0.18, (0.12 * title_overlap) + (0.06 * body_overlap))
