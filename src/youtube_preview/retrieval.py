from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.content.recommendation import _aggregate_and_rerank
from src.pipeline.ai_client import AIClient
from src.pipeline.storage import SupabaseStorage

from .models import EpisodeContext, ExtractedEntities, RelatedContentItem


@dataclass
class RetrievalResult:
    related_episodes: list[RelatedContentItem]
    related_case_studies: list[RelatedContentItem]


def retrieve_related_content(
    storage: SupabaseStorage,
    ai: AIClient,
    context: EpisodeContext,
    entities: ExtractedEntities,
    fetch_k: int = 80,
) -> RetrievalResult:
    query_text = _build_query_text(context, entities)
    query_embedding = ai.embed_texts([query_text])[0]

    rows = storage.query_similar_content_chunks(
        query_embedding,
        top_k=fetch_k,
        content_types=["episode", "case_study"],
        language=context.language,
        exclude_content_item_id=context.content_item_id,
    )

    ranked = _aggregate_and_rerank(rows, top_k=max(12, fetch_k // 4))

    episodes: list[RelatedContentItem] = []
    cases: list[RelatedContentItem] = []
    seen_links: set[str] = set()

    for item in ranked:
        if item.content_type not in {"episode", "case_study"}:
            continue
        if item.score < 0.68:
            continue

        url = str(item.url).strip() if item.url else None
        dedupe_key = (url or item.title).lower()
        if dedupe_key in seen_links:
            continue
        seen_links.add(dedupe_key)

        rationale = _build_rationale(item.matched_chunks)
        selection_reason = _selection_reason(score=float(item.score), rationale=rationale)
        related = RelatedContentItem(
            content_item_id=item.content_item_id,
            content_type=item.content_type,
            title=item.title,
            url=url,
            score=float(item.score),
            rationale=rationale,
            selection_reason=selection_reason,
        )

        if item.content_type == "episode" and len(episodes) < 3:
            episodes.append(related)
        if item.content_type == "case_study" and len(cases) < 2:
            cases.append(related)

        if len(episodes) >= 3 and len(cases) >= 2:
            break

    return RetrievalResult(related_episodes=episodes, related_case_studies=cases)


def _build_query_text(context: EpisodeContext, entities: ExtractedEntities) -> str:
    parts: list[str] = [
        context.title,
        " ".join(context.guest_names[:3]),
        " ".join(entities.main_topics[:8]),
        " ".join(entities.keywords[:12]),
    ]

    if context.transcript:
        parts.append(context.transcript[:9000])

    return "\n\n".join(part.strip() for part in parts if part.strip())


def _build_rationale(matched_chunks: list[dict[str, Any]]) -> str:
    for chunk in matched_chunks:
        text = str(chunk.get("text") or "").strip()
        if text:
            return text[:140] + ("..." if len(text) > 140 else "")
    return "Relevancia semántica alta con el episodio actual."


def _selection_reason(score: float, rationale: str) -> str:
    if score >= 0.82:
        prefix = "Coincidencia semántica muy alta"
    elif score >= 0.74:
        prefix = "Coincidencia semántica alta"
    else:
        prefix = "Coincidencia semántica suficiente"
    return f"{prefix}: {rationale}"
