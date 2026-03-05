from __future__ import annotations

import logging
from pathlib import Path

from src.config import Settings
from src.pipeline.ai_client import AIClient
from src.pipeline.storage import SupabaseStorage

from .scoring import (
    aggregate_score,
    article_profile_text,
    code_exact_match,
    episode_profile_text,
    lexical_score,
)

logger = logging.getLogger(__name__)


class MatchSummary(dict):
    pass



def run_matching(
    settings: Settings,
    schema_path: Path,
    force_offline: bool = False,
    auto_threshold: float | None = None,
    auto_margin: float | None = None,
    top_candidates: int = 5,
) -> MatchSummary:
    storage = SupabaseStorage(settings.supabase_db_url)
    ai = AIClient(settings, force_offline=force_offline)

    threshold = auto_threshold if auto_threshold is not None else settings.auto_match_threshold
    margin = auto_margin if auto_margin is not None else settings.auto_match_margin

    summary: MatchSummary = MatchSummary(
        episodes_total=0,
        auto_matched=0,
        review_required=0,
        unmatched=0,
    )

    try:
        storage.ensure_schema(schema_path)
        episodes = storage.list_episodes()
        articles = storage.list_runroom_articles()

        # Prioritize Spanish pages while still allowing EN fallbacks.
        articles_sorted = sorted(articles, key=lambda a: 0 if a.get("lang") == "es" else 1)

        article_profiles = [article_profile_text(article) for article in articles_sorted]
        article_embeddings = ai.embed_texts(article_profiles)

        summary["episodes_total"] = len(episodes)

        for episode in episodes:
            episode_id = int(episode["id"])
            storage.clear_candidates_for_episode(episode_id)

            ep_text = episode_profile_text(episode)
            ep_emb = ai.embed_texts([ep_text])[0]

            ranked: list[tuple[dict, float, str, float, float]] = []
            for article, article_emb in zip(articles_sorted, article_embeddings):
                lex = lexical_score(episode, article)
                sem = ai.cosine_similarity(ep_emb, article_emb)
                code_exact = code_exact_match(episode, article)
                breakdown = aggregate_score(code_exact=code_exact, lexical=lex, semantic=sem)

                if article.get("lang") == "es":
                    breakdown.score = min(1.0, breakdown.score + 0.02)

                ranked.append((article, breakdown.score, breakdown.method, breakdown.lexical_score, breakdown.semantic_score))

            ranked.sort(key=lambda item: item[1], reverse=True)

            if not ranked:
                storage.set_episode_match(episode_id, None, "unmatched", None)
                summary["unmatched"] += 1
                continue

            best = ranked[0]
            second = ranked[1] if len(ranked) > 1 else None
            margin_ok = True if not second else (best[1] - second[1]) >= margin
            should_auto = best[1] >= threshold and margin_ok

            for idx, (article, score, method, _lex, _sem) in enumerate(ranked[:top_candidates]):
                is_selected = bool(should_auto and idx == 0)
                review_required = not should_auto
                storage.insert_candidate(
                    episode_id=episode_id,
                    article_id=int(article["id"]),
                    score=float(score),
                    method=method,
                    is_selected=is_selected,
                    review_required=review_required,
                )

            if should_auto:
                best_article = best[0]
                storage.set_episode_match(
                    episode_id=episode_id,
                    url=str(best_article["url"]),
                    status="auto_matched",
                    confidence=float(best[1]),
                )
                summary["auto_matched"] += 1
            else:
                storage.set_episode_match(
                    episode_id=episode_id,
                    url=None,
                    status="review_required",
                    confidence=float(best[1]),
                )
                summary["review_required"] += 1

    finally:
        storage.close()

    return summary
