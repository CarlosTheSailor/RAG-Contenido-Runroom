from __future__ import annotations

from pathlib import Path
from typing import Any

from src.pipeline.models import RunroomArticle
from src.pipeline.storage import SupabaseStorage


class MatchingRepository:
    def __init__(self, storage: SupabaseStorage):
        self._storage = storage

    def upsert_runroom_article(self, article: RunroomArticle) -> int:
        return self._storage.upsert_runroom_article(article)

    def upsert_runroom_articles(self, articles: list[RunroomArticle]) -> int:
        return self._storage.upsert_runroom_articles(articles)

    def list_runroom_articles(self) -> list[dict[str, Any]]:
        return self._storage.list_runroom_articles()

    def list_review_candidates(self, limit_episodes: int | None = None) -> list[dict[str, Any]]:
        return self._storage.list_review_candidates(limit_episodes=limit_episodes)

    def export_review_report(self, output_path: Path) -> int:
        return self._storage.export_review_report(output_path=output_path)

    def episode_exists(self, episode_id: int) -> bool:
        return self._storage.episode_exists(episode_id=episode_id)

    def set_manual_match(self, episode_id: int, article_id: int, confidence: float | None = None) -> None:
        self._storage.set_manual_match(episode_id=episode_id, article_id=article_id, confidence=confidence)
