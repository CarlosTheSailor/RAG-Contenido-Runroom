from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from src.application.newsletter_linkedin_generator import NewsletterLinkedInResult
from src.config import Settings
from src.interfaces.http.services import QueryApiService


def _settings(newsletter_rag_min_score: float) -> Settings:
    return Settings(
        supabase_db_url="postgresql://user:pass@localhost:5432/db",
        openai_api_key=None,
        openai_base_url="https://api.openai.com/v1",
        youtube_api_key=None,
        youtube_api_base_url="https://www.googleapis.com/youtube/v3",
        openai_embedding_model="text-embedding-3-large",
        openai_metadata_model="gpt-4.1-mini",
        openai_newsletter_model="gpt-4.1-mini",
        newsletter_rag_min_score=newsletter_rag_min_score,
        embedding_dim=1536,
        runroom_sitemap_url="https://www.runroom.com/sitemap.xml",
        auto_match_threshold=0.86,
        auto_match_margin=0.06,
        log_level="INFO",
    )


class _FakeGenerator:
    def __init__(self, settings: Settings, assets_dir: Path):  # noqa: ARG002
        self._settings = settings
        self._assets_dir = assets_dir

    def generate(self, payload, related_content=None, force_offline: bool = False):  # noqa: ANN001, ARG002
        return NewsletterLinkedInResult(
            output_text=f"ok: {payload.idea}",
            related_content=list(related_content or []),
            warnings=[],
            used_examples=[],
        )


class NewsletterLinkedInServiceTests(unittest.TestCase):
    @patch("src.interfaces.http.services.NewsletterLinkedInGenerator", new=_FakeGenerator)
    def test_filters_related_content_by_score_threshold(self) -> None:
        service = QueryApiService(settings=_settings(newsletter_rag_min_score=0.74), schema_path=Path("sql"))
        service.recommend_content = lambda **_: {  # type: ignore[method-assign]
            "results": [
                {"title": "A", "content_type": "episode", "score": 0.8, "matched_chunks": []},
                {"title": "B", "content_type": "case_study", "score": 0.6, "matched_chunks": []},
            ]
        }

        result = service.generate_newsletter_linkedin(idea="idea")

        self.assertEqual(len(result["related_content"]), 1)
        self.assertEqual(result["related_content"][0]["title"], "A")
        self.assertEqual(result["warnings"], [])

    @patch("src.interfaces.http.services.NewsletterLinkedInGenerator", new=_FakeGenerator)
    def test_warns_when_no_related_content_passes_threshold(self) -> None:
        service = QueryApiService(settings=_settings(newsletter_rag_min_score=0.9), schema_path=Path("sql"))
        service.recommend_content = lambda **_: {  # type: ignore[method-assign]
            "results": [
                {"title": "A", "content_type": "episode", "score": 0.8, "matched_chunks": []},
                {"title": "B", "content_type": "case_study", "score": 0.6, "matched_chunks": []},
            ]
        }

        result = service.generate_newsletter_linkedin(idea="idea")

        self.assertEqual(result["related_content"], [])
        self.assertTrue(any("score >=" in warning for warning in result["warnings"]))

    @patch("src.interfaces.http.services.NewsletterLinkedInGenerator", new=_FakeGenerator)
    def test_recommendation_scope_includes_runroom_lab(self) -> None:
        service = QueryApiService(settings=_settings(newsletter_rag_min_score=0.74), schema_path=Path("sql"))
        captured: dict[str, object] = {}

        def _fake_recommend_content(**kwargs):  # type: ignore[no-untyped-def]
            captured.update(kwargs)
            return {"results": []}

        service.recommend_content = _fake_recommend_content  # type: ignore[method-assign]
        service.generate_newsletter_linkedin(idea="idea")

        self.assertEqual(
            captured.get("content_types"),
            ["episode", "case_study", "runroom_lab"],
        )


if __name__ == "__main__":
    unittest.main()
