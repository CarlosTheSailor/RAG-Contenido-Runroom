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
    def test_recommendation_scope_uses_all_available_types_without_type_bias(self) -> None:
        service = QueryApiService(settings=_settings(newsletter_rag_min_score=0.74), schema_path=Path("sql"))
        captured: dict[str, object] = {}

        def _fake_recommend_content(**kwargs):  # type: ignore[no-untyped-def]
            captured.update(kwargs)
            return {"results": []}

        service.recommend_content = _fake_recommend_content  # type: ignore[method-assign]
        service.generate_newsletter_linkedin(idea="idea")

        self.assertIsNone(captured.get("content_types"))
        self.assertEqual(captured.get("prefer_type_diversity"), False)
        self.assertEqual(captured.get("apply_runroom_lab_lexical_boost"), False)

    def test_list_newsletter_ideas_prioritizes_fresh_topics_then_used(self) -> None:
        service = QueryApiService(settings=_settings(newsletter_rag_min_score=0.74), schema_path=Path("sql"))

        def _fake_list_theme_intel_topics(**kwargs):  # type: ignore[no-untyped-def]
            status = kwargs.get("status")
            if status == "new":
                return [
                    {
                        "id": 1,
                        "title": "Nuevo",
                        "context_text": "Contexto nuevo",
                        "canonical_text": "Canonico nuevo",
                        "score": 5,
                        "last_seen_at": "2026-03-20T10:00:00+00:00",
                        "status": "new",
                    }
                ]
            if status == "in_progress":
                return [
                    {
                        "id": 2,
                        "title": "En progreso",
                        "context_text": "Contexto progreso",
                        "canonical_text": "Canonico progreso",
                        "score": 4,
                        "last_seen_at": "2026-03-19T10:00:00+00:00",
                        "status": "in_progress",
                    }
                ]
            if status == "used":
                return [
                    {
                        "id": 3,
                        "title": "Usado",
                        "context_text": "Contexto usado",
                        "canonical_text": "Canonico usado",
                        "score": 3,
                        "last_seen_at": "2026-03-18T10:00:00+00:00",
                        "status": "used",
                    }
                ]
            return []

        service.list_theme_intel_topics = _fake_list_theme_intel_topics  # type: ignore[method-assign]
        result = service.list_newsletter_linkedin_ideas(limit=3)

        self.assertEqual([idea["topic_id"] for idea in result["ideas"]], [1, 2, 3])
        self.assertFalse(result["pool_exhausted"])

    def test_list_newsletter_ideas_excludes_seen_and_marks_pool_exhausted(self) -> None:
        service = QueryApiService(settings=_settings(newsletter_rag_min_score=0.74), schema_path=Path("sql"))

        def _fake_list_theme_intel_topics(**kwargs):  # type: ignore[no-untyped-def]
            status = kwargs.get("status")
            if status == "new":
                return [
                    {
                        "id": 11,
                        "title": "Tema once",
                        "context_text": "Contexto once",
                        "canonical_text": "Canonico once",
                        "score": 2,
                        "last_seen_at": "2026-03-20T10:00:00+00:00",
                        "status": "new",
                    }
                ]
            if status == "in_progress":
                return [
                    {
                        "id": 12,
                        "title": "Tema doce",
                        "context_text": "Contexto doce",
                        "canonical_text": "Canonico doce",
                        "score": 1.5,
                        "last_seen_at": "2026-03-19T10:00:00+00:00",
                        "status": "in_progress",
                    }
                ]
            return []

        service.list_theme_intel_topics = _fake_list_theme_intel_topics  # type: ignore[method-assign]
        result = service.list_newsletter_linkedin_ideas(exclude_topic_ids=[11], limit=2)

        self.assertEqual([idea["topic_id"] for idea in result["ideas"]], [12])
        self.assertTrue(result["pool_exhausted"])

    def test_list_newsletter_ideas_builds_context_preview(self) -> None:
        service = QueryApiService(settings=_settings(newsletter_rag_min_score=0.74), schema_path=Path("sql"))

        service.list_theme_intel_topics = lambda **kwargs: [  # type: ignore[method-assign]
            {
                "id": 44,
                "title": "Tema largo",
                "context_text": "Linea uno.\n\nLinea dos con bastante detalle para comprobar que el preview compacta espacios.",
                "canonical_text": "Canonico largo",
                "score": 2.1,
                "last_seen_at": "2026-03-20T10:00:00+00:00",
                "status": kwargs.get("status"),
            }
        ] if kwargs.get("status") == "new" else []

        result = service.list_newsletter_linkedin_ideas(limit=1)

        self.assertEqual(result["ideas"][0]["topic_id"], 44)
        self.assertIn("Linea uno. Linea dos", result["ideas"][0]["context_preview"])


if __name__ == "__main__":
    unittest.main()
