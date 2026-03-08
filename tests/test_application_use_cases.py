from __future__ import annotations

import unittest

from src.application.use_cases.query_similar import QuerySimilarRequest, QuerySimilarUseCase
from src.application.use_cases.recommend_content import RecommendContentRequest, RecommendContentUseCase


class _FakeEmbeddingClient:
    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [[0.1, 0.2, 0.3] for _ in texts]


class _FakeLegacyRepository:
    def query_similar_chunks(self, query_embedding: list[float], top_k: int = 8) -> list[dict[str, object]]:
        return [
            {
                "similarity": 0.91,
                "episode_code": "r085",
                "episode_title": "Episode 85",
                "runroom_article_url": "https://runroom.com/realworld/r085",
                "start_ts_sec": 125.0,
                "text": "texto de prueba",
            }
        ][:top_k]


class _FakeContentRepository:
    def query_similar_content_chunks(
        self,
        query_embedding: list[float],
        top_k: int = 60,
        content_types: list[str] | None = None,
        source: str | None = None,
        language: str | None = None,
        exclude_content_item_id: int | None = None,
    ) -> list[dict[str, object]]:
        return [
            {
                "content_item_id": 10,
                "content_type": "episode",
                "title": "Episode A",
                "url": "https://example.com/a",
                "metadata_json": {"source": "rw"},
                "chunk_id": 1,
                "section_key": "other",
                "chunk_text": "texto episodio",
                "similarity": 0.95,
            },
            {
                "content_item_id": 11,
                "content_type": "case_study",
                "title": "Case B",
                "url": "https://example.com/b",
                "metadata_json": {"source": "rw"},
                "chunk_id": 2,
                "section_key": "results",
                "chunk_text": "texto case",
                "similarity": 0.9,
            },
        ][:top_k]


class ApplicationUseCasesTests(unittest.TestCase):
    def test_query_similar_use_case_with_fake_dependencies(self) -> None:
        use_case = QuerySimilarUseCase(
            embedding_client=_FakeEmbeddingClient(),
            repository=_FakeLegacyRepository(),
        )
        result = use_case.execute(QuerySimilarRequest(text="customer centric", top_k=3))

        self.assertEqual(result.query, "customer centric")
        self.assertEqual(result.top_k, 3)
        self.assertEqual(len(result.results), 1)
        self.assertEqual(result.results[0].start_ts_hhmmss, "00:02:05")

    def test_recommend_content_use_case_with_fake_dependencies(self) -> None:
        use_case = RecommendContentUseCase(
            embedding_client=_FakeEmbeddingClient(),
            repository=_FakeContentRepository(),
        )
        result = use_case.execute(
            RecommendContentRequest(
                text="product discovery",
                top_k=2,
                fetch_k=20,
                group_by_type=True,
            )
        )

        self.assertTrue(result.grouped)
        self.assertEqual(result.total_candidates, 2)
        self.assertIn("episode", result.results_by_type or {})
        self.assertIn("case_study", result.results_by_type or {})


if __name__ == "__main__":
    unittest.main()
