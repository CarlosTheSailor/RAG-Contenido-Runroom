from __future__ import annotations

import unittest

from src.content.recommendation import _aggregate_and_rerank


class ContentRecommendationTests(unittest.TestCase):
    def test_aggregate_and_rerank_returns_diverse_topk(self) -> None:
        rows = [
            {
                "content_item_id": 1,
                "content_type": "episode",
                "title": "Episode A",
                "url": "https://example.com/a",
                "metadata_json": {"client": "A"},
                "chunk_id": 101,
                "section_key": "other",
                "chunk_text": "texto episodio a",
                "similarity": 0.95,
            },
            {
                "content_item_id": 1,
                "content_type": "episode",
                "title": "Episode A",
                "url": "https://example.com/a",
                "metadata_json": {"client": "A"},
                "chunk_id": 102,
                "section_key": "other",
                "chunk_text": "texto episodio a 2",
                "similarity": 0.90,
            },
            {
                "content_item_id": 2,
                "content_type": "episode",
                "title": "Episode B",
                "url": "https://example.com/b",
                "metadata_json": {"client": "B"},
                "chunk_id": 201,
                "section_key": "other",
                "chunk_text": "texto episodio b",
                "similarity": 0.94,
            },
            {
                "content_item_id": 3,
                "content_type": "case_study",
                "title": "Case C",
                "url": "https://example.com/c",
                "metadata_json": {"client": "C"},
                "chunk_id": 301,
                "section_key": "results",
                "chunk_text": "texto case c",
                "similarity": 0.92,
            },
        ]

        ranked = _aggregate_and_rerank(rows, top_k=3)

        self.assertEqual(len(ranked), 3)
        self.assertGreater(ranked[0].score, 0)
        self.assertTrue(any(item.content_type == "case_study" for item in ranked))


if __name__ == "__main__":
    unittest.main()
