from __future__ import annotations

import unittest

from src.matching.scoring import aggregate_score, code_exact_match, lexical_score


class MatchingScoreTests(unittest.TestCase):
    def test_code_exact_match(self) -> None:
        episode = {"episode_code": "e001", "title": "Customer Experience", "guest_names": []}
        article = {"episode_code_hint": "e001", "slug": "e001-customer-experience", "title": "E001"}
        self.assertTrue(code_exact_match(episode, article))

    def test_lexical_score_guest_boost(self) -> None:
        episode = {
            "episode_code": "r072",
            "title": "Cultura de producto en Buffer",
            "guest_names": ["Mike San Roman"],
        }
        article = {
            "episode_code_hint": None,
            "slug": "cultura-producto-buffer",
            "title": "Cultura de producto en Buffer, con Mike San Román",
            "description": "Entrevista con Mike",
        }
        score = lexical_score(episode, article)
        self.assertGreater(score, 0.2)

    def test_aggregate_prefers_code_exact(self) -> None:
        breakdown = aggregate_score(code_exact=True, lexical=0.2, semantic=0.1)
        self.assertEqual(breakdown.method, "code_exact")
        self.assertGreaterEqual(breakdown.score, 0.99)


if __name__ == "__main__":
    unittest.main()
