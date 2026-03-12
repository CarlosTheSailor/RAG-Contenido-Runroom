from __future__ import annotations

import unittest

from src.theme_intel.service import _select_mixed_related_candidates


class ThemeIntelRelatedSelectionTests(unittest.TestCase):
    def test_forces_runroom_lab_when_available(self) -> None:
        candidates = [
            {"content_item_id": 1, "content_type": "episode", "score": 0.96, "title": "e1"},
            {"content_item_id": 2, "content_type": "episode", "score": 0.95, "title": "e2"},
            {"content_item_id": 3, "content_type": "case_study", "score": 0.90, "title": "c1"},
            {"content_item_id": 4, "content_type": "runroom_lab", "score": 0.10, "title": "l1"},
            {"content_item_id": 5, "content_type": "episode", "score": 0.89, "title": "e3"},
        ]

        selected = _select_mixed_related_candidates(
            candidates=candidates,
            top_k=3,
            forced_min_by_type={"runroom_lab": 1},
        )

        self.assertEqual(len(selected), 3)
        self.assertTrue(any(str(item.get("content_type")) == "runroom_lab" for item in selected))

    def test_respects_top_k_and_no_duplicates(self) -> None:
        candidates = [
            {"content_item_id": 1, "content_type": "episode", "score": 0.96},
            {"content_item_id": 1, "content_type": "episode", "score": 0.91},
            {"content_item_id": 2, "content_type": "runroom_lab", "score": 0.40},
            {"content_item_id": 3, "content_type": "case_study", "score": 0.39},
        ]
        selected = _select_mixed_related_candidates(
            candidates=candidates,
            top_k=2,
            forced_min_by_type={"runroom_lab": 1},
        )

        self.assertEqual(len(selected), 2)
        ids = [int(item["content_item_id"]) for item in selected]
        self.assertEqual(len(ids), len(set(ids)))

if __name__ == "__main__":
    unittest.main()
