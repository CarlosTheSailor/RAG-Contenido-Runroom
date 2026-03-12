from __future__ import annotations

import unittest

from src.theme_intel.service import _normalize_content_type_key, _select_mixed_related_candidates


class ThemeIntelRelatedSelectionTests(unittest.TestCase):
    def test_normalize_content_type_key_handles_hyphens_spaces_and_case(self) -> None:
        self.assertEqual(_normalize_content_type_key("runroom_lab"), "runroom_lab")
        self.assertEqual(_normalize_content_type_key("runroom-lab"), "runroom_lab")
        self.assertEqual(_normalize_content_type_key("Runroom Lab"), "runroom_lab")
        self.assertEqual(_normalize_content_type_key("case_study"), "case_study")

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

    def test_forced_types_match_hyphenated_candidate_content_type(self) -> None:
        candidates = [
            {"content_item_id": 10, "content_type": "episode", "score": 0.91},
            {"content_item_id": 11, "content_type": "runroom-lab", "score": 0.11},
            {"content_item_id": 12, "content_type": "case-study", "score": 0.10},
        ]
        selected = _select_mixed_related_candidates(
            candidates=candidates,
            top_k=3,
            forced_min_by_type={"runroom_lab": 1, "case_study": 1},
        )

        selected_types = {str(item.get("content_type")) for item in selected}
        self.assertIn("runroom_lab", selected_types)
        self.assertIn("case_study", selected_types)

if __name__ == "__main__":
    unittest.main()
