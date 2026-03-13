from __future__ import annotations

import unittest

from src.theme_intel.service import RUN_DEBUG_EVENT_LIMIT, _append_run_debug_event, _short_debug_text


class ThemeIntelRunDebugTests(unittest.TestCase):
    def test_append_run_debug_event_updates_stage_and_timestamp(self) -> None:
        stats: dict[str, object] = {}

        _append_run_debug_event(stats, "theme_embedding_start", "Generando embedding.", theme_index=3)

        self.assertEqual(stats["current_stage"], "theme_embedding_start")
        self.assertEqual(stats["current_stage_message"], "Generando embedding.")
        self.assertIn("last_progress_at", stats)
        self.assertEqual(len(stats["debug_events"]), 1)
        self.assertEqual(stats["debug_events"][0]["theme_index"], 3)

    def test_append_run_debug_event_caps_history(self) -> None:
        stats: dict[str, object] = {}

        for index in range(RUN_DEBUG_EVENT_LIMIT + 5):
            _append_run_debug_event(stats, f"stage_{index}", f"event {index}")

        events = stats["debug_events"]
        self.assertEqual(len(events), RUN_DEBUG_EVENT_LIMIT)
        self.assertEqual(events[0]["stage"], "stage_5")
        self.assertEqual(events[-1]["stage"], f"stage_{RUN_DEBUG_EVENT_LIMIT + 4}")

    def test_short_debug_text_trims_long_values(self) -> None:
        self.assertEqual(_short_debug_text("  hola   mundo  ", 20), "hola mundo")
        self.assertEqual(_short_debug_text("abcdefghij", 7), "abcd...")


if __name__ == "__main__":
    unittest.main()
