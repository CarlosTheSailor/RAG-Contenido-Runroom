from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.pipeline.storage import SimilarContentQueryError
from src.theme_intel.service import ThemeIntelService, _resolve_run_status


class _FakeRecommendResponse:
    def __init__(self, results: list[dict[str, object]]) -> None:
        self._results = results

    def to_dict(self) -> dict[str, object]:
        return {"results": list(self._results)}


class _SequencedRecommendContentUseCase:
    scripted_results: list[object] = []
    calls: list[dict[str, object]] = []

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        pass

    def execute(self, request):  # noqa: ANN001
        self.__class__.calls.append(
            {
                "content_types": list(request.content_types or []),
                "top_k": request.top_k,
                "fetch_k": request.fetch_k,
                "statement_timeout_ms": request.statement_timeout_ms,
                "lock_timeout_ms": request.lock_timeout_ms,
            }
        )
        if not self.__class__.scripted_results:
            raise AssertionError("No hay respuesta simulada para RecommendContentUseCase.execute().")
        result = self.__class__.scripted_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return _FakeRecommendResponse(result)


class ThemeIntelRelatedRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = ThemeIntelService(
            settings=SimpleNamespace(supabase_db_url="postgresql://demo/demo"),
            schema_path=Path("sql"),
        )

    def _run_related(
        self,
        scripted_results: list[object],
        *,
        related_counts_by_type: dict[str, int],
    ) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
        events: list[dict[str, object]] = []
        warnings: list[dict[str, object]] = []

        def log(stage: str, message: str, **payload: object) -> None:
            event = {"stage": stage, "message": message}
            event.update(payload)
            events.append(event)

        _SequencedRecommendContentUseCase.scripted_results = list(scripted_results)
        _SequencedRecommendContentUseCase.calls = []

        with (
            patch("src.theme_intel.service.RecommendContentUseCase", _SequencedRecommendContentUseCase),
            patch("src.theme_intel.service.OpenAIEmbeddingClient", lambda *args, **kwargs: object()),
            patch("src.theme_intel.service.ContentChunksRepository", lambda storage: storage),
        ):
            result = self.service._recommend_related_content(
                storage=object(),
                text="texto de prueba",
                top_k=3,
                related_counts_by_type=related_counts_by_type,
                progress_logger=log,
                progress_context={"topic_id": 41, "theme_index": 1, "themes_total": 3},
                warning_collector=warnings,
            )

        return result, warnings, events

    def test_typed_query_timeout_becomes_warning_and_selection_continues(self) -> None:
        scripted_results = [
            [
                {"content_item_id": 1, "content_type": "case_study", "score": 0.91, "title": "base case"},
                {"content_item_id": 2, "content_type": "runroom_lab", "score": 0.81, "title": "base lab"},
            ],
            SimilarContentQueryError(
                message="canceling statement due to statement timeout",
                content_types=["episode"],
                duration_ms=15012,
                sqlstate="57014",
                statement_timeout_ms=15000,
                lock_timeout_ms=2000,
            ),
        ]

        selected, warnings, events = self._run_related(
            scripted_results,
            related_counts_by_type={"case_study": 1, "episode": 1},
        )

        self.assertGreaterEqual(len(selected), 1)
        self.assertEqual(len(warnings), 1)
        self.assertEqual(warnings[0]["stage"], "theme_related_typed_query_failed")
        self.assertEqual(warnings[0]["forced_type"], "episode")
        self.assertTrue(warnings[0]["sql_timeout"])
        failed_events = [event for event in events if event["stage"] == "theme_related_typed_query_failed"]
        self.assertEqual(len(failed_events), 1)
        self.assertEqual(failed_events[0]["forced_type"], "episode")
        self.assertIn("duration_ms", failed_events[0])
        self.assertTrue(any(event["stage"] == "theme_related_selection_done" for event in events))

    def test_base_query_failure_still_uses_typed_queries(self) -> None:
        scripted_results = [
            SimilarContentQueryError(
                message="canceling statement due to statement timeout",
                content_types=[],
                duration_ms=15008,
                sqlstate="57014",
                statement_timeout_ms=15000,
                lock_timeout_ms=2000,
            ),
            [{"content_item_id": 10, "content_type": "case_study", "score": 0.72, "title": "forced case"}],
            [{"content_item_id": 11, "content_type": "episode", "score": 0.61, "title": "forced episode"}],
        ]

        selected, warnings, events = self._run_related(
            scripted_results,
            related_counts_by_type={"case_study": 1, "episode": 1},
        )

        self.assertEqual({int(item["content_item_id"]) for item in selected}, {10, 11})
        self.assertEqual(warnings[0]["stage"], "theme_related_base_query_failed")
        self.assertTrue(any(event["stage"] == "theme_related_typed_query_done" for event in events))

    def test_success_path_persists_durations_for_done_events(self) -> None:
        scripted_results = [
            [{"content_item_id": 1, "content_type": "episode", "score": 0.91, "title": "base episode"}],
            [{"content_item_id": 2, "content_type": "case_study", "score": 0.71, "title": "forced case"}],
        ]

        _, warnings, events = self._run_related(
            scripted_results,
            related_counts_by_type={"case_study": 1},
        )

        self.assertEqual(warnings, [])
        done_stages = {
            event["stage"]: event
            for event in events
            if event["stage"] in {"theme_related_base_query_done", "theme_related_typed_query_done", "theme_related_merge_done", "theme_related_selection_done"}
        }
        self.assertEqual(
            set(done_stages.keys()),
            {"theme_related_base_query_done", "theme_related_typed_query_done", "theme_related_merge_done", "theme_related_selection_done"},
        )
        for event in done_stages.values():
            self.assertIn("duration_ms", event)

    def test_skips_typed_query_when_base_already_covers_forced_type(self) -> None:
        scripted_results = [
            [
                {"content_item_id": 1, "content_type": "episode", "score": 0.91, "title": "base episode"},
                {"content_item_id": 2, "content_type": "episode", "score": 0.88, "title": "base episode 2"},
            ],
        ]

        selected, warnings, events = self._run_related(
            scripted_results,
            related_counts_by_type={"episode": 1},
        )

        self.assertEqual(len(selected), 2)
        self.assertEqual(warnings, [])
        self.assertEqual(len(_SequencedRecommendContentUseCase.calls), 1)
        skipped_events = [event for event in events if event["stage"] == "theme_related_typed_query_skipped"]
        self.assertEqual(len(skipped_events), 1)
        self.assertEqual(skipped_events[0]["forced_type"], "episode")
        self.assertEqual(skipped_events[0]["base_coverage"], 2)

    def test_resolve_run_status_marks_partial_failed_when_themes_exist(self) -> None:
        self.assertEqual(
            _resolve_run_status(
                stats={"themes_created": 1, "themes_merged": 0},
                errors=[{"stage": "theme_related_typed_query_failed"}],
            ),
            "partial_failed",
        )
        self.assertEqual(
            _resolve_run_status(
                stats={"themes_created": 0, "themes_merged": 0},
                errors=[{"stage": "theme_related_typed_query_failed"}],
            ),
            "failed",
        )
        self.assertEqual(
            _resolve_run_status(
                stats={"themes_created": 0, "themes_merged": 0},
                errors=[],
            ),
            "succeeded",
        )


if __name__ == "__main__":
    unittest.main()
