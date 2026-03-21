from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.theme_intel.models import SourceDocumentInput
from src.theme_intel.service import ThemeIntelService


class _FakeStorage:
    def ensure_schema(self, _schema_path: Path) -> None:
        return None

    def close(self) -> None:
        return None


class _FakeRepo:
    def __init__(self, candidates: list[dict[str, object]]) -> None:
        self.candidates = list(candidates)
        self.upserts: list[dict[str, object]] = []

    def list_html_fallback_source_documents(self, limit: int | None = None) -> list[dict[str, object]]:
        if limit is None:
            return list(self.candidates)
        return list(self.candidates[:limit])

    def upsert_source_document(self, run_id: int, doc: SourceDocumentInput, source_type: str, source_account: str) -> int:
        self.upserts.append(
            {
                "run_id": run_id,
                "doc": doc,
                "source_type": source_type,
                "source_account": source_account,
            }
        )
        return len(self.upserts)


class _FakeGmailClient:
    return_map: dict[str, SourceDocumentInput] = {}
    calls: list[dict[str, object]] = []

    def __init__(self, settings) -> None:  # noqa: ANN001
        self._settings = settings

    def get_messages(self, message_ids: list[str], ignore_missing: bool = False) -> dict[str, SourceDocumentInput]:
        self.__class__.calls.append(
            {
                "message_ids": list(message_ids),
                "ignore_missing": ignore_missing,
            }
        )
        return {message_id: self.__class__.return_map[message_id] for message_id in message_ids if message_id in self.__class__.return_map}


class ThemeIntelBackfillTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = ThemeIntelService(
            settings=SimpleNamespace(supabase_db_url="postgresql://demo/demo"),
            schema_path=Path("sql"),
        )

    def test_backfill_dry_run_reports_matches_without_updating(self) -> None:
        repo = _FakeRepo(
            [
                {
                    "id": 10,
                    "run_id": 5,
                    "source_type": "gmail",
                    "source_account": "newsletters@runroom.com",
                    "source_external_id": "msg-10",
                    "subject": "Placeholder",
                }
            ]
        )
        storage = _FakeStorage()

        with (
            patch("src.theme_intel.service.SupabaseStorage", lambda dsn: storage),
            patch("src.theme_intel.service.ThemeIntelRepository", lambda _storage: repo),
        ):
            summary = self.service.backfill_html_fallback_source_documents(dry_run=True)

        self.assertTrue(summary["dry_run"])
        self.assertEqual(summary["matched"], 1)
        self.assertEqual(summary["updated"], 0)
        self.assertEqual(repo.upserts, [])

    def test_backfill_updates_changed_docs_and_skips_missing_messages(self) -> None:
        repo = _FakeRepo(
            [
                {
                    "id": 10,
                    "run_id": 5,
                    "source_type": "gmail",
                    "source_account": "newsletters@runroom.com",
                    "source_external_id": "msg-10",
                    "raw_text": "old raw",
                    "cleaned_text": "old cleaned",
                    "links_json": [],
                    "metadata_json": {},
                    "subject": "Placeholder",
                },
                {
                    "id": 11,
                    "run_id": 5,
                    "source_type": "gmail",
                    "source_account": "newsletters@runroom.com",
                    "source_external_id": "msg-missing",
                    "raw_text": "old raw",
                    "cleaned_text": "old cleaned",
                    "links_json": [],
                    "metadata_json": {},
                    "subject": "Missing",
                },
            ]
        )
        storage = _FakeStorage()
        _FakeGmailClient.calls = []
        _FakeGmailClient.return_map = {
            "msg-10": SourceDocumentInput(
                source_external_id="msg-10",
                source_thread_id="thr-10",
                subject="Placeholder",
                sender="sender@example.com",
                received_at=None,
                labels=["AI"],
                raw_text="new raw from html",
                cleaned_text="new cleaned from html",
                links=["https://example.com/post"],
                metadata={"extraction_mode": "html_fallback"},
            )
        }

        with (
            patch("src.theme_intel.service.SupabaseStorage", lambda dsn: storage),
            patch("src.theme_intel.service.ThemeIntelRepository", lambda _storage: repo),
            patch("src.theme_intel.service.GmailClient", _FakeGmailClient),
        ):
            summary = self.service.backfill_html_fallback_source_documents()

        self.assertFalse(summary["dry_run"])
        self.assertEqual(summary["matched"], 2)
        self.assertEqual(summary["processed"], 2)
        self.assertEqual(summary["updated"], 1)
        self.assertEqual(summary["skipped_missing"], 1)
        self.assertEqual(summary["unchanged"], 0)
        self.assertEqual(len(repo.upserts), 1)
        self.assertEqual(repo.upserts[0]["run_id"], 5)
        self.assertEqual(repo.upserts[0]["doc"].metadata["extraction_mode"], "html_fallback")
        self.assertEqual(_FakeGmailClient.calls[0]["message_ids"], ["msg-10", "msg-missing"])
        self.assertTrue(_FakeGmailClient.calls[0]["ignore_missing"])


if __name__ == "__main__":
    unittest.main()
