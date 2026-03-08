from __future__ import annotations

import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

from src.config import Settings
from src.pipeline.manual_episode_ingest import (
    DuplicateEpisodeSourceFilenameError,
    ingest_uploaded_episode,
)


def _settings() -> Settings:
    return Settings(
        supabase_db_url="postgresql://user:pass@localhost:5432/db",
        openai_api_key="test-key",
        openai_base_url="https://api.openai.com/v1",
        youtube_api_key=None,
        youtube_api_base_url="https://www.googleapis.com/youtube/v3",
        openai_embedding_model="text-embedding-3-large",
        openai_metadata_model="gpt-4.1-mini",
        openai_newsletter_model=None,
        newsletter_rag_min_score=0.74,
        embedding_dim=1536,
        runroom_sitemap_url="https://www.runroom.com/sitemap.xml",
        auto_match_threshold=0.86,
        auto_match_margin=0.06,
        log_level="INFO",
    )


class ManualEpisodeIngestTests(unittest.TestCase):
    @patch("src.pipeline.manual_episode_ingest.fetch_url_text")
    @patch("src.pipeline.manual_episode_ingest.AIClient")
    @patch("src.pipeline.manual_episode_ingest.SupabaseStorage")
    def test_ingest_uploaded_episode_success(self, storage_cls, ai_cls, fetch_url_text_mock) -> None:
        fetch_url_text_mock.return_value = "<html><body><h1>R999 - Episodio de prueba</h1></body></html>"

        storage = storage_cls.return_value
        storage.get_episode_by_source_filename.return_value = None
        storage.upsert_episode.return_value = 123
        storage.sync_episode_to_canonical.return_value = 456

        ai = ai_cls.return_value
        ai.embed_texts.side_effect = lambda texts: [[0.1, 0.2] for _ in texts]
        ai.chunk_metadata.return_value = {"topic": "producto"}

        transcript_bytes = (
            b"[00:00:00.000] - Carlos\nHola equipo.\n"
            b"[00:00:05.000] - Invitado\nGracias por invitarme."
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            summary = ingest_uploaded_episode(
                settings=_settings(),
                schema_path=Path("sql"),
                source_filename="episodio.txt",
                transcript_bytes=transcript_bytes,
                runroom_url="https://www.runroom.com/realworld/r999",
                transcripts_dir=Path(temp_dir),
            )

            self.assertEqual(summary["episode_id"], 123)
            self.assertEqual(summary["content_item_id"], 456)
            self.assertEqual(summary["source_filename"], "episodio.txt")
            self.assertEqual(summary["title"], "R999 - Episodio de prueba")
            self.assertGreaterEqual(summary["chunks_written"], 1)
            self.assertTrue((Path(temp_dir) / "episodio.txt").exists())

        storage.set_episode_match.assert_called_once_with(
            episode_id=123,
            url="https://www.runroom.com/realworld/r999",
            status="manual_matched",
            confidence=1.0,
        )
        storage.sync_episode_to_canonical.assert_called_once_with(123)

    @patch("src.pipeline.manual_episode_ingest.fetch_url_text")
    @patch("src.pipeline.manual_episode_ingest.AIClient")
    @patch("src.pipeline.manual_episode_ingest.SupabaseStorage")
    def test_ingest_uploaded_episode_fails_when_missing_h1(self, storage_cls, _ai_cls, fetch_url_text_mock) -> None:
        fetch_url_text_mock.return_value = "<html><body><h2>Sin titulo</h2></body></html>"
        storage = storage_cls.return_value
        storage.get_episode_by_source_filename.return_value = None

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(ValueError):
                ingest_uploaded_episode(
                    settings=_settings(),
                    schema_path=Path("sql"),
                    source_filename="episodio.txt",
                    transcript_bytes=b"[00:00:00.000] - Carlos\nHola",
                    runroom_url="https://www.runroom.com/realworld/r999",
                    transcripts_dir=Path(temp_dir),
                )

        storage.upsert_episode.assert_not_called()

    @patch("src.pipeline.manual_episode_ingest.fetch_url_text")
    @patch("src.pipeline.manual_episode_ingest.AIClient")
    @patch("src.pipeline.manual_episode_ingest.SupabaseStorage")
    def test_ingest_uploaded_episode_propagates_fetch_error(self, storage_cls, _ai_cls, fetch_url_text_mock) -> None:
        fetch_url_text_mock.side_effect = urllib.error.URLError("network")
        storage = storage_cls.return_value
        storage.get_episode_by_source_filename.return_value = None

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(urllib.error.URLError):
                ingest_uploaded_episode(
                    settings=_settings(),
                    schema_path=Path("sql"),
                    source_filename="episodio.txt",
                    transcript_bytes=b"[00:00:00.000] - Carlos\nHola",
                    runroom_url="https://www.runroom.com/realworld/r999",
                    transcripts_dir=Path(temp_dir),
                )

    @patch("src.pipeline.manual_episode_ingest.fetch_url_text")
    @patch("src.pipeline.manual_episode_ingest.AIClient")
    @patch("src.pipeline.manual_episode_ingest.SupabaseStorage")
    def test_ingest_uploaded_episode_blocks_duplicate_source_filename(
        self,
        storage_cls,
        _ai_cls,
        fetch_url_text_mock,
    ) -> None:
        storage = storage_cls.return_value
        storage.get_episode_by_source_filename.return_value = {"id": 1}

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(DuplicateEpisodeSourceFilenameError):
                ingest_uploaded_episode(
                    settings=_settings(),
                    schema_path=Path("sql"),
                    source_filename="episodio.txt",
                    transcript_bytes=b"[00:00:00.000] - Carlos\nHola",
                    runroom_url="https://www.runroom.com/realworld/r999",
                    transcripts_dir=Path(temp_dir),
                )

        fetch_url_text_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
