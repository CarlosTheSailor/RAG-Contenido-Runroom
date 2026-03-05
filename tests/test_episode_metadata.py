from __future__ import annotations

import unittest
from pathlib import Path

from src.pipeline.episode_metadata import infer_episode_info
from src.pipeline.models import TranscriptSegment


class EpisodeMetadataTests(unittest.TestCase):
    def test_infer_code_and_guest_from_filename(self) -> None:
        path = Path("v1 Realworld R085_Nacho Bassino_1080p.mp4.txt")
        segments = [
            TranscriptSegment("00:00:00.000", 0.0, "Carlos Iglesias", "Intro"),
            TranscriptSegment("00:00:10.000", 10.0, "Nacho Bassino", "Hola"),
        ]

        info = infer_episode_info(path, segments)
        self.assertEqual(info.episode_code, "r085")
        self.assertIn("Nacho", info.title)
        self.assertIn("Nacho Bassino", info.guest_names)

    def test_infer_guest_from_speaker_when_title_missing(self) -> None:
        path = Path("Entrevista Rosa Medallia - 30_11_20 12.42.mp3.txt")
        segments = [
            TranscriptSegment("00:00:00.000", 0.0, "Carlos Iglesias", "Intro"),
            TranscriptSegment("00:00:12.000", 12.0, "Rosa Medallia", "Contenido"),
        ]

        info = infer_episode_info(path, segments)
        self.assertIsNone(info.episode_code)
        self.assertIn("Rosa Medallia", info.guest_names)


if __name__ == "__main__":
    unittest.main()
