from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.pipeline.parser import parse_transcript


class ParserTests(unittest.TestCase):
    def test_parse_with_speaker(self) -> None:
        content = """[00:00:00.21] - Carlos Iglesias
Hola mundo.
[00:00:10.30] - Invitada
Respuesta corta.
"""
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8") as f:
            f.write(content)
            temp_path = Path(f.name)
        try:
            segments = parse_transcript(temp_path)
            self.assertEqual(len(segments), 2)
            self.assertEqual(segments[0].speaker, "Carlos Iglesias")
            self.assertEqual(segments[1].speaker, "Invitada")
            self.assertIn("Hola mundo", segments[0].text)
        finally:
            temp_path.unlink(missing_ok=True)

    def test_parse_without_speaker(self) -> None:
        content = """[00:00:00.720]
Texto sin speaker.
[00:00:05.120]
Otro bloque.
"""
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8") as f:
            f.write(content)
            temp_path = Path(f.name)
        try:
            segments = parse_transcript(temp_path)
            self.assertEqual(len(segments), 2)
            self.assertIsNone(segments[0].speaker)
            self.assertTrue(segments[0].start_ts_sec < segments[1].start_ts_sec)
        finally:
            temp_path.unlink(missing_ok=True)

    def test_parse_all_real_transcriptions(self) -> None:
        base = Path(__file__).resolve().parents[1] / "transcripciones"
        files = sorted(base.glob("*.txt"))
        self.assertGreaterEqual(len(files), 71)

        parsed_ok = 0
        for file_path in files:
            segments = parse_transcript(file_path)
            if segments:
                parsed_ok += 1
            self.assertGreaterEqual(len(segments), 1, msg=f"No segments: {file_path.name}")

        self.assertEqual(parsed_ok, len(files))


if __name__ == "__main__":
    unittest.main()
