from __future__ import annotations

import unittest

from src.pipeline.chunking import build_chunks
from src.pipeline.models import TranscriptSegment


class ChunkingTests(unittest.TestCase):
    def test_chunking_with_overlap(self) -> None:
        segments = [
            TranscriptSegment(
                raw_timestamp="00:00:00.000",
                start_ts_sec=0.0,
                speaker="Carlos",
                text=(
                    "Este es un texto largo. "
                    "Habla sobre customer experience y producto digital. "
                    "Incluye varios conceptos y ejemplos reales. "
                    "Tambien menciona estrategia y organizacion."
                ),
            ),
            TranscriptSegment(
                raw_timestamp="00:00:15.000",
                start_ts_sec=15.0,
                speaker="Invitada",
                text=(
                    "Seguimos con mas contenido relevante para probar el chunking. "
                    "Se discute research, discovery y datos cualitativos. "
                    "Terminamos con aprendizajes accionables para equipos."
                ),
            ),
        ]

        chunks = build_chunks(segments, target_tokens=28, overlap_tokens=8)
        self.assertGreaterEqual(len(chunks), 2)

        first_tail = set(chunks[0].text.lower().split()[-6:])
        second_head = set(chunks[1].text.lower().split()[:12])
        self.assertGreaterEqual(len(first_tail.intersection(second_head)), 1)

    def test_chunking_preserves_order(self) -> None:
        segments = [
            TranscriptSegment("00:00:00.000", 0.0, None, "Uno dos tres."),
            TranscriptSegment("00:00:05.000", 5.0, None, "Cuatro cinco seis."),
        ]
        chunks = build_chunks(segments, target_tokens=100, overlap_tokens=0)
        self.assertEqual(len(chunks), 1)
        self.assertIn("Uno dos tres", chunks[0].text)
        self.assertIn("Cuatro cinco seis", chunks[0].text)


if __name__ == "__main__":
    unittest.main()
