from __future__ import annotations

import unittest

from src.content.chunking import chunk_sections
from src.content.models import CanonicalSection


class ContentChunkingTests(unittest.TestCase):
    def test_chunking_preserves_section_boundaries(self) -> None:
        sections = [
            CanonicalSection(
                section_order=0,
                section_key="description",
                section_title="Descripción",
                text=(
                    "Este bloque explica el contexto del caso. "
                    "Incluye detalles y objetivos de negocio. "
                    "También menciona aprendizajes iniciales."
                ),
                token_count=30,
                metadata={},
                source_locator={"line_start": 10, "line_end": 18},
            ),
            CanonicalSection(
                section_order=1,
                section_key="results",
                section_title="Resultados",
                text=(
                    "El impacto fue inmediato. "
                    "Se mejoraron métricas de adopción y conversión. "
                    "El equipo interno validó el enfoque."
                ),
                token_count=28,
                metadata={},
                source_locator={"line_start": 19, "line_end": 28},
            ),
        ]

        chunks = chunk_sections(sections, target_tokens=20, overlap_tokens=6)
        self.assertGreaterEqual(len(chunks), 3)

        first_section_chunks = [c for c in chunks if c.section_order == 0]
        second_section_chunks = [c for c in chunks if c.section_order == 1]

        self.assertGreaterEqual(len(first_section_chunks), 1)
        self.assertGreaterEqual(len(second_section_chunks), 1)

        for chunk in first_section_chunks:
            self.assertEqual(chunk.section_key, "description")

        for chunk in second_section_chunks:
            self.assertEqual(chunk.section_key, "results")


if __name__ == "__main__":
    unittest.main()
