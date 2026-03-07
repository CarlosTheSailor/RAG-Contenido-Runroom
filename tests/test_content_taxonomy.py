from __future__ import annotations

import unittest

from src.content.taxonomy import canonical_section_key


class ContentTaxonomyTests(unittest.TestCase):
    def test_section_key_synonyms(self) -> None:
        self.assertEqual(canonical_section_key("Reto"), "challenge")
        self.assertEqual(canonical_section_key("Desafío"), "challenge")
        self.assertEqual(canonical_section_key("Solución y Procesos"), "solution")
        self.assertEqual(canonical_section_key("Tecnologías Utilizadas"), "technologies")
        self.assertEqual(canonical_section_key("Áreas"), "areas")
        self.assertEqual(canonical_section_key("Próximos pasos"), "next_steps")


if __name__ == "__main__":
    unittest.main()
