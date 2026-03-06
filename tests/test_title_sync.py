from __future__ import annotations

import unittest

from src.matching.title_sync import extract_first_h1


class TitleSyncTests(unittest.TestCase):
    def test_extract_first_h1_simple(self) -> None:
        html = '<html><body><h1 class="t-title3">R083 - Mentalidad startup, con Adriana Landaverde</h1></body></html>'
        title = extract_first_h1(html)
        self.assertEqual(title, "R083 - Mentalidad startup, con Adriana Landaverde")

    def test_extract_first_h1_with_nested_tags_and_entities(self) -> None:
        html = (
            "<h1>R085 - IA en <span>Product Management</span>, "
            "con Nacho Bassino &amp; equipo</h1>"
        )
        title = extract_first_h1(html)
        self.assertEqual(title, "R085 - IA en Product Management, con Nacho Bassino & equipo")

    def test_extract_first_h1_missing(self) -> None:
        html = "<html><body><h2>Sin h1</h2></body></html>"
        self.assertIsNone(extract_first_h1(html))

    def test_extract_first_h1_uses_first_occurrence(self) -> None:
        html = "<h1>Primero</h1><h1>Segundo</h1>"
        self.assertEqual(extract_first_h1(html), "Primero")


if __name__ == "__main__":
    unittest.main()
