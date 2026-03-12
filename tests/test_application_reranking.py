from __future__ import annotations

import unittest

from src.application.reranking import aggregate_and_rerank


class ApplicationRerankingTests(unittest.TestCase):
    def test_filters_boilerplate_chunks_and_drops_items_without_signal(self) -> None:
        rows = [
            {
                "content_item_id": 1,
                "content_type": "runroom_lab",
                "title": "Runroom LAB IA en Product Management",
                "url": "https://info.runroom.com/lab-ia-product-management",
                "metadata_json": {},
                "chunk_id": 1001,
                "section_key": "other",
                "chunk_text": "Casos Servicios Nosotros Academy Realworld",
                "similarity": 0.99,
            },
            {
                "content_item_id": 1,
                "content_type": "runroom_lab",
                "title": "Runroom LAB IA en Product Management",
                "url": "https://info.runroom.com/lab-ia-product-management",
                "metadata_json": {},
                "chunk_id": 1002,
                "section_key": "other",
                "chunk_text": "Integración de IA en Product Management para priorizar mejor y acelerar discovery.",
                "similarity": 0.31,
            },
            {
                "content_item_id": 2,
                "content_type": "runroom_lab",
                "title": "Runroom LAB Otro",
                "url": "https://info.runroom.com/lab-otro",
                "metadata_json": {},
                "chunk_id": 2001,
                "section_key": "other",
                "chunk_text": "Casos Servicios Nosotros Academy Realworld",
                "similarity": 0.98,
            },
        ]

        ranked = aggregate_and_rerank(rows=rows, top_k=5, query_text="ia product management")

        self.assertEqual(len(ranked), 1)
        self.assertEqual(ranked[0].content_item_id, 1)

    def test_runroom_lab_lexical_boost_improves_thematic_item_order(self) -> None:
        rows = [
            {
                "content_item_id": 10,
                "content_type": "runroom_lab",
                "title": "Runroom LAB IA en Product Management",
                "url": "https://info.runroom.com/lab-ia-product-management",
                "metadata_json": {},
                "chunk_id": 1010,
                "section_key": "other",
                "chunk_text": "Aplicación de IA en equipos de product management para optimizar workflows.",
                "similarity": 0.20,
            },
            {
                "content_item_id": 11,
                "content_type": "runroom_lab",
                "title": "Runroom LAB Liderar desde los valores",
                "url": "https://info.runroom.com/liderar-desde-los-valores",
                "metadata_json": {},
                "chunk_id": 1110,
                "section_key": "other",
                "chunk_text": "Liderazgo y cultura organizacional en equipos.",
                "similarity": 0.21,
            },
        ]

        ranked = aggregate_and_rerank(
            rows=rows,
            top_k=2,
            query_text="automatizacion ia product management",
        )

        self.assertEqual(len(ranked), 2)
        self.assertEqual(ranked[0].content_item_id, 10)


if __name__ == "__main__":
    unittest.main()
