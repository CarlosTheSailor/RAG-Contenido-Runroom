from __future__ import annotations

import unittest

from src.youtube_preview.models import Chapter, EpisodeContext, ExtractedEntities, ProposedDescription, TranscriptChunk
from src.youtube_preview.qa_validator import validate_description


class YouTubeDescriptionQATests(unittest.TestCase):
    def test_validator_accepts_valid_description(self) -> None:
        context = EpisodeContext(
            episode_id=85,
            runroom_identifier="r085",
            content_item_id=123,
            source_filename="v1 Realworld R085_Nacho Bassino_1080p.mp4.txt",
            episode_code="r085",
            title="Producto y estrategia con Nacho Bassino",
            slug="r085",
            runroom_article_url="https://www.runroom.com/realworld/r085-nacho-bassino",
            youtube_url=None,
            youtube_video_id=None,
            guest_names=["Nacho Bassino"],
            language="es",
            transcript_path="transcripciones/r085.txt",
            transcript="texto largo de transcripcion",
            chunks=[
                TranscriptChunk(start_ts_sec=0, end_ts_sec=140, text="Introducción"),
                TranscriptChunk(start_ts_sec=140, end_ts_sec=280, text="Bloque principal"),
                TranscriptChunk(start_ts_sec=280, end_ts_sec=420, text="Conclusiones"),
            ],
            current_description=(
                "Descripción previa\n\n"
                "Más sobre Realworld y Runroom:\n"
                "https://www.runroom.com/realworld/r085-nacho-bassino"
            ),
            current_description_source="content_item.metadata",
            current_description_source_detail="content_item.metadata",
            brand_block=(
                "Más sobre Realworld y Runroom:\n"
                "https://www.runroom.com/realworld/r085-nacho-bassino"
            ),
        )

        entities = ExtractedEntities(
            keywords=["estrategia", "producto", "discovery"],
            entities=["Nacho Bassino", "Runroom"],
            main_topics=["estrategia de producto"],
            guest_names=["Nacho Bassino"],
        )

        proposed_text = "\n".join(
            [
                "En este episodio de Realworld, Nacho Bassino explica estrategia de producto y discovery aplicado a equipos digitales.",
                "",
                "## Resumen del episodio",
                "- Aprendizajes prácticos sobre estrategia y ejecución.",
                "- Cómo priorizar hipótesis con foco de negocio.",
                "- Relación entre discovery, entrega y métricas.",
                "",
                "## Timestamps",
                "- 00:00 - Introducción al contexto",
                "- 02:20 - Frameworks de priorización",
                "- 04:40 - Cierre y recomendaciones",
                "",
                "## Episodios relacionados",
                "- [R084](https://www.runroom.com/realworld/r085-nacho-bassino)",
                "",
                "Más sobre Realworld y Runroom:",
                "https://www.runroom.com/realworld/r085-nacho-bassino",
            ]
        )

        proposed = ProposedDescription(
            markdown=proposed_text,
            intro="intro",
            summary_paragraphs=["a", "b", "c"],
            chapters=[
                Chapter(timestamp="00:00", start_sec=0, label="Intro"),
                Chapter(timestamp="02:20", start_sec=140, label="Bloque"),
                Chapter(timestamp="04:40", start_sec=280, label="Cierre"),
            ],
            related_episodes=[],
            related_case_studies=[],
            used_existing_timestamps=False,
            chapters_source="transcript_chunks",
        )

        report = validate_description(context=context, entities=entities, proposed=proposed)
        checks = {check.key: check for check in report.checks}

        self.assertTrue(checks["intro_seo"].passed)
        self.assertTrue(checks["timestamps_start"].passed)
        self.assertTrue(checks["timestamps_ascending"].passed)
        self.assertTrue(checks["chapters_minimum"].passed)
        self.assertTrue(checks["chapter_duration"].passed)
        self.assertTrue(checks["brand_block_preserved"].passed)
        self.assertTrue(checks["no_hallucinated_urls"].passed)

    def test_validator_rejects_hallucinated_urls(self) -> None:
        context = EpisodeContext(
            episode_id=1,
            runroom_identifier="e001",
            content_item_id=None,
            source_filename="a.txt",
            episode_code="e001",
            title="Titulo",
            slug="e001",
            runroom_article_url="https://www.runroom.com/realworld/e001",
            youtube_url=None,
            youtube_video_id=None,
            guest_names=["Invitada"],
            language="es",
            transcript_path="a.txt",
            transcript="",
            chunks=[
                TranscriptChunk(start_ts_sec=0, end_ts_sec=20, text="A"),
                TranscriptChunk(start_ts_sec=20, end_ts_sec=40, text="B"),
                TranscriptChunk(start_ts_sec=40, end_ts_sec=60, text="C"),
            ],
            current_description="",
            current_description_source="missing",
            current_description_source_detail="missing",
            brand_block=None,
        )
        entities = ExtractedEntities(keywords=["producto"], entities=["Invitada"], main_topics=["producto"], guest_names=["Invitada"])
        proposed = ProposedDescription(
            markdown=(
                "Invitada habla de producto y estrategia.\n\n"
                "## Timestamps\n"
                "- 00:00 - Intro\n"
                "- 00:20 - Desarrollo\n"
                "- 00:40 - Cierre\n"
                "\n"
                "Link externo: https://example.com/fake"
            ),
            intro="",
            summary_paragraphs=[],
            chapters=[
                Chapter(timestamp="00:00", start_sec=0, label="Intro"),
                Chapter(timestamp="00:20", start_sec=20, label="Desarrollo"),
                Chapter(timestamp="00:40", start_sec=40, label="Cierre"),
            ],
            related_episodes=[],
            related_case_studies=[],
            used_existing_timestamps=False,
            chapters_source="transcript_chunks",
        )

        report = validate_description(context=context, entities=entities, proposed=proposed)
        checks = {check.key: check for check in report.checks}
        self.assertFalse(checks["no_hallucinated_urls"].passed)

    def test_validator_skips_final_chapter_duration_when_episode_end_unknown(self) -> None:
        context = EpisodeContext(
            episode_id=2,
            runroom_identifier="e002",
            content_item_id=None,
            source_filename="b.txt",
            episode_code="e002",
            title="Titulo",
            slug="e002",
            runroom_article_url=None,
            youtube_url=None,
            youtube_video_id=None,
            guest_names=["Invitado"],
            language="es",
            transcript_path="b.txt",
            transcript="",
            chunks=[
                TranscriptChunk(start_ts_sec=0, end_ts_sec=20, text="A"),
                TranscriptChunk(start_ts_sec=20, end_ts_sec=40, text="B"),
                # End equals last chapter start -> final duration unknown/non-reliable.
                TranscriptChunk(start_ts_sec=40, end_ts_sec=40, text="C"),
            ],
            current_description="",
            current_description_source="missing",
            current_description_source_detail="missing",
            brand_block=None,
        )
        entities = ExtractedEntities(keywords=["producto"], entities=["Invitado"], main_topics=["producto"], guest_names=["Invitado"])
        proposed = ProposedDescription(
            markdown=(
                "Invitado habla de producto y estrategia en profundidad con ejemplos de ejecución.\n\n"
                "Capítulos:\n"
                "- 00:00 - Intro\n"
                "- 00:20 - Desarrollo\n"
                "- 00:40 - Cierre\n"
            ),
            intro="",
            summary_paragraphs=["p1", "p2"],
            chapters=[
                Chapter(timestamp="00:00", start_sec=0, label="Intro"),
                Chapter(timestamp="00:20", start_sec=20, label="Desarrollo"),
                Chapter(timestamp="00:40", start_sec=40, label="Cierre"),
            ],
            related_episodes=[],
            related_case_studies=[],
            used_existing_timestamps=False,
            chapters_source="transcript_chunks",
        )

        report = validate_description(context=context, entities=entities, proposed=proposed)
        checks = {check.key: check for check in report.checks}
        self.assertTrue(checks["chapter_duration"].passed)


if __name__ == "__main__":
    unittest.main()
