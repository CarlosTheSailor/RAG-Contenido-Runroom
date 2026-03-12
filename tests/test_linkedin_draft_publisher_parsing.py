from __future__ import annotations

import unittest
from datetime import datetime
from pathlib import Path
import time
from unittest.mock import patch

from src.config import Settings
from src.linkedin_draft_publisher.models import DraftStage2Output, TopicCandidate
from src.linkedin_draft_publisher.parsing import normalize_references, parse_json_payload
from src.linkedin_draft_publisher.service import (
    LinkedInDraftPublisherService,
    _compute_stage_percentiles,
    _curate_topic_bundle_for_prompt,
    _find_related_candidate_by_id,
    _is_transient_http_error,
    _only_length_contract_error,
    _run_callable_with_timeout,
    _select_related_candidates,
    _text_contains_url,
    _validate_editorial_output,
)


def _settings() -> Settings:
    return Settings(
        supabase_db_url="postgresql://user:pass@localhost:5432/db",
        openai_api_key=None,
        openai_base_url="https://api.openai.com/v1",
        youtube_api_key=None,
        youtube_api_base_url="https://www.googleapis.com/youtube/v3",
        openai_embedding_model="text-embedding-3-large",
        openai_metadata_model="gpt-4.1-mini",
        openai_newsletter_model="gpt-4.1-mini",
        newsletter_rag_min_score=0.74,
        embedding_dim=1536,
        runroom_sitemap_url="https://www.runroom.com/sitemap.xml",
        auto_match_threshold=0.86,
        auto_match_margin=0.06,
        log_level="INFO",
        openai_theme_intel_model="gpt-4.1-mini",
    )


class _FakeStorage:
    def __init__(self, _db_url: str):
        self._db_url = _db_url

    def ensure_schema(self, _schema_path: Path) -> None:
        return None

    def close(self) -> None:
        return None


class _ExplodingRepo:
    instances: list["_ExplodingRepo"] = []

    def __init__(self, _storage: _FakeStorage):
        self.mark_finished_calls: list[dict[str, object]] = []
        _ExplodingRepo.instances.append(self)

    def get_run(self, *, run_id: int) -> dict[str, object]:
        _ = run_id
        return {
            "id": 77,
            "status": "queued",
            "origin_category": "product",
            "offline_mode": False,
            "target_count": 5,
            "topics_fetch_limit": 40,
            "related_top_k": 10,
            "related_counts_by_type_json": {},
            "buyer_persona_objetivo": "Product Managers",
            "client_name": "linkedin_draft_publisher",
        }

    def mark_run_started(self, *, run_id: int) -> dict[str, object]:
        return {"id": run_id, "status": "running"}

    def list_topic_candidates_unused_by_client(
        self,
        *,
        primary_category_key: str,
        client_name: str,
        limit: int,
    ) -> list[dict[str, object]]:
        raise RuntimeError(
            f"boom listing topic candidates: {primary_category_key}/{client_name}/{limit}"
        )

    def list_run_items(self, *, run_id: int) -> list[dict[str, object]]:
        _ = run_id
        return []

    def mark_run_finished(
        self,
        *,
        run_id: int,
        status: str,
        stats: dict[str, object],
        errors: list[dict[str, object]],
    ) -> dict[str, object]:
        payload = {
            "run_id": run_id,
            "status": status,
            "stats": stats,
            "errors": errors,
        }
        self.mark_finished_calls.append(payload)
        return payload


class _FakeThemeRepo:
    def __init__(self, _storage: _FakeStorage):
        return None


class _FakeContentRepo:
    def __init__(self, _storage: _FakeStorage):
        return None


class LinkedInDraftPublisherParsingTests(unittest.TestCase):
    def test_parse_json_payload_accepts_code_fence(self) -> None:
        raw = """
        ```json
        {"selected_topic_ids":[10,11,12]}
        ```
        """
        parsed = parse_json_payload(raw)
        self.assertEqual(parsed["selected_topic_ids"], [10, 11, 12])

    def test_normalize_references_dedupes_by_source_and_url(self) -> None:
        refs = normalize_references(
            [
                {"fuente": "Bloomberg", "url": "", "newsletter_origen": "OnlyCFO"},
                {"fuente": "Bloomberg", "url": "https://bloomberg.com/x", "newsletter_origen": "OnlyCFO"},
                {"fuente": "PitchBook", "url": "https://pitchbook.com/a", "newsletter_origen": "OnlyCFO"},
                {"fuente": "PitchBook", "url": "https://pitchbook.com/a", "newsletter_origen": "OnlyCFO"},
            ]
        )
        self.assertEqual(len(refs), 2)
        bloomberg = [item for item in refs if item["fuente"] == "Bloomberg"][0]
        self.assertEqual(bloomberg["url"], "https://bloomberg.com/x")

    def test_select_related_candidates_forces_coverage_by_type(self) -> None:
        rows = [
            {"content_item_id": 1, "content_type": "episode", "score": 0.9},
            {"content_item_id": 2, "content_type": "case_study", "score": 0.8},
            {"content_item_id": 3, "content_type": "runroom_lab", "score": 0.1},
            {"content_item_id": 4, "content_type": "episode", "score": 0.7},
        ]
        selected = _select_related_candidates(
            candidates=rows,
            top_k=3,
            forced_counts={"runroom_lab": 1},
            available_types=["episode", "case_study", "runroom_lab"],
        )
        selected_ids = [int(item["content_item_id"]) for item in selected]
        self.assertIn(3, selected_ids)

    @patch("src.linkedin_draft_publisher.service.ContentChunksRepository", new=_FakeContentRepo)
    @patch("src.linkedin_draft_publisher.service.ThemeIntelRepository", new=_FakeThemeRepo)
    @patch("src.linkedin_draft_publisher.service.LinkedInDraftPublisherRepository", new=_ExplodingRepo)
    @patch("src.linkedin_draft_publisher.service.SupabaseStorage", new=_FakeStorage)
    def test_execute_run_marks_failed_on_early_exception(self) -> None:
        _ExplodingRepo.instances.clear()
        service = LinkedInDraftPublisherService(settings=_settings(), schema_path=Path("sql"))
        service.execute_run(run_id=77, force_offline=True)

        repo = _ExplodingRepo.instances[-1]
        self.assertEqual(len(repo.mark_finished_calls), 1)
        payload = repo.mark_finished_calls[0]
        self.assertEqual(payload["status"], "failed")
        stats = payload["stats"]
        assert isinstance(stats, dict)
        self.assertEqual(stats.get("stage"), "list_topic_candidates")
        errors = payload["errors"]
        assert isinstance(errors, list)
        self.assertTrue(any(str(item.get("stage")) == "list_topic_candidates" for item in errors))

    def test_pick_topics_accepts_datetime_last_seen(self) -> None:
        settings = _settings()
        settings = Settings(**{**settings.__dict__, "openai_api_key": "test-key"})
        service = LinkedInDraftPublisherService(settings=settings, schema_path=Path("sql"))

        class _StubPrompts:
            def load_topic_selection_system(self) -> str:
                return "system"

            def load_topic_selection_user(self) -> str:
                return "{{buyer_persona_objetivo}} {{target_count}} {{candidates_json}}"

        service._prompts = _StubPrompts()  # type: ignore[attr-defined]
        with patch.object(service, "_openai_chat", return_value=None):
            selected = service._pick_topics(
                candidates=[
                    TopicCandidate(
                        topic_id=123,
                        title="Topic",
                        context_text="Context",
                        canonical_text="Canonical",
                        score=1.0,
                        last_seen_at=datetime(2026, 3, 12, 10, 0, 0),
                    )
                ],
                buyer_persona_objetivo="PMs",
                target_count=1,
                force_offline=False,
                model="gpt-5-mini",
            )
        self.assertEqual(selected, [123])

    def test_model_resolution_by_stage(self) -> None:
        settings = _settings()
        settings = Settings(
            **{
                **settings.__dict__,
                "linkedin_draft_publisher_topic_selection_model": "gpt-5-mini",
                "linkedin_draft_publisher_stage1_model": "gpt-5",
                "linkedin_draft_publisher_stage2_model": "gpt-5",
            }
        )
        service = LinkedInDraftPublisherService(settings=settings, schema_path=Path("sql"))
        self.assertEqual(service._resolve_model_for_stage("topic_selection"), "gpt-5-mini")  # type: ignore[attr-defined]
        self.assertEqual(service._resolve_model_for_stage("draft_stage1"), "gpt-5")  # type: ignore[attr-defined]
        self.assertEqual(service._resolve_model_for_stage("draft_stage2"), "gpt-5")  # type: ignore[attr-defined]

    def test_selected_related_null_does_not_pick_default(self) -> None:
        rows = [{"content_item_id": 10, "title": "A", "url": "https://example.com"}]
        selected = _find_related_candidate_by_id(related_candidates=rows, content_item_id=None)
        self.assertIsNone(selected)

    def test_integration_contract_requires_selected_url_in_text(self) -> None:
        self.assertTrue(
            _text_contains_url(
                "Texto con fuente https://info.runroom.com/lab-ia-product-management integrada.",
                "https://info.runroom.com/lab-ia-product-management",
            )
        )

    def test_validate_editorial_output_detects_contract_violations(self) -> None:
        stage2 = DraftStage2Output(
            titulo="Titulo",
            por_que_importa_ahora="Incluye fuente (Fuente XYZ).",
            borrador_post="Texto corto.",
            referencias_abstract=[{"fuente": "ABC", "url": "https://example.com/fuente", "newsletter_origen": "ABC"}],
            selected_related_content_item_id=10,
            selected_related_rationale="x",
            stage_source="llm",
        )
        flags, errors = _validate_editorial_output(
            stage2=stage2,
            selected_related={"content_item_id": 10, "url": "https://example.com/related"},
            enforce_related_with_candidates=True,
        )
        self.assertFalse(flags["length_1800_2600"])
        self.assertFalse(flags["why_now_no_urls_or_attribution"])
        self.assertFalse(flags["related_url_integrated"])
        self.assertTrue(len(errors) >= 3)

    def test_validate_editorial_output_accepts_valid_contract(self) -> None:
        related_url = "https://example.com/related"
        ref_url = "https://example.com/ref"
        body = (
            "Contexto estratégico y evidencia operativa para buyer persona. " * 35
            + f"Referencia clave Fuente Uno ({ref_url}) con trazabilidad completa. "
            + f"Integración natural de contenido relacionado {related_url} para ampliar perspectiva."
        )
        # Asegura rango contractual.
        if len(body) < 1800:
            body = body + (" Datos adicionales cuantitativos y aprendizajes aplicables." * 20)
        body = body[:2400]
        stage2 = DraftStage2Output(
            titulo="Titulo valido",
            por_que_importa_ahora="El impacto es inmediato en margen, riesgo y velocidad de ejecución.",
            borrador_post=body,
            referencias_abstract=[{"fuente": "Fuente Uno", "url": ref_url, "newsletter_origen": "Newsletter A"}],
            selected_related_content_item_id=99,
            selected_related_rationale="Encaje directo",
            stage_source="llm",
        )
        flags, errors = _validate_editorial_output(
            stage2=stage2,
            selected_related={"content_item_id": 99, "url": related_url},
            enforce_related_with_candidates=True,
        )
        self.assertTrue(all(bool(v) for v in flags.values()))
        self.assertEqual(errors, [])
        self.assertFalse(
            _text_contains_url(
                "Texto sin referencia al contenido relacionado.",
                "https://info.runroom.com/lab-ia-product-management",
            )
        )

    def test_curated_topic_bundle_respects_limits_and_dedupes_links(self) -> None:
        topic_bundle = {
            "topic": {"id": 1, "title": "IA en producto"},
            "evidences": [
                {"dato": "Mejora 32% productividad", "fuente": "Newsletter A", "url_referencia": "https://a.com/1"},
                {"dato": "Ahorro 15%", "fuente": "Newsletter B", "url_referencia": "https://b.com/1"},
                {"dato": "Retencion +9%", "fuente": "Newsletter C", "url_referencia": "https://c.com/1"},
            ],
            "source_documents": [
                {
                    "source_document_id": 10,
                    "subject": "IA Product Weekly",
                    "sender": "news@x.com",
                    "received_at": "2026-03-12T10:00:00Z",
                    "links_json": [
                        "https://example.com/a",
                        "https://example.com/a",
                        "https://example.com/b",
                    ],
                    "link_type": "primary",
                },
                {
                    "source_document_id": 11,
                    "subject": "Otro",
                    "sender": "news@y.com",
                    "received_at": "2026-03-12T10:00:00Z",
                    "links_json": ["https://example.com/c"],
                    "link_type": "primary",
                },
            ],
        }
        curated = _curate_topic_bundle_for_prompt(
            topic_bundle=topic_bundle,
            anchor_text="IA producto productividad",
            evidence_limit=2,
            doc_limit=1,
        )
        self.assertEqual(len(curated["evidences"]), 2)
        self.assertEqual(len(curated["source_documents"]), 1)
        links = curated["source_documents"][0]["links_json"]
        self.assertEqual(len(links), 2)
        self.assertEqual(links[0], "https://example.com/a")

    def test_compute_stage_percentiles_and_transient_detection(self) -> None:
        percentiles = _compute_stage_percentiles(
            {
                "draft_stage1": [100.0, 200.0, 300.0],
                "lookup_related": [50.0, 60.0],
            },
            0.95,
        )
        self.assertIn("draft_stage1", percentiles)
        self.assertTrue(percentiles["draft_stage1"] >= 280.0)
        self.assertTrue(_is_transient_http_error(RuntimeError("HTTP 429: rate limit")))
        self.assertTrue(_is_transient_http_error(RuntimeError("timed out while connecting")))
        self.assertFalse(_is_transient_http_error(RuntimeError("HTTP 400: bad request")))

    def test_only_length_contract_error_detection(self) -> None:
        self.assertTrue(_only_length_contract_error(["borrador_post debe estar entre 1600 y 3200 caracteres."]))
        self.assertFalse(
            _only_length_contract_error(
                [
                    "borrador_post debe estar entre 1600 y 3200 caracteres.",
                    "No se integra la URL exacta del contenido relacionado seleccionado.",
                ]
            )
        )
        self.assertFalse(_only_length_contract_error([]))

    def test_run_callable_with_timeout(self) -> None:
        self.assertEqual(_run_callable_with_timeout(fn=lambda: 42, timeout_seconds=1.0), 42)

        def _slow() -> int:
            time.sleep(0.2)
            return 1

        with self.assertRaises(TimeoutError):
            _run_callable_with_timeout(fn=_slow, timeout_seconds=0.05)


if __name__ == "__main__":
    unittest.main()
