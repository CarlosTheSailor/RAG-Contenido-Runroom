from __future__ import annotations

import unittest

from fastapi.responses import RedirectResponse
from fastapi.testclient import TestClient

from src.config import APIRuntimeSettings
from src.interfaces.http.app import create_app
from src.pipeline.manual_episode_ingest import DuplicateEpisodeSourceFilenameError


class _FakeService:
    def query_similar(self, text: str, top_k: int, offline_mode: bool = False) -> dict[str, object]:
        return {
            "query": text,
            "top_k": top_k,
            "results": [
                {
                    "similarity": 0.9,
                    "episode_code": "r085",
                    "episode_title": "Episode 85",
                    "runroom_article_url": None,
                    "start_ts_sec": 12.0,
                    "start_ts_hhmmss": "00:00:12",
                    "text": "chunk",
                }
            ],
        }

    def recommend_content(
        self,
        text: str,
        top_k: int,
        fetch_k: int,
        content_types: list[str] | None = None,
        source: str | None = None,
        language: str | None = None,
        group_by_type: bool = False,
        offline_mode: bool = False,
    ) -> dict[str, object]:
        return {
            "query": text,
            "top_k": top_k,
            "total_candidates": 1,
            "grouped": group_by_type,
            "results": [] if group_by_type else [{"content_item_id": 1, "content_type": "episode", "score": 0.91}],
            "results_by_type": {"episode": [{"content_item_id": 1, "score": 0.91}]} if group_by_type else None,
        }

    def generate_newsletter_linkedin(
        self,
        idea: str,
        referencias: str | None = None,
        audiencia: str | None = None,
        objetivo_secundario: str | None = None,
        longitud: str | None = None,
        metafora_visual: str | None = None,
        texto_a_incluir: str | None = None,
        offline_mode: bool = False,
    ) -> dict[str, object]:
        return {
            "output_text": f"Newsletter sobre: {idea}",
            "related_content": [
                {
                    "title": "Item 1",
                    "url": "https://example.com/item1",
                    "content_type": "episode",
                    "score": 0.9,
                    "excerpt": "extracto",
                }
            ],
            "warnings": [],
            "used_examples": ["post_ejemplo1.txt", "post_ejemplo2.txt"],
        }

    def list_newsletter_linkedin_ideas(
        self,
        exclude_topic_ids: list[int] | None = None,
        limit: int = 10,
        offline_mode: bool = False,
    ) -> dict[str, object]:
        excluded = set(exclude_topic_ids or [])
        rows = [
            {
                "topic_id": 11,
                "title": "Discovery con evidencia",
                "context_preview": "Contexto breve de discovery.",
                "canonical_text": "Texto canonico 11",
                "score": 3.1,
                "last_seen_at": "2026-03-20T10:00:00+00:00",
                "status": "new",
            },
            {
                "topic_id": 12,
                "title": "Métricas guardarraíl",
                "context_preview": "Contexto breve de metricas.",
                "canonical_text": "Texto canonico 12",
                "score": 2.8,
                "last_seen_at": "2026-03-19T10:00:00+00:00",
                "status": "in_progress",
            },
        ]
        filtered = [row for row in rows if int(row["topic_id"]) not in excluded][:limit]
        return {
            "ideas": filtered,
            "pool_exhausted": len(filtered) < limit,
        }

    def ingest_case_study_url(self, url: str) -> dict[str, object]:
        return {
            "url": url,
            "summary": {
                "documents_total": 1,
                "items_upserted": 1,
                "sections_written": 3,
                "chunks_written": 6,
                "dry_run": False,
            },
        }

    def ingest_runroom_lab_url(self, url: str) -> dict[str, object]:
        return {
            "url": url,
            "summary": {
                "documents_total": 1,
                "items_upserted": 1,
                "sections_written": 2,
                "chunks_written": 5,
                "dry_run": False,
            },
        }

    def ingest_episode_upload(
        self,
        transcript_filename: str,
        transcript_bytes: bytes,
        runroom_url: str,
    ) -> dict[str, object]:
        if transcript_filename == "duplicate.txt":
            raise DuplicateEpisodeSourceFilenameError("duplicate")
        return {
            "runroom_url": runroom_url,
            "summary": {
                "source_filename": transcript_filename,
                "transcript_path": f"transcripciones/{transcript_filename}",
                "episode_id": 999,
                "content_item_id": 1999,
                "episode_code": "r999",
                "title": "R999 - Episodio de prueba",
                "runroom_url": runroom_url,
                "chunks_written": 4,
                "canonical_synced": True,
            },
        }


class _FakeGoogleOAuthClient:
    def __init__(self, userinfo: dict[str, object]) -> None:
        self._userinfo = userinfo

    async def authorize_redirect(self, request, redirect_uri: str):  # noqa: ANN001
        return RedirectResponse(url=redirect_uri, status_code=302)

    async def authorize_access_token(self, request):  # noqa: ANN001
        return {"userinfo": self._userinfo}

    async def parse_id_token(self, request, token):  # noqa: ANN001
        return token.get("userinfo")


class WebAuthFlowTests(unittest.TestCase):
    def _runtime(self) -> APIRuntimeSettings:
        return APIRuntimeSettings(
            api_key="secret",
            session_secret="session-secret",
            google_oauth_redirect_uri="http://testserver/auth/google/callback",
            google_oauth_allowed_domain="runroom.com",
        )

    def test_app_page_redirects_without_session(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)

        response = client.get("/app", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["location"], "/")

    def test_callback_rejects_non_allowed_domain(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@gmail.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)

        response = client.get("/auth/google/callback")

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()["detail"], "Email domain not allowed")

    def test_callback_accepts_allowed_domain_and_allows_app(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)

        callback = client.get("/auth/google/callback", follow_redirects=False)
        app_response = client.get("/app")

        self.assertEqual(callback.status_code, 302)
        self.assertEqual(callback.headers["location"], "/app")
        self.assertEqual(app_response.status_code, 200)
        self.assertIn("Runroom Content RAG", app_response.text)
        self.assertIn("Ingesta manual de episodio Realworld", app_response.text)

    def test_app_api_requires_session(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)

        response = client.post("/app/api/query-similar", json={"text": "cx", "top_k": 3})

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["detail"], "Authentication required")

    def test_app_api_query_similar_with_session(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)
        client.get("/auth/google/callback", follow_redirects=False)

        response = client.post("/app/api/query-similar", json={"text": "cx", "top_k": 3})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("request_id", payload)
        self.assertEqual(payload["query"], "cx")
        self.assertEqual(payload["top_k"], 3)

    def test_newsletter_page_redirects_without_session(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)

        response = client.get("/app/newsletters-linkedin", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["location"], "/")

    def test_newsletter_generate_requires_session(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)

        response = client.post("/app/api/newsletters-linkedin/generate", json={"idea": "idea"})

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["detail"], "Authentication required")

    def test_newsletter_generate_validates_payload(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)
        client.get("/auth/google/callback", follow_redirects=False)

        response = client.post("/app/api/newsletters-linkedin/generate", json={"idea": ""})

        self.assertEqual(response.status_code, 422)

    def test_newsletter_generate_with_session_contract(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)
        client.get("/auth/google/callback", follow_redirects=False)

        response = client.post(
            "/app/api/newsletters-linkedin/generate",
            json={"idea": "sistemas de incentivos"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("request_id", payload)
        self.assertIn("output_text", payload)
        self.assertIn("related_content", payload)
        self.assertIn("warnings", payload)
        self.assertIn("used_examples", payload)
        self.assertGreaterEqual(len(payload["related_content"]), 1)

    def test_newsletter_ideas_requires_session(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)

        response = client.post("/app/api/newsletters-linkedin/ideas", json={"exclude_topic_ids": [], "limit": 10})

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["detail"], "Authentication required")

    def test_newsletter_ideas_with_session_contract(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)
        client.get("/auth/google/callback", follow_redirects=False)

        response = client.post(
            "/app/api/newsletters-linkedin/ideas",
            json={"exclude_topic_ids": [11], "limit": 10},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("request_id", payload)
        self.assertIn("ideas", payload)
        self.assertIn("pool_exhausted", payload)
        self.assertEqual(len(payload["ideas"]), 1)
        self.assertEqual(payload["ideas"][0]["topic_id"], 12)

    def test_new_case_study_page_redirects_without_session(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)

        response = client.get("/app/nuevo-case-study", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["location"], "/")

    def test_new_case_study_page_with_session(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)
        client.get("/auth/google/callback", follow_redirects=False)

        response = client.get("/app/nuevo-case-study")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Nueva ingesta de case study", response.text)

    def test_case_study_ingest_requires_session(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)

        response = client.post(
            "/app/api/case-studies/ingest-url",
            json={"url": "https://www.runroom.com/cases/caso-valido"},
        )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["detail"], "Authentication required")

    def test_case_study_ingest_rejects_url_outside_policy(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)
        client.get("/auth/google/callback", follow_redirects=False)

        response = client.post(
            "/app/api/case-studies/ingest-url",
            json={"url": "https://example.com/cases/no-valido"},
        )

        self.assertEqual(response.status_code, 422)

    def test_case_study_ingest_with_session_contract(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)
        client.get("/auth/google/callback", follow_redirects=False)

        response = client.post(
            "/app/api/case-studies/ingest-url",
            json={"url": "https://www.runroom.com/cases/caso-valido"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("request_id", payload)
        self.assertEqual(payload["url"], "https://www.runroom.com/cases/caso-valido")
        self.assertIn("summary", payload)
        self.assertEqual(payload["summary"]["items_upserted"], 1)

    def test_new_runroom_lab_page_redirects_without_session(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)

        response = client.get("/app/nuevo-runroom-lab", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["location"], "/")

    def test_new_runroom_lab_page_with_session(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)
        client.get("/auth/google/callback", follow_redirects=False)

        response = client.get("/app/nuevo-runroom-lab")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Nueva ingesta de runroom_lab", response.text)

    def test_runroom_lab_ingest_requires_session(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)

        response = client.post(
            "/app/api/runroom-labs/ingest-url",
            json={"url": "https://info.runroom.com/lab-heart-of-agile"},
        )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["detail"], "Authentication required")

    def test_runroom_lab_ingest_rejects_url_outside_policy(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)
        client.get("/auth/google/callback", follow_redirects=False)

        response = client.post(
            "/app/api/runroom-labs/ingest-url",
            json={"url": "https://example.com/lab-heart-of-agile"},
        )

        self.assertEqual(response.status_code, 422)

    def test_runroom_lab_ingest_with_session_contract(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)
        client.get("/auth/google/callback", follow_redirects=False)

        response = client.post(
            "/app/api/runroom-labs/ingest-url",
            json={"url": "https://info.runroom.com/lab-heart-of-agile"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("request_id", payload)
        self.assertEqual(payload["url"], "https://info.runroom.com/lab-heart-of-agile")
        self.assertIn("summary", payload)
        self.assertEqual(payload["summary"]["items_upserted"], 1)

    def test_new_realworld_episode_page_redirects_without_session(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)

        response = client.get("/app/nuevo-episodio-realworld", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["location"], "/")

    def test_new_realworld_episode_page_with_session(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)
        client.get("/auth/google/callback", follow_redirects=False)

        response = client.get("/app/nuevo-episodio-realworld")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Nueva ingesta de episodio Realworld", response.text)

    def test_episode_ingest_requires_session(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)

        response = client.post(
            "/app/api/episodes/ingest",
            data={"runroom_url": "https://www.runroom.com/realworld/r999"},
            files={"transcript_file": ("episodio.txt", b"[00:00:00.000] - Host\nHola", "text/plain")},
        )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["detail"], "Authentication required")

    def test_episode_ingest_rejects_url_outside_policy(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)
        client.get("/auth/google/callback", follow_redirects=False)

        response = client.post(
            "/app/api/episodes/ingest",
            data={"runroom_url": "https://example.com/realworld/r999"},
            files={"transcript_file": ("episodio.txt", b"[00:00:00.000] - Host\nHola", "text/plain")},
        )

        self.assertEqual(response.status_code, 422)

    def test_episode_ingest_rejects_invalid_file_extension(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)
        client.get("/auth/google/callback", follow_redirects=False)

        response = client.post(
            "/app/api/episodes/ingest",
            data={"runroom_url": "https://www.runroom.com/realworld/r999"},
            files={"transcript_file": ("episodio.md", b"hola", "text/markdown")},
        )

        self.assertEqual(response.status_code, 422)

    def test_episode_ingest_rejects_duplicate_filename(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)
        client.get("/auth/google/callback", follow_redirects=False)

        response = client.post(
            "/app/api/episodes/ingest",
            data={"runroom_url": "https://www.runroom.com/realworld/r999"},
            files={"transcript_file": ("duplicate.txt", b"[00:00:00.000] - Host\nHola", "text/plain")},
        )

        self.assertEqual(response.status_code, 409)

    def test_episode_ingest_with_session_contract(self) -> None:
        app = create_app(
            service=_FakeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(
                userinfo={"email": "person@runroom.com", "email_verified": True, "name": "Person"}
            ),
        )
        client = TestClient(app)
        client.get("/auth/google/callback", follow_redirects=False)

        response = client.post(
            "/app/api/episodes/ingest",
            data={"runroom_url": "https://www.runroom.com/realworld/r999"},
            files={"transcript_file": ("episodio.txt", b"[00:00:00.000] - Host\nHola", "text/plain")},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("request_id", payload)
        self.assertEqual(payload["runroom_url"], "https://www.runroom.com/realworld/r999")
        self.assertEqual(payload["summary"]["source_filename"], "episodio.txt")
        self.assertEqual(payload["summary"]["episode_id"], 999)


if __name__ == "__main__":
    unittest.main()
