from __future__ import annotations

import unittest

from fastapi.responses import RedirectResponse
from fastapi.testclient import TestClient

from src.config import APIRuntimeSettings
from src.interfaces.http.app import create_app


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
        self.assertIn("Runroom Content Query", app_response.text)

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


if __name__ == "__main__":
    unittest.main()
