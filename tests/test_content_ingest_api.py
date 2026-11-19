from __future__ import annotations

import unittest
import urllib.error

from fastapi.testclient import TestClient

from src.config import APIRuntimeSettings
from src.interfaces.http.app import create_app

_ENDPOINT = "/v1/content/ingest-urls"
_EDITION = "https://carlosiglesias.info/newsletter/el-equipo-era-el-llm/"


class _FakeService:
    def __init__(self, failing_urls: set[str] | None = None) -> None:
        self.calls: list[tuple[str, str]] = []
        self._failing_urls = failing_urls or set()

    def ingest_manual_url(self, url: str, content_type: str) -> dict[str, object]:
        self.calls.append((url, content_type))
        if url in self._failing_urls:
            raise urllib.error.URLError("unreachable")
        return {
            "url": url,
            "content_type": content_type,
            "summary": {
                "documents_total": 1,
                "items_upserted": 1,
                "sections_written": 3,
                "chunks_written": 6,
                "dry_run": False,
            },
        }


def _client(service: _FakeService, ingest_api_key: str | None = "ingest-secret") -> TestClient:
    runtime = APIRuntimeSettings(api_key="secret", session_secret="session-secret", ingest_api_key=ingest_api_key)
    return TestClient(create_app(service=service, api_key="secret", runtime_settings=runtime))


class ContentIngestUrlsApiTests(unittest.TestCase):
    def test_ingests_urls_with_ingest_key(self) -> None:
        service = _FakeService()
        response = _client(service).post(
            _ENDPOINT,
            headers={"X-API-Key": "ingest-secret"},
            json={"urls": [_EDITION, _EDITION, "https://www.runroom.com/realworld/un-articulo"]},
        )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["content_type"], "article")
        self.assertEqual((body["total"], body["succeeded"], body["failed"]), (2, 2, 0))
        self.assertEqual(body["results"][0]["summary"]["chunks_written"], 6)
        # Repeated URLs are ingested once.
        self.assertEqual(
            service.calls,
            [(_EDITION, "article"), ("https://www.runroom.com/realworld/un-articulo", "article")],
        )

    def test_read_api_key_cannot_ingest(self) -> None:
        service = _FakeService()
        client = _client(service)

        for headers in ({"X-API-Key": "secret"}, {"X-API-Key": "wrong"}, {}):
            with self.subTest(headers=headers):
                response = client.post(_ENDPOINT, headers=headers, json={"urls": [_EDITION]})
                self.assertEqual(response.status_code, 401)
        self.assertEqual(service.calls, [])

    def test_ingest_key_does_not_open_read_endpoints(self) -> None:
        response = _client(_FakeService()).get("/health", headers={"X-API-Key": "ingest-secret"})
        self.assertEqual(response.status_code, 401)

    def test_endpoint_is_disabled_without_ingest_key_configured(self) -> None:
        service = _FakeService()
        response = _client(service, ingest_api_key=None).post(
            _ENDPOINT, headers={"X-API-Key": "secret"}, json={"urls": [_EDITION]}
        )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(service.calls, [])

    def test_any_disallowed_url_rejects_the_whole_request(self) -> None:
        service = _FakeService()
        client = _client(service)

        for payload in (
            {"urls": [_EDITION, "https://carlosiglesias.info.evil.com/newsletter/x/"]},
            {"urls": ["ftp://carlosiglesias.info/newsletter/x/"]},
            {"urls": [_EDITION], "content_type": "training"},
            {"urls": [_EDITION], "content_type": "case_study"},
            {"urls": []},
            {"urls": [f"https://www.runroom.com/realworld/a-{n}" for n in range(6)]},
        ):
            with self.subTest(payload=payload):
                response = client.post(_ENDPOINT, headers={"X-API-Key": "ingest-secret"}, json=payload)
                self.assertEqual(response.status_code, 422)
        self.assertEqual(service.calls, [])

    def test_partial_failure_returns_200_with_per_url_detail(self) -> None:
        broken = "https://carlosiglesias.info/newsletter/caida/"
        response = _client(_FakeService(failing_urls={broken})).post(
            _ENDPOINT, headers={"X-API-Key": "ingest-secret"}, json={"urls": [broken, _EDITION]}
        )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual((body["succeeded"], body["failed"]), (1, 1))
        self.assertEqual([item["status"] for item in body["results"]], ["error", "ok"])
        self.assertIn("No se pudo cargar la URL externa", body["results"][0]["detail"])

    def test_total_failure_returns_502(self) -> None:
        response = _client(_FakeService(failing_urls={_EDITION})).post(
            _ENDPOINT, headers={"X-API-Key": "ingest-secret"}, json={"urls": [_EDITION]}
        )

        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json()["failed"], 1)


if __name__ == "__main__":
    unittest.main()
