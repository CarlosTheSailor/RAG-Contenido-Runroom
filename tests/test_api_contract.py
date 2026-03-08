from __future__ import annotations

import unittest

from fastapi.testclient import TestClient

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


class ApiContractTests(unittest.TestCase):
    def setUp(self) -> None:
        app = create_app(service=_FakeService(), api_key="secret")
        self.client = TestClient(app)
        self.auth_header = {"X-API-Key": "secret"}

    def test_health_requires_api_key(self) -> None:
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 401)

    def test_query_similar_contract(self) -> None:
        response = self.client.post(
            "/v1/query-similar",
            headers=self.auth_header,
            json={"text": "cx strategy", "top_k": 5},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("request_id", payload)
        self.assertEqual(payload["query"], "cx strategy")
        self.assertEqual(payload["top_k"], 5)
        self.assertEqual(len(payload["results"]), 1)

    def test_recommend_content_validates_payload(self) -> None:
        response = self.client.post(
            "/v1/recommend-content",
            headers=self.auth_header,
            json={"text": "draft", "top_k": 0},
        )
        self.assertEqual(response.status_code, 422)

    def test_recommend_content_contract_grouped(self) -> None:
        response = self.client.post(
            "/v1/recommend-content",
            headers=self.auth_header,
            json={"text": "newsletter", "group_by_type": True, "content_types": ["episode"]},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["grouped"])
        self.assertIn("request_id", payload)
        self.assertIn("episode", payload["results_by_type"])


if __name__ == "__main__":
    unittest.main()
