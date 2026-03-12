from __future__ import annotations

import unittest

from fastapi.responses import RedirectResponse
from fastapi.testclient import TestClient

from src.config import APIRuntimeSettings
from src.interfaces.http.app import create_app


class _FakeThemeService:
    def __init__(self) -> None:
        self._runs: dict[int, dict[str, object]] = {}
        self._next_run_id = 1
        self._schedules: dict[int, dict[str, object]] = {}
        self._next_schedule_id = 1
        self._next_config_id = 1
        self._next_execution_id = 1
        self._linkedin_runs: dict[int, dict[str, object]] = {}
        self._next_linkedin_run_id = 1

    # Existing app protocol methods
    def query_similar(self, text: str, top_k: int, offline_mode: bool = False):  # noqa: ANN001, ARG002
        return {"query": text, "top_k": top_k, "results": []}

    def recommend_content(self, text: str, top_k: int, fetch_k: int, **kwargs):  # noqa: ANN001, ARG002
        return {
            "query": text,
            "top_k": top_k,
            "total_candidates": 0,
            "grouped": bool(kwargs.get("group_by_type")),
            "results": [],
            "results_by_type": {},
        }

    def generate_newsletter_linkedin(self, idea: str, **kwargs):  # noqa: ANN001, ARG002
        return {"output_text": idea, "related_content": [], "warnings": [], "used_examples": []}

    def ingest_case_study_url(self, url: str):  # noqa: ANN001
        return {"url": url, "summary": {"documents_total": 0, "items_upserted": 0, "sections_written": 0, "chunks_written": 0, "dry_run": False}}

    def ingest_runroom_lab_url(self, url: str):  # noqa: ANN001
        return {"url": url, "summary": {"documents_total": 0, "items_upserted": 0, "sections_written": 0, "chunks_written": 0, "dry_run": False}}

    def ingest_episode_upload(self, transcript_filename: str, transcript_bytes: bytes, runroom_url: str):  # noqa: ANN001, ARG002
        return {"runroom_url": runroom_url, "summary": {"source_filename": transcript_filename, "transcript_path": "x", "episode_id": 1, "content_item_id": 1, "episode_code": "r001", "title": "x", "runroom_url": runroom_url, "chunks_written": 1, "canonical_synced": True}}

    # Theme intel methods
    def create_theme_intel_run(self, gmail_query: str, origin_category: str, mark_as_read: bool, limit_messages: int = 100, triggered_by_email: str | None = None):  # noqa: ANN001
        run_id = self._next_run_id
        self._next_run_id += 1
        self._runs[run_id] = {
            "id": run_id,
            "status": "queued",
            "gmail_query": gmail_query,
            "origin_category": origin_category,
            "mark_as_read": mark_as_read,
            "limit_messages": limit_messages,
            "triggered_by_email": triggered_by_email,
        }
        return {"run_id": run_id, "status": "queued"}

    def execute_theme_intel_run(self, run_id: int, offline_mode: bool = False):  # noqa: ANN001, ARG002
        if run_id in self._runs:
            self._runs[run_id]["status"] = "succeeded"

    def get_theme_intel_run(self, run_id: int):  # noqa: ANN001
        return self._runs.get(run_id)

    def get_latest_theme_intel_run(self):  # noqa: ANN001
        if not self._runs:
            return None
        latest_id = max(self._runs.keys())
        return self._runs.get(latest_id)

    def list_theme_intel_run_source_documents(self, run_id: int):  # noqa: ANN001
        if run_id not in self._runs:
            return []
        return [
            {
                "id": 100,
                "run_id": run_id,
                "source_external_id": "msg-1",
                "subject": "Demo newsletter",
                "cleaned_text_preview": "Texto limpio",
            }
        ]

    def list_theme_intel_topics(self, **kwargs):  # noqa: ANN001, ARG002
        self._last_list_topics_kwargs = dict(kwargs)
        return [
            {
                "id": 10,
                "title": "Tema demo",
                "context_text": "Contexto",
                "canonical_text": "Tema demo | Contexto",
                "primary_category_key": "cx",
                "primary_category_label": "cx",
                "status": "new",
                "score": 0.91,
                "origin_source_type": "gmail",
                "origin_source_account": "newsletters@runroom.com",
                "origin_query": "label:cx is:unread",
                "times_seen": 1,
                "first_seen_at": "2026-01-01T00:00:00Z",
                "last_seen_at": "2026-01-01T00:00:00Z",
                "semantic_score": None,
                "tags": [],
                "related_content": [],
            }
        ]

    def update_theme_intel_topic_status(self, topic_id: int, status: str):  # noqa: ANN001
        return {"id": topic_id, "status": status}

    def register_theme_intel_topic_usage(self, topic_id: int, client_name: str, artifact_id: str | None = None, metadata: dict | None = None):  # noqa: ANN001
        return {"usage_id": 1, "topic_id": topic_id, "client_name": client_name, "artifact_id": artifact_id, "metadata": metadata or {}}

    def refresh_theme_intel_related_content(  # noqa: ANN001, ARG002
        self,
        topic_id: int,
        top_k: int | None = 10,
        content_types: list[str] | None = None,
        related_counts_by_type: dict | None = None,
        offline_mode: bool = False,
    ):
        return {"topic_id": topic_id, "related_items": top_k}

    def get_theme_intel_topic_detail(self, topic_id: int):  # noqa: ANN001
        return {
            "id": topic_id,
            "title": "Tema demo",
            "tags": [],
            "related_content": [],
            "evidences": [],
            "usage": [],
            "source_documents": [],
        }

    def create_theme_intel_schedule(  # noqa: ANN001
        self,
        name: str,
        enabled: bool = True,
        every_n_days: int = 1,
        run_time_local: str = "09:00",
        timezone_name: str = "Europe/Madrid",
    ):
        schedule_id = self._next_schedule_id
        self._next_schedule_id += 1
        payload = {
            "id": schedule_id,
            "name": name,
            "enabled": enabled,
            "every_n_days": every_n_days,
            "run_time_local": run_time_local,
            "timezone": timezone_name,
            "configs": [],
        }
        self._schedules[schedule_id] = payload
        return payload

    def list_theme_intel_schedules(self):  # noqa: ANN001
        return [self._schedules[key] for key in sorted(self._schedules.keys(), reverse=True)]

    def update_theme_intel_schedule(self, schedule_id: int, **kwargs):  # noqa: ANN001
        schedule = self._schedules.get(schedule_id)
        if not schedule:
            return None
        for key, value in kwargs.items():
            if value is None:
                continue
            if key == "timezone_name":
                schedule["timezone"] = value
                continue
            schedule[key] = value
        return schedule

    def create_theme_intel_schedule_config(  # noqa: ANN001
        self,
        schedule_id: int,
        execution_order: int,
        gmail_query: str,
        origin_category: str,
        mark_as_read: bool,
        limit_messages: int,
        enabled: bool = True,
    ):
        schedule = self._schedules.get(schedule_id)
        if not schedule:
            raise ValueError("Schedule no encontrado.")
        config_id = self._next_config_id
        self._next_config_id += 1
        config = {
            "id": config_id,
            "schedule_id": schedule_id,
            "execution_order": execution_order,
            "gmail_query": gmail_query,
            "origin_category": origin_category,
            "mark_as_read": mark_as_read,
            "limit_messages": limit_messages,
            "enabled": enabled,
        }
        schedule["configs"].append(config)
        return config

    def update_theme_intel_schedule_config(self, schedule_id: int, config_id: int, **kwargs):  # noqa: ANN001
        schedule = self._schedules.get(schedule_id)
        if not schedule:
            return None
        for config in schedule["configs"]:
            if int(config["id"]) != int(config_id):
                continue
            for key, value in kwargs.items():
                if value is None:
                    continue
                config[key] = value
            return config
        return None

    def run_theme_intel_schedule_now(self, schedule_id: int, offline_mode: bool = False):  # noqa: ANN001, ARG002
        if schedule_id not in self._schedules:
            raise ValueError("Schedule no encontrado.")
        execution_id = self._next_execution_id
        self._next_execution_id += 1
        execution = {
            "id": execution_id,
            "schedule_id": schedule_id,
            "status": "succeeded",
            "items": [],
        }
        schedule = self._schedules[schedule_id]
        executions = schedule.get("executions")
        if not isinstance(executions, list):
            executions = []
            schedule["executions"] = executions
        executions.insert(0, execution)
        return {"status": "ok", "executed": 1, "execution": execution}

    def list_theme_intel_schedule_executions(self, schedule_id: int, limit: int = 20):  # noqa: ANN001, ARG002
        schedule = self._schedules.get(schedule_id)
        if not schedule:
            return []
        executions = schedule.get("executions")
        if not isinstance(executions, list):
            return []
        return executions[:limit]

    def tick_theme_intel_scheduler(self, offline_mode: bool = False):  # noqa: ANN001, ARG002
        return {"status": "ok", "due_schedules": 0, "executed_schedules": 0, "executions": []}

    # LinkedIn draft publisher methods
    def create_linkedin_draft_publisher_run(  # noqa: ANN001
        self,
        origin_category: str,
        slack_channel: str,
        buyer_persona_objetivo: str,
        triggered_by_email: str | None = None,
        offline_mode: bool = False,
    ):
        run_id = self._next_linkedin_run_id
        self._next_linkedin_run_id += 1
        self._linkedin_runs[run_id] = {
            "id": run_id,
            "status": "queued",
            "origin_category": origin_category,
            "slack_channel": slack_channel,
            "buyer_persona_objetivo": buyer_persona_objetivo,
            "triggered_by_email": triggered_by_email,
            "offline_mode": offline_mode,
            "items": [],
        }
        return {"run_id": run_id, "status": "queued"}

    def execute_linkedin_draft_publisher_run(self, run_id: int, offline_mode: bool = False):  # noqa: ANN001, ARG002
        run = self._linkedin_runs.get(run_id)
        if not run:
            return
        run["status"] = "succeeded"
        run["items"] = [
            {
                "id": 1,
                "run_id": run_id,
                "item_index": 1,
                "topic_id": 10,
                "status": "succeeded",
                "title": "Draft demo",
                "draft_final_text": "Contenido final",
                "related_selected_json": {"content_item_id": 99, "title": "Contenido relacionado"},
                "references_json": [{"fuente": "Demo", "url": "https://example.com", "newsletter_origen": "Demo"}],
                "draft_publish_json": {"ok": True, "edit_url": "https://linkedin-drafts.runroom.dev/edit/1"},
                "slack_publish_json": {"ok": True},
                "warnings_json": [],
                "errors_json": [],
            }
        ]

    def get_linkedin_draft_publisher_run(self, run_id: int):  # noqa: ANN001
        return self._linkedin_runs.get(run_id)

    def get_latest_linkedin_draft_publisher_run(self):  # noqa: ANN001
        if not self._linkedin_runs:
            return None
        latest_id = max(self._linkedin_runs.keys())
        return self._linkedin_runs.get(latest_id)

    def get_linkedin_draft_publisher_run_result(self, run_id: int):  # noqa: ANN001
        run = self._linkedin_runs.get(run_id)
        if not run:
            return None
        return {
            "run": {k: v for k, v in run.items() if k != "items"},
            "items": run.get("items", []),
            "total": len(run.get("items", [])),
        }


class _FakeGoogleOAuthClient:
    async def authorize_redirect(self, request, redirect_uri: str):  # noqa: ANN001
        return RedirectResponse(url=redirect_uri, status_code=302)

    async def authorize_access_token(self, request):  # noqa: ANN001
        return {"userinfo": {"email": "person@runroom.com", "email_verified": True, "name": "Person"}}

    async def parse_id_token(self, request, token):  # noqa: ANN001
        return token.get("userinfo")


class ThemeIntelApiTests(unittest.TestCase):
    def _runtime(self) -> APIRuntimeSettings:
        return APIRuntimeSettings(
            api_key="secret",
            session_secret="session-secret",
            google_oauth_redirect_uri="http://testserver/auth/google/callback",
            google_oauth_allowed_domain="runroom.com",
        )

    def _client(self) -> TestClient:
        app = create_app(
            service=_FakeThemeService(),
            api_key="secret",
            runtime_settings=self._runtime(),
            google_oauth_client=_FakeGoogleOAuthClient(),
        )
        return TestClient(app)

    def test_theme_intel_page_requires_session(self) -> None:
        client = self._client()

        response = client.get("/app/theme-intel", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["location"], "/")

    def test_theme_run_create_and_get_with_session(self) -> None:
        client = self._client()
        client.get("/auth/google/callback", follow_redirects=False)

        create_response = client.post(
            "/app/api/theme-intel/runs",
            json={
                "gmailQuery": "label:cx is:unread",
                "originCategory": "cx",
                "markAsRead": False,
                "limitMessages": 20,
            },
        )
        self.assertEqual(create_response.status_code, 200)
        payload = create_response.json()
        self.assertIn("run_id", payload)

        run_response = client.get(f"/app/api/theme-intel/runs/{payload['run_id']}")
        self.assertEqual(run_response.status_code, 200)
        self.assertIn("run", run_response.json())

        latest_response = client.get("/app/api/theme-intel/runs/latest")
        self.assertEqual(latest_response.status_code, 200)
        self.assertEqual(latest_response.json()["run"]["id"], payload["run_id"])

    def test_theme_topics_list_v1_requires_api_key(self) -> None:
        client = self._client()

        unauthorized = client.get("/v1/theme-intel/topics")
        self.assertEqual(unauthorized.status_code, 401)

        authorized = client.get("/v1/theme-intel/topics", headers={"X-API-Key": "secret"})
        self.assertEqual(authorized.status_code, 200)
        body = authorized.json()
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["topics"][0]["primary_category_key"], "cx")

    def test_theme_debug_endpoints_contract(self) -> None:
        client = self._client()
        client.get("/auth/google/callback", follow_redirects=False)

        create_response = client.post(
            "/app/api/theme-intel/runs",
            json={
                "gmailQuery": "label:cx is:unread",
                "originCategory": "cx",
                "markAsRead": False,
                "limitMessages": 20,
            },
        )
        run_id = create_response.json()["run_id"]

        docs_response = client.get(f"/app/api/theme-intel/runs/{run_id}/documents")
        self.assertEqual(docs_response.status_code, 200)
        self.assertIn("documents", docs_response.json())

        topic_response = client.get("/app/api/theme-intel/topics/10")
        self.assertEqual(topic_response.status_code, 200)
        self.assertIn("topic", topic_response.json())
        self.assertIn("source_documents", topic_response.json()["topic"])

    def test_theme_schedule_crud_and_run_now_with_session(self) -> None:
        client = self._client()
        client.get("/auth/google/callback", follow_redirects=False)

        create_schedule = client.post(
            "/app/api/theme-intel/schedules",
            json={
                "name": "Morning CX",
                "enabled": True,
                "every_n_days": 2,
                "run_time_local": "09:30",
                "timezone": "Europe/Madrid",
            },
        )
        self.assertEqual(create_schedule.status_code, 200)
        schedule_id = create_schedule.json()["schedule"]["id"]

        list_schedules = client.get("/app/api/theme-intel/schedules")
        self.assertEqual(list_schedules.status_code, 200)
        self.assertGreaterEqual(list_schedules.json()["total"], 1)

        create_config = client.post(
            f"/app/api/theme-intel/schedules/{schedule_id}/configs",
            json={
                "execution_order": 1,
                "gmail_query": "label:cx is:unread",
                "origin_category": "cx",
                "mark_as_read": False,
                "limit_messages": 100,
                "enabled": True,
            },
        )
        self.assertEqual(create_config.status_code, 200)
        config_id = create_config.json()["config"]["id"]

        patch_config = client.patch(
            f"/app/api/theme-intel/schedules/{schedule_id}/configs/{config_id}",
            json={"enabled": False},
        )
        self.assertEqual(patch_config.status_code, 200)
        self.assertFalse(patch_config.json()["config"]["enabled"])

        run_now = client.post(
            f"/app/api/theme-intel/schedules/{schedule_id}/run-now",
            json={"offline_mode": True},
        )
        self.assertEqual(run_now.status_code, 200)
        self.assertIn("result", run_now.json())

        executions = client.get(f"/app/api/theme-intel/schedules/{schedule_id}/executions")
        self.assertEqual(executions.status_code, 200)
        self.assertIn("executions", executions.json())

    def test_theme_scheduler_tick_v1_requires_api_key(self) -> None:
        client = self._client()

        unauthorized = client.post("/v1/theme-intel/scheduler/tick", json={"offline_mode": True})
        self.assertEqual(unauthorized.status_code, 401)

        authorized = client.post(
            "/v1/theme-intel/scheduler/tick",
            headers={"X-API-Key": "secret"},
            json={"offline_mode": True},
        )
        self.assertEqual(authorized.status_code, 200)
        self.assertIn("result", authorized.json())

        authorized_no_body = client.post(
            "/v1/theme-intel/scheduler/tick",
            headers={"X-API-Key": "secret"},
        )
        self.assertEqual(authorized_no_body.status_code, 200)

    def test_theme_topics_endpoint_lists_topics(self) -> None:
        client = self._client()
        client.get("/auth/google/callback", follow_redirects=False)

        response = client.get("/app/api/theme-intel/topics?primary_category=cx&status=new")
        self.assertEqual(response.status_code, 200)
        self.assertIn("topics", response.json())

    def test_theme_topics_endpoint_ignores_legacy_related_query_params(self) -> None:
        client = self._client()
        client.get("/auth/google/callback", follow_redirects=False)

        response = client.get(
            "/app/api/theme-intel/topics?related_content_types=runroom_lab&related_counts_by_type=runroom_lab=3"
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("topics", response.json())

    def test_linkedin_draft_publisher_create_get_result_with_session(self) -> None:
        client = self._client()
        client.get("/auth/google/callback", follow_redirects=False)

        create_response = client.post(
            "/app/api/linkedin-draft-publisher/runs",
            json={
                "originCategory": "product",
                "slackChannel": "C0AG54SV7DG",
                "buyerPersonaObjetivo": "Product Managers",
                "offline_mode": True,
            },
        )
        self.assertEqual(create_response.status_code, 200)
        run_id = int(create_response.json()["run_id"])

        run_response = client.get(f"/app/api/linkedin-draft-publisher/runs/{run_id}")
        self.assertEqual(run_response.status_code, 200)
        self.assertIn("run", run_response.json())

        result_response = client.get(f"/app/api/linkedin-draft-publisher/runs/{run_id}/result")
        self.assertEqual(result_response.status_code, 200)
        payload = result_response.json()["result"]
        self.assertIn("items", payload)


if __name__ == "__main__":
    unittest.main()
