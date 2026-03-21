from __future__ import annotations

import os
import secrets
import threading
import urllib.error
from pathlib import Path
from typing import Any, Dict, Optional, Protocol
from urllib.parse import urlparse
from uuid import uuid4

from authlib.integrations.starlette_client import OAuth, OAuthError
from fastapi import BackgroundTasks, Depends, FastAPI, File, Form, HTTPException, Request, Security, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import APIKeyHeader
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from src.config import APIRuntimeSettings, Settings
from src.interfaces.http.schemas import (
    CaseStudyIngestUrlRequestModel,
    CaseStudyIngestUrlResponseModel,
    EpisodeIngestResponseModel,
    LinkedInDraftPublisherScheduleConfigCreateRequestModel,
    LinkedInDraftPublisherScheduleConfigResponseModel,
    LinkedInDraftPublisherScheduleConfigUpdateRequestModel,
    LinkedInDraftPublisherScheduleCreateRequestModel,
    LinkedInDraftPublisherScheduleExecutionsResponseModel,
    LinkedInDraftPublisherScheduleListResponseModel,
    LinkedInDraftPublisherScheduleResponseModel,
    LinkedInDraftPublisherScheduleRunNowRequestModel,
    LinkedInDraftPublisherScheduleRunNowResponseModel,
    LinkedInDraftPublisherScheduleUpdateRequestModel,
    LinkedInDraftPublisherSchedulerTickRequestModel,
    LinkedInDraftPublisherSchedulerTickResponseModel,
    LinkedInDraftPublisherRunCreateRequestModel,
    LinkedInDraftPublisherRunCreateResponseModel,
    LinkedInDraftPublisherRunGetResponseModel,
    LinkedInDraftPublisherRunResultResponseModel,
    NewsletterLinkedInGenerateRequestModel,
    NewsletterLinkedInGenerateResponseModel,
    NewsletterLinkedInIdeasRequestModel,
    NewsletterLinkedInIdeasResponseModel,
    QuerySimilarRequestModel,
    QuerySimilarResponseModel,
    RecommendContentRequestModel,
    RecommendContentResponseModel,
    RunroomLabIngestUrlRequestModel,
    RunroomLabIngestUrlResponseModel,
    ThemeIntelRelatedRefreshRequestModel,
    ThemeIntelRelatedRefreshResponseModel,
    ThemeIntelRunCreateRequestModel,
    ThemeIntelRunCreateResponseModel,
    ThemeIntelRunDocumentsResponseModel,
    ThemeIntelRunGetResponseModel,
    ThemeIntelSourceDocumentResponseModel,
    ThemeIntelScheduleConfigCreateRequestModel,
    ThemeIntelScheduleConfigResponseModel,
    ThemeIntelScheduleConfigUpdateRequestModel,
    ThemeIntelScheduleCreateRequestModel,
    ThemeIntelScheduleExecutionsResponseModel,
    ThemeIntelScheduleListResponseModel,
    ThemeIntelScheduleResponseModel,
    ThemeIntelScheduleRunNowRequestModel,
    ThemeIntelScheduleRunNowResponseModel,
    ThemeIntelScheduleUpdateRequestModel,
    ThemeIntelSchedulerTickRequestModel,
    ThemeIntelSchedulerTickResponseModel,
    ThemeIntelTopicDetailResponseModel,
    ThemeIntelTopicListResponseModel,
    ThemeIntelTopicStatusUpdateRequestModel,
    ThemeIntelTopicStatusUpdateResponseModel,
    ThemeIntelTopicUsageRequestModel,
    ThemeIntelTopicUsageResponseModel,
)
from src.interfaces.http.services import QueryApiService
from src.pipeline.manual_episode_ingest import DuplicateEpisodeSourceFilenameError


def _spawn_detached_job(target: Any, *args: Any) -> None:
    thread = threading.Thread(
        target=target,
        args=args,
        daemon=True,
    )
    thread.start()


class QueryServicePort(Protocol):
    def query_similar(self, text: str, top_k: int, offline_mode: bool = False) -> dict[str, Any]:
        ...

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
    ) -> dict[str, Any]:
        ...

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
    ) -> dict[str, Any]:
        ...

    def list_newsletter_linkedin_ideas(
        self,
        exclude_topic_ids: list[int] | None = None,
        limit: int = 10,
        offline_mode: bool = False,
    ) -> dict[str, Any]:
        ...

    def ingest_case_study_url(self, url: str) -> dict[str, Any]:
        ...

    def ingest_runroom_lab_url(self, url: str) -> dict[str, Any]:
        ...

    def ingest_episode_upload(
        self,
        transcript_filename: str,
        transcript_bytes: bytes,
        runroom_url: str,
    ) -> dict[str, Any]:
        ...

    def create_theme_intel_run(
        self,
        gmail_query: str,
        origin_category: str,
        mark_as_read: bool,
        limit_messages: int = 100,
        triggered_by_email: str | None = None,
    ) -> dict[str, Any]:
        ...

    def execute_theme_intel_run(self, run_id: int, offline_mode: bool = False) -> None:
        ...

    def get_theme_intel_run(self, run_id: int) -> dict[str, Any] | None:
        ...

    def get_latest_theme_intel_run(self) -> dict[str, Any] | None:
        ...

    def list_theme_intel_run_source_documents(self, run_id: int) -> list[dict[str, Any]]:
        ...

    def get_theme_intel_source_document(self, source_document_id: int) -> dict[str, Any] | None:
        ...

    def list_theme_intel_topics(
        self,
        primary_category: str | None = None,
        status: str | None = None,
        tags_any: list[str] | None = None,
        tags_all: list[str] | None = None,
        min_score: float | None = None,
        semantic_query: str | None = None,
        limit: int = 50,
        offset: int = 0,
        offline_mode: bool = False,
    ) -> list[dict[str, Any]]:
        ...

    def update_theme_intel_topic_status(self, topic_id: int, status: str) -> dict[str, Any] | None:
        ...

    def register_theme_intel_topic_usage(
        self,
        topic_id: int,
        client_name: str,
        artifact_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        ...

    def refresh_theme_intel_related_content(
        self,
        topic_id: int,
        top_k: int | None = 10,
        content_types: list[str] | None = None,
        related_counts_by_type: dict[str, int] | None = None,
        offline_mode: bool = False,
    ) -> dict[str, Any]:
        ...

    def get_theme_intel_topic_detail(self, topic_id: int) -> dict[str, Any] | None:
        ...

    def create_theme_intel_schedule(
        self,
        name: str,
        enabled: bool = True,
        every_n_days: int = 1,
        run_time_local: str = "09:00",
        timezone_name: str = "Europe/Madrid",
    ) -> dict[str, Any]:
        ...

    def list_theme_intel_schedules(self) -> list[dict[str, Any]]:
        ...

    def update_theme_intel_schedule(
        self,
        schedule_id: int,
        name: str | None = None,
        enabled: bool | None = None,
        every_n_days: int | None = None,
        run_time_local: str | None = None,
        timezone_name: str | None = None,
    ) -> dict[str, Any] | None:
        ...

    def create_theme_intel_schedule_config(
        self,
        schedule_id: int,
        execution_order: int,
        gmail_query: str,
        origin_category: str,
        mark_as_read: bool,
        limit_messages: int,
        enabled: bool = True,
    ) -> dict[str, Any]:
        ...

    def update_theme_intel_schedule_config(
        self,
        schedule_id: int,
        config_id: int,
        execution_order: int | None = None,
        gmail_query: str | None = None,
        origin_category: str | None = None,
        mark_as_read: bool | None = None,
        limit_messages: int | None = None,
        enabled: bool | None = None,
    ) -> dict[str, Any] | None:
        ...

    def run_theme_intel_schedule_now(self, schedule_id: int, offline_mode: bool = False) -> dict[str, Any]:
        ...

    def list_theme_intel_schedule_executions(self, schedule_id: int, limit: int = 20) -> list[dict[str, Any]]:
        ...

    def tick_theme_intel_scheduler(self, offline_mode: bool = False) -> dict[str, Any]:
        ...

    def create_linkedin_draft_publisher_schedule(
        self,
        name: str,
        enabled: bool = True,
        every_n_days: int = 1,
        run_time_local: str = "09:00",
        timezone_name: str = "Europe/Madrid",
    ) -> dict[str, Any]:
        ...

    def list_linkedin_draft_publisher_schedules(self) -> list[dict[str, Any]]:
        ...

    def update_linkedin_draft_publisher_schedule(
        self,
        schedule_id: int,
        name: str | None = None,
        enabled: bool | None = None,
        every_n_days: int | None = None,
        run_time_local: str | None = None,
        timezone_name: str | None = None,
    ) -> dict[str, Any] | None:
        ...

    def create_linkedin_draft_publisher_schedule_config(
        self,
        schedule_id: int,
        execution_order: int,
        origin_category: str,
        slack_channel: str,
        buyer_persona_objetivo: str,
        enabled: bool = True,
    ) -> dict[str, Any]:
        ...

    def update_linkedin_draft_publisher_schedule_config(
        self,
        schedule_id: int,
        config_id: int,
        execution_order: int | None = None,
        origin_category: str | None = None,
        slack_channel: str | None = None,
        buyer_persona_objetivo: str | None = None,
        enabled: bool | None = None,
    ) -> dict[str, Any] | None:
        ...

    def run_linkedin_draft_publisher_schedule_now(self, schedule_id: int, offline_mode: bool = False) -> dict[str, Any]:
        ...

    def list_linkedin_draft_publisher_schedule_executions(self, schedule_id: int, limit: int = 20) -> list[dict[str, Any]]:
        ...

    def tick_linkedin_draft_publisher_scheduler(self, offline_mode: bool = False) -> dict[str, Any]:
        ...

    def create_linkedin_draft_publisher_run(
        self,
        origin_category: str,
        slack_channel: str,
        buyer_persona_objetivo: str,
        triggered_by_email: str | None = None,
        offline_mode: bool = False,
    ) -> dict[str, Any]:
        ...

    def execute_linkedin_draft_publisher_run(self, run_id: int, offline_mode: bool = False) -> None:
        ...

    def get_linkedin_draft_publisher_run(self, run_id: int) -> dict[str, Any] | None:
        ...

    def get_latest_linkedin_draft_publisher_run(self) -> dict[str, Any] | None:
        ...

    def get_linkedin_draft_publisher_run_result(self, run_id: int) -> dict[str, Any] | None:
        ...


class GoogleOAuthClientPort(Protocol):
    async def authorize_redirect(self, request: Request, redirect_uri: str) -> Any:
        ...

    async def authorize_access_token(self, request: Request) -> dict[str, Any]:
        ...

    async def parse_id_token(self, request: Request, token: dict[str, Any]) -> dict[str, Any] | None:
        ...


def _build_google_oauth_client(runtime: APIRuntimeSettings) -> GoogleOAuthClientPort | None:
    if not (
        runtime.google_oauth_client_id
        and runtime.google_oauth_client_secret
        and runtime.google_oauth_redirect_uri
    ):
        return None

    oauth = OAuth()
    oauth.register(
        name="google",
        server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
        client_id=runtime.google_oauth_client_id,
        client_secret=runtime.google_oauth_client_secret,
        client_kwargs={"scope": "openid email profile"},
    )
    return oauth.create_client("google")


def _session_user(request: Request) -> dict[str, str] | None:
    payload = request.session.get("user")
    if not isinstance(payload, dict):
        return None

    email = payload.get("email")
    name = payload.get("name")
    if not isinstance(email, str) or not email.strip():
        return None
    if not isinstance(name, str) or not name.strip():
        name = email
    return {"email": email, "name": name}


def _email_matches_domain(email: str, allowed_domain: str) -> bool:
    domain = email.rsplit("@", 1)[-1].lower()
    return domain == allowed_domain.lower()


def _validate_runroom_case_study_url(url: str) -> str:
    candidate = url.strip()
    parsed = urlparse(candidate)
    host = (parsed.hostname or "").lower()

    if parsed.scheme not in {"http", "https"}:
        raise ValueError("URL inválida: solo se permiten esquemas http/https.")
    if host not in {"runroom.com", "www.runroom.com"}:
        raise ValueError("URL inválida: solo se permiten hosts runroom.com o www.runroom.com.")
    if not parsed.path.startswith("/cases/"):
        raise ValueError("URL inválida: el path debe empezar por /cases/.")

    return candidate


def _validate_runroom_episode_url(url: str) -> str:
    candidate = url.strip()
    parsed = urlparse(candidate)
    host = (parsed.hostname or "").lower()

    if parsed.scheme not in {"http", "https"}:
        raise ValueError("URL invalida: solo se permiten esquemas http/https.")
    if host not in {"runroom.com", "www.runroom.com"}:
        raise ValueError("URL invalida: solo se permiten hosts runroom.com o www.runroom.com.")
    if not (parsed.path.startswith("/realworld/") or parsed.path.startswith("/en/realworld/")):
        raise ValueError("URL invalida: el path debe empezar por /realworld/ o /en/realworld/.")

    return candidate


def _validate_runroom_lab_url(url: str) -> str:
    candidate = url.strip()
    parsed = urlparse(candidate)
    host = (parsed.hostname or "").lower()
    path = parsed.path.rstrip("/")

    if parsed.scheme not in {"http", "https"}:
        raise ValueError("URL invalida: solo se permiten esquemas http/https.")
    if host not in {"runroom.com", "www.runroom.com", "info.runroom.com"}:
        raise ValueError(
            "URL invalida: solo se permiten hosts runroom.com, www.runroom.com o info.runroom.com."
        )
    if not path:
        raise ValueError("URL invalida: la URL debe apuntar a una pagina concreta del LAB.")
    if path == "/runroom-lab-todas-las-ediciones":
        raise ValueError("URL invalida: usa la URL de una edicion concreta del LAB, no la del indice.")

    return candidate


def create_app(
    service: Optional[QueryServicePort] = None,
    api_key: Optional[str] = None,
    runtime_settings: Optional[APIRuntimeSettings] = None,
    google_oauth_client: Optional[GoogleOAuthClientPort] = None,
) -> FastAPI:
    runtime = runtime_settings
    if service is None:
        settings = Settings.from_env()
        runtime = runtime or APIRuntimeSettings.from_env()
        service = QueryApiService(settings=settings, schema_path=runtime.schema_path)
        api_key = runtime.api_key
    elif runtime is None and api_key:
        runtime = APIRuntimeSettings(
            api_key=api_key,
            session_secret=api_key,
        )
    elif runtime is None:
        runtime = APIRuntimeSettings.from_env()
        api_key = runtime.api_key

    if not api_key:
        raise ValueError("API key is required to initialize HTTP API")

    app = FastAPI(title="Runroom Content Query API", version="1.0.0")
    app.add_middleware(
        SessionMiddleware,
        secret_key=runtime.session_secret,
        max_age=runtime.session_max_age_seconds,
        https_only=runtime.session_cookie_secure,
        same_site="lax",
    )

    templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))
    oauth_client = google_oauth_client or _build_google_oauth_client(runtime)
    api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

    def require_api_key(client_key: Optional[str] = Security(api_key_header)) -> None:
        if not client_key or not secrets.compare_digest(client_key, api_key):
            raise HTTPException(status_code=401, detail="Invalid API key")

    def require_session_api_user(request: Request) -> dict[str, str]:
        user = _session_user(request)
        if user is None:
            raise HTTPException(status_code=401, detail="Authentication required")
        return user

    def query_similar_payload(payload: QuerySimilarRequestModel) -> Dict[str, Any]:
        result = service.query_similar(
            text=payload.text,
            top_k=payload.top_k,
            offline_mode=payload.offline_mode,
        )
        return {
            "request_id": str(uuid4()),
            "query": result["query"],
            "top_k": result["top_k"],
            "results": result["results"],
        }

    def recommend_payload(payload: RecommendContentRequestModel) -> Dict[str, Any]:
        result = service.recommend_content(
            text=payload.text,
            top_k=payload.top_k,
            fetch_k=payload.fetch_k,
            content_types=payload.content_types,
            source=payload.source,
            language=payload.language,
            group_by_type=payload.group_by_type,
            offline_mode=payload.offline_mode,
        )
        return {
            "request_id": str(uuid4()),
            **result,
        }

    def newsletter_linkedin_payload(payload: NewsletterLinkedInGenerateRequestModel) -> Dict[str, Any]:
        result = service.generate_newsletter_linkedin(
            idea=payload.idea,
            referencias=payload.referencias,
            audiencia=payload.audiencia,
            objetivo_secundario=payload.objetivo_secundario,
            longitud=payload.longitud,
            metafora_visual=payload.metafora_visual,
            texto_a_incluir=payload.texto_a_incluir,
            offline_mode=payload.offline_mode,
        )
        return {
            "request_id": str(uuid4()),
            **result,
        }

    def newsletter_linkedin_ideas_payload(payload: NewsletterLinkedInIdeasRequestModel) -> Dict[str, Any]:
        result = service.list_newsletter_linkedin_ideas(
            exclude_topic_ids=payload.exclude_topic_ids,
            limit=payload.limit,
            offline_mode=payload.offline_mode,
        )
        return {
            "request_id": str(uuid4()),
            **result,
        }

    def case_study_ingest_payload(payload: CaseStudyIngestUrlRequestModel) -> Dict[str, Any]:
        try:
            url = _validate_runroom_case_study_url(payload.url)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        try:
            result = service.ingest_case_study_url(url=url)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            raise HTTPException(status_code=502, detail=f"No se pudo cargar la URL externa: {exc}") from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail="Error inesperado durante la ingesta del case study.") from exc

        return {
            "request_id": str(uuid4()),
            "url": str(result["url"]),
            "summary": result["summary"],
        }

    def runroom_lab_ingest_payload(payload: RunroomLabIngestUrlRequestModel) -> Dict[str, Any]:
        try:
            url = _validate_runroom_lab_url(payload.url)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        try:
            result = service.ingest_runroom_lab_url(url=url)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            raise HTTPException(status_code=502, detail=f"No se pudo cargar la URL externa: {exc}") from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail="Error inesperado durante la ingesta del runroom lab.") from exc

        return {
            "request_id": str(uuid4()),
            "url": str(result["url"]),
            "summary": result["summary"],
        }

    def episode_ingest_payload(
        transcript_filename: str,
        transcript_bytes: bytes,
        runroom_url: str,
    ) -> Dict[str, Any]:
        try:
            url = _validate_runroom_episode_url(runroom_url)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        try:
            result = service.ingest_episode_upload(
                transcript_filename=transcript_filename,
                transcript_bytes=transcript_bytes,
                runroom_url=url,
            )
        except DuplicateEpisodeSourceFilenameError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            raise HTTPException(status_code=502, detail=f"No se pudo cargar la URL externa: {exc}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail="Error inesperado durante la ingesta del episodio.") from exc

        return {
            "request_id": str(uuid4()),
            "runroom_url": str(result["runroom_url"]),
            "summary": result["summary"],
        }

    def _parse_csv_tags(value: Optional[str]) -> list[str]:
        if value is None:
            return []
        return [item.strip() for item in value.split(",") if item.strip()]

    def list_theme_topics_payload(
        primary_category: Optional[str] = None,
        status: Optional[str] = None,
        tags_any: Optional[str] = None,
        tags_all: Optional[str] = None,
        min_score: Optional[float] = None,
        semantic_query: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
        offline_mode: bool = False,
    ) -> Dict[str, Any]:
        topics = service.list_theme_intel_topics(
            primary_category=primary_category,
            status=status,
            tags_any=_parse_csv_tags(tags_any),
            tags_all=_parse_csv_tags(tags_all),
            min_score=min_score,
            semantic_query=semantic_query,
            limit=limit,
            offset=offset,
            offline_mode=offline_mode,
        )
        return {
            "request_id": str(uuid4()),
            "total": len(topics),
            "topics": topics,
        }

    def list_theme_schedules_payload() -> Dict[str, Any]:
        schedules = service.list_theme_intel_schedules()
        return {
            "request_id": str(uuid4()),
            "total": len(schedules),
            "schedules": schedules,
        }

    def list_linkedin_draft_publisher_schedules_payload() -> Dict[str, Any]:
        schedules = service.list_linkedin_draft_publisher_schedules()
        return {
            "request_id": str(uuid4()),
            "total": len(schedules),
            "schedules": schedules,
        }

    @app.get("/", response_class=HTMLResponse)
    def root(request: Request) -> Any:
        if _session_user(request):
            return RedirectResponse(url="/app", status_code=302)
        return templates.TemplateResponse(
            request,
            "login.html",
            {
                "oauth_enabled": oauth_client is not None,
                "allowed_domain": runtime.google_oauth_allowed_domain,
            },
        )

    @app.get("/auth/google/start")
    async def auth_google_start(request: Request) -> Any:
        if oauth_client is None or not runtime.google_oauth_redirect_uri:
            raise HTTPException(status_code=503, detail="Google OAuth is not configured")
        return await oauth_client.authorize_redirect(request, runtime.google_oauth_redirect_uri)

    @app.get("/auth/google/callback")
    async def auth_google_callback(request: Request) -> Any:
        if oauth_client is None:
            raise HTTPException(status_code=503, detail="Google OAuth is not configured")
        try:
            token = await oauth_client.authorize_access_token(request)
        except OAuthError as exc:
            raise HTTPException(status_code=401, detail=f"OAuth failed: {exc.error}") from exc

        userinfo = token.get("userinfo")
        if not isinstance(userinfo, dict):
            userinfo = await oauth_client.parse_id_token(request, token) or {}

        email = str(userinfo.get("email", "")).strip().lower()
        name = str(userinfo.get("name", "")).strip() or email
        email_verified = userinfo.get("email_verified", False)
        verified = email_verified is True or str(email_verified).lower() == "true"
        if not email or not verified:
            raise HTTPException(status_code=403, detail="Verified Google account required")
        if not _email_matches_domain(email=email, allowed_domain=runtime.google_oauth_allowed_domain):
            raise HTTPException(status_code=403, detail="Email domain not allowed")

        request.session["user"] = {
            "email": email,
            "name": name,
        }
        return RedirectResponse(url="/app", status_code=302)

    @app.post("/auth/logout")
    def auth_logout(request: Request) -> Any:
        request.session.clear()
        return RedirectResponse(url="/", status_code=303)

    @app.get("/app", response_class=HTMLResponse)
    def app_page(request: Request) -> Any:
        user = _session_user(request)
        if user is None:
            return RedirectResponse(url="/", status_code=302)
        return templates.TemplateResponse(
            request,
            "app.html",
            {"user": user},
        )

    @app.get("/app/newsletters-linkedin", response_class=HTMLResponse)
    def app_newsletters_linkedin_page(request: Request) -> Any:
        user = _session_user(request)
        if user is None:
            return RedirectResponse(url="/", status_code=302)
        return templates.TemplateResponse(
            request,
            "newsletter_linkedin.html",
            {"user": user},
        )

    @app.get("/app/theme-intel", response_class=HTMLResponse)
    def app_theme_intel_page(request: Request) -> Any:
        user = _session_user(request)
        if user is None:
            return RedirectResponse(url="/", status_code=302)
        return templates.TemplateResponse(
            request,
            "theme_intel.html",
            {"user": user},
        )

    @app.get("/app/linkedin-draft-publisher", response_class=HTMLResponse)
    def app_linkedin_draft_publisher_page(request: Request) -> Any:
        user = _session_user(request)
        if user is None:
            return RedirectResponse(url="/", status_code=302)
        return templates.TemplateResponse(
            request,
            "linkedin_draft_publisher.html",
            {"user": user},
        )

    @app.get("/app/nuevo-case-study", response_class=HTMLResponse)
    def app_new_case_study_page(request: Request) -> Any:
        user = _session_user(request)
        if user is None:
            return RedirectResponse(url="/", status_code=302)
        return templates.TemplateResponse(
            request,
            "new_case_study.html",
            {"user": user},
        )

    @app.get("/app/nuevo-runroom-lab", response_class=HTMLResponse)
    def app_new_runroom_lab_page(request: Request) -> Any:
        user = _session_user(request)
        if user is None:
            return RedirectResponse(url="/", status_code=302)
        return templates.TemplateResponse(
            request,
            "new_runroom_lab.html",
            {"user": user},
        )

    @app.get("/app/nuevo-episodio-realworld", response_class=HTMLResponse)
    def app_new_realworld_episode_page(request: Request) -> Any:
        user = _session_user(request)
        if user is None:
            return RedirectResponse(url="/", status_code=302)
        return templates.TemplateResponse(
            request,
            "new_realworld_episode.html",
            {"user": user},
        )

    @app.get("/health")
    def health(_: None = Security(require_api_key)) -> Dict[str, str]:
        return {"status": "ok", "request_id": str(uuid4())}

    @app.post("/v1/query-similar", response_model=QuerySimilarResponseModel)
    def query_similar(payload: QuerySimilarRequestModel, _: None = Security(require_api_key)) -> Dict[str, Any]:
        return query_similar_payload(payload)

    @app.post("/v1/recommend-content", response_model=RecommendContentResponseModel)
    def recommend_content(payload: RecommendContentRequestModel, _: None = Security(require_api_key)) -> Dict[str, Any]:
        return recommend_payload(payload)

    @app.post("/app/api/query-similar", response_model=QuerySimilarResponseModel)
    def app_query_similar(
        payload: QuerySimilarRequestModel,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        return query_similar_payload(payload)

    @app.post("/app/api/recommend-content", response_model=RecommendContentResponseModel)
    def app_recommend_content(
        payload: RecommendContentRequestModel,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        return recommend_payload(payload)

    @app.post(
        "/app/api/newsletters-linkedin/generate",
        response_model=NewsletterLinkedInGenerateResponseModel,
    )
    def app_generate_newsletter_linkedin(
        payload: NewsletterLinkedInGenerateRequestModel,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        return newsletter_linkedin_payload(payload)

    @app.post(
        "/app/api/newsletters-linkedin/ideas",
        response_model=NewsletterLinkedInIdeasResponseModel,
    )
    def app_list_newsletter_linkedin_ideas(
        payload: NewsletterLinkedInIdeasRequestModel,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        return newsletter_linkedin_ideas_payload(payload)

    @app.post(
        "/app/api/case-studies/ingest-url",
        response_model=CaseStudyIngestUrlResponseModel,
    )
    def app_ingest_case_study_url(
        payload: CaseStudyIngestUrlRequestModel,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        return case_study_ingest_payload(payload)

    @app.post(
        "/app/api/runroom-labs/ingest-url",
        response_model=RunroomLabIngestUrlResponseModel,
    )
    def app_ingest_runroom_lab_url(
        payload: RunroomLabIngestUrlRequestModel,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        return runroom_lab_ingest_payload(payload)

    @app.post(
        "/app/api/episodes/ingest",
        response_model=EpisodeIngestResponseModel,
    )
    async def app_ingest_realworld_episode(
        runroom_url: str = Form(...),
        transcript_file: UploadFile = File(...),
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        filename = Path(transcript_file.filename or "").name.strip()
        if not filename:
            raise HTTPException(status_code=422, detail="Debes subir un archivo de transcripcion.")
        if Path(filename).suffix.lower() != ".txt":
            raise HTTPException(status_code=422, detail="El archivo debe tener extension .txt.")

        transcript_bytes = await transcript_file.read()
        await transcript_file.close()
        if not transcript_bytes or not transcript_bytes.strip():
            raise HTTPException(status_code=422, detail="El archivo de transcripcion esta vacio.")

        return episode_ingest_payload(
            transcript_filename=filename,
            transcript_bytes=transcript_bytes,
            runroom_url=runroom_url,
        )

    @app.post(
        "/app/api/linkedin-draft-publisher/runs",
        response_model=LinkedInDraftPublisherRunCreateResponseModel,
    )
    def app_create_linkedin_draft_publisher_run(
        payload: LinkedInDraftPublisherRunCreateRequestModel,
        user: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        try:
            created = service.create_linkedin_draft_publisher_run(
                origin_category=payload.originCategory,
                slack_channel=payload.slackChannel,
                buyer_persona_objetivo=payload.buyerPersonaObjetivo,
                triggered_by_email=user["email"],
                offline_mode=payload.offline_mode,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        run_id = int(created["run_id"])
        _spawn_detached_job(
            service.execute_linkedin_draft_publisher_run,
            run_id,
            payload.offline_mode,
        )
        return {
            "request_id": str(uuid4()),
            "run_id": run_id,
            "status": str(created.get("status") or "queued"),
        }

    @app.get(
        "/app/api/linkedin-draft-publisher/runs/{run_id}",
        response_model=LinkedInDraftPublisherRunGetResponseModel,
    )
    def app_get_linkedin_draft_publisher_run(
        run_id: int,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        run = service.get_linkedin_draft_publisher_run(run_id=run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="LinkedIn draft run not found")
        return {"request_id": str(uuid4()), "run": run}

    @app.get(
        "/app/api/linkedin-draft-publisher/runs/{run_id}/result",
        response_model=LinkedInDraftPublisherRunResultResponseModel,
    )
    def app_get_linkedin_draft_publisher_run_result(
        run_id: int,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        result = service.get_linkedin_draft_publisher_run_result(run_id=run_id)
        if result is None:
            raise HTTPException(status_code=404, detail="LinkedIn draft run not found")
        return {"request_id": str(uuid4()), "result": result}

    @app.post(
        "/app/api/linkedin-draft-publisher/schedules",
        response_model=LinkedInDraftPublisherScheduleResponseModel,
    )
    def app_create_linkedin_draft_publisher_schedule(
        payload: LinkedInDraftPublisherScheduleCreateRequestModel,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        try:
            schedule = service.create_linkedin_draft_publisher_schedule(
                name=payload.name,
                enabled=payload.enabled,
                every_n_days=payload.every_n_days,
                run_time_local=payload.run_time_local,
                timezone_name=payload.timezone,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {"request_id": str(uuid4()), "schedule": schedule}

    @app.get(
        "/app/api/linkedin-draft-publisher/schedules",
        response_model=LinkedInDraftPublisherScheduleListResponseModel,
    )
    def app_list_linkedin_draft_publisher_schedules(
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        return list_linkedin_draft_publisher_schedules_payload()

    @app.patch(
        "/app/api/linkedin-draft-publisher/schedules/{schedule_id}",
        response_model=LinkedInDraftPublisherScheduleResponseModel,
    )
    def app_update_linkedin_draft_publisher_schedule(
        schedule_id: int,
        payload: LinkedInDraftPublisherScheduleUpdateRequestModel,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        try:
            schedule = service.update_linkedin_draft_publisher_schedule(
                schedule_id=schedule_id,
                name=payload.name,
                enabled=payload.enabled,
                every_n_days=payload.every_n_days,
                run_time_local=payload.run_time_local,
                timezone_name=payload.timezone,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        if schedule is None:
            raise HTTPException(status_code=404, detail="LinkedIn draft schedule not found")
        return {"request_id": str(uuid4()), "schedule": schedule}

    @app.post(
        "/app/api/linkedin-draft-publisher/schedules/{schedule_id}/configs",
        response_model=LinkedInDraftPublisherScheduleConfigResponseModel,
    )
    def app_create_linkedin_draft_publisher_schedule_config(
        schedule_id: int,
        payload: LinkedInDraftPublisherScheduleConfigCreateRequestModel,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        try:
            config = service.create_linkedin_draft_publisher_schedule_config(
                schedule_id=schedule_id,
                execution_order=payload.executionOrder,
                origin_category=payload.originCategory,
                slack_channel=payload.slackChannel,
                buyer_persona_objetivo=payload.buyerPersonaObjetivo,
                enabled=payload.enabled,
            )
        except ValueError as exc:
            detail = str(exc)
            status_code = 404 if "Schedule no encontrado" in detail else 422
            raise HTTPException(status_code=status_code, detail=detail) from exc
        return {"request_id": str(uuid4()), "config": config}

    @app.patch(
        "/app/api/linkedin-draft-publisher/schedules/{schedule_id}/configs/{config_id}",
        response_model=LinkedInDraftPublisherScheduleConfigResponseModel,
    )
    def app_update_linkedin_draft_publisher_schedule_config(
        schedule_id: int,
        config_id: int,
        payload: LinkedInDraftPublisherScheduleConfigUpdateRequestModel,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        try:
            config = service.update_linkedin_draft_publisher_schedule_config(
                schedule_id=schedule_id,
                config_id=config_id,
                execution_order=payload.executionOrder,
                origin_category=payload.originCategory,
                slack_channel=payload.slackChannel,
                buyer_persona_objetivo=payload.buyerPersonaObjetivo,
                enabled=payload.enabled,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        if config is None:
            raise HTTPException(status_code=404, detail="LinkedIn draft schedule config not found")
        return {"request_id": str(uuid4()), "config": config}

    @app.post(
        "/app/api/linkedin-draft-publisher/schedules/{schedule_id}/run-now",
        response_model=LinkedInDraftPublisherScheduleRunNowResponseModel,
    )
    def app_run_linkedin_draft_publisher_schedule_now(
        schedule_id: int,
        payload: LinkedInDraftPublisherScheduleRunNowRequestModel,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        try:
            result = service.run_linkedin_draft_publisher_schedule_now(
                schedule_id=schedule_id,
                offline_mode=payload.offline_mode,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"request_id": str(uuid4()), "result": result}

    @app.get(
        "/app/api/linkedin-draft-publisher/schedules/{schedule_id}/executions",
        response_model=LinkedInDraftPublisherScheduleExecutionsResponseModel,
    )
    def app_list_linkedin_draft_publisher_schedule_executions(
        schedule_id: int,
        limit: int = 20,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        try:
            executions = service.list_linkedin_draft_publisher_schedule_executions(
                schedule_id=schedule_id,
                limit=limit,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {
            "request_id": str(uuid4()),
            "schedule_id": schedule_id,
            "total": len(executions),
            "executions": executions,
        }

    @app.post(
        "/app/api/linkedin-draft-publisher/scheduler/tick",
        response_model=LinkedInDraftPublisherSchedulerTickResponseModel,
    )
    def app_tick_linkedin_draft_publisher_scheduler(
        payload: Optional[LinkedInDraftPublisherSchedulerTickRequestModel] = None,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        result = service.tick_linkedin_draft_publisher_scheduler(offline_mode=bool(payload and payload.offline_mode))
        return {"request_id": str(uuid4()), "result": result}

    @app.post(
        "/app/api/theme-intel/runs",
        response_model=ThemeIntelRunCreateResponseModel,
    )
    def app_create_theme_run(
        payload: ThemeIntelRunCreateRequestModel,
        background_tasks: BackgroundTasks,
        user: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        try:
            created = service.create_theme_intel_run(
                gmail_query=payload.gmailQuery,
                origin_category=payload.originCategory,
                mark_as_read=payload.markAsRead,
                limit_messages=payload.limitMessages,
                triggered_by_email=user["email"],
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        run_id = int(created["run_id"])
        background_tasks.add_task(service.execute_theme_intel_run, run_id, payload.offline_mode)
        return {
            "request_id": str(uuid4()),
            "run_id": run_id,
            "status": str(created.get("status") or "queued"),
        }

    @app.get(
        "/app/api/theme-intel/runs/latest",
        response_model=ThemeIntelRunGetResponseModel,
    )
    def app_get_latest_theme_run(
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        run = service.get_latest_theme_intel_run()
        if run is None:
            raise HTTPException(status_code=404, detail="Theme run not found")
        return {"request_id": str(uuid4()), "run": run}

    @app.get(
        "/app/api/theme-intel/runs/{run_id}",
        response_model=ThemeIntelRunGetResponseModel,
    )
    def app_get_theme_run(
        run_id: int,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        run = service.get_theme_intel_run(run_id=run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Theme run not found")
        return {"request_id": str(uuid4()), "run": run}

    @app.get(
        "/app/api/theme-intel/runs/{run_id}/documents",
        response_model=ThemeIntelRunDocumentsResponseModel,
    )
    def app_get_theme_run_documents(
        run_id: int,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        run = service.get_theme_intel_run(run_id=run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Theme run not found")
        documents = service.list_theme_intel_run_source_documents(run_id=run_id)
        return {
            "request_id": str(uuid4()),
            "run_id": run_id,
            "total": len(documents),
            "documents": documents,
        }

    @app.get(
        "/app/api/theme-intel/source-documents/{source_document_id}",
        response_model=ThemeIntelSourceDocumentResponseModel,
    )
    def app_get_theme_source_document(
        source_document_id: int,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        document = service.get_theme_intel_source_document(source_document_id=source_document_id)
        if document is None:
            raise HTTPException(status_code=404, detail="Theme source document not found")
        return {
            "request_id": str(uuid4()),
            "document": document,
        }

    @app.get(
        "/app/api/theme-intel/topics",
        response_model=ThemeIntelTopicListResponseModel,
    )
    def app_list_theme_topics(
        primary_category: Optional[str] = None,
        status: Optional[str] = None,
        tags_any: Optional[str] = None,
        tags_all: Optional[str] = None,
        min_score: Optional[float] = None,
        semantic_query: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
        offline_mode: bool = False,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        return list_theme_topics_payload(
            primary_category=primary_category,
            status=status,
            tags_any=tags_any,
            tags_all=tags_all,
            min_score=min_score,
            semantic_query=semantic_query,
            limit=limit,
            offset=offset,
            offline_mode=offline_mode,
        )

    @app.patch(
        "/app/api/theme-intel/topics/{topic_id}/status",
        response_model=ThemeIntelTopicStatusUpdateResponseModel,
    )
    def app_update_theme_topic_status(
        topic_id: int,
        payload: ThemeIntelTopicStatusUpdateRequestModel,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        topic = service.update_theme_intel_topic_status(topic_id=topic_id, status=payload.status)
        if topic is None:
            raise HTTPException(status_code=404, detail="Theme topic not found")
        return {"request_id": str(uuid4()), "topic": topic}

    @app.post(
        "/app/api/theme-intel/topics/{topic_id}/usage",
        response_model=ThemeIntelTopicUsageResponseModel,
    )
    def app_register_theme_topic_usage(
        topic_id: int,
        payload: ThemeIntelTopicUsageRequestModel,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        usage = service.register_theme_intel_topic_usage(
            topic_id=topic_id,
            client_name=payload.client_name,
            artifact_id=payload.artifact_id,
            metadata=payload.metadata,
        )
        return {"request_id": str(uuid4()), "usage": usage}

    @app.get(
        "/app/api/theme-intel/topics/{topic_id}",
        response_model=ThemeIntelTopicDetailResponseModel,
    )
    def app_get_theme_topic_detail(
        topic_id: int,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        topic = service.get_theme_intel_topic_detail(topic_id=topic_id)
        if topic is None:
            raise HTTPException(status_code=404, detail="Theme topic not found")
        return {"request_id": str(uuid4()), "topic": topic}

    @app.post(
        "/app/api/theme-intel/topics/{topic_id}/related-content/refresh",
        response_model=ThemeIntelRelatedRefreshResponseModel,
    )
    def app_refresh_theme_related_content(
        topic_id: int,
        payload: ThemeIntelRelatedRefreshRequestModel,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        try:
            result = service.refresh_theme_intel_related_content(
                topic_id=topic_id,
                top_k=payload.top_k,
                content_types=payload.content_types,
                related_counts_by_type=payload.related_counts_by_type,
                offline_mode=payload.offline_mode,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"request_id": str(uuid4()), "result": result}

    @app.post(
        "/app/api/theme-intel/schedules",
        response_model=ThemeIntelScheduleResponseModel,
    )
    def app_create_theme_schedule(
        payload: ThemeIntelScheduleCreateRequestModel,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        try:
            schedule = service.create_theme_intel_schedule(
                name=payload.name,
                enabled=payload.enabled,
                every_n_days=payload.every_n_days,
                run_time_local=payload.run_time_local,
                timezone_name=payload.timezone,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {"request_id": str(uuid4()), "schedule": schedule}

    @app.get(
        "/app/api/theme-intel/schedules",
        response_model=ThemeIntelScheduleListResponseModel,
    )
    def app_list_theme_schedules(
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        return list_theme_schedules_payload()

    @app.patch(
        "/app/api/theme-intel/schedules/{schedule_id}",
        response_model=ThemeIntelScheduleResponseModel,
    )
    def app_update_theme_schedule(
        schedule_id: int,
        payload: ThemeIntelScheduleUpdateRequestModel,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        try:
            schedule = service.update_theme_intel_schedule(
                schedule_id=schedule_id,
                name=payload.name,
                enabled=payload.enabled,
                every_n_days=payload.every_n_days,
                run_time_local=payload.run_time_local,
                timezone_name=payload.timezone,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        if schedule is None:
            raise HTTPException(status_code=404, detail="Theme schedule not found")
        return {"request_id": str(uuid4()), "schedule": schedule}

    @app.post(
        "/app/api/theme-intel/schedules/{schedule_id}/configs",
        response_model=ThemeIntelScheduleConfigResponseModel,
    )
    def app_create_theme_schedule_config(
        schedule_id: int,
        payload: ThemeIntelScheduleConfigCreateRequestModel,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        try:
            config = service.create_theme_intel_schedule_config(
                schedule_id=schedule_id,
                execution_order=payload.execution_order,
                gmail_query=payload.gmail_query,
                origin_category=payload.origin_category,
                mark_as_read=payload.mark_as_read,
                limit_messages=payload.limit_messages,
                enabled=payload.enabled,
            )
        except ValueError as exc:
            detail = str(exc)
            status_code = 404 if "Schedule no encontrado" in detail else 422
            raise HTTPException(status_code=status_code, detail=detail) from exc
        return {"request_id": str(uuid4()), "config": config}

    @app.patch(
        "/app/api/theme-intel/schedules/{schedule_id}/configs/{config_id}",
        response_model=ThemeIntelScheduleConfigResponseModel,
    )
    def app_update_theme_schedule_config(
        schedule_id: int,
        config_id: int,
        payload: ThemeIntelScheduleConfigUpdateRequestModel,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        try:
            config = service.update_theme_intel_schedule_config(
                schedule_id=schedule_id,
                config_id=config_id,
                execution_order=payload.execution_order,
                gmail_query=payload.gmail_query,
                origin_category=payload.origin_category,
                mark_as_read=payload.mark_as_read,
                limit_messages=payload.limit_messages,
                enabled=payload.enabled,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        if config is None:
            raise HTTPException(status_code=404, detail="Theme schedule config not found")
        return {"request_id": str(uuid4()), "config": config}

    @app.post(
        "/app/api/theme-intel/schedules/{schedule_id}/run-now",
        response_model=ThemeIntelScheduleRunNowResponseModel,
    )
    def app_run_theme_schedule_now(
        schedule_id: int,
        payload: ThemeIntelScheduleRunNowRequestModel,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        try:
            result = service.run_theme_intel_schedule_now(
                schedule_id=schedule_id,
                offline_mode=payload.offline_mode,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"request_id": str(uuid4()), "result": result}

    @app.get(
        "/app/api/theme-intel/schedules/{schedule_id}/executions",
        response_model=ThemeIntelScheduleExecutionsResponseModel,
    )
    def app_list_theme_schedule_executions(
        schedule_id: int,
        limit: int = 20,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        try:
            executions = service.list_theme_intel_schedule_executions(
                schedule_id=schedule_id,
                limit=limit,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {
            "request_id": str(uuid4()),
            "schedule_id": schedule_id,
            "total": len(executions),
            "executions": executions,
        }

    @app.post(
        "/app/api/theme-intel/scheduler/tick",
        response_model=ThemeIntelSchedulerTickResponseModel,
    )
    def app_tick_theme_scheduler(
        payload: Optional[ThemeIntelSchedulerTickRequestModel] = None,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        result = service.tick_theme_intel_scheduler(offline_mode=bool(payload and payload.offline_mode))
        return {"request_id": str(uuid4()), "result": result}

    @app.post(
        "/v1/theme-intel/runs",
        response_model=ThemeIntelRunCreateResponseModel,
    )
    def create_theme_run_v1(
        payload: ThemeIntelRunCreateRequestModel,
        background_tasks: BackgroundTasks,
        _: None = Security(require_api_key),
    ) -> Dict[str, Any]:
        try:
            created = service.create_theme_intel_run(
                gmail_query=payload.gmailQuery,
                origin_category=payload.originCategory,
                mark_as_read=payload.markAsRead,
                limit_messages=payload.limitMessages,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        run_id = int(created["run_id"])
        background_tasks.add_task(service.execute_theme_intel_run, run_id, payload.offline_mode)
        return {
            "request_id": str(uuid4()),
            "run_id": run_id,
            "status": str(created.get("status") or "queued"),
        }

    @app.get(
        "/v1/theme-intel/runs/latest",
        response_model=ThemeIntelRunGetResponseModel,
    )
    def get_latest_theme_run_v1(
        _: None = Security(require_api_key),
    ) -> Dict[str, Any]:
        run = service.get_latest_theme_intel_run()
        if run is None:
            raise HTTPException(status_code=404, detail="Theme run not found")
        return {"request_id": str(uuid4()), "run": run}

    @app.get(
        "/v1/theme-intel/runs/{run_id}",
        response_model=ThemeIntelRunGetResponseModel,
    )
    def get_theme_run_v1(
        run_id: int,
        _: None = Security(require_api_key),
    ) -> Dict[str, Any]:
        run = service.get_theme_intel_run(run_id=run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Theme run not found")
        return {"request_id": str(uuid4()), "run": run}

    @app.get(
        "/v1/theme-intel/runs/{run_id}/documents",
        response_model=ThemeIntelRunDocumentsResponseModel,
    )
    def get_theme_run_documents_v1(
        run_id: int,
        _: None = Security(require_api_key),
    ) -> Dict[str, Any]:
        run = service.get_theme_intel_run(run_id=run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Theme run not found")
        documents = service.list_theme_intel_run_source_documents(run_id=run_id)
        return {
            "request_id": str(uuid4()),
            "run_id": run_id,
            "total": len(documents),
            "documents": documents,
        }

    @app.get(
        "/v1/theme-intel/source-documents/{source_document_id}",
        response_model=ThemeIntelSourceDocumentResponseModel,
    )
    def get_theme_source_document_v1(
        source_document_id: int,
        _: None = Security(require_api_key),
    ) -> Dict[str, Any]:
        document = service.get_theme_intel_source_document(source_document_id=source_document_id)
        if document is None:
            raise HTTPException(status_code=404, detail="Theme source document not found")
        return {
            "request_id": str(uuid4()),
            "document": document,
        }

    @app.get(
        "/v1/theme-intel/topics",
        response_model=ThemeIntelTopicListResponseModel,
    )
    def list_theme_topics_v1(
        primary_category: Optional[str] = None,
        status: Optional[str] = None,
        tags_any: Optional[str] = None,
        tags_all: Optional[str] = None,
        min_score: Optional[float] = None,
        semantic_query: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
        offline_mode: bool = False,
        _: None = Security(require_api_key),
    ) -> Dict[str, Any]:
        return list_theme_topics_payload(
            primary_category=primary_category,
            status=status,
            tags_any=tags_any,
            tags_all=tags_all,
            min_score=min_score,
            semantic_query=semantic_query,
            limit=limit,
            offset=offset,
            offline_mode=offline_mode,
        )

    @app.patch(
        "/v1/theme-intel/topics/{topic_id}/status",
        response_model=ThemeIntelTopicStatusUpdateResponseModel,
    )
    def update_theme_topic_status_v1(
        topic_id: int,
        payload: ThemeIntelTopicStatusUpdateRequestModel,
        _: None = Security(require_api_key),
    ) -> Dict[str, Any]:
        topic = service.update_theme_intel_topic_status(topic_id=topic_id, status=payload.status)
        if topic is None:
            raise HTTPException(status_code=404, detail="Theme topic not found")
        return {"request_id": str(uuid4()), "topic": topic}

    @app.post(
        "/v1/theme-intel/topics/{topic_id}/usage",
        response_model=ThemeIntelTopicUsageResponseModel,
    )
    def register_theme_topic_usage_v1(
        topic_id: int,
        payload: ThemeIntelTopicUsageRequestModel,
        _: None = Security(require_api_key),
    ) -> Dict[str, Any]:
        usage = service.register_theme_intel_topic_usage(
            topic_id=topic_id,
            client_name=payload.client_name,
            artifact_id=payload.artifact_id,
            metadata=payload.metadata,
        )
        return {"request_id": str(uuid4()), "usage": usage}

    @app.get(
        "/v1/theme-intel/topics/{topic_id}",
        response_model=ThemeIntelTopicDetailResponseModel,
    )
    def get_theme_topic_detail_v1(
        topic_id: int,
        _: None = Security(require_api_key),
    ) -> Dict[str, Any]:
        topic = service.get_theme_intel_topic_detail(topic_id=topic_id)
        if topic is None:
            raise HTTPException(status_code=404, detail="Theme topic not found")
        return {"request_id": str(uuid4()), "topic": topic}

    @app.post(
        "/v1/theme-intel/topics/{topic_id}/related-content/refresh",
        response_model=ThemeIntelRelatedRefreshResponseModel,
    )
    def refresh_theme_related_content_v1(
        topic_id: int,
        payload: ThemeIntelRelatedRefreshRequestModel,
        _: None = Security(require_api_key),
    ) -> Dict[str, Any]:
        try:
            result = service.refresh_theme_intel_related_content(
                topic_id=topic_id,
                top_k=payload.top_k,
                content_types=payload.content_types,
                related_counts_by_type=payload.related_counts_by_type,
                offline_mode=payload.offline_mode,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"request_id": str(uuid4()), "result": result}

    @app.post(
        "/v1/theme-intel/scheduler/tick",
        response_model=ThemeIntelSchedulerTickResponseModel,
    )
    def tick_theme_scheduler_v1(
        payload: Optional[ThemeIntelSchedulerTickRequestModel] = None,
        _: None = Security(require_api_key),
    ) -> Dict[str, Any]:
        result = service.tick_theme_intel_scheduler(offline_mode=bool(payload and payload.offline_mode))
        return {"request_id": str(uuid4()), "result": result}

    @app.post(
        "/v1/linkedin-draft-publisher/scheduler/tick",
        response_model=LinkedInDraftPublisherSchedulerTickResponseModel,
    )
    def tick_linkedin_draft_publisher_scheduler_v1(
        payload: Optional[LinkedInDraftPublisherSchedulerTickRequestModel] = None,
        _: None = Security(require_api_key),
    ) -> Dict[str, Any]:
        result = service.tick_linkedin_draft_publisher_scheduler(
            offline_mode=bool(payload and payload.offline_mode)
        )
        return {"request_id": str(uuid4()), "result": result}

    if runtime is not None:
        app.state.runtime = runtime
    return app


if os.getenv("API_KEY") and os.getenv("SUPABASE_DB_URL"):
    app = create_app()
else:  # pragma: no cover - convenience import path for local tests/tooling
    app = FastAPI(title="Runroom Content Query API", version="1.0.0")
