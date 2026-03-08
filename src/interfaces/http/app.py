from __future__ import annotations

import os
import secrets
import urllib.error
from pathlib import Path
from typing import Any, Dict, Optional, Protocol
from urllib.parse import urlparse
from uuid import uuid4

from authlib.integrations.starlette_client import OAuth, OAuthError
from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, Security, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import APIKeyHeader
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from src.config import APIRuntimeSettings, Settings
from src.interfaces.http.schemas import (
    CaseStudyIngestUrlRequestModel,
    CaseStudyIngestUrlResponseModel,
    EpisodeIngestResponseModel,
    NewsletterLinkedInGenerateRequestModel,
    NewsletterLinkedInGenerateResponseModel,
    QuerySimilarRequestModel,
    QuerySimilarResponseModel,
    RecommendContentRequestModel,
    RecommendContentResponseModel,
)
from src.interfaces.http.services import QueryApiService
from src.pipeline.manual_episode_ingest import DuplicateEpisodeSourceFilenameError


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

    def ingest_case_study_url(self, url: str) -> dict[str, Any]:
        ...

    def ingest_episode_upload(
        self,
        transcript_filename: str,
        transcript_bytes: bytes,
        runroom_url: str,
    ) -> dict[str, Any]:
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
        "/app/api/case-studies/ingest-url",
        response_model=CaseStudyIngestUrlResponseModel,
    )
    def app_ingest_case_study_url(
        payload: CaseStudyIngestUrlRequestModel,
        _: dict[str, str] = Depends(require_session_api_user),
    ) -> Dict[str, Any]:
        return case_study_ingest_payload(payload)

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

    if runtime is not None:
        app.state.runtime = runtime
    return app


if os.getenv("API_KEY") and os.getenv("SUPABASE_DB_URL"):
    app = create_app()
else:  # pragma: no cover - convenience import path for local tests/tooling
    app = FastAPI(title="Runroom Content Query API", version="1.0.0")
