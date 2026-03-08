from __future__ import annotations

import os
import secrets
from typing import Any, Dict, Optional, Protocol
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Security
from fastapi.security import APIKeyHeader

from src.config import APIRuntimeSettings, Settings
from src.interfaces.http.schemas import (
    QuerySimilarRequestModel,
    QuerySimilarResponseModel,
    RecommendContentRequestModel,
    RecommendContentResponseModel,
)
from src.interfaces.http.services import QueryApiService


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


def create_app(
    service: Optional[QueryServicePort] = None,
    api_key: Optional[str] = None,
) -> FastAPI:
    runtime: Optional[APIRuntimeSettings] = None
    if service is None:
        settings = Settings.from_env()
        runtime = APIRuntimeSettings.from_env()
        service = QueryApiService(settings=settings, schema_path=runtime.schema_path)
        api_key = runtime.api_key

    if not api_key:
        raise ValueError("API key is required to initialize HTTP API")

    app = FastAPI(title="Runroom Content Query API", version="1.0.0")
    api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

    def require_api_key(client_key: Optional[str] = Security(api_key_header)) -> None:
        if not client_key or not secrets.compare_digest(client_key, api_key):
            raise HTTPException(status_code=401, detail="Invalid API key")

    @app.get("/health")
    def health(_: None = Security(require_api_key)) -> Dict[str, str]:
        return {"status": "ok", "request_id": str(uuid4())}

    @app.post("/v1/query-similar", response_model=QuerySimilarResponseModel)
    def query_similar(payload: QuerySimilarRequestModel, _: None = Security(require_api_key)) -> Dict[str, Any]:
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

    @app.post("/v1/recommend-content", response_model=RecommendContentResponseModel)
    def recommend_content(payload: RecommendContentRequestModel, _: None = Security(require_api_key)) -> Dict[str, Any]:
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

    @app.get("/")
    def root(_: None = Security(require_api_key)) -> Dict[str, str]:
        return {"message": "Runroom Content Query API", "request_id": str(uuid4())}

    if runtime is not None:
        app.state.runtime = runtime
    return app


if os.getenv("API_KEY") and os.getenv("SUPABASE_DB_URL"):
    app = create_app()
else:  # pragma: no cover - convenience import path for local tests/tooling
    app = FastAPI(title="Runroom Content Query API", version="1.0.0")
