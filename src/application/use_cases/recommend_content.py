from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.application.reranking import aggregate_and_rerank
from src.domain.ports import CanonicalChunkQueryRepositoryPort, EmbeddingClientPort


@dataclass(frozen=True)
class RecommendContentRequest:
    text: str
    top_k: int = 8
    fetch_k: int = 60
    content_types: list[str] | None = None
    source: str | None = None
    language: str | None = None
    group_by_type: bool = False


@dataclass(frozen=True)
class RecommendContentResponse:
    query: str
    top_k: int
    total_candidates: int
    grouped: bool
    results: list[dict[str, Any]] | None = None
    results_by_type: dict[str, list[dict[str, Any]]] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "query": self.query,
            "top_k": self.top_k,
            "total_candidates": self.total_candidates,
            "grouped": self.grouped,
        }
        if self.grouped:
            payload["results_by_type"] = self.results_by_type or {}
            return payload
        payload["results"] = self.results or []
        return payload


class RecommendContentUseCase:
    def __init__(
        self,
        embedding_client: EmbeddingClientPort,
        repository: CanonicalChunkQueryRepositoryPort,
    ):
        self._embedding_client = embedding_client
        self._repository = repository

    def execute(self, request: RecommendContentRequest) -> RecommendContentResponse:
        query = request.text.strip()
        if not query:
            return RecommendContentResponse(
                query=request.text,
                top_k=request.top_k,
                total_candidates=0,
                grouped=request.group_by_type,
                results=[],
                results_by_type={},
            )

        vector = self._embedding_client.embed_texts([query])[0]
        rows = self._repository.query_similar_content_chunks(
            vector,
            top_k=request.fetch_k,
            content_types=request.content_types,
            source=request.source,
            language=request.language,
        )
        ranked = aggregate_and_rerank(rows, top_k=request.top_k, query_text=query)

        if request.group_by_type:
            grouped: dict[str, list[dict[str, Any]]] = {}
            for item in ranked:
                grouped.setdefault(item.content_type, []).append(item.to_dict())
            return RecommendContentResponse(
                query=query,
                top_k=request.top_k,
                total_candidates=len(rows),
                grouped=True,
                results_by_type=grouped,
            )

        return RecommendContentResponse(
            query=query,
            top_k=request.top_k,
            total_candidates=len(rows),
            grouped=False,
            results=[item.to_dict() for item in ranked],
        )
