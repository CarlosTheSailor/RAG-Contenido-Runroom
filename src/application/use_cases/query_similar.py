from __future__ import annotations

from dataclasses import dataclass

from src.domain.ports import EmbeddingClientPort, LegacyChunkQueryRepositoryPort


@dataclass(frozen=True)
class QuerySimilarRequest:
    text: str
    top_k: int = 8


@dataclass(frozen=True)
class QuerySimilarResultItem:
    similarity: float
    episode_code: str | None
    episode_title: str
    runroom_article_url: str | None
    start_ts_sec: float
    text: str

    @property
    def start_ts_hhmmss(self) -> str:
        total = int(self.start_ts_sec)
        hh = total // 3600
        mm = (total % 3600) // 60
        ss = total % 60
        return f"{hh:02d}:{mm:02d}:{ss:02d}"

    def to_dict(self) -> dict[str, str | float | None]:
        return {
            "similarity": self.similarity,
            "episode_code": self.episode_code,
            "episode_title": self.episode_title,
            "runroom_article_url": self.runroom_article_url,
            "start_ts_sec": self.start_ts_sec,
            "start_ts_hhmmss": self.start_ts_hhmmss,
            "text": self.text,
        }


@dataclass(frozen=True)
class QuerySimilarResponse:
    query: str
    top_k: int
    results: list[QuerySimilarResultItem]

    def to_dict(self) -> dict[str, object]:
        return {
            "query": self.query,
            "top_k": self.top_k,
            "results": [item.to_dict() for item in self.results],
        }


class QuerySimilarUseCase:
    def __init__(
        self,
        embedding_client: EmbeddingClientPort,
        repository: LegacyChunkQueryRepositoryPort,
    ):
        self._embedding_client = embedding_client
        self._repository = repository

    def execute(self, request: QuerySimilarRequest) -> QuerySimilarResponse:
        query = request.text.strip()
        if not query:
            return QuerySimilarResponse(query=request.text, top_k=request.top_k, results=[])

        vector = self._embedding_client.embed_texts([query])[0]
        rows = self._repository.query_similar_chunks(vector, top_k=request.top_k)

        results: list[QuerySimilarResultItem] = []
        for row in rows:
            results.append(
                QuerySimilarResultItem(
                    similarity=float(row.get("similarity") or 0.0),
                    episode_code=row.get("episode_code"),
                    episode_title=str(row.get("episode_title") or "-"),
                    runroom_article_url=row.get("runroom_article_url"),
                    start_ts_sec=float(row.get("start_ts_sec") or 0.0),
                    text=str(row.get("text") or ""),
                )
            )

        return QuerySimilarResponse(query=query, top_k=request.top_k, results=results)
