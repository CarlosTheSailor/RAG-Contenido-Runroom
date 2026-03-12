from __future__ import annotations

from src.config import Settings
from src.pipeline.ai_client import AIClient


class OpenAIEmbeddingClient:
    def __init__(
        self,
        settings: Settings,
        force_offline: bool = False,
        allow_fallback: bool = True,
    ):
        self._client = AIClient(
            settings=settings,
            force_offline=force_offline,
            allow_embedding_fallback=allow_fallback,
        )

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return self._client.embed_texts(texts)
