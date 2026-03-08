from __future__ import annotations

from src.config import Settings
from src.pipeline.ai_client import AIClient


class OpenAIEmbeddingClient:
    def __init__(self, settings: Settings, force_offline: bool = False):
        self._client = AIClient(settings=settings, force_offline=force_offline)

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return self._client.embed_texts(texts)
