from __future__ import annotations

from typing import Any

from src.pipeline.models import Chunk, EpisodeInfo
from src.pipeline.storage import SupabaseStorage


class EpisodesRepository:
    def __init__(self, storage: SupabaseStorage):
        self._storage = storage

    def upsert_episode(self, episode: EpisodeInfo) -> int:
        return self._storage.upsert_episode(episode)

    def replace_chunks(self, episode_id: int, chunks: list[Chunk]) -> None:
        self._storage.replace_chunks(episode_id=episode_id, chunks=chunks)

    def list_episodes(self) -> list[dict[str, Any]]:
        return self._storage.list_episodes()

    def list_chunks_for_episode(self, episode_id: int) -> list[dict[str, Any]]:
        return self._storage.list_chunks_for_episode(episode_id=episode_id)

    def sync_episode_to_canonical(self, episode_id: int) -> int | None:
        return self._storage.sync_episode_to_canonical(episode_id=episode_id)
