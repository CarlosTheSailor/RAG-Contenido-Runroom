from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path



def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    supabase_db_url: str
    openai_api_key: str | None
    openai_base_url: str
    youtube_api_key: str | None
    youtube_api_base_url: str
    openai_embedding_model: str
    openai_metadata_model: str
    embedding_dim: int
    runroom_sitemap_url: str
    auto_match_threshold: float
    auto_match_margin: float
    log_level: str

    @classmethod
    def from_env(cls) -> "Settings":
        db_url = os.getenv("SUPABASE_DB_URL", "").strip()
        if not db_url:
            raise ValueError("SUPABASE_DB_URL is required")

        return cls(
            supabase_db_url=db_url,
            openai_api_key=os.getenv("OPENAI_API_KEY") or None,
            openai_base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/"),
            youtube_api_key=os.getenv("YOUTUBE_API_KEY") or None,
            youtube_api_base_url=os.getenv("YOUTUBE_API_BASE_URL", "https://www.googleapis.com/youtube/v3").rstrip("/"),
            openai_embedding_model=os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-large"),
            openai_metadata_model=os.getenv("OPENAI_METADATA_MODEL", "gpt-4.1-mini"),
            embedding_dim=int(os.getenv("EMBEDDING_DIM", "1536")),
            runroom_sitemap_url=os.getenv("RUNROOM_SITEMAP_URL", "https://www.runroom.com/sitemap.xml"),
            auto_match_threshold=float(os.getenv("AUTO_MATCH_THRESHOLD", "0.86")),
            auto_match_margin=float(os.getenv("AUTO_MATCH_MARGIN", "0.06")),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        )


@dataclass(frozen=True)
class RuntimeOptions:
    transcripts_dir: Path = Path("transcripciones")
    target_tokens: int = 220
    overlap_tokens: int = 40
    batch_size: int = 32
    offline_mode: bool = False
