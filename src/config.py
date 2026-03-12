from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path



def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _as_float_in_range(value: str | None, *, default: float, min_value: float, max_value: float, name: str) -> float:
    if value is None or not value.strip():
        parsed = default
    else:
        parsed = float(value.strip())
    if parsed < min_value or parsed > max_value:
        raise ValueError(f"{name} must be between {min_value} and {max_value}")
    return parsed


@dataclass(frozen=True)
class Settings:
    supabase_db_url: str
    openai_api_key: str | None
    openai_base_url: str
    youtube_api_key: str | None
    youtube_api_base_url: str
    openai_embedding_model: str
    openai_metadata_model: str
    openai_newsletter_model: str | None
    newsletter_rag_min_score: float
    embedding_dim: int
    runroom_sitemap_url: str
    auto_match_threshold: float
    auto_match_margin: float
    log_level: str
    openai_theme_intel_model: str | None = None
    theme_intel_dedupe_threshold: float = 0.9
    theme_intel_dedupe_window_days: int = 30
    gmail_oauth_client_id: str | None = None
    gmail_oauth_client_secret: str | None = None
    gmail_oauth_refresh_token: str | None = None
    gmail_source_account: str = "newsletters@runroom.com"
    linkedin_draft_publisher_openai_model: str | None = None
    linkedin_draft_publisher_topic_selection_model: str = "gpt-5-mini"
    linkedin_draft_publisher_stage1_model: str = "gpt-5.2-chat-latest"
    linkedin_draft_publisher_stage2_model: str = "gpt-5.2-chat-latest"
    linkedin_draft_publisher_enforce_related_integration: bool = True
    linkedin_draft_publisher_openai_timeout_seconds: int = 45
    linkedin_draft_publisher_stale_run_minutes: int = 5
    linkedin_draft_publisher_client_name: str = "linkedin_draft_publisher"
    linkedin_draft_publisher_drafts_api_url: str = ""
    linkedin_draft_publisher_drafts_api_secret: str = ""
    linkedin_draft_publisher_drafts_editor_base_url: str = "https://linkedin-drafts.runroom.dev"
    linkedin_draft_publisher_slack_bot_token: str = ""
    linkedin_draft_publisher_slack_post_url: str = "https://slack.com/api/chat.postMessage"
    linkedin_draft_publisher_topics_target_count: int = 5
    linkedin_draft_publisher_topics_fetch_limit: int = 40
    linkedin_draft_publisher_related_top_k: int = 10
    linkedin_draft_publisher_related_counts_by_type: str = ""
    linkedin_draft_publisher_max_concurrency: int = 2
    linkedin_draft_publisher_stage_timeout_seconds: int = 90
    linkedin_draft_publisher_context_evidence_limit: int = 8
    linkedin_draft_publisher_context_doc_limit: int = 5
    linkedin_draft_publisher_related_fetch_multiplier: int = 8
    linkedin_draft_publisher_http_retry_max: int = 2
    linkedin_draft_publisher_min_chars: int = 1600
    linkedin_draft_publisher_max_chars: int = 3200

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
            openai_newsletter_model=os.getenv("OPENAI_NEWSLETTER_MODEL") or None,
            newsletter_rag_min_score=_as_float_in_range(
                os.getenv("NEWSLETTER_RAG_MIN_SCORE"),
                default=0.74,
                min_value=0.0,
                max_value=1.0,
                name="NEWSLETTER_RAG_MIN_SCORE",
            ),
            embedding_dim=int(os.getenv("EMBEDDING_DIM", "1536")),
            runroom_sitemap_url=os.getenv("RUNROOM_SITEMAP_URL", "https://www.runroom.com/sitemap.xml"),
            auto_match_threshold=float(os.getenv("AUTO_MATCH_THRESHOLD", "0.86")),
            auto_match_margin=float(os.getenv("AUTO_MATCH_MARGIN", "0.06")),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            openai_theme_intel_model=os.getenv("OPENAI_THEME_INTEL_MODEL") or None,
            theme_intel_dedupe_threshold=_as_float_in_range(
                os.getenv("THEME_INTEL_DEDUPE_THRESHOLD"),
                default=0.9,
                min_value=0.0,
                max_value=1.0,
                name="THEME_INTEL_DEDUPE_THRESHOLD",
            ),
            theme_intel_dedupe_window_days=int(os.getenv("THEME_INTEL_DEDUPE_WINDOW_DAYS", "30")),
            gmail_oauth_client_id=os.getenv("GMAIL_OAUTH_CLIENT_ID") or None,
            gmail_oauth_client_secret=os.getenv("GMAIL_OAUTH_CLIENT_SECRET") or None,
            gmail_oauth_refresh_token=os.getenv("GMAIL_OAUTH_REFRESH_TOKEN") or None,
            gmail_source_account=os.getenv("GMAIL_SOURCE_ACCOUNT", "newsletters@runroom.com"),
            linkedin_draft_publisher_openai_model=os.getenv("LINKEDIN_DRAFT_PUBLISHER_OPENAI_MODEL") or None,
            linkedin_draft_publisher_topic_selection_model=(
                os.getenv("LINKEDIN_DRAFT_PUBLISHER_TOPIC_SELECTION_MODEL", "gpt-5-mini").strip() or "gpt-5-mini"
            ),
            linkedin_draft_publisher_stage1_model=(
                os.getenv("LINKEDIN_DRAFT_PUBLISHER_STAGE1_MODEL", "gpt-5.2-chat-latest").strip()
                or "gpt-5.2-chat-latest"
            ),
            linkedin_draft_publisher_stage2_model=(
                os.getenv("LINKEDIN_DRAFT_PUBLISHER_STAGE2_MODEL", "gpt-5.2-chat-latest").strip()
                or "gpt-5.2-chat-latest"
            ),
            linkedin_draft_publisher_enforce_related_integration=_as_bool(
                os.getenv("LINKEDIN_DRAFT_PUBLISHER_ENFORCE_RELATED_INTEGRATION"),
                default=True,
            ),
            linkedin_draft_publisher_openai_timeout_seconds=int(
                os.getenv("LINKEDIN_DRAFT_PUBLISHER_OPENAI_TIMEOUT_SECONDS", "45")
            ),
            linkedin_draft_publisher_stale_run_minutes=max(
                1,
                int(os.getenv("LINKEDIN_DRAFT_PUBLISHER_STALE_RUN_MINUTES", "5")),
            ),
            linkedin_draft_publisher_client_name=os.getenv(
                "LINKEDIN_DRAFT_PUBLISHER_CLIENT_NAME",
                "linkedin_draft_publisher",
            ),
            linkedin_draft_publisher_drafts_api_url=os.getenv(
                "LINKEDIN_DRAFT_PUBLISHER_DRAFTS_API_URL",
                "",
            ),
            linkedin_draft_publisher_drafts_api_secret=os.getenv(
                "LINKEDIN_DRAFT_PUBLISHER_DRAFTS_API_SECRET",
                "",
            ),
            linkedin_draft_publisher_drafts_editor_base_url=os.getenv(
                "LINKEDIN_DRAFT_PUBLISHER_DRAFTS_EDITOR_BASE_URL",
                "https://linkedin-drafts.runroom.dev",
            ),
            linkedin_draft_publisher_slack_bot_token=os.getenv(
                "LINKEDIN_DRAFT_PUBLISHER_SLACK_BOT_TOKEN",
                "",
            ),
            linkedin_draft_publisher_slack_post_url=os.getenv(
                "LINKEDIN_DRAFT_PUBLISHER_SLACK_POST_URL",
                "https://slack.com/api/chat.postMessage",
            ),
            linkedin_draft_publisher_topics_target_count=int(
                os.getenv("LINKEDIN_DRAFT_PUBLISHER_TOPICS_TARGET_COUNT", "5")
            ),
            linkedin_draft_publisher_topics_fetch_limit=int(
                os.getenv("LINKEDIN_DRAFT_PUBLISHER_TOPICS_FETCH_LIMIT", "40")
            ),
            linkedin_draft_publisher_related_top_k=int(
                os.getenv("LINKEDIN_DRAFT_PUBLISHER_RELATED_TOP_K", "10")
            ),
            linkedin_draft_publisher_related_counts_by_type=os.getenv(
                "LINKEDIN_DRAFT_PUBLISHER_RELATED_COUNTS_BY_TYPE",
                "",
            ),
            linkedin_draft_publisher_max_concurrency=max(
                1,
                int(os.getenv("LINKEDIN_DRAFT_PUBLISHER_MAX_CONCURRENCY", "2")),
            ),
            linkedin_draft_publisher_stage_timeout_seconds=max(
                15,
                int(os.getenv("LINKEDIN_DRAFT_PUBLISHER_STAGE_TIMEOUT_SECONDS", "90")),
            ),
            linkedin_draft_publisher_context_evidence_limit=max(
                1,
                int(os.getenv("LINKEDIN_DRAFT_PUBLISHER_CONTEXT_EVIDENCE_LIMIT", "8")),
            ),
            linkedin_draft_publisher_context_doc_limit=max(
                1,
                int(os.getenv("LINKEDIN_DRAFT_PUBLISHER_CONTEXT_DOC_LIMIT", "5")),
            ),
            linkedin_draft_publisher_related_fetch_multiplier=max(
                2,
                int(os.getenv("LINKEDIN_DRAFT_PUBLISHER_RELATED_FETCH_MULTIPLIER", "8")),
            ),
            linkedin_draft_publisher_http_retry_max=max(
                0,
                int(os.getenv("LINKEDIN_DRAFT_PUBLISHER_HTTP_RETRY_MAX", "2")),
            ),
            linkedin_draft_publisher_min_chars=max(
                800,
                int(os.getenv("LINKEDIN_DRAFT_PUBLISHER_MIN_CHARS", "1600")),
            ),
            linkedin_draft_publisher_max_chars=max(
                1200,
                int(os.getenv("LINKEDIN_DRAFT_PUBLISHER_MAX_CHARS", "3200")),
            ),
        )


@dataclass(frozen=True)
class RuntimeOptions:
    transcripts_dir: Path = Path("transcripciones")
    target_tokens: int = 220
    overlap_tokens: int = 40
    batch_size: int = 32
    offline_mode: bool = False


@dataclass(frozen=True)
class APIRuntimeSettings:
    api_key: str
    host: str = "0.0.0.0"
    port: int = 8000
    schema_path: Path = Path("sql")
    google_oauth_client_id: str | None = None
    google_oauth_client_secret: str | None = None
    google_oauth_redirect_uri: str | None = None
    google_oauth_allowed_domain: str = "runroom.com"
    session_secret: str = "change-me"
    session_max_age_seconds: int = 86400
    session_cookie_secure: bool = False

    @classmethod
    def from_env(cls) -> "APIRuntimeSettings":
        api_key = os.getenv("API_KEY", "").strip()
        if not api_key:
            raise ValueError("API_KEY is required")

        host = os.getenv("HOST", "0.0.0.0").strip() or "0.0.0.0"
        port = int(os.getenv("PORT", "8000"))
        schema_path = Path(os.getenv("SCHEMA_PATH", "sql"))
        google_oauth_client_id = os.getenv("GOOGLE_OAUTH_CLIENT_ID", "").strip() or None
        google_oauth_client_secret = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET", "").strip() or None
        google_oauth_redirect_uri = os.getenv("GOOGLE_OAUTH_REDIRECT_URI", "").strip() or None
        google_oauth_allowed_domain = os.getenv("GOOGLE_OAUTH_ALLOWED_DOMAIN", "runroom.com").strip() or "runroom.com"
        session_secret = os.getenv("SESSION_SECRET", "").strip() or api_key
        session_max_age_seconds = int(os.getenv("SESSION_MAX_AGE_SECONDS", "86400"))
        session_cookie_secure = _as_bool(os.getenv("SESSION_COOKIE_SECURE"), default=False)
        return cls(
            api_key=api_key,
            host=host,
            port=port,
            schema_path=schema_path,
            google_oauth_client_id=google_oauth_client_id,
            google_oauth_client_secret=google_oauth_client_secret,
            google_oauth_redirect_uri=google_oauth_redirect_uri,
            google_oauth_allowed_domain=google_oauth_allowed_domain.lower(),
            session_secret=session_secret,
            session_max_age_seconds=session_max_age_seconds,
            session_cookie_secure=session_cookie_secure,
        )
