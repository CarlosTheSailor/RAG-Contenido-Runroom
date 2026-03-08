from __future__ import annotations

"""Phase 0 local preview pipeline for improved YouTube descriptions."""

import json
from pathlib import Path
from typing import Any

from src.config import Settings
from src.pipeline.ai_client import AIClient
from src.pipeline.storage import SupabaseStorage

from .diff_renderer import render_description_diff
from .entity_extraction import extract_entities
from .generation import generate_proposed_description
from .qa_validator import validate_description
from .retrieval import retrieve_related_content
from .transcript_loader import load_episode_context, resolve_episode
from .youtube_client import YouTubeClient


class PreviewSummary(dict):
    pass


def run_preview_youtube_description(
    settings: Settings,
    schema_path: Path,
    episode_identifier: str,
    youtube_url: str | None = None,
    current_description_file: Path | None = None,
    cli_env_debug: dict[str, object] | None = None,
    output_root: Path = Path("output"),
    offline_mode: bool = False,
) -> PreviewSummary:
    """Run the end-to-end local preview for one episode and persist output artifacts."""
    storage = SupabaseStorage(settings.supabase_db_url)
    ai = AIClient(settings=settings, force_offline=offline_mode)

    try:
        storage.ensure_schema(schema_path)

        episode_row = resolve_episode(storage, episode_identifier)
        youtube_video_id: str | None = None
        youtube_current_description: str | None = None
        youtube_api_success = False
        youtube_api_error: str | None = None
        youtube_client_error_type: str | None = None
        youtube_client_error_message: str | None = None
        youtube_client_debug: dict[str, object] = {}

        if youtube_url:
            yt = YouTubeClient(settings=settings)
            youtube_client_debug = yt.debug_info()
            print(
                "[debug] YouTubeClient key "
                f"present={str(bool(youtube_client_debug.get('env_key_present_at_youtube_client_init'))).lower()} "
                f"prefix={str(youtube_client_debug.get('env_key_prefix') or '')} "
                f"length={int(youtube_client_debug.get('env_key_length') or 0)}"
            )
            try:
                snippet = yt.fetch_video_snippet(youtube_url=youtube_url)
                youtube_video_id = snippet.video_id
                youtube_current_description = snippet.description
                youtube_api_success = True
            except Exception as exc:
                youtube_video_id = yt.extract_video_id(youtube_url)
                youtube_api_success = False
                youtube_api_error = str(exc)
                youtube_client_error_type = type(exc).__name__
                youtube_client_error_message = str(exc)

        context = load_episode_context(
            storage=storage,
            episode_row=episode_row,
            youtube_url=youtube_url,
            youtube_video_id=youtube_video_id,
            current_description_override=youtube_current_description,
            current_description_override_source="youtube_api" if youtube_current_description is not None else None,
            current_description_override_detail=f"youtube_api:video_id={youtube_video_id}"
            if youtube_current_description is not None and youtube_video_id
            else None,
            current_description_file=current_description_file,
        )

        entities = extract_entities(context)
        retrieval = retrieve_related_content(storage=storage, ai=ai, context=context, entities=entities)
        proposed = generate_proposed_description(ai=ai, context=context, entities=entities, retrieval=retrieval)
        qa_report = validate_description(context=context, entities=entities, proposed=proposed)
        cli_key_present = bool((cli_env_debug or {}).get("present"))
        cli_key_length = int((cli_env_debug or {}).get("length") or 0)
        client_key_present = bool(youtube_client_debug.get("env_key_present_at_youtube_client_init"))
        client_key_length = int(youtube_client_debug.get("env_key_length") or 0)
        qa_report.debug = {
            "current_description_length": len(context.current_description),
            "current_description_source": context.current_description_source,
            "youtube_api_success": youtube_api_success,
            "env_key_present_at_cli_entry": cli_key_present,
            "env_key_present_at_youtube_client_init": client_key_present,
            "env_key_length": max(cli_key_length, client_key_length),
            "youtube_client_error_type": youtube_client_error_type,
            "youtube_client_error_message": youtube_client_error_message or youtube_api_error,
            "current_description": {
                "source": context.current_description_source,
                "source_detail": context.current_description_source_detail,
                "length": len(context.current_description),
                "brand_block_detected": bool(context.brand_block),
                "current_description_length": len(context.current_description),
            },
            "episode_context": {
                "runroom_episode_identifier": context.runroom_identifier,
                "runroom_url": context.runroom_article_url,
                "youtube_url": context.youtube_url,
                "youtube_video_id": context.youtube_video_id,
                "current_description_source": context.current_description_source,
                "youtube_api_success": youtube_api_success,
                "youtube_api_error": youtube_api_error,
                "youtube_client_error_type": youtube_client_error_type,
                "youtube_client_error_message": youtube_client_error_message,
                "env_key_present_at_youtube_client_init": client_key_present,
                "env_key_length": client_key_length,
                "env_key_prefix": str(youtube_client_debug.get("env_key_prefix") or ""),
            },
            "chapters": {
                "source": proposed.chapters_source,
                "used_existing_timestamps": proposed.used_existing_timestamps,
                "count": len(proposed.chapters),
                "items": [
                    {
                        "timestamp": chapter.timestamp,
                        "start_sec": chapter.start_sec,
                        "label": chapter.label,
                    }
                    for chapter in proposed.chapters
                ],
            },
            "related_content_debug": {
                "episodes": [_related_to_dict(item) for item in proposed.related_episodes],
                "case_studies": [_related_to_dict(item) for item in proposed.related_case_studies],
            },
        }

        output_dir = output_root / context.slug
        output_dir.mkdir(parents=True, exist_ok=True)

        proposed_path = output_dir / "proposed_description.md"
        qa_path = output_dir / "qa_report.json"
        diff_path = output_dir / "diff.md"

        proposed_path.write_text(proposed.markdown, encoding="utf-8")
        qa_path.write_text(json.dumps(qa_report.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")

        diff_markdown = render_description_diff(
            current_description=context.current_description,
            proposed_description=proposed.markdown,
            current_source=context.current_description_source,
        )
        diff_path.write_text(diff_markdown, encoding="utf-8")

        return PreviewSummary(
            ok=True,
            episode_id=context.episode_id,
            episode_title=context.title,
            episode_slug=context.slug,
            used_existing_timestamps=proposed.used_existing_timestamps,
            related_episodes=[_related_to_dict(item) for item in proposed.related_episodes],
            related_case_studies=[_related_to_dict(item) for item in proposed.related_case_studies],
            qa_passed=qa_report.passed,
            current_description_source=context.current_description_source,
            youtube_url=context.youtube_url,
            youtube_video_id=context.youtube_video_id,
            youtube_api_success=youtube_api_success,
            chapters_source=proposed.chapters_source,
            output={
                "proposed_description": str(proposed_path),
                "qa_report": str(qa_path),
                "diff": str(diff_path),
            },
        )
    finally:
        storage.close()


def _related_to_dict(item: Any) -> dict[str, Any]:
    return {
        "content_item_id": item.content_item_id,
        "content_type": item.content_type,
        "title": item.title,
        "url": item.url,
        "score": item.score,
        "rationale": item.rationale,
        "selection_reason": item.selection_reason,
    }
