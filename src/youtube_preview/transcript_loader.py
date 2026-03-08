from __future__ import annotations

"""Episode selection and transcript reconstruction for YouTube preview."""

import re
from pathlib import Path
from typing import Any

from src.pipeline.normalization import normalize_text, slugify
from src.pipeline.parser import parse_transcript
from src.pipeline.storage import SupabaseStorage

from .models import EpisodeContext, TranscriptChunk
from .utils import extract_youtube_video_id, normalize_url, slug_from_url


def resolve_episode(storage: SupabaseStorage, identifier: str) -> dict[str, Any]:
    needle = identifier.strip()
    if not needle:
        raise ValueError("episode identifier cannot be empty")

    rows = storage.list_episodes()
    if not rows:
        raise ValueError("no episodes found in database")

    if needle.isdigit():
        episode_id = int(needle)
        exact = storage.get_episode_by_id(episode_id)
        if exact:
            return exact

    lowered = needle.lower()
    normalized_input_url = normalize_url(needle) if needle.startswith(("http://", "https://")) else None

    for row in rows:
        if _matches_identifier(row, lowered, normalized_input_url):
            return row

    raise ValueError(f"episode not found for identifier: {identifier}")


def load_episode_context(
    storage: SupabaseStorage,
    episode_row: dict[str, Any],
    youtube_url: str | None = None,
    youtube_video_id: str | None = None,
    current_description_override: str | None = None,
    current_description_override_source: str | None = None,
    current_description_override_detail: str | None = None,
    current_description_file: Path | None = None,
) -> EpisodeContext:
    episode_id = int(episode_row["id"])
    chunks_rows = storage.list_chunks_for_episode(episode_id)
    chunks = _build_chunks(chunks_rows, transcript_path=str(episode_row.get("transcript_path") or ""))
    transcript = "\n\n".join(chunk.text for chunk in chunks if chunk.text.strip())

    content_item = storage.get_content_item_by_legacy_episode_id(episode_id)
    (
        current_description,
        description_source,
        description_source_detail,
    ) = _resolve_current_description(
        storage=storage,
        episode_row=episode_row,
        content_item=content_item,
        current_description_override=current_description_override,
        current_description_override_source=current_description_override_source,
        current_description_override_detail=current_description_override_detail,
        current_description_file=current_description_file,
    )
    brand_block = _extract_brand_block(current_description)
    youtube_url_clean = youtube_url.strip() if isinstance(youtube_url, str) and youtube_url.strip() else None
    parsed_video_id = youtube_video_id or extract_youtube_video_id(youtube_url_clean)

    episode_code = str(episode_row.get("episode_code") or "").strip() or None
    slug = episode_code or slugify(str(episode_row.get("title") or "episode")) or f"episode-{episode_id}"
    runroom_identifier = episode_code or str(episode_id)

    guest_names = _as_guest_list(episode_row.get("guest_names"))
    if not guest_names and content_item:
        metadata = _as_dict(content_item.get("metadata_json"))
        guest_names = _as_guest_list(metadata.get("guest_names"))

    return EpisodeContext(
        episode_id=episode_id,
        runroom_identifier=runroom_identifier,
        content_item_id=int(content_item["id"]) if content_item else None,
        source_filename=str(episode_row.get("source_filename") or ""),
        episode_code=episode_code,
        title=str(episode_row.get("title") or f"Episode {episode_id}"),
        slug=slug,
        runroom_article_url=str(episode_row.get("runroom_article_url") or "").strip() or None,
        youtube_url=youtube_url_clean,
        youtube_video_id=parsed_video_id,
        guest_names=guest_names,
        language=str(episode_row.get("language") or "es"),
        transcript_path=str(episode_row.get("transcript_path") or ""),
        transcript=transcript,
        chunks=chunks,
        current_description=current_description,
        current_description_source=description_source,
        current_description_source_detail=description_source_detail,
        brand_block=brand_block,
    )


def _matches_identifier(row: dict[str, Any], lowered_identifier: str, normalized_input_url: str | None) -> bool:
    if str(row.get("episode_code") or "").strip().lower() == lowered_identifier:
        return True

    title_slug = slugify(str(row.get("title") or "").strip())
    if title_slug and title_slug == lowered_identifier:
        return True

    runroom_url = str(row.get("runroom_article_url") or "").strip()
    if runroom_url:
        if normalize_url(runroom_url) == normalized_input_url:
            return True
        slug = slug_from_url(runroom_url)
        if slug and slug.lower() == lowered_identifier:
            return True

    source = str(row.get("source_filename") or "").strip().lower()
    if source and (source == lowered_identifier or Path(source).stem == lowered_identifier):
        return True

    return False


def _build_chunks(rows: list[dict[str, Any]], transcript_path: str) -> list[TranscriptChunk]:
    if rows:
        out = [
            TranscriptChunk(
                start_ts_sec=float(row.get("start_ts_sec") or 0.0),
                end_ts_sec=float(row.get("end_ts_sec") or 0.0),
                speaker=row.get("speaker"),
                text=str(row.get("text") or ""),
                metadata=_as_dict(row.get("metadata_json")),
            )
            for row in rows
        ]
        return [chunk for chunk in out if chunk.text.strip()]

    if not transcript_path:
        return []

    path = Path(transcript_path)
    if not path.exists():
        return []

    segments = parse_transcript(path)
    if not segments:
        return []

    out: list[TranscriptChunk] = []
    for idx, seg in enumerate(segments):
        next_start = segments[idx + 1].start_ts_sec if idx + 1 < len(segments) else seg.start_ts_sec + 90.0
        out.append(
            TranscriptChunk(
                start_ts_sec=float(seg.start_ts_sec),
                end_ts_sec=float(max(seg.start_ts_sec + 1.0, next_start)),
                speaker=seg.speaker,
                text=seg.text,
                metadata={},
            )
        )
    return out


def _extract_current_description(
    storage: SupabaseStorage,
    episode: dict[str, Any],
    content_item: dict[str, Any] | None,
) -> tuple[str, str]:
    if content_item is not None:
        metadata = _as_dict(content_item.get("metadata_json"))
        custom = _as_dict(content_item.get("custom_metadata_json"))

        for bag in (custom, metadata):
            direct = _pick_string(
                bag,
                keys=(
                    "youtube_description",
                    "youtube_description_current",
                    "current_youtube_description",
                    "description_youtube",
                ),
            )
            if direct:
                return normalize_text(direct), "content_item.metadata"

            youtube = _as_dict(bag.get("youtube"))
            nested = _pick_string(youtube, keys=("description", "current_description"))
            if nested:
                return normalize_text(nested), "content_item.metadata.youtube"

        raw_text = str(content_item.get("raw_text") or "").strip()
        in_raw = _extract_youtube_description_from_raw_text(raw_text)
        if in_raw:
            return normalize_text(in_raw), "content_item.raw_text"

    episode_description = _pick_string(_as_dict(episode), keys=("youtube_description", "description"))
    if episode_description:
        return normalize_text(episode_description), "episodes"

    article_url = str(episode.get("runroom_article_url") or "").strip()
    if article_url:
        article = storage.get_runroom_article_by_url(article_url)
        if article:
            article_description = str(article.get("description") or "").strip()
            if article_description:
                return normalize_text(article_description), "runroom_articles.description"

    return "", "missing"


def _resolve_current_description(
    storage: SupabaseStorage,
    episode_row: dict[str, Any],
    content_item: dict[str, Any] | None,
    current_description_override: str | None,
    current_description_override_source: str | None,
    current_description_override_detail: str | None,
    current_description_file: Path | None,
) -> tuple[str, str, str]:
    if isinstance(current_description_override, str):
        normalized = normalize_text(current_description_override)
        source = current_description_override_source or "override"
        detail = current_description_override_detail or source
        if normalized:
            return normalized, source, detail

    if current_description_file is not None:
        if not current_description_file.exists():
            raise FileNotFoundError(f"Current description file not found: {current_description_file}")
        file_text = current_description_file.read_text(encoding="utf-8", errors="ignore")
        normalized = normalize_text(file_text)
        if normalized:
            return normalized, "file", f"file:{current_description_file}"
        return "", "file", f"file_empty:{current_description_file}"

    from_db, db_detail = _extract_current_description(storage, episode_row, content_item)
    if from_db:
        return from_db, "db", db_detail
    return "", "missing", "missing"


def _extract_youtube_description_from_raw_text(raw_text: str) -> str | None:
    if not raw_text:
        return None

    # Fallback parser for dumps that include labeled blocks.
    patterns = (
        r"(?is)youtube\s+description\s*[:\-]\s*(.+?)\n(?:\w[\w\s]{0,40}:\s|\Z)",
        r"(?is)descripci[oó]n\s+youtube\s*[:\-]\s*(.+?)\n(?:\w[\w\s]{0,40}:\s|\Z)",
    )
    for pattern in patterns:
        match = re.search(pattern, raw_text)
        if match:
            value = match.group(1).strip()
            if value:
                return value
    return None


def _extract_brand_block(description: str) -> str | None:
    if not description.strip():
        return None

    marker_re = re.compile(r"(?im)^(.*(?:runroom|realworld|s[ií]guenos|newsletter|suscr).*)$")
    marker_lines = list(marker_re.finditer(description))
    if marker_lines:
        start = marker_lines[-1].start()
        tail = description[start:].strip()
        if ("http://" in tail.lower()) or ("https://" in tail.lower()):
            return tail

    blocks = [part.strip() for part in re.split(r"\n\s*\n", description) if part.strip()]
    if not blocks:
        return None

    markers = ("runroom", "realworld", "síguenos", "siguenos", "newsletter", "suscr")
    for block in reversed(blocks):
        lowered = block.lower()
        if any(marker in lowered for marker in markers) and ("http://" in lowered or "https://" in lowered):
            return block

    return None


def _pick_string(mapping: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _as_guest_list(raw: Any) -> list[str]:
    if isinstance(raw, list):
        out = [str(item).strip() for item in raw if str(item).strip()]
        return _dedupe(out)
    if isinstance(raw, str) and raw.strip():
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        return _dedupe(parts)
    return []


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}
