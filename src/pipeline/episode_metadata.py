from __future__ import annotations

import re
from pathlib import Path

from .models import EpisodeInfo, TranscriptSegment
from .normalization import normalize_text

CODE_RE = re.compile(r"(?i)(?<![a-z0-9])([er]\d{3})(?![a-z0-9])")
HOST_NAMES = {
    "carlos iglesias",
    "carlos",
}



def _strip_media_suffixes(stem: str) -> str:
    changed = stem
    for suffix in (".txt", ".mp3", ".mp4", ".wav", ".m4a"):
        if changed.lower().endswith(suffix):
            changed = changed[: -len(suffix)]
    return changed


def _cleanup_title(raw: str, code: str | None) -> str:
    title = raw
    title = re.sub(r"(?i)^v\d+[\s_-]*realworld[\s_-]*", "", title)
    title = re.sub(r"(?i)^realworld[\s_-]*", "", title)
    title = re.sub(r"(?i)^rw[\s_-]*", "", title)
    if code:
        title = re.sub(rf"(?i)\b{re.escape(code)}\b[\s_-]*", "", title, count=1)
    title = re.sub(r"(?i)-?enmundoreal-ivoox\d+", "", title)
    title = re.sub(r"(?i)_?1080p(mp4)?", "", title)
    title = re.sub(r"\s+-\s+\d{1,2}_\d{1,2}_\d{2,4}.*$", "", title)
    title = title.replace("_", " ")
    title = re.sub(r"\s+", " ", title).strip(" -_")
    return normalize_text(title)


def _extract_guests(title: str, segments: list[TranscriptSegment]) -> list[str]:
    guests: list[str] = []

    con_match = re.search(r"(?i)\bcon\s+([^.,;]+(?:\s+[^.,;]+)*)", title)
    if con_match:
        raw_guest = con_match.group(1).strip()
        if raw_guest:
            guests.append(raw_guest)

    # Some file names are "Name Surname" with no "con"; use first non-host speakers.
    speaker_candidates: list[str] = []
    for seg in segments[:30]:
        if not seg.speaker:
            continue
        speaker = normalize_text(seg.speaker)
        speaker_norm = speaker.lower()
        if speaker_norm in HOST_NAMES:
            continue
        if speaker not in speaker_candidates:
            speaker_candidates.append(speaker)

    if not guests and speaker_candidates:
        guests.extend(speaker_candidates[:3])

    # Deduplicate preserving order.
    seen: set[str] = set()
    deduped: list[str] = []
    for guest in guests:
        key = guest.lower()
        if key not in seen:
            deduped.append(guest)
            seen.add(key)
    return deduped


def infer_episode_info(path: Path, segments: list[TranscriptSegment]) -> EpisodeInfo:
    source_filename = path.name
    stem = _strip_media_suffixes(source_filename)
    code_match = CODE_RE.search(stem)
    episode_code = code_match.group(1).lower() if code_match else None

    cleaned_title = _cleanup_title(stem, episode_code)
    if not cleaned_title:
        cleaned_title = stem

    guests = _extract_guests(cleaned_title, segments)

    return EpisodeInfo(
        source_filename=source_filename,
        transcript_path=str(path),
        episode_code=episode_code,
        title=cleaned_title,
        guest_names=guests,
        language="es",
    )
