from __future__ import annotations

import re
from pathlib import Path

from .models import TranscriptSegment
from .normalization import normalize_text

TIMESTAMP_RE = re.compile(
    r"^\[(?P<h>\d{2}):(?P<m>\d{2}):(?P<s>\d{2})[\.,](?P<ms>\d{2,3})\]\s*(?:-\s*(?P<speaker>[^\n]+))?$"
)


def _to_seconds(h: str, m: str, s: str, ms: str) -> float:
    ms_norm = int(ms) / (100.0 if len(ms) == 2 else 1000.0)
    return int(h) * 3600 + int(m) * 60 + int(s) + ms_norm


def parse_transcript(path: Path) -> list[TranscriptSegment]:
    raw = path.read_text(encoding="utf-8", errors="ignore")
    lines = raw.replace("\r\n", "\n").replace("\r", "\n").split("\n")

    segments: list[TranscriptSegment] = []
    current_ts: str | None = None
    current_sec: float | None = None
    current_speaker: str | None = None
    buffer: list[str] = []

    def flush_current() -> None:
        nonlocal buffer, current_ts, current_sec, current_speaker
        if current_ts is None or current_sec is None:
            buffer = []
            return
        text = normalize_text("\n".join(buffer))
        if text:
            segments.append(
                TranscriptSegment(
                    raw_timestamp=current_ts,
                    start_ts_sec=current_sec,
                    speaker=current_speaker,
                    text=text,
                )
            )
        buffer = []

    for line in lines:
        stripped = line.strip()
        match = TIMESTAMP_RE.match(stripped)
        if match:
            flush_current()
            h = match.group("h")
            m = match.group("m")
            s = match.group("s")
            ms = match.group("ms")
            current_ts = f"{h}:{m}:{s}.{ms}"
            current_sec = _to_seconds(h, m, s, ms)
            speaker = (match.group("speaker") or "").strip()
            current_speaker = speaker if speaker else None
            continue

        if current_ts is not None:
            if stripped:
                buffer.append(stripped)

    flush_current()

    if segments:
        return segments

    fallback_text = normalize_text(raw)
    if not fallback_text:
        return []
    return [
        TranscriptSegment(
            raw_timestamp="00:00:00.000",
            start_ts_sec=0.0,
            speaker=None,
            text=fallback_text,
        )
    ]
