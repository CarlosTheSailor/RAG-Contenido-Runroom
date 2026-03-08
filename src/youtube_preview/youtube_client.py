from __future__ import annotations

"""Read-only YouTube Data API client for preview mode."""

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

from src.config import Settings

from .utils import extract_youtube_video_id


@dataclass
class YouTubeSnippet:
    video_id: str
    title: str
    description: str
    channel_title: str

    def to_dict(self) -> dict[str, str]:
        return {
            "title": self.title,
            "description": self.description,
            "channelTitle": self.channel_title,
        }


class YouTubeClient:
    def __init__(self, settings: Settings):
        runtime_env_key = (os.getenv("YOUTUBE_API_KEY") or "").strip()
        settings_key = (settings.youtube_api_key or "").strip()
        # Source of truth: runtime environment.
        self._api_key = runtime_env_key or settings_key or None
        self._base_url = settings.youtube_api_base_url.rstrip("/")
        self._debug = {
            "env_key_present_at_youtube_client_init": bool(runtime_env_key),
            "env_key_length": len(runtime_env_key or settings_key),
            "env_key_prefix": (runtime_env_key or settings_key)[:6] if (runtime_env_key or settings_key) else "",
        }

    def extract_video_id(self, youtube_url: str) -> str | None:
        return extract_youtube_video_id(youtube_url)

    def fetch_video_snippet(self, youtube_url: str) -> YouTubeSnippet:
        video_id = self.extract_video_id(youtube_url)
        if not video_id:
            raise ValueError(f"Could not parse YouTube video_id from URL: {youtube_url}")
        return self.fetch_video_snippet_by_id(video_id)

    def fetch_video_snippet_by_id(self, video_id: str) -> YouTubeSnippet:
        if not self._api_key:
            raise ValueError("YOUTUBE_API_KEY is required for YouTube API reads")

        query = urllib.parse.urlencode(
            {
                "part": "snippet",
                "id": video_id,
                "key": self._api_key,
            }
        )
        url = f"{self._base_url}/videos?{query}"
        req = urllib.request.Request(url, method="GET")

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"YouTube API HTTP {exc.code}: {detail}") from exc

        payload = json.loads(body)
        items = payload.get("items") or []
        if not items:
            raise ValueError(f"No YouTube video found for id={video_id}")

        snippet = items[0].get("snippet") or {}
        return YouTubeSnippet(
            video_id=video_id,
            title=str(snippet.get("title") or "").strip(),
            description=str(snippet.get("description") or "").strip(),
            channel_title=str(snippet.get("channelTitle") or "").strip(),
        )

    def debug_info(self) -> dict[str, object]:
        return dict(self._debug)
