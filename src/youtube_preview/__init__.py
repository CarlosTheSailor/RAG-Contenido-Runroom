"""Phase 0 preview pipeline for improving Realworld YouTube descriptions."""

from typing import Any

__all__ = ["run_preview_youtube_description"]


def run_preview_youtube_description(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from .pipeline import run_preview_youtube_description as _run

    return _run(*args, **kwargs)
