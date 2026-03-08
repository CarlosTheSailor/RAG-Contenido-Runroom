from __future__ import annotations

import csv
import html
import logging
import re
import urllib.request
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

H1_RE = re.compile(r"<h1\b[^>]*>(?P<content>.*?)</h1>", re.IGNORECASE | re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")


def sync_episode_titles_from_h1(
    storage: Any,
    only_statuses: list[str],
    limit: int | None = None,
    dry_run: bool = False,
    report_csv: Path | None = None,
) -> dict[str, Any]:
    episodes = storage.list_episodes_for_title_sync(statuses=only_statuses, limit=limit)

    processed = 0
    updated = 0
    unchanged = 0
    errors = 0
    article_rows_updated = 0
    report_rows: list[dict[str, Any]] = []

    for episode in episodes:
        processed += 1
        episode_id = int(episode["id"])
        status = str(episode.get("match_status") or "")
        url = str(episode.get("runroom_article_url") or "").strip()
        old_title = str(episode.get("title") or "").strip()

        if not url:
            errors += 1
            report_rows.append(
                {
                    "episode_id": episode_id,
                    "match_status": status,
                    "url": url,
                    "old_title": old_title,
                    "new_title": "",
                    "result": "error",
                    "error": "missing_url",
                }
            )
            continue

        try:
            html_text = fetch_url_text(url)
            new_title = extract_first_h1(html_text)
            if not new_title:
                raise ValueError("missing_h1")
        except Exception as exc:
            errors += 1
            report_rows.append(
                {
                    "episode_id": episode_id,
                    "match_status": status,
                    "url": url,
                    "old_title": old_title,
                    "new_title": "",
                    "result": "error",
                    "error": str(exc),
                }
            )
            continue

        if new_title == old_title:
            unchanged += 1
            report_rows.append(
                {
                    "episode_id": episode_id,
                    "match_status": status,
                    "url": url,
                    "old_title": old_title,
                    "new_title": new_title,
                    "result": "unchanged",
                    "error": "",
                }
            )
            continue

        if dry_run:
            updated += 1
            report_rows.append(
                {
                    "episode_id": episode_id,
                    "match_status": status,
                    "url": url,
                    "old_title": old_title,
                    "new_title": new_title,
                    "result": "would_update",
                    "error": "",
                }
            )
            continue

        rows = storage.update_episode_and_article_title(
            episode_id=episode_id,
            runroom_article_url=url,
            new_title=new_title,
        )
        updated += 1
        article_rows_updated += int(rows.get("runroom_articles_updated", 0))
        report_rows.append(
            {
                "episode_id": episode_id,
                "match_status": status,
                "url": url,
                "old_title": old_title,
                "new_title": new_title,
                "result": "updated",
                "error": "",
            }
        )

    if report_csv:
        write_title_sync_report(report_csv, report_rows)

    return {
        "processed": processed,
        "updated": updated,
        "unchanged": unchanged,
        "errors": errors,
        "runroom_articles_updated": article_rows_updated,
        "statuses": only_statuses,
        "dry_run": dry_run,
        "report_csv": str(report_csv) if report_csv else None,
    }


def extract_first_h1(html_text: str) -> str | None:
    match = H1_RE.search(html_text)
    if not match:
        return None

    raw = match.group("content")
    no_tags = TAG_RE.sub(" ", raw)
    unescaped = html.unescape(no_tags)
    normalized = re.sub(r"\s+", " ", unescaped).strip()
    normalized = re.sub(r"\s+([,.;:!?])", r"\1", normalized)
    return normalized or None


def fetch_url_text(url: str, timeout: int = 30) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "rag-contenidos-runroom/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def write_title_sync_report(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "episode_id",
                "match_status",
                "url",
                "old_title",
                "new_title",
                "result",
                "error",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
