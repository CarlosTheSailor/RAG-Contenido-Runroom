from __future__ import annotations

import argparse
import csv
import json
import logging
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from src.config import RuntimeOptions, Settings
from src.logging_utils import configure_logging
from src.pipeline.models import RunroomArticle

logger = logging.getLogger(__name__)



def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Realworld transcript RAG pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest_cmd = sub.add_parser("ingest-transcripts", help="Parse transcripts, chunk, enrich and load into Supabase")
    ingest_cmd.add_argument("--transcripts-dir", default="transcripciones", type=Path)
    ingest_cmd.add_argument("--target-tokens", default=220, type=int)
    ingest_cmd.add_argument("--overlap-tokens", default=40, type=int)
    ingest_cmd.add_argument("--batch-size", default=32, type=int)
    ingest_cmd.add_argument("--offline-mode", action="store_true", help="Use deterministic local metadata/embeddings")

    sync_cmd = sub.add_parser("sync-runroom-sitemap", help="Fetch Runroom sitemap and upsert realworld URLs")
    sync_cmd.add_argument("--fetch-meta", action="store_true", default=True)
    sync_cmd.add_argument("--no-fetch-meta", action="store_true", help="Skip article page fetch and use slug as title")

    match_cmd = sub.add_parser("match-episodes", help="Match episodes to runroom articles")
    match_cmd.add_argument("--offline-mode", action="store_true", help="Use deterministic local embeddings")
    match_cmd.add_argument("--auto-threshold", type=float, default=None)
    match_cmd.add_argument("--auto-margin", type=float, default=None)
    match_cmd.add_argument("--top-candidates", type=int, default=5)

    query_cmd = sub.add_parser("query-similar", help="Search chunks by semantic similarity")
    query_cmd.add_argument("--text", required=True)
    query_cmd.add_argument("--top-k", default=8, type=int)
    query_cmd.add_argument("--offline-mode", action="store_true", help="Use deterministic local embeddings")

    export_cmd = sub.add_parser("export-review-report", help="Export review_required matches to CSV")
    export_cmd.add_argument("--output", type=Path, default=Path("reports/review_report.csv"))

    review_cmd = sub.add_parser("review-matches", help="Review and select matches interactively")
    review_cmd.add_argument("--limit", type=int, default=None, help="Max review episodes in this run")

    apply_cmd = sub.add_parser(
        "apply-manual-overrides",
        help="Apply manual episode->URL matches from CSV",
    )
    apply_cmd.add_argument("--csv", type=Path, required=True, help="CSV path with manual overrides")
    apply_cmd.add_argument("--dry-run", action="store_true", help="Validate input without writing to database")

    title_sync_cmd = sub.add_parser(
        "sync-episode-titles-from-h1",
        help="Sync episode and article titles from each episode page h1",
    )
    title_sync_cmd.add_argument("--dry-run", action="store_true", help="Only report differences without writing")
    title_sync_cmd.add_argument("--limit", type=int, default=None, help="Process only first N matched episodes")
    title_sync_cmd.add_argument(
        "--only-status",
        default="auto_matched,manual_matched",
        help="Comma-separated episode match_status filter",
    )
    title_sync_cmd.add_argument(
        "--report-csv",
        type=Path,
        default=None,
        help="Optional CSV file path with per-episode diff/result",
    )

    return parser



def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    settings = Settings.from_env()
    configure_logging(settings.log_level)

    schema_path = Path("sql/001_init.sql")
    if args.command == "ingest-transcripts":
        from src.pipeline.ingest import ingest_transcripts

        runtime = RuntimeOptions(
            transcripts_dir=args.transcripts_dir,
            target_tokens=args.target_tokens,
            overlap_tokens=args.overlap_tokens,
            batch_size=args.batch_size,
            offline_mode=args.offline_mode,
        )
        summary = ingest_transcripts(settings=settings, options=runtime, schema_path=schema_path)
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return

    if args.command == "sync-runroom-sitemap":
        from src.matching.sync import sync_runroom_sitemap

        fetch_meta = not bool(args.no_fetch_meta)
        summary = sync_runroom_sitemap(settings=settings, schema_path=schema_path, fetch_metadata=fetch_meta)
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return

    if args.command == "match-episodes":
        from src.matching.matcher import run_matching

        summary = run_matching(
            settings=settings,
            schema_path=schema_path,
            force_offline=args.offline_mode,
            auto_threshold=args.auto_threshold,
            auto_margin=args.auto_margin,
            top_candidates=args.top_candidates,
        )
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return

    if args.command == "query-similar":
        from src.pipeline.ai_client import AIClient
        from src.pipeline.storage import SupabaseStorage

        storage = SupabaseStorage(settings.supabase_db_url)
        try:
            storage.ensure_schema(schema_path)
            ai = AIClient(settings, force_offline=args.offline_mode)
            vector = ai.embed_texts([args.text])[0]
            results = storage.query_similar_chunks(vector, top_k=args.top_k)
        finally:
            storage.close()

        for row in results:
            ts = _fmt_seconds(float(row["start_ts_sec"]))
            sim = float(row["similarity"])
            print(f"[{sim:.4f}] {row['episode_code'] or '-'} {row['episode_title']} @ {ts}")
            print(f"  URL: {row.get('runroom_article_url') or '-'}")
            print(f"  Texto: {str(row['text'])[:240].strip()}\n")
        return

    if args.command == "export-review-report":
        from src.pipeline.storage import SupabaseStorage

        storage = SupabaseStorage(settings.supabase_db_url)
        try:
            storage.ensure_schema(schema_path)
            total = storage.export_review_report(args.output)
        finally:
            storage.close()
        print(json.dumps({"rows_exported": total, "output": str(args.output)}, indent=2, ensure_ascii=False))
        return

    if args.command == "review-matches":
        from src.pipeline.storage import SupabaseStorage

        storage = SupabaseStorage(settings.supabase_db_url)
        try:
            storage.ensure_schema(schema_path)
            rows = storage.list_review_candidates(limit_episodes=args.limit)
            summary = _interactive_review_matches(storage, rows)
        finally:
            storage.close()
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return

    if args.command == "apply-manual-overrides":
        from src.pipeline.storage import SupabaseStorage

        storage = SupabaseStorage(settings.supabase_db_url)
        try:
            storage.ensure_schema(schema_path)
            summary = _apply_manual_overrides(storage, args.csv, dry_run=args.dry_run)
        finally:
            storage.close()
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return

    if args.command == "sync-episode-titles-from-h1":
        from src.matching.title_sync import sync_episode_titles_from_h1
        from src.pipeline.storage import SupabaseStorage

        statuses = _parse_comma_values(args.only_status)
        storage = SupabaseStorage(settings.supabase_db_url)
        try:
            storage.ensure_schema(schema_path)
            summary = sync_episode_titles_from_h1(
                storage=storage,
                only_statuses=statuses,
                limit=args.limit,
                dry_run=args.dry_run,
                report_csv=args.report_csv,
            )
        finally:
            storage.close()
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return



def _fmt_seconds(seconds: float) -> str:
    total = int(seconds)
    hh = total // 3600
    mm = (total % 3600) // 60
    ss = total % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}"


def _parse_comma_values(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _interactive_review_matches(storage: Any, rows: list[dict[str, Any]]) -> dict[str, int]:
    if not rows:
        print("No hay episodios en review_required.")
        return {"episodes_reviewed": 0, "selected": 0, "skipped": 0}

    grouped: list[tuple[int, list[dict[str, Any]]]] = []
    current_id: int | None = None
    bucket: list[dict[str, Any]] = []
    for row in rows:
        ep_id = int(row["episode_id"])
        if current_id is None:
            current_id = ep_id
        if ep_id != current_id:
            grouped.append((current_id, bucket))
            current_id = ep_id
            bucket = []
        bucket.append(row)
    if current_id is not None and bucket:
        grouped.append((current_id, bucket))

    selected = 0
    skipped = 0
    for ep_id, candidates in grouped:
        first = candidates[0]
        episode_code = first.get("episode_code") or "-"
        episode_title = first.get("episode_title") or "-"
        source_filename = first.get("source_filename") or "-"
        print(f"\nEpisode {ep_id} | {episode_code} | {episode_title}")
        print(f"  archivo: {source_filename}")
        for idx, item in enumerate(candidates, start=1):
            score = float(item.get("score") or 0.0)
            method = str(item.get("method") or "-")
            title = str(item.get("candidate_title") or "-")
            url = str(item.get("candidate_url") or "-")
            print(f"  {idx}. [{score:.3f}] ({method}) {title}")
            print(f"     {url}")

        while True:
            raw = input("Elige 1-5, s=skip, q=quit: ").strip().lower()
            if raw in {"q", "quit"}:
                return {
                    "episodes_reviewed": selected + skipped,
                    "selected": selected,
                    "skipped": skipped,
                }
            if raw in {"s", "skip", ""}:
                skipped += 1
                break
            if raw.isdigit():
                pick = int(raw)
                if 1 <= pick <= len(candidates):
                    chosen = candidates[pick - 1]
                    storage.set_manual_match(
                        episode_id=ep_id,
                        article_id=int(chosen["article_id"]),
                        confidence=float(chosen.get("score") or 0.0),
                    )
                    selected += 1
                    break
            print("Entrada no válida.")

    return {
        "episodes_reviewed": selected + skipped,
        "selected": selected,
        "skipped": skipped,
    }


def _apply_manual_overrides(storage: Any, csv_path: Path, dry_run: bool = False) -> dict[str, int]:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    applied = 0
    skipped = 0
    invalid = 0
    total = 0

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            episode_raw = str(row.get("episode_id", "")).strip()
            url = str(row.get("url", "")).strip()
            title = str(row.get("title", "")).strip()
            description = str(row.get("description", "")).strip()
            lang = str(row.get("lang", "")).strip().lower()
            episode_code_hint = str(row.get("episode_code_hint", "")).strip().lower() or None
            confidence_raw = str(row.get("confidence", "")).strip()

            if not episode_raw or not url:
                invalid += 1
                continue

            try:
                episode_id = int(episode_raw)
            except ValueError:
                invalid += 1
                continue

            if not storage.episode_exists(episode_id):
                skipped += 1
                continue

            slug = _slug_from_url(url)
            if not slug:
                invalid += 1
                continue

            if not title:
                title = slug.replace("-", " ").strip()
            if not lang:
                lang = "en" if "/en/realworld/" in url else "es"

            confidence: float | None = 1.0
            if confidence_raw:
                try:
                    confidence = float(confidence_raw)
                except ValueError:
                    confidence = 1.0

            if dry_run:
                applied += 1
                continue

            article_id = storage.upsert_runroom_article(
                RunroomArticle(
                    url=url,
                    slug=slug,
                    title=title,
                    description=description,
                    lang=lang,
                    episode_code_hint=episode_code_hint,
                )
            )
            storage.set_manual_match(episode_id=episode_id, article_id=article_id, confidence=confidence)
            applied += 1

    return {
        "rows_total": total,
        "applied": applied,
        "skipped_episode_not_found": skipped,
        "invalid_rows": invalid,
        "dry_run": dry_run,
    }


def _slug_from_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    if not path:
        return ""
    parts = path.split("/")
    if len(parts) >= 3 and parts[0] == "en" and parts[1] == "realworld":
        return parts[2]
    if len(parts) >= 2 and parts[0] == "realworld":
        return parts[1]
    return parts[-1]


if __name__ == "__main__":
    main()
