from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from src.application.use_cases.query_similar import QuerySimilarRequest, QuerySimilarUseCase
from src.application.use_cases.recommend_content import RecommendContentRequest, RecommendContentUseCase
from src.config import RuntimeOptions, Settings
from src.infrastructure.ai.openai_embedding_client import OpenAIEmbeddingClient
from src.infrastructure.repositories.content_chunks import ContentChunksRepository
from src.infrastructure.repositories.legacy_chunks import LegacyChunksRepository
from src.pipeline.models import RunroomArticle


def dispatch_command(args: argparse.Namespace, settings: Settings, schema_path: Path) -> None:
    if args.command == "migrate-schema":
        from src.pipeline.storage import SupabaseStorage

        storage = SupabaseStorage(settings.supabase_db_url)
        try:
            storage.ensure_schema(args.schema_path)
        finally:
            storage.close()
        print(json.dumps({"ok": True, "schema_path": str(args.schema_path)}, indent=2, ensure_ascii=False))
        return

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

    if args.command == "ingest-case-studies-markdown":
        from src.content.ingest import ingest_case_studies_markdown

        summary = ingest_case_studies_markdown(
            settings=settings,
            schema_path=schema_path,
            input_path=args.input,
            target_tokens=args.target_tokens,
            overlap_tokens=args.overlap_tokens,
            batch_size=args.batch_size,
            offline_mode=args.offline_mode,
            dry_run=args.dry_run,
        )
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return

    if args.command == "ingest-case-study-url":
        from src.content.ingest import ingest_case_study_url

        summary = ingest_case_study_url(
            settings=settings,
            schema_path=schema_path,
            url=args.url,
            target_tokens=args.target_tokens,
            overlap_tokens=args.overlap_tokens,
            batch_size=args.batch_size,
            offline_mode=args.offline_mode,
            dry_run=args.dry_run,
        )
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return

    if args.command == "ingest-runroom-labs":
        from src.content.ingest import ingest_runroom_labs

        summary = ingest_runroom_labs(
            settings=settings,
            schema_path=schema_path,
            index_url=args.index_url,
            target_tokens=args.target_tokens,
            overlap_tokens=args.overlap_tokens,
            batch_size=args.batch_size,
            offline_mode=args.offline_mode,
            dry_run=args.dry_run,
        )
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return

    if args.command == "backfill-canonical-content":
        from src.content.backfill import backfill_episodes_to_canonical

        summary = backfill_episodes_to_canonical(
            settings=settings,
            schema_path=schema_path,
            dry_run=args.dry_run,
            limit=args.limit,
        )
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
        _execute_query_similar(args=args, settings=settings, schema_path=schema_path)
        return

    if args.command == "recommend-content":
        _execute_recommend_content(args=args, settings=settings, schema_path=schema_path)
        return

    if args.command == "preview-youtube-description":
        from src.youtube_preview import run_preview_youtube_description

        cli_env_debug = _masked_env_debug("YOUTUBE_API_KEY")
        print(
            "[debug] CLI YOUTUBE_API_KEY "
            f"present={str(cli_env_debug['present']).lower()} "
            f"prefix={cli_env_debug['prefix']} "
            f"length={cli_env_debug['length']}"
        )

        summary = run_preview_youtube_description(
            settings=settings,
            schema_path=schema_path,
            episode_identifier=args.episode,
            youtube_url=args.youtube_url,
            current_description_file=args.current_description_file,
            cli_env_debug=cli_env_debug,
            output_root=args.output_dir,
            offline_mode=args.offline_mode,
        )
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return

    if args.command == "reembed-content":
        from src.pipeline.ai_client import AIClient
        from src.pipeline.storage import SupabaseStorage

        storage = SupabaseStorage(settings.supabase_db_url)
        ai = AIClient(settings=settings, force_offline=args.offline_mode)

        updated = 0
        try:
            storage.ensure_schema(schema_path)
            rows = storage.list_content_chunks_for_reembed(content_type=args.content_type, item_id=args.item_id)
            for i in range(0, len(rows), args.batch_size):
                batch = rows[i : i + args.batch_size]
                texts = [str(row.get("text") or "") for row in batch]
                vectors = ai.embed_texts(texts)
                for row, vector in zip(batch, vectors):
                    storage.update_content_chunk_embedding(int(row["id"]), vector)
                    updated += 1
        finally:
            storage.close()

        print(json.dumps({"chunks_updated": updated}, indent=2, ensure_ascii=False))
        return

    if args.command == "materialize-content-relations":
        from src.content.recommendation import materialize_relations

        summary = materialize_relations(
            settings=settings,
            schema_path=schema_path,
            top_k_per_item=args.top_k_per_item,
            limit_items=args.limit_items,
            content_types=_parse_comma_values(args.content_types),
            min_score=args.min_score,
        )
        print(json.dumps(summary, indent=2, ensure_ascii=False))
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


def _execute_query_similar(args: argparse.Namespace, settings: Settings, schema_path: Path) -> None:
    from src.pipeline.storage import SupabaseStorage

    storage = SupabaseStorage(settings.supabase_db_url)
    try:
        storage.ensure_schema(schema_path)
        use_case = QuerySimilarUseCase(
            embedding_client=OpenAIEmbeddingClient(settings=settings, force_offline=args.offline_mode),
            repository=LegacyChunksRepository(storage),
        )
        response = use_case.execute(QuerySimilarRequest(text=args.text, top_k=args.top_k))
    finally:
        storage.close()

    for row in response.results:
        print(f"[{row.similarity:.4f}] {row.episode_code or '-'} {row.episode_title} @ {row.start_ts_hhmmss}")
        print(f"  URL: {row.runroom_article_url or '-'}")
        print(f"  Texto: {row.text[:240].strip()}\n")


def _execute_recommend_content(args: argparse.Namespace, settings: Settings, schema_path: Path) -> None:
    from src.pipeline.storage import SupabaseStorage

    query_text = args.text
    if args.text_file:
        query_text = args.text_file.read_text(encoding="utf-8")

    storage = SupabaseStorage(settings.supabase_db_url)
    try:
        storage.ensure_schema(schema_path)
        use_case = RecommendContentUseCase(
            embedding_client=OpenAIEmbeddingClient(settings=settings, force_offline=args.offline_mode),
            repository=ContentChunksRepository(storage),
        )
        response = use_case.execute(
            RecommendContentRequest(
                text=query_text,
                top_k=args.top_k,
                fetch_k=args.fetch_k,
                content_types=_parse_comma_values(args.content_types),
                source=args.source,
                language=args.lang,
                group_by_type=args.group_by_type,
            )
        )
    finally:
        storage.close()

    print(json.dumps(response.to_dict(), indent=2, ensure_ascii=False))


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


def _masked_env_debug(var_name: str) -> dict[str, Any]:
    value = (os.getenv(var_name) or "").strip()
    return {
        "present": bool(value),
        "prefix": value[:6] if value else "",
        "length": len(value),
    }
