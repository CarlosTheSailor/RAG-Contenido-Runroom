from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any

from src.config import Settings
from src.pipeline.storage import SimilarContentQueryError, SupabaseStorage

DEFAULT_RELATED_AUDIT_STATEMENT_TIMEOUT_MS = 15000
DEFAULT_RELATED_AUDIT_LOCK_TIMEOUT_MS = 2000
DEFAULT_RELATED_AUDIT_FETCH_K = 80
DEFAULT_RELATED_AUDIT_PRIMARY_TYPE = "episode"
DEFAULT_RELATED_AUDIT_COMPARE_TYPE = "case_study"


def run_related_content_audit(
    *,
    settings: Settings,
    schema_path: Path,
    topic_id: int,
    run_id: int | None = None,
    primary_type: str = DEFAULT_RELATED_AUDIT_PRIMARY_TYPE,
    compare_type: str = DEFAULT_RELATED_AUDIT_COMPARE_TYPE,
    fetch_k: int = DEFAULT_RELATED_AUDIT_FETCH_K,
    statement_timeout_ms: int = DEFAULT_RELATED_AUDIT_STATEMENT_TIMEOUT_MS,
    lock_timeout_ms: int = DEFAULT_RELATED_AUDIT_LOCK_TIMEOUT_MS,
    output_path: Path | None = None,
) -> dict[str, Any]:
    storage = SupabaseStorage(settings.supabase_db_url)
    captured_at = datetime.now(tz=timezone.utc)
    try:
        storage.ensure_schema(schema_path)
        inventory = storage.count_content_inventory_by_type()
        activity_before = storage.list_content_query_activity()
        locks_before = storage.list_content_query_locks()
        embedding = storage.get_theme_topic_embedding(topic_id)
        if not embedding:
            raise ValueError(f"No se encontro embedding para topic_id={topic_id}.")

        plans: dict[str, dict[str, Any]] = {}
        for content_type in _ordered_unique_types([primary_type, compare_type]):
            plans[content_type] = _collect_explain_plan(
                storage=storage,
                embedding=embedding,
                content_type=content_type,
                fetch_k=fetch_k,
                statement_timeout_ms=statement_timeout_ms,
                lock_timeout_ms=lock_timeout_ms,
            )

        activity_after = storage.list_content_query_activity()
        locks_after = storage.list_content_query_locks()
    finally:
        storage.close()

    output = output_path or _default_output_path(topic_id=topic_id, captured_at=captured_at)
    output.parent.mkdir(parents=True, exist_ok=True)

    summary = {
        "topic_id": int(topic_id),
        "run_id": int(run_id) if run_id is not None else None,
        "captured_at": captured_at.isoformat(),
        "primary_type": primary_type,
        "compare_type": compare_type,
        "fetch_k": int(fetch_k),
        "statement_timeout_ms": int(statement_timeout_ms),
        "lock_timeout_ms": int(lock_timeout_ms),
        "inventory": inventory,
        "ratios": _build_inventory_ratios(inventory=inventory, primary_type=primary_type, compare_type=compare_type),
        "plans": plans,
        "activity_before": activity_before,
        "activity_after": activity_after,
        "locks_before": locks_before,
        "locks_after": locks_after,
        "output": str(output),
    }

    output.write_text(_render_audit_report(summary), encoding="utf-8")
    return summary


def _collect_explain_plan(
    *,
    storage: SupabaseStorage,
    embedding: list[float],
    content_type: str,
    fetch_k: int,
    statement_timeout_ms: int,
    lock_timeout_ms: int,
) -> dict[str, Any]:
    started = perf_counter()
    try:
        plan_lines = storage.explain_query_similar_content_chunks(
            query_embedding=embedding,
            top_k=fetch_k,
            content_types=[content_type],
            statement_timeout_ms=statement_timeout_ms,
            lock_timeout_ms=lock_timeout_ms,
        )
        duration_ms = int((perf_counter() - started) * 1000)
        return {
            "content_type": content_type,
            "ok": True,
            "duration_ms": duration_ms,
            "plan": plan_lines,
        }
    except Exception as exc:
        duration_ms = int((perf_counter() - started) * 1000)
        return {
            "content_type": content_type,
            "ok": False,
            "duration_ms": duration_ms,
            "error": _serialize_query_error(exc),
            "plan": [],
        }


def _serialize_query_error(exc: Exception) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "type": exc.__class__.__name__,
        "message": str(exc).strip() or exc.__class__.__name__,
    }
    if isinstance(exc, SimilarContentQueryError):
        payload.update(
            {
                "sqlstate": exc.sqlstate,
                "statement_timeout_ms": exc.statement_timeout_ms,
                "lock_timeout_ms": exc.lock_timeout_ms,
                "duration_ms": exc.duration_ms,
                "sql_timeout": exc.is_statement_timeout,
                "lock_timeout": exc.is_lock_timeout,
                "content_types": list(exc.content_types),
            }
        )
    return payload


def _build_inventory_ratios(*, inventory: list[dict[str, Any]], primary_type: str, compare_type: str) -> dict[str, Any]:
    primary_row = _find_inventory_row(inventory, primary_type)
    compare_row = _find_inventory_row(inventory, compare_type)
    primary_chunks = int(primary_row.get("chunk_count") or 0)
    compare_chunks = int(compare_row.get("chunk_count") or 0)
    primary_items = int(primary_row.get("item_count") or 0)
    compare_items = int(compare_row.get("item_count") or 0)

    return {
        "primary_chunk_count": primary_chunks,
        "compare_chunk_count": compare_chunks,
        "primary_item_count": primary_items,
        "compare_item_count": compare_items,
        "chunk_ratio_primary_vs_compare": _safe_ratio(primary_chunks, compare_chunks),
        "item_ratio_primary_vs_compare": _safe_ratio(primary_items, compare_items),
    }


def _find_inventory_row(inventory: list[dict[str, Any]], content_type: str) -> dict[str, Any]:
    normalized = str(content_type or "").strip().lower()
    for row in inventory:
        if str(row.get("content_type") or "").strip().lower() == normalized:
            return dict(row)
    return {"content_type": content_type, "item_count": 0, "chunk_count": 0}


def _safe_ratio(primary: int, compare: int) -> float | None:
    if compare <= 0:
        return None
    return round(primary / compare, 3)


def _default_output_path(*, topic_id: int, captured_at: datetime) -> Path:
    stamp = captured_at.astimezone(timezone.utc).strftime("%Y%m%d")
    return Path("docs") / f"THEME_INTEL_RELATED_AUDIT_TOPIC_{int(topic_id)}_{stamp}.md"


def _ordered_unique_types(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for raw in values:
        value = str(raw or "").strip()
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        output.append(value)
    return output


def _render_audit_report(summary: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Theme Intel Related Content Audit")
    lines.append("")
    lines.append(f"- Captured at (UTC): `{summary['captured_at']}`")
    lines.append(f"- Topic ID: `{summary['topic_id']}`")
    if summary.get("run_id") is not None:
        lines.append(f"- Run ID: `{summary['run_id']}`")
    lines.append(f"- Primary type: `{summary['primary_type']}`")
    lines.append(f"- Compare type: `{summary['compare_type']}`")
    lines.append(f"- Fetch K (SQL LIMIT): `{summary['fetch_k']}`")
    lines.append(f"- Statement timeout: `{summary['statement_timeout_ms']} ms`")
    lines.append(f"- Lock timeout: `{summary['lock_timeout_ms']} ms`")
    lines.append("")
    lines.append("## Inventory Snapshot")
    lines.append("")
    lines.append("| content_type | item_count | chunk_count |")
    lines.append("| --- | ---: | ---: |")
    for row in summary.get("inventory", []):
        lines.append(
            f"| `{row.get('content_type')}` | {int(row.get('item_count') or 0)} | {int(row.get('chunk_count') or 0)} |"
        )
    ratios = summary.get("ratios", {})
    lines.append("")
    lines.append("## Inventory Ratios")
    lines.append("")
    lines.append(f"- Chunk ratio `{summary['primary_type']}` vs `{summary['compare_type']}`: `{_format_ratio(ratios.get('chunk_ratio_primary_vs_compare'))}`")
    lines.append(f"- Item ratio `{summary['primary_type']}` vs `{summary['compare_type']}`: `{_format_ratio(ratios.get('item_ratio_primary_vs_compare'))}`")
    lines.append("")
    lines.append("## EXPLAIN Results")
    lines.append("")
    for content_type, plan in summary.get("plans", {}).items():
        lines.append(f"### `{content_type}`")
        lines.append("")
        lines.append(f"- OK: `{str(bool(plan.get('ok'))).lower()}`")
        lines.append(f"- Duration: `{int(plan.get('duration_ms') or 0)} ms`")
        if plan.get("error"):
            lines.append(f"- Error: `{json.dumps(plan['error'], ensure_ascii=False)}`")
        lines.append("")
        lines.append("```text")
        if plan.get("plan"):
            lines.extend(str(line) for line in plan["plan"])
        else:
            lines.append("<no plan output>")
        lines.append("```")
        lines.append("")
    lines.append("## pg_stat_activity (before)")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(summary.get("activity_before", []), indent=2, ensure_ascii=False, default=str))
    lines.append("```")
    lines.append("")
    lines.append("## pg_stat_activity (after)")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(summary.get("activity_after", []), indent=2, ensure_ascii=False, default=str))
    lines.append("```")
    lines.append("")
    lines.append("## pg_locks (before)")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(summary.get("locks_before", []), indent=2, ensure_ascii=False, default=str))
    lines.append("```")
    lines.append("")
    lines.append("## pg_locks (after)")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(summary.get("locks_after", []), indent=2, ensure_ascii=False, default=str))
    lines.append("```")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- The goal is to compare the typed vector query for `episode` against the comparison type using the current `theme_topic_embeddings` vector.")
    lines.append("- If `episode` times out or appears in `pg_stat_activity` without finishing, the hang is likely in the typed SQL path rather than Gmail or theme extraction.")
    return "\n".join(lines) + "\n"


def _format_ratio(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.3f}"
