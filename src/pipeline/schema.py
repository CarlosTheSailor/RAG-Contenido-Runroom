from __future__ import annotations

from pathlib import Path

import psycopg


def apply_migrations(conn: psycopg.Connection, schema_path: Path) -> list[str]:
    migration_files = _resolve_migration_files(schema_path)
    if not migration_files:
        return []

    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
    conn.commit()

    applied: list[str] = []
    for migration_path in migration_files:
        version = migration_path.name
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM schema_migrations WHERE version = %s", (version,))
            exists = cur.fetchone() is not None
            if exists:
                continue

            sql = migration_path.read_text(encoding="utf-8")
            cur.execute(sql)
            cur.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (version,))
        conn.commit()
        applied.append(version)

    return applied


def _resolve_migration_files(schema_path: Path) -> list[Path]:
    if schema_path.is_dir():
        return sorted(schema_path.glob("*.sql"))

    if schema_path.is_file():
        return [schema_path]

    return []
