from __future__ import annotations

import re
import unittest
from pathlib import Path

_SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
_CREATE_IVFFLAT_RE = re.compile(
    r"CREATE\s+INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<name>\w+)\s+ON\s+[\w.]+\s+USING\s+ivfflat", re.IGNORECASE
)
_DROP_INDEX_RE = re.compile(r"DROP\s+INDEX\s+(?:IF\s+EXISTS\s+)?(?:public\.)?(?P<name>\w+)", re.IGNORECASE)


def _strip_sql_comments(sql: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


class VectorIndexMigrationTests(unittest.TestCase):
    def test_no_ivfflat_index_survives_the_migrations(self) -> None:
        # ivfflat with the default probes = 1 silently drops most nearest neighbours
        # (see sql/013_drop_ivfflat_vector_indexes.sql). Replaying the migrations in
        # order must leave none behind.
        live: set[str] = set()
        for path in sorted(_SQL_DIR.glob("*.sql")):
            sql = _strip_sql_comments(path.read_text(encoding="utf-8"))
            events = [(m.start(), "create", m.group("name")) for m in _CREATE_IVFFLAT_RE.finditer(sql)]
            events += [(m.start(), "drop", m.group("name")) for m in _DROP_INDEX_RE.finditer(sql)]
            for _, action, name in sorted(events):
                if action == "create":
                    live.add(name)
                else:
                    live.discard(name)

        self.assertEqual(live, set())


if __name__ == "__main__":
    unittest.main()
