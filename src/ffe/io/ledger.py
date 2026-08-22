"""One row per (job, member). This is how you answer 'did yesterday's drop
land, and did anything get rejected?' without going digging."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

_DDL = """
CREATE TABLE IF NOT EXISTS jobs (
  job_id TEXT PRIMARY KEY, feed TEXT, table_name TEXT, started TEXT,
  finished TEXT, status TEXT, members INTEGER, rows INTEGER,
  rejects INTEGER, snapshot TEXT, error TEXT
);
CREATE TABLE IF NOT EXISTS members (
  job_id TEXT, member TEXT, status TEXT, parser TEXT,
  rows_parsed INTEGER, rows_rejected INTEGER, ragged INTEGER,
  staged TEXT, duration_ms INTEGER, error TEXT,
  PRIMARY KEY (job_id, member)
);
"""


class Ledger:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.executescript(_DDL)
        self.conn.commit()

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def start(self, job_id: str, feed: str, table: str, members: int) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO jobs (job_id, feed, table_name, started, "
            "status, members) VALUES (?,?,?,?,?,?)",
            (job_id, feed, table, self._now(), "running", members),
        )
        self.conn.commit()

    def member(self, job_id: str, rec: dict) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO members (job_id, member, status, parser, "
            "rows_parsed, rows_rejected, ragged, staged, duration_ms, error) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                job_id,
                rec["member"],
                rec["status"],
                rec.get("parser"),
                rec.get("rows_parsed", 0),
                rec.get("rows_rejected", 0),
                rec.get("ragged", 0),
                rec.get("staged"),
                rec.get("duration_ms", 0),
                json.dumps(rec["error"]) if rec.get("error") else None,
            ),
        )
        self.conn.commit()

    def finish(self, job_id: str, status: str, rows: int, rejects: int,
               snapshot: str | None = None, error: str | None = None) -> None:
        self.conn.execute(
            "UPDATE jobs SET finished=?, status=?, rows=?, rejects=?, snapshot=?, "
            "error=? WHERE job_id=?",
            (self._now(), status, rows, rejects, snapshot, error, job_id),
        )
        self.conn.commit()

    def history(self, feed: str, limit: int = 5) -> list[dict]:
        cur = self.conn.execute(
            "SELECT job_id, started, finished, status, members, rows, rejects "
            "FROM jobs WHERE feed=? ORDER BY started DESC LIMIT ?",
            (feed, limit),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
