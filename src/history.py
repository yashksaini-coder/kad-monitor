"""
SQLite-backed snapshot history so dashboard charts survive page reloads.

Stores a slim per-tick projection, not the full snapshot (which carries node
lists and result tables).  sqlite3 is synchronous; a sub-millisecond INSERT
every 0.5s is negligible on the trio loop.
# ponytail: sync sqlite on the event loop — move to a worker thread if ticks ever get slow
"""

from __future__ import annotations

import json
import sqlite3
import time


class SnapshotHistory:
    PRUNE_EVERY_S = 300.0

    def __init__(self, db_path: str, retention_hours: float = 24.0) -> None:
        self._conn = sqlite3.connect(db_path)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS snapshots (ts REAL PRIMARY KEY, data TEXT NOT NULL)"
        )
        self._conn.commit()
        self._retention_s = retention_hours * 3600
        self._last_prune = time.monotonic()

    @staticmethod
    def _slim(snapshot: dict) -> dict:
        coord = snapshot.get("coordinator", {})
        rates = coord.get("rates", {})
        conc = coord.get("concurrency", {})
        return {
            "ts": snapshot.get("ts", time.time()),
            "qps": rates.get("throughput_qps", 0.0),
            "avg_ms": rates.get("avg_duration_ms", 0.0),
            "concurrency": conc.get("current", 0) + conc.get("acquiring", 0),
        }

    def append(self, snapshot: dict) -> None:
        slim = self._slim(snapshot)
        self._conn.execute(
            "INSERT OR REPLACE INTO snapshots VALUES (?, ?)",
            (slim["ts"], json.dumps(slim)),
        )
        self._conn.commit()
        if time.monotonic() - self._last_prune > self.PRUNE_EVERY_S or self._last_prune == 0.0:
            self._conn.execute(
                "DELETE FROM snapshots WHERE ts < ?", (slim["ts"] - self._retention_s,)
            )
            self._conn.commit()
            self._last_prune = time.monotonic()

    def query(self, minutes: float, max_points: int = 240) -> list[dict]:
        cutoff = time.time() - minutes * 60
        rows = self._conn.execute(
            "SELECT data FROM snapshots WHERE ts >= ? ORDER BY ts", (cutoff,)
        ).fetchall()
        step = max(1, len(rows) // max_points)
        return [json.loads(r[0]) for r in rows[::step]]
