"""SQLite helpers for Kais monitor."""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S"
DEFAULT_BASE_PATH = os.environ.get("KAIS_MONITOR_BASE", "/var/lib/kais-monitor")
DB_FILENAME = "kais.sqlite3"


def ensure_storage(base_path: str = DEFAULT_BASE_PATH) -> Path:
    """Ensure the storage directory exists and return it as Path."""
    base = Path(base_path)
    base.mkdir(parents=True, exist_ok=True)
    return base


class Database:
    """Thin wrapper around sqlite3 with helpers tailored for the app."""

    def __init__(self, base_path: str = DEFAULT_BASE_PATH) -> None:
        self.base_path = ensure_storage(base_path)
        self.db_path = self.base_path / DB_FILENAME
        self._conn = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._setup()

    def close(self) -> None:
        self._conn.close()

    @contextmanager
    def cursor(self) -> Iterator[sqlite3.Cursor]:
        cur = self._conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def _setup(self) -> None:
        with self.cursor() as cur:
            cur.executescript(
                """
                PRAGMA foreign_keys = ON;
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    source_url TEXT,
                    file_url TEXT,
                    last_seen_date TEXT,
                    first_seen_at TEXT,
                    last_seen_at TEXT,
                    monitored INTEGER NOT NULL DEFAULT 0,
                    ignored INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'unknown',
                    UNIQUE(title, COALESCE(file_url, ''))
                );

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL,
                    observed_date_in_table TEXT NOT NULL,
                    observed_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS downloads (
                    id INTEGER PRIMARY KEY,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    file_path TEXT NOT NULL,
                    sha256 TEXT,
                    size INTEGER,
                    downloaded_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS settings (
                    id INTEGER PRIMARY KEY,
                    key TEXT NOT NULL UNIQUE,
                    value TEXT
                );
                """
            )
            self._conn.commit()

    # ------------------------------------------------------------------
    # Settings helpers
    # ------------------------------------------------------------------
    def get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        with self.cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key = ?", (key,))
            row = cur.fetchone()
        return row["value"] if row else default

    def set_setting(self, key: str, value: str) -> None:
        with self.cursor() as cur:
            cur.execute(
                "INSERT INTO settings(key, value) VALUES(?, ?)\n                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
            self._conn.commit()

    # ------------------------------------------------------------------
    # Item helpers
    # ------------------------------------------------------------------
    def get_item_by_identity(self, title: str, file_url: Optional[str]) -> Optional[sqlite3.Row]:
        with self.cursor() as cur:
            cur.execute(
                "SELECT * FROM items WHERE title = ? AND COALESCE(file_url, '') = COALESCE(?, '')",
                (title, file_url),
            )
            return cur.fetchone()

    def create_item(
        self,
        title: str,
        source_url: str,
        file_url: Optional[str],
        observed_date: str,
        now: datetime,
    ) -> int:
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO items(title, source_url, file_url, last_seen_date, first_seen_at, last_seen_at, status)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    title,
                    source_url,
                    file_url,
                    observed_date,
                    now.strftime(ISO_FORMAT),
                    now.strftime(ISO_FORMAT),
                    "new",
                ),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def update_item_seen(self, item_id: int, observed_date: str, now: datetime, status: str) -> None:
        with self.cursor() as cur:
            cur.execute(
                """
                UPDATE items
                SET last_seen_date=?, last_seen_at=?, status=?
                WHERE id=?
                """,
                (observed_date, now.strftime(ISO_FORMAT), status, item_id),
            )
            self._conn.commit()

    def mark_item_flags(self, item_id: int, *, monitored: Optional[bool] = None, ignored: Optional[bool] = None) -> None:
        fields: List[str] = []
        params: List[object] = []
        if monitored is not None:
            fields.append("monitored = ?")
            params.append(1 if monitored else 0)
        if ignored is not None:
            fields.append("ignored = ?")
            params.append(1 if ignored else 0)
        if not fields:
            return
        params.append(item_id)
        with self.cursor() as cur:
            cur.execute(f"UPDATE items SET {', '.join(fields)} WHERE id = ?", params)
            self._conn.commit()

    def get_items(self, *, monitored: Optional[bool] = None, status: Optional[str] = None) -> List[sqlite3.Row]:
        clauses: List[str] = []
        params: List[object] = []
        if monitored is not None:
            clauses.append("monitored = ?")
            params.append(1 if monitored else 0)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        where = ""
        if clauses:
            where = "WHERE " + " AND ".join(clauses)
        query = f"SELECT * FROM items {where} ORDER BY COALESCE(datetime(last_seen_at), datetime('1970-01-01T00:00:00')) DESC, title"
        with self.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def get_item(self, item_id: int) -> Optional[sqlite3.Row]:
        with self.cursor() as cur:
            cur.execute("SELECT * FROM items WHERE id = ?", (item_id,))
            return cur.fetchone()

    # ------------------------------------------------------------------
    # Events & downloads
    # ------------------------------------------------------------------
    def add_event(self, item_id: int, kind: str, observed_date: str, now: datetime) -> int:
        with self.cursor() as cur:
            cur.execute(
                "INSERT INTO events(item_id, kind, observed_date_in_table, observed_at) VALUES(?, ?, ?, ?)",
                (item_id, kind, observed_date, now.strftime(ISO_FORMAT)),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def get_events_for_item(self, item_id: int) -> List[sqlite3.Row]:
        with self.cursor() as cur:
            cur.execute(
                "SELECT * FROM events WHERE item_id = ? ORDER BY datetime(observed_at) DESC",
                (item_id,),
            )
            return cur.fetchall()

    def record_download(self, item_id: int, file_path: str, sha256: Optional[str], size: int, now: datetime) -> int:
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO downloads(item_id, file_path, sha256, size, downloaded_at)
                VALUES(?, ?, ?, ?, ?)
                """,
                (item_id, file_path, sha256, size, now.strftime(ISO_FORMAT)),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def get_download(self, download_id: int) -> Optional[sqlite3.Row]:
        with self.cursor() as cur:
            cur.execute("SELECT * FROM downloads WHERE id = ?", (download_id,))
            return cur.fetchone()

    def get_stats(self) -> Dict[str, int]:
        with self.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(status = 'new') AS new_count,
                    SUM(status = 'updated') AS updated_count,
                    SUM(monitored = 1) AS monitored_count,
                    SUM(ignored = 1) AS ignored_count
                FROM items
                """
            )
            row = cur.fetchone()
        if not row:
            return {"total": 0, "new_count": 0, "updated_count": 0, "monitored_count": 0, "ignored_count": 0}
        return {
            "total": row["total"] or 0,
            "new_count": row["new_count"] or 0,
            "updated_count": row["updated_count"] or 0,
            "monitored_count": row["monitored_count"] or 0,
            "ignored_count": row["ignored_count"] or 0,
        }


def iter_items(connection: sqlite3.Connection) -> Iterable[sqlite3.Row]:
    with connection:  # type: ignore[arg-type]
        cur = connection.execute("SELECT * FROM items")
        yield from cur
