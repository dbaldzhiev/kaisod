"""SQLite helpers for Kais monitor."""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S"
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_DATA_ROOT = (_PROJECT_ROOT / "data").resolve()
DEFAULT_BASE_PATH = os.environ.get("KAIS_MONITOR_BASE") or str(_DEFAULT_DATA_ROOT)
DB_FILENAME = "kais.sqlite3"
CONFIG_FILE_ENV = "KAIS_MONITOR_STORAGE_CONFIG"
CONFIG_DIR_ENV = "KAIS_MONITOR_CONFIG_DIR"


def _config_directory() -> Path:
    override = os.environ.get(CONFIG_DIR_ENV)
    if override:
        return Path(override).expanduser()
    xdg_home = os.environ.get("XDG_CONFIG_HOME")
    if xdg_home:
        return Path(xdg_home).expanduser() / "kais-monitor"
    return Path.home() / ".config" / "kais-monitor"


def _config_path() -> Path:
    override = os.environ.get(CONFIG_FILE_ENV)
    if override:
        path = Path(override).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        return path
    directory = _config_directory()
    directory.mkdir(parents=True, exist_ok=True)
    return directory / "storage.json"


def load_configured_base_path() -> Path:
    """Return the configured base path, respecting env vars and config file."""

    env_path = os.environ.get("KAIS_MONITOR_BASE")
    if env_path:
        return Path(env_path).expanduser()

    config = _config_path()
    if config.exists():
        try:
            data = json.loads(config.read_text())
            stored = data.get("base_path")
            if stored:
                return Path(str(stored)).expanduser()
        except Exception:
            # Fall through to default when config cannot be parsed
            pass

    return Path(DEFAULT_BASE_PATH).expanduser()


def save_configured_base_path(path: Path) -> None:
    """Persist the selected base path for future runs."""

    config = _config_path()
    payload = {"base_path": str(Path(path).expanduser())}
    config.write_text(json.dumps(payload, ensure_ascii=False, indent=2))


def relocate_storage_directory(source: Path, destination: Path) -> Path:
    """Move the storage directory (DB + files) to a new absolute path."""

    src = Path(source).expanduser()
    dest = Path(destination).expanduser()

    if not dest.is_absolute():
        raise ValueError("Storage path must be an absolute path")

    try:
        src_resolved = src.resolve()
    except FileNotFoundError:
        src_resolved = src

    dest_resolved = dest.resolve() if dest.exists() else dest

    if src_resolved == dest_resolved:
        ensure_storage(str(dest_resolved))
        return dest_resolved

    if src.exists():
        # Prevent moving into or above itself which would create recursion
        if str(dest_resolved).startswith(str(src_resolved) + os.sep):
            raise ValueError("Destination cannot be within the current storage directory")
        if dest.exists():
            if any(dest.iterdir()):
                raise ValueError("Destination directory must be empty to adopt storage")
            dest.rmdir()
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dest))
    else:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.mkdir(parents=True, exist_ok=True)

    ensure_storage(str(dest))
    return dest.resolve()


def ensure_storage(base_path: str = DEFAULT_BASE_PATH) -> Path:
    """Ensure the storage directory exists and return it as Path."""
    base = Path(base_path)
    base.mkdir(parents=True, exist_ok=True)
    return base


def _normalize_file_url(file_url: Optional[str]) -> str:
    """Normalize optional file URLs for storage and comparisons."""
    return file_url or ""


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
                    file_url TEXT NOT NULL DEFAULT '',
                    path TEXT NOT NULL DEFAULT '',
                    last_seen_date TEXT,
                    first_seen_at TEXT,
                    last_seen_at TEXT,
                    monitored INTEGER NOT NULL DEFAULT 0,
                    ignored INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'unknown',
                    UNIQUE(title, file_url)
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
                    downloaded_at TEXT NOT NULL,
                    observed_date_in_table TEXT
                );

                CREATE TABLE IF NOT EXISTS settings (
                    id INTEGER PRIMARY KEY,
                    key TEXT NOT NULL UNIQUE,
                    value TEXT
                );
                """
            )
            self._conn.commit()

        # Lightweight migrations for new columns
        with self.cursor() as cur:
            cur.execute("PRAGMA table_info(items)")
            columns = {row[1] for row in cur.fetchall()}
            if "path" not in columns:
                cur.execute("ALTER TABLE items ADD COLUMN path TEXT NOT NULL DEFAULT ''")
                self._conn.commit()
                # Best effort backfill using historic title representation
                cur.execute("SELECT id, title, path FROM items")
                rows = cur.fetchall()
                for row in rows:
                    current_path = row["path"] or ""
                    if current_path:
                        continue
                    title = row["title"] or ""
                    path_guess = title.replace(" / ", "/")
                    cur.execute("UPDATE items SET path = ? WHERE id = ?", (path_guess, row["id"]))
                self._conn.commit()

            cur.execute("PRAGMA table_info(downloads)")
            download_columns = {row[1] for row in cur.fetchall()}
            if "observed_date_in_table" not in download_columns:
                cur.execute("ALTER TABLE downloads ADD COLUMN observed_date_in_table TEXT")
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
        normalized_url = _normalize_file_url(file_url)
        with self.cursor() as cur:
            cur.execute(
                "SELECT * FROM items WHERE title = ? AND file_url = ?",
                (title, normalized_url),
            )
            return cur.fetchone()

    def create_item(
        self,
        title: str,
        source_url: str,
        file_url: Optional[str],
        path: str,
        observed_date: str,
        now: datetime,
    ) -> int:
        normalized_url = _normalize_file_url(file_url)
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO items(title, source_url, file_url, path, last_seen_date, first_seen_at, last_seen_at, status)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    title,
                    source_url,
                    normalized_url,
                    path,
                    observed_date,
                    now.strftime(ISO_FORMAT),
                    now.strftime(ISO_FORMAT),
                    "new",
                ),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def update_item_seen(
        self,
        item_id: int,
        observed_date: str,
        now: datetime,
        status: str,
        *,
        path: Optional[str] = None,
    ) -> None:
        fields = ["last_seen_date = ?", "last_seen_at = ?", "status = ?"]
        params: List[object] = [observed_date, now.strftime(ISO_FORMAT), status]
        if path is not None:
            fields.append("path = ?")
            params.append(path)
        params.append(item_id)
        with self.cursor() as cur:
            cur.execute(f"UPDATE items SET {', '.join(fields)} WHERE id=?", params)
            self._conn.commit()

    def mark_item_flags(self, item_id: int, *, monitored: Optional[bool] = None, ignored: Optional[bool] = None) -> None:
        fields: List[str] = []
        params: List[object] = []
        if monitored is not None:
            fields.append("monitored = ?")
            params.append(1 if monitored else 0)
            if monitored:
                fields.append("ignored = 0")
        if ignored is not None:
            fields.append("ignored = ?")
            params.append(1 if ignored else 0)
            if ignored:
                fields.append("monitored = 0")
        if not fields:
            return
        params.append(item_id)
        with self.cursor() as cur:
            cur.execute(f"UPDATE items SET {', '.join(fields)} WHERE id = ?", params)
            self._conn.commit()

    def mark_items_by_path(
        self,
        path_prefix: str,
        *,
        monitored: Optional[bool] = None,
        ignored: Optional[bool] = None,
    ) -> int:
        if monitored is None and ignored is None:
            return 0
        prefix = path_prefix.rstrip("/")
        if prefix and not prefix.startswith("/"):
            prefix = f"/{prefix}"
        updates: List[str] = []
        params: List[object] = []
        if monitored is not None:
            updates.append("monitored = ?")
            params.append(1 if monitored else 0)
            if monitored:
                updates.append("ignored = 0")
        if ignored is not None:
            updates.append("ignored = ?")
            params.append(1 if ignored else 0)
            if ignored:
                updates.append("monitored = 0")
        where_clause = ""
        where_params: List[object] = []
        if prefix:
            like_param = f"{prefix}/%"
            where_clause = "WHERE path = ? OR path LIKE ?"
            where_params.extend([prefix, like_param])
        with self.cursor() as cur:
            sql = f"UPDATE items SET {', '.join(updates)}"
            if where_clause:
                sql = f"{sql} {where_clause}"
                cur.execute(sql, params + where_params)
            else:
                cur.execute(sql, params)
            self._conn.commit()
            return cur.rowcount

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
        query = f"""
            SELECT
                items.*,
                latest.file_path AS last_download_path,
                latest.downloaded_at AS last_downloaded_at,
                latest.observed_date_in_table AS last_download_observed_date
            FROM items
            LEFT JOIN (
                SELECT d.item_id,
                       d.file_path,
                       d.downloaded_at,
                       d.observed_date_in_table
                FROM downloads AS d
                INNER JOIN (
                    SELECT item_id, MAX(datetime(downloaded_at)) AS max_downloaded_at
                    FROM downloads
                    GROUP BY item_id
                ) AS latest_meta
                ON latest_meta.item_id = d.item_id
                AND datetime(d.downloaded_at) = latest_meta.max_downloaded_at
            ) AS latest
            ON latest.item_id = items.id
            {where}
            ORDER BY COALESCE(datetime(items.last_seen_at), datetime('1970-01-01T00:00:00')) DESC, items.title
        """
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

    def record_download(
        self,
        item_id: int,
        file_path: str,
        sha256: Optional[str],
        size: int,
        now: datetime,
        observed_date: Optional[str],
    ) -> int:
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO downloads(item_id, file_path, sha256, size, downloaded_at, observed_date_in_table)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (item_id, file_path, sha256, size, now.strftime(ISO_FORMAT), observed_date),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def get_latest_download(self, item_id: int) -> Optional[sqlite3.Row]:
        with self.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM downloads
                WHERE item_id = ?
                ORDER BY datetime(downloaded_at) DESC
                LIMIT 1
                """,
                (item_id,),
            )
            return cur.fetchone()

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
