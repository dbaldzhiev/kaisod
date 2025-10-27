"""Flask application entry point."""
from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional

from flask import Flask, Response, flash, jsonify, redirect, render_template, request, send_file, url_for

if __package__ in (None, ""):
    import sys

    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PARENT_DIR = os.path.dirname(CURRENT_DIR)
    if PARENT_DIR not in sys.path:
        sys.path.insert(0, PARENT_DIR)

    from server import crawler, detector  # type: ignore[no-redef]
    from server.downloader import DownloadError, download_item  # type: ignore[no-redef]
    from server.models import Database, ensure_storage  # type: ignore[no-redef]
    from server.time_utils import utcnow  # type: ignore[no-redef]
else:
    from . import crawler, detector
    from .downloader import DownloadError, download_item
    from .models import Database, ensure_storage
    from .time_utils import utcnow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

INTERVALS = {
    "6h": timedelta(hours=6),
    "1d": timedelta(days=1),
    "6d": timedelta(days=6),
}

DEFAULT_BASE = os.environ.get("KAIS_MONITOR_BASE", "/var/lib/kais-monitor")
BASE_PATH = ensure_storage(str(DEFAULT_BASE))
DB = Database(base_path=str(BASE_PATH))

ScanCallable = Callable[[], Optional[detector.ScanResult]]


def serialize_datetime(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.isoformat()


def normalize_path(raw_path: Optional[str], title: str) -> str:
    candidate = (raw_path or "").strip()
    if not candidate:
        candidate = title.replace(" / ", "/")
    if not candidate.startswith("/"):
        candidate = "/" + candidate.lstrip("/")
    return candidate.rstrip("/") or "/"


def compute_stats(node: "TreeNode") -> None:
    if node.node_type == "file":
        node.total = 1
        monitored = bool(node.item and node.item.get("monitored"))
        ignored = bool(node.item and node.item.get("ignored"))
        node.monitored = 1 if monitored else 0
        node.ignored = 1 if ignored else 0
        node.monitor_state = "all" if monitored else "none"
        node.ignore_state = "all" if ignored else "none"
        sync_state = node.item.get("sync_state") if node.item else None
        node.synced = 1 if sync_state == "synced" else 0
        node.outdated = 1 if sync_state == "outdated" else 0
        node.missing = 1 if sync_state in {"missing", "missing-file"} else 0
        return

    total = 0
    monitored_total = 0
    ignored_total = 0
    synced_total = 0
    outdated_total = 0
    missing_total = 0
    for child in node.children.values():
        compute_stats(child)
        total += child.total
        monitored_total += child.monitored
        ignored_total += child.ignored
        synced_total += child.synced
        outdated_total += child.outdated
        missing_total += child.missing

    node.total = total
    node.monitored = monitored_total
    node.ignored = ignored_total
    node.synced = synced_total
    node.outdated = outdated_total
    node.missing = missing_total
    if total <= 0:
        node.monitor_state = "none"
        node.ignore_state = "none"
        return
    if monitored_total == total:
        node.monitor_state = "all"
    elif monitored_total == 0:
        node.monitor_state = "none"
    else:
        node.monitor_state = "partial"

    if ignored_total == total:
        node.ignore_state = "all"
    elif ignored_total == 0:
        node.ignore_state = "none"
    else:
        node.ignore_state = "partial"


def prune_empty(node: "TreeNode") -> None:
    for key, child in list(node.children.items()):
        if child.node_type == "directory":
            prune_empty(child)
            if child.total == 0:
                del node.children[key]


def build_tree(rows: List[dict]) -> "TreeNode":
    root = TreeNode(name="root", path="/", node_type="directory")
    for row in rows:
        title = row.get("title", "")
        normalized = normalize_path(row.get("path"), title)
        segments = [segment for segment in normalized.split("/") if segment]
        if not segments:
            continue
        node = root
        current_path = ""
        for index, segment in enumerate(segments):
            current_path = f"{current_path}/{segment}" if current_path else f"/{segment}"
            if index == len(segments) - 1:
                node.children[segment] = TreeNode(
                    name=segment,
                    path=current_path,
                    node_type="file",
                    item=row,
                )
            else:
                if segment not in node.children:
                    node.children[segment] = TreeNode(
                        name=segment,
                        path=current_path,
                        node_type="directory",
                    )
                node = node.children[segment]
    compute_stats(root)
    prune_empty(root)
    return root


@dataclass
class TreeNode:
    name: str
    path: str
    node_type: str  # "directory" or "file"
    item: Optional[dict] = None
    children: Dict[str, "TreeNode"] = field(default_factory=dict)
    total: int = 0
    monitored: int = 0
    ignored: int = 0
    monitor_state: str = "none"
    ignore_state: str = "none"
    synced: int = 0
    outdated: int = 0
    missing: int = 0

    def sorted_children(self) -> List["TreeNode"]:
        return sorted(
            self.children.values(),
            key=lambda node: (0 if node.node_type == "directory" else 1, node.name.lower()),
        )

    @property
    def unsynced(self) -> int:
        return self.outdated + self.missing


def annotate_item(entry: dict, base_path: Path) -> None:
    """Enrich an item row with local storage metadata and sync state."""

    entry["monitored"] = bool(entry.get("monitored"))
    entry["ignored"] = bool(entry.get("ignored"))

    last_download_at = entry.get("last_downloaded_at")
    if isinstance(last_download_at, datetime):
        entry["last_downloaded_at"] = last_download_at.isoformat()
    elif last_download_at is not None:
        entry["last_downloaded_at"] = str(last_download_at)

    last_observed = entry.get("last_download_observed_date")
    if isinstance(last_observed, datetime):
        entry["last_download_observed_date"] = last_observed.isoformat()
    elif last_observed is not None:
        entry["last_download_observed_date"] = str(last_observed)

    local_root = base_path / str(entry.get("id"))
    latest_extract = local_root / "latest"
    entry["local_root"] = str(local_root)
    entry["latest_extract_path"] = str(latest_extract)
    entry["latest_extract_exists"] = latest_extract.exists()

    last_download_path = entry.get("last_download_path")
    download_exists = bool(last_download_path and Path(str(last_download_path)).exists())
    entry["last_download_exists"] = download_exists

    has_local_files = download_exists or entry["latest_extract_exists"]
    entry["has_local_files"] = has_local_files

    monitored = entry["monitored"]
    last_seen = entry.get("last_seen_date")
    observed_download = entry.get("last_download_observed_date")

    if not monitored:
        state = "not-monitored"
        label = "Not monitored"
    elif not has_local_files or not entry.get("last_downloaded_at"):
        state = "missing"
        label = "Not downloaded"
    else:
        if observed_download and last_seen and str(observed_download) < str(last_seen):
            state = "outdated"
            label = "Needs sync"
        elif observed_download is None and last_seen:
            state = "outdated"
            label = "Needs sync"
        else:
            state = "synced"
            label = "Up to date"

    entry["sync_state"] = state
    entry["sync_label"] = label


class ScanManager:
    def __init__(self, db: Database) -> None:
        self.db = db
        self.interval_key = db.get_setting("scan_interval", "6h") or "6h"
        self.interval = INTERVALS.get(self.interval_key, INTERVALS["6h"])
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.last_scan_at: Optional[datetime] = None
        self.next_scan_at: Optional[datetime] = None
        self._callback: Optional[ScanCallable] = None
        self._is_running = False
        self.progress: Dict[str, Optional[object]] = {
            "stage": "idle",
            "message": "Idle",
            "current_path": None,
            "processed": 0,
            "total": 0,
        }
        self.last_result: Optional[Dict[str, int]] = None

    def start(self, callback: ScanCallable) -> None:
        self._callback = callback
        if self.thread and self.thread.is_alive():
            return
        self.schedule_next(utcnow())
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=2)

    def set_interval(self, key: str) -> None:
        with self.lock:
            self.interval_key = key
            self.interval = INTERVALS[key]
            self.db.set_setting("scan_interval", key)
            self.schedule_next(utcnow())

    def schedule_next(self, base_time: Optional[datetime] = None) -> None:
        with self.lock:
            base = base_time or utcnow()
            self.next_scan_at = base + self.interval

    def begin_scan(self) -> bool:
        with self.lock:
            if self._is_running:
                return False
            self._is_running = True
            self.progress.update(
                {
                    "stage": "starting",
                    "message": "Starting scan",
                    "current_path": None,
                    "processed": 0,
                    "total": 0,
                }
            )
            return True

    def complete_scan(self, finished_at: datetime, summary: Dict[str, int]) -> None:
        with self.lock:
            self.last_scan_at = finished_at
            self.last_result = summary
            self.schedule_next(finished_at)
            self.progress.update(
                {
                    "stage": "idle",
                    "message": "Idle",
                    "current_path": None,
                    "processed": 0,
                    "total": 0,
                }
            )
            self._is_running = False

    def update_progress(
        self,
        *,
        stage: Optional[str] = None,
        message: Optional[str] = None,
        current_path: Optional[str] = None,
        processed: Optional[int] = None,
        total: Optional[int] = None,
    ) -> None:
        with self.lock:
            if stage is not None:
                self.progress["stage"] = stage
            if message is not None:
                self.progress["message"] = message
            if current_path is not None:
                self.progress["current_path"] = current_path
            if processed is not None:
                self.progress["processed"] = processed
            if total is not None:
                self.progress["total"] = total

    def crawler_progress(self, stage: str, payload: Dict[str, object]) -> None:
        if stage == "start":
            self.update_progress(stage="fetch", message=str(payload.get("message")))
        elif stage == "token":
            self.update_progress(stage="fetch", message=str(payload.get("message")))
        elif stage == "listing":
            path = payload.get("path") or "/"
            entries = payload.get("entries")
            message = f"Listing {path} ({entries} entries)"
            self.update_progress(stage="listing", message=message, current_path=str(path))
        elif stage == "file":
            path = payload.get("path") or ""
            count = int(payload.get("count", 0))
            message = f"Parsed {count} files"
            self.update_progress(
                stage="parsing",
                message=message,
                current_path=str(path),
                processed=count,
                total=count,
            )

    def is_running(self) -> bool:
        with self.lock:
            return self._is_running

    def get_status(self) -> Dict[str, object]:
        with self.lock:
            return {
                "status": "running" if self._is_running else "idle",
                "last_scan_at": self.last_scan_at,
                "next_scan_at": self.next_scan_at,
                "progress": dict(self.progress),
                "last_result": self.last_result,
            }

    def trigger_manual(self, callback: ScanCallable) -> bool:
        if self.is_running():
            return False

        def _runner() -> None:
            callback()

        thread = threading.Thread(target=_runner, daemon=True)
        thread.start()
        return True

    def _run(self) -> None:
        while not self.stop_event.is_set():
            with self.lock:
                next_time = self.next_scan_at
            if next_time is None:
                self.schedule_next()
                continue
            wait = max(0, (next_time - utcnow()).total_seconds())
            if self.stop_event.wait(wait):
                break
            self._execute_scan()

    def _execute_scan(self) -> None:
        if not self._callback:
            return
        try:
            result = self._callback()
            if result and result.errors:
                logger.error("Scan completed with errors: %s", result.errors)
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Scheduled scan failed: %s", exc)


SCAN_MANAGER = ScanManager(DB)


def run_scan() -> Optional[detector.ScanResult]:
    if not SCAN_MANAGER.begin_scan():
        logger.info("Scan requested while another is running; skipping")
        return None

    now = utcnow()
    result = detector.ScanResult()
    scraped: List[crawler.ScrapedItem] = []

    try:
        try:
            scraped = crawler.fetch_items(progress=SCAN_MANAGER.crawler_progress)
        except Exception as exc:
            logger.exception("Failed to fetch items: %s", exc)
            result.errors.append(str(exc))
            SCAN_MANAGER.update_progress(
                stage="error",
                message=f"Fetch failed: {exc}",
            )
        else:
            total_scraped = len(scraped)
            SCAN_MANAGER.update_progress(
                stage="processing",
                message=f"Processing {total_scraped} entries",
                processed=0,
                total=total_scraped,
                current_path=None,
            )
            result = detector.process_scan(DB, scraped, now=now)
            SCAN_MANAGER.update_progress(
                stage="processing",
                message=(
                    f"Detected {len(result.new_items)} new / {len(result.updated_items)} updated entries"
                ),
                processed=total_scraped,
                total=total_scraped,
                current_path=None,
            )

            downloads = [
                item_id
                for item_id in result.new_items + result.updated_items
                if item_id is not None
            ]
            total_downloads = len(downloads)
            if total_downloads:
                for index, item_id in enumerate(downloads, start=1):
                    row = DB.get_item(item_id)
                    if not row:
                        continue
                    if not row["file_url"] or not row["monitored"] or row["ignored"]:
                        continue
                    path = row["path"] or row["title"]
                    SCAN_MANAGER.update_progress(
                        stage="downloading",
                        message=f"Downloading {path}",
                        processed=index,
                        total=total_downloads,
                        current_path=path,
                    )
                    try:
                        download_item(DB, item_id, row["file_url"], row["last_seen_date"])
                    except DownloadError as exc:
                        logger.error("Download failed for item %s: %s", item_id, exc)
                        result.errors.append(str(exc))
                SCAN_MANAGER.update_progress(
                    stage="downloading",
                    message="Downloads complete",
                    processed=total_downloads,
                    total=total_downloads,
                    current_path=None,
                )
            else:
                SCAN_MANAGER.update_progress(
                    stage="processing",
                    message="No monitored changes detected",
                    processed=total_scraped,
                    total=total_scraped,
                    current_path=None,
                )
    finally:
        summary = {
            "new": len(result.new_items),
            "updated": len(result.updated_items),
            "unchanged": len(result.unchanged_items),
            "errors": len(result.errors),
        }
        SCAN_MANAGER.complete_scan(now, summary)
    return result


def create_app() -> Flask:
    app = Flask(__name__)
    app.secret_key = "kais-monitor-secret"

    SCAN_MANAGER.start(run_scan)

    @app.template_filter("format_datetime")
    def format_datetime_filter(value: Optional[object]) -> str:
        if value in (None, ""):
            return "—"
        if isinstance(value, datetime):
            dt_value = value
        else:
            try:
                dt_value = datetime.fromisoformat(str(value))
            except ValueError:
                return str(value)
        return dt_value.strftime("%d.%m.%Y %H:%M")

    @app.context_processor
    def inject_status() -> Dict[str, object]:
        return {"scan_status": SCAN_MANAGER.get_status()}

    @app.route("/")
    def dashboard() -> str:
        stats = DB.get_stats()
        status = SCAN_MANAGER.get_status()
        return render_template("dashboard.html", stats=stats, status=status)

    @app.post("/scan")
    def manual_scan() -> Response:
        if SCAN_MANAGER.is_running():
            flash("A scan is already in progress", "info")
        elif SCAN_MANAGER.trigger_manual(run_scan):
            flash("Scan started", "success")
        else:
            flash("Unable to start scan", "error")
        return redirect(url_for("dashboard"))

    @app.route("/items")
    def items() -> str:
        monitored = request.args.get("monitored")
        status = request.args.get("status")
        monitored_flag: Optional[bool] = None
        if monitored is not None:
            monitored_flag = monitored.lower() in {"1", "true", "yes"}
        rows = DB.get_items(monitored=monitored_flag, status=status)
        row_dicts = [dict(row) for row in rows]
        for entry in row_dicts:
            annotate_item(entry, DB.base_path)
        tree_root = build_tree(row_dicts)
        return render_template(
            "items.html",
            items=row_dicts,
            tree=tree_root,
            monitored_filter=monitored,
            status_filter=status,
            storage_root=str(DB.base_path),
        )

    @app.route("/monitored")
    def monitored_items() -> str:
        rows = DB.get_items(monitored=True)
        items = [dict(row) for row in rows]
        for entry in items:
            annotate_item(entry, DB.base_path)
        return render_template("monitored.html", items=items, storage_root=str(DB.base_path))

    @app.post("/items/<int:item_id>/monitor")
    def toggle_monitor(item_id: int) -> Response:
        data = request.get_json(force=True)
        monitored = bool(data.get("monitored"))
        ignored = data.get("ignored")
        if ignored is not None:
            ignored_flag = bool(ignored)
        else:
            ignored_flag = not monitored
        DB.mark_item_flags(item_id, monitored=monitored, ignored=ignored_flag)
        return jsonify({"ok": True})

    @app.post("/items/<int:item_id>/ignore")
    def toggle_ignore(item_id: int) -> Response:
        data = request.get_json(force=True)
        ignored = bool(data.get("ignored"))
        DB.mark_item_flags(item_id, ignored=ignored)
        return jsonify({"ok": True})

    @app.get("/items/<int:item_id>/history")
    def item_history(item_id: int) -> str:
        item = DB.get_item(item_id)
        if not item:
            flash("Item not found", "error")
            return redirect(url_for("items"))
        events = DB.get_events_for_item(item_id)
        return render_template("history.html", item=item, events=events)

    @app.get("/downloads/<int:download_id>")
    def download(download_id: int):
        entry = DB.get_download(download_id)
        if not entry:
            flash("Download not found", "error")
            return redirect(url_for("items"))
        return send_file(entry["file_path"], as_attachment=True)

    @app.get("/export.csv")
    def export_csv() -> Response:
        rows = DB.get_items()
        csv_lines = ["id,title,last_seen_date,status,monitored,ignored"]
        for row in rows:
            csv_lines.append(
                ",".join(
                    [
                        str(row["id"]),
                        f'"{row["title"].replace("\"", "''")}"',
                        row["last_seen_date"] or "",
                        row["status"],
                        "1" if row["monitored"] else "0",
                        "1" if row["ignored"] else "0",
                    ]
                )
            )
        return Response("\n".join(csv_lines), mimetype="text/csv")

    @app.get("/export.json")
    def export_json() -> Response:
        rows = DB.get_items()
        payload = []
        for row in rows:
            history = [dict(event) for event in DB.get_events_for_item(row["id"])]
            payload.append({**dict(row), "events": history})
        return Response(json.dumps(payload, ensure_ascii=False, indent=2), mimetype="application/json")

    @app.get("/settings")
    def settings() -> str:
        return render_template("settings.html", current=SCAN_MANAGER.interval_key)

    @app.post("/settings/interval")
    def update_interval() -> Response:
        data = request.get_json(force=True)
        value = data.get("value")
        if value not in INTERVALS:
            return jsonify({"ok": False, "error": "Invalid interval"}), 400
        SCAN_MANAGER.set_interval(value)
        return jsonify({"ok": True})

    @app.post("/sections/monitor")
    def section_monitor() -> Response:
        data = request.get_json(force=True)
        path = str(data.get("path") or "/")
        monitored = bool(data.get("monitored"))
        ignored_value = data.get("ignored")
        ignored = bool(ignored_value) if ignored_value is not None else (not monitored)
        updated = DB.mark_items_by_path(path, monitored=monitored, ignored=ignored)
        return jsonify({"ok": True, "updated": updated})

    @app.post("/sections/ignore")
    def section_ignore() -> Response:
        data = request.get_json(force=True)
        path = str(data.get("path") or "/")
        ignored = bool(data.get("ignored"))
        updated = DB.mark_items_by_path(path, ignored=ignored)
        return jsonify({"ok": True, "updated": updated})

    @app.get("/scan/status")
    def scan_status_api() -> Response:
        status = SCAN_MANAGER.get_status()
        payload = {
            "status": status.get("status"),
            "last_scan_at": serialize_datetime(status.get("last_scan_at")),
            "next_scan_at": serialize_datetime(status.get("next_scan_at")),
            "progress": status.get("progress"),
            "last_result": status.get("last_result"),
        }
        return jsonify(payload)

    return app


def main() -> None:
    app = create_app()
    app.run(host="0.0.0.0", port=5000, debug=False)


if __name__ == "__main__":
    main()
