"""Flask application entry point."""
from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timedelta
from typing import Callable, Optional

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


class ScanManager:
    def __init__(self, db: Database) -> None:
        self.db = db
        self.interval_key = db.get_setting("scan_interval", "6h") or "6h"
        self.interval = INTERVALS.get(self.interval_key, INTERVALS["6h"])
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.last_scan_at: Optional[datetime] = None
        self.next_scan_at: Optional[datetime] = None
        self._callback: Optional[Callable[[], detector.ScanResult]] = None

    def start(self, callback: Callable[[], detector.ScanResult]) -> None:
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
            if result.errors:
                logger.error("Scan completed with errors: %s", result.errors)
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Scheduled scan failed: %s", exc)


SCAN_MANAGER = ScanManager(DB)


def run_scan() -> detector.ScanResult:
    now = utcnow()
    try:
        scraped = crawler.fetch_items()
    except Exception as exc:
        logger.exception("Failed to fetch items: %s", exc)
        result = detector.ScanResult()
        result.errors.append(str(exc))
        SCAN_MANAGER.last_scan_at = now
        SCAN_MANAGER.schedule_next(now)
        return result

    result = detector.process_scan(DB, scraped, now=now)
    for item_id in result.new_items + result.updated_items:
        row = DB.get_item(item_id)
        if not row:
            continue
        if not row["file_url"] or not row["monitored"] or row["ignored"]:
            continue
        try:
            download_item(DB, item_id, row["file_url"], row["last_seen_date"])
        except DownloadError as exc:
            logger.error("Download failed for item %s: %s", item_id, exc)
            result.errors.append(str(exc))
    SCAN_MANAGER.last_scan_at = now
    SCAN_MANAGER.schedule_next(now)
    return result


def create_app() -> Flask:
    app = Flask(__name__)
    app.secret_key = "kais-monitor-secret"

    SCAN_MANAGER.start(run_scan)

    @app.route("/")
    def dashboard() -> str:
        stats = DB.get_stats()
        return render_template(
            "dashboard.html",
            stats=stats,
            last_scan=SCAN_MANAGER.last_scan_at,
            next_scan=SCAN_MANAGER.next_scan_at,
        )

    @app.post("/scan")
    def manual_scan() -> Response:
        result = run_scan()
        if result.errors:
            flash("Scan completed with errors", "error")
        else:
            flash("Scan complete", "success")
        return redirect(url_for("dashboard"))

    @app.route("/items")
    def items() -> str:
        monitored = request.args.get("monitored")
        status = request.args.get("status")
        monitored_flag: Optional[bool] = None
        if monitored is not None:
            monitored_flag = monitored.lower() in {"1", "true", "yes"}
        rows = DB.get_items(monitored=monitored_flag, status=status)
        return render_template(
            "items.html",
            items=rows,
            monitored_filter=monitored,
            status_filter=status,
            last_scan=SCAN_MANAGER.last_scan_at,
        )

    @app.post("/items/<int:item_id>/monitor")
    def toggle_monitor(item_id: int) -> Response:
        data = request.get_json(force=True)
        monitored = bool(data.get("monitored"))
        DB.mark_item_flags(item_id, monitored=monitored)
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
        return render_template("history.html", item=item, events=events, last_scan=SCAN_MANAGER.last_scan_at)

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
        return render_template("settings.html", current=SCAN_MANAGER.interval_key, last_scan=SCAN_MANAGER.last_scan_at)

    @app.post("/settings/interval")
    def update_interval() -> Response:
        data = request.get_json(force=True)
        value = data.get("value")
        if value not in INTERVALS:
            return jsonify({"ok": False, "error": "Invalid interval"}), 400
        SCAN_MANAGER.set_interval(value)
        return jsonify({"ok": True})

    return app


def main() -> None:
    app = create_app()
    app.run(host="0.0.0.0", port=5000, debug=False)


if __name__ == "__main__":
    main()
