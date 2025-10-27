"""File download helper."""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Optional

import requests

from .models import Database, ensure_storage
from .time_utils import utcnow


class DownloadError(Exception):
    """Raised when downloading a monitored item fails."""


def compute_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def download_item(
    db: Database,
    item_id: int,
    file_url: str,
    observed_date: str,
    session: Optional[requests.Session] = None,
) -> Optional[int]:
    """Download the file for the monitored item and persist metadata."""
    session = session or requests.Session()
    resp = session.get(file_url, timeout=60)
    if resp.status_code != 200:
        raise DownloadError(f"Failed to download {file_url}: {resp.status_code}")

    item = db.get_item(item_id)
    if not item:
        return None

    date_dir = observed_date
    item_dir = ensure_storage(str(db.base_path / str(item_id)))
    target_dir = ensure_storage(str(item_dir / date_dir))

    filename = file_url.split("/")[-1] or f"download-{item_id}"
    file_path = Path(target_dir) / filename
    file_path.write_bytes(resp.content)

    sha256 = compute_sha256(file_path)
    size = file_path.stat().st_size
    now = utcnow()
    download_id = db.record_download(item_id, str(file_path), sha256, size, now)
    return download_id
