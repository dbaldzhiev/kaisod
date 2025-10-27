"""Crawler utilities for scraping Kais OpenData."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Dict, Iterable, List, Optional

import requests
from bs4 import BeautifulSoup
from urllib.parse import quote

logger = logging.getLogger(__name__)

BASE_URL = "https://kais.cadastre.bg"
OPEN_DATA_PATH = "/bg/OpenData"
READ_ENDPOINT = f"{BASE_URL}{OPEN_DATA_PATH}/Read"
DOWNLOAD_ENDPOINT = f"{BASE_URL}{OPEN_DATA_PATH}/Download"
TOKEN_FIELD = "__RequestVerificationToken"
REQUEST_TIMEOUT = 30


@dataclass
class ScrapedItem:
    title: str
    date_text: str
    date: datetime
    source_url: str
    file_url: Optional[str]
    path: str


def fetch_html(session: Optional[requests.Session] = None) -> str:
    sess = session or requests.Session()
    url = f"{BASE_URL}{OPEN_DATA_PATH}"
    resp = sess.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def _extract_token(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    token_input = soup.find("input", {"name": TOKEN_FIELD})
    if not token_input or not token_input.get("value"):
        raise ValueError("Could not locate verification token on page")
    return token_input["value"].strip()


def _request_listing(session: requests.Session, token: str, path: str) -> List[Dict[str, object]]:
    data: Dict[str, object] = {TOKEN_FIELD: token, "path": path or "/"}
    if data["path"] != "/":
        data["target"] = data["path"]
    resp = session.post(READ_ENDPOINT, data=data, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    try:
        payload = resp.json()
    except ValueError as exc:  # pragma: no cover - defensive
        raise ValueError("Received invalid JSON from listing endpoint") from exc

    if isinstance(payload, dict):
        for key in ("data", "Data"):
            if key in payload and isinstance(payload[key], list):
                return payload[key]
        raise ValueError("Unexpected payload structure from listing endpoint")
    if isinstance(payload, list):
        return payload
    raise ValueError("Unexpected payload type from listing endpoint")


ProgressCallback = Callable[[str, Dict[str, object]], None]


def _iter_file_entries(
    session: requests.Session,
    token: str,
    progress: Optional[ProgressCallback] = None,
) -> Iterable[Dict[str, object]]:
    stack: List[str] = ["/"]
    visited: set[str] = set()
    while stack:
        current = stack.pop()
        if current in visited:
            continue
        visited.add(current)
        entries = _request_listing(session, token, current)
        if progress:
            progress(
                "listing",
                {
                    "path": current,
                    "entries": len(entries),
                },
            )
        for entry in entries:
            path = entry.get("Path")
            if not isinstance(path, str) or not path:
                continue
            if entry.get("IsDirectory"):
                stack.append(path)
                continue
            yield entry


def _build_item(entry: Dict[str, object]) -> ScrapedItem:
    path = str(entry["Path"])
    raw_timestamp = entry.get("Modified") or entry.get("Created")
    if not isinstance(raw_timestamp, str):
        raw_timestamp = entry.get("ModifiedUtc") or entry.get("CreatedUtc")
    if not isinstance(raw_timestamp, str):
        raise ValueError(f"Missing timestamp for entry {path}")
    try:
        observed = datetime.fromisoformat(raw_timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Unable to parse timestamp '{raw_timestamp}' for {path}") from exc

    title = path.replace("/", " / ")
    date_text = observed.strftime("%d.%m.%Y %H:%M")
    download_url = f"{DOWNLOAD_ENDPOINT}?path={quote(path, safe='/()')}"

    return ScrapedItem(
        title=title,
        date_text=date_text,
        date=observed,
        source_url=f"{BASE_URL}{OPEN_DATA_PATH}",
        file_url=download_url,
        path=path,
    )


def extract_items(
    html: str,
    session: requests.Session,
    progress: Optional[ProgressCallback] = None,
) -> List[ScrapedItem]:
    token = _extract_token(html)
    items: List[ScrapedItem] = []
    for entry in _iter_file_entries(session, token, progress):
        try:
            item = _build_item(entry)
        except ValueError as exc:
            logger.warning("Skipping entry %s: %s", entry.get("Path"), exc)
            continue
        items.append(item)
        if progress:
            progress(
                "file",
                {
                    "path": item.path,
                    "count": len(items),
                },
            )
    items.sort(key=lambda item: item.title)
    return items


def fetch_items(
    session: Optional[requests.Session] = None,
    progress: Optional[ProgressCallback] = None,
) -> List[ScrapedItem]:
    sess = session or requests.Session()
    if progress:
        progress("start", {"message": "Fetching OpenData root"})
    html = fetch_html(session=sess)
    if progress:
        progress("token", {"message": "Token extracted"})
    return extract_items(html, sess, progress)


def serialize(item: ScrapedItem) -> dict:
    return {
        "title": item.title,
        "date_text": item.date_text,
        "date_iso": item.date.isoformat(),
        "source_url": item.source_url,
        "file_url": item.file_url,
        "path": item.path,
    }
