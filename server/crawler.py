"""Crawler utilities for scraping Kais OpenData."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL = "https://kais.cadastre.bg"
OPEN_DATA_PATH = "/bg/OpenData"
DATE_FORMATS = [
    "%d.%m.%Y",
    "%d.%m.%y",
]


@dataclass
class ScrapedItem:
    title: str
    date_text: str
    date: datetime
    source_url: str
    file_url: Optional[str]


def fetch_html(session: Optional[requests.Session] = None) -> str:
    sess = session or requests.Session()
    url = f"{BASE_URL}{OPEN_DATA_PATH}"
    resp = sess.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def parse_date(text: str) -> datetime:
    cleaned = text.strip().replace("\xa0", " ")
    if cleaned.lower().startswith("на "):
        cleaned = cleaned[3:].strip()
    if cleaned.endswith(" г.") or cleaned.endswith(" г"):
        cleaned = cleaned[: -3].strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unable to parse date: {text}")


def extract_items(html: str) -> List[ScrapedItem]:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        raise ValueError("Could not find data table on page")

    items: List[ScrapedItem] = []
    for row in table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 3:
            continue
        title_cell = cells[0]
        date_cell = cells[1]
        link_cell = cells[2]

        title = " ".join(title_cell.get_text(strip=True).split())
        date_text = date_cell.get_text(strip=True)
        if not title or not date_text:
            continue
        try:
            date_value = parse_date(date_text)
        except ValueError as exc:
            logger.warning("Skipping row with unparsable date %s: %s", date_text, exc)
            continue

        link = link_cell.find("a")
        file_url: Optional[str] = None
        if link and link.get("href"):
            href = link["href"].strip()
            if href.startswith("http"):
                file_url = href
            else:
                file_url = f"{BASE_URL}{href}" if href.startswith("/") else f"{BASE_URL}/{href}"

        source_link = title_cell.find("a")
        if source_link and source_link.get("href"):
            href = source_link["href"].strip()
            if href.startswith("http"):
                source_url = href
            else:
                source_url = f"{BASE_URL}{href}" if href.startswith("/") else f"{BASE_URL}/{href}"
        else:
            source_url = f"{BASE_URL}{OPEN_DATA_PATH}"

        items.append(
            ScrapedItem(
                title=title,
                date_text=date_text,
                date=date_value,
                source_url=source_url,
                file_url=file_url,
            )
        )
    return items


def fetch_items(session: Optional[requests.Session] = None) -> List[ScrapedItem]:
    html = fetch_html(session=session)
    return extract_items(html)


def serialize(item: ScrapedItem) -> dict:
    return {
        "title": item.title,
        "date_text": item.date_text,
        "date_iso": item.date.isoformat(),
        "source_url": item.source_url,
        "file_url": item.file_url,
    }
