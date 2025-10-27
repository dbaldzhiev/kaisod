"""Detection logic for new or updated items."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

from . import models
from .crawler import ScrapedItem
from .time_utils import utcnow


class ScanResult:
    """Represents the outcome of a scan."""

    def __init__(self) -> None:
        self.new_items: List[int] = []
        self.updated_items: List[int] = []
        self.unchanged_items: List[int] = []
        self.errors: List[str] = []

    def to_dict(self) -> Dict[str, List[int]]:
        return {
            "new_items": self.new_items,
            "updated_items": self.updated_items,
            "unchanged_items": self.unchanged_items,
            "errors": self.errors,
        }


def process_scan(db: models.Database, scraped_items: Iterable[ScrapedItem], now: Optional[datetime] = None) -> ScanResult:
    """Apply detection rules against the database."""
    now = now or utcnow()
    result = ScanResult()

    for item in scraped_items:
        row = db.get_item_by_identity(item.title, item.file_url)
        observed_dt = item.date
        if observed_dt.tzinfo is not None:
            observed_dt = observed_dt.astimezone(timezone.utc).replace(tzinfo=None)
        observed_date = observed_dt.isoformat(timespec="seconds")
        if row is None:
            item_id = db.create_item(
                title=item.title,
                source_url=item.source_url,
                file_url=item.file_url,
                path=item.path,
                observed_date=observed_date,
                now=now,
            )
            db.add_event(item_id, "NEW", observed_date, now)
            db.update_item_seen(item_id, observed_date, now, "new", path=item.path)
            result.new_items.append(item_id)
            continue

        last_seen_date = row["last_seen_date"]
        if last_seen_date is None or observed_date > last_seen_date:
            db.add_event(int(row["id"]), "UPDATED", observed_date, now)
            db.update_item_seen(int(row["id"]), observed_date, now, "updated", path=item.path)
            result.updated_items.append(int(row["id"]))
        else:
            db.update_item_seen(int(row["id"]), last_seen_date, now, "seen", path=item.path)
            result.unchanged_items.append(int(row["id"]))

    return result
