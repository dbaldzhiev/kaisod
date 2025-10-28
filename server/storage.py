"""Helpers for mapping KAIS paths to local storage."""
from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import unquote, urlparse

_SANITIZE_RE = re.compile(r"[^0-9A-Za-z._()\-\s]+")


def _sanitize_segment(segment: str) -> str:
    """Return a filesystem-safe segment preserving human readability."""

    cleaned = _SANITIZE_RE.sub("_", segment.strip())
    cleaned = cleaned.strip()
    if not cleaned:
        return "segment"
    return cleaned


def _normalized_segments(
    raw_path: Optional[str],
    *,
    title: Optional[str],
    item_id: int,
) -> List[str]:
    """Normalize the KAIS path into sanitized path segments."""

    candidate = (raw_path or "").strip()
    if not candidate and title:
        candidate = title.replace(" / ", "/")
    candidate = candidate.replace("\\", "/").strip("/")
    pieces = [piece for piece in candidate.split("/") if piece and piece not in {".", ".."}]
    sanitized = [_sanitize_segment(piece) for piece in pieces if _sanitize_segment(piece)]
    if sanitized:
        return sanitized
    fallback_title = title or f"item-{item_id}"
    return [_sanitize_segment(fallback_title) or f"item-{item_id}"]


def _derive_filename(
    file_url: Optional[str],
    *,
    fallback: str,
    item_id: int,
) -> str:
    """Return a safe filename based on the download URL or fallback value."""

    if file_url:
        parsed = urlparse(file_url)
        candidate = Path(unquote(parsed.path or "")).name
        if candidate:
            cleaned = _sanitize_segment(candidate)
            if cleaned:
                return cleaned
    cleaned_fallback = _sanitize_segment(fallback)
    if cleaned_fallback:
        return cleaned_fallback
    return f"download-{item_id}"


def resolve_item_storage(
    base_path: Path,
    *,
    raw_path: Optional[str],
    title: Optional[str],
    file_url: Optional[str],
    item_id: int,
) -> Tuple[Path, Path, str]:
    """Compute the root directory, relative path, and archive filename for an item."""

    segments = _normalized_segments(raw_path, title=title, item_id=item_id)
    directory_segments = segments[:-1]
    leaf = segments[-1]

    relative_path = Path(*directory_segments, leaf) if directory_segments else Path(leaf)
    root = base_path.joinpath(relative_path)
    filename = _derive_filename(file_url, fallback=leaf, item_id=item_id)
    return root, relative_path, filename
