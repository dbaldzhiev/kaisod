"""Helpers for mapping KAIS paths to local storage."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

_SANITIZE_RE = re.compile(r"[^0-9A-Za-z._()\-]+")

_CYRILLIC_MAP: Dict[str, str] = {
    # Uppercase
    "А": "A",
    "Б": "B",
    "В": "V",
    "Г": "G",
    "Д": "D",
    "Е": "E",
    "Ж": "Zh",
    "З": "Z",
    "И": "I",
    "Й": "Y",
    "К": "K",
    "Л": "L",
    "М": "M",
    "Н": "N",
    "О": "O",
    "П": "P",
    "Р": "R",
    "С": "S",
    "Т": "T",
    "У": "U",
    "Ф": "F",
    "Х": "H",
    "Ц": "Ts",
    "Ч": "Ch",
    "Ш": "Sh",
    "Щ": "Sht",
    "Ъ": "A",
    "Ы": "Y",
    "Ь": "Y",
    "Ю": "Yu",
    "Я": "Ya",
    "Ё": "Yo",
    "Ѝ": "I",
    "І": "I",
    # Lowercase
    "а": "a",
    "б": "b",
    "в": "v",
    "г": "g",
    "д": "d",
    "е": "e",
    "ж": "zh",
    "з": "z",
    "и": "i",
    "й": "y",
    "к": "k",
    "л": "l",
    "м": "m",
    "н": "n",
    "о": "o",
    "п": "p",
    "р": "r",
    "с": "s",
    "т": "t",
    "у": "u",
    "ф": "f",
    "х": "h",
    "ц": "ts",
    "ч": "ch",
    "ш": "sh",
    "щ": "sht",
    "ъ": "a",
    "ы": "y",
    "ь": "y",
    "ю": "yu",
    "я": "ya",
    "ё": "yo",
    "ѝ": "i",
    "і": "i",
}


def _transliterate(value: str) -> str:
    """Transliterate Cyrillic characters to their Latin equivalents."""

    return "".join(_CYRILLIC_MAP.get(char, char) for char in value)


def _sanitize_segment(segment: str) -> str:
    """Return a filesystem-safe segment preserving human readability."""

    transliterated = _transliterate(segment.strip())
    with_underscores = re.sub(r"\s+", "_", transliterated)
    cleaned = _SANITIZE_RE.sub("_", with_underscores)
    cleaned = re.sub(r"_+", "_", cleaned)
    cleaned = cleaned.strip("_")
    if not cleaned:
        return ""
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
    sanitized: List[str] = []
    for piece in pieces:
        cleaned_piece = _sanitize_segment(piece)
        if cleaned_piece:
            sanitized.append(cleaned_piece)
    if sanitized:
        return sanitized
    fallback_title = title or f"item-{item_id}"
    fallback = _sanitize_segment(fallback_title)
    return [fallback or f"item-{item_id}"]


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
