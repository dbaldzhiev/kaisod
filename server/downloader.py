"""File download helper."""
from __future__ import annotations

import hashlib
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Iterable, Optional, Set
import logging

import requests
import shapefile
import zipfile

from .models import Database, ensure_storage
from .storage import resolve_item_storage, transliterate_cyrillic
from .time_utils import utcnow


class DownloadError(Exception):
    """Raised when downloading a monitored item fails."""


def compute_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()
logger = logging.getLogger(__name__)

ProgressCallback = Callable[[str, Dict[str, object]], None]

DOWNLOAD_CHUNK_SIZE = 65536
DOWNLOAD_PROGRESS_STEP = 5  # percentage points
DOWNLOAD_PROGRESS_BYTES = 5 * 1024 * 1024  # 5 MiB when size unknown
BLOB_FOLDER_NAME = "kais-blob"


def _emit(progress: Optional[ProgressCallback], stage: str, payload: Dict[str, object]) -> None:
    if progress:
        try:
            progress(stage, payload)
        except Exception:
            # Progress callbacks are best effort and must not break downloads
            pass


def _safe_extract(archive: zipfile.ZipFile, destination: Path, progress: Optional[ProgressCallback]) -> None:
    members = archive.infolist()
    total = len(members)
    dest_root = destination.resolve()
    dest_root_str = str(dest_root)
    _emit(progress, "extract:start", {"destination": str(destination), "members": total})
    for index, info in enumerate(members, start=1):
        target = destination / info.filename
        resolved = target.resolve()
        resolved_str = str(resolved)
        if os.path.commonpath([dest_root_str, resolved_str]) != dest_root_str:
            raise DownloadError("Archive contains unsafe paths")
        if info.is_dir():
            resolved.mkdir(parents=True, exist_ok=True)
        else:
            resolved.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(info) as src, resolved.open("wb") as dst:
                shutil.copyfileobj(src, dst)
        _emit(
            progress,
            "extract:member",
            {"index": index, "total": total, "name": info.filename},
        )
    _emit(progress, "extract:complete", {"destination": str(destination), "members": total})


def _transliterate_path(path: Path, root: Path) -> Path:
    relative = path.relative_to(root)
    transliterated_parts = [transliterate_cyrillic(part) for part in relative.parts]
    return root.joinpath(*transliterated_parts)


def _deduplicate_target(target: Path) -> Path:
    if not target.exists():
        return target

    parent = target.parent
    stem = target.stem
    suffix = target.suffix
    counter = 1
    while True:
        candidate = parent / f"{stem}-{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def _rename_to_latin(root: Path) -> None:
    for path in sorted(root.rglob("*"), key=lambda p: len(p.relative_to(root).parts), reverse=True):
        target = _transliterate_path(path, root)
        if target == path:
            continue

        target.parent.mkdir(parents=True, exist_ok=True)
        unique_target = _deduplicate_target(target)
        path.rename(unique_target)


def _iter_data_directories(root: Path) -> Iterable[Path]:
    seen: Set[Path] = set()
    logger.debug("Scanning extracted tree for data directories", extra={"root": str(root)})
    for candidate in root.rglob("*"):
        if not candidate.is_dir():
            continue
        category = _detect_blob_category(candidate)
        if category:
            resolved = candidate.resolve()
            if resolved not in seen:
                seen.add(resolved)
                logger.info(
                    "Found data directory %s (category=%s)", resolved, category
                )
                yield candidate


def _sanitize_blob_component(component: str) -> str:
    transliterated = transliterate_cyrillic(component)
    cleaned = re.sub(r"[^0-9A-Za-z._()\-]+", "_", transliterated)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "part"


def _build_blob_name(item_id: int, source_root: Path, file_path: Path) -> str:
    relative = file_path.relative_to(source_root)
    pieces = [_sanitize_blob_component(part) for part in relative.parts]
    joined = "_".join(piece for piece in pieces if piece)
    if not joined:
        joined = "file"
    return f"{item_id}_{source_root.name}_{joined}"


def _detect_blob_category(data_root: Path) -> Optional[str]:
    name = data_root.name.lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", name).strip("_")
    if "sgradi" in normalized:
        return "sgradi"
    if "pozemleni_imoti" in normalized or normalized == "pozemleni":
        return "pozemleni_imoti"
    return None


def _sync_blob_copy(
    db: Database, item_id: int, extract_root: Path, progress: Optional[ProgressCallback]
) -> Path:
    blob_root = ensure_storage(str(Path(db.base_path) / BLOB_FOLDER_NAME))
    removed = 0
    for stale in blob_root.rglob(f"{item_id}_*"):
        if stale.is_file():
            try:
                stale.unlink()
                removed += 1
            except OSError:
                logger.warning("Failed to delete stale blob copy %s", stale)

    _emit(
        progress,
        "blob:start",
        {"extract_root": str(extract_root), "blob_root": str(blob_root), "removed": removed},
    )

    copied = 0
    for data_root in _iter_data_directories(extract_root):
        category = _detect_blob_category(data_root)
        destination_root = blob_root / category if category else blob_root
        destination_root.mkdir(parents=True, exist_ok=True)
        for file_path in data_root.rglob("*"):
            if not file_path.is_file():
                continue
            destination_name = _build_blob_name(item_id, data_root, file_path)
            destination = destination_root / destination_name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(file_path, destination)
            copied += 1
    _emit(
        progress,
        "blob:complete",
        {"extract_root": str(extract_root), "blob_root": str(blob_root), "copied": copied},
    )
    if copied == 0:
        logger.warning("No pozemleni_imoti/sgradi directories found in %s", extract_root)
    else:
        logger.info("Copied %s files into %s", copied, blob_root)

    return blob_root


def _read_prj(shp_path: Path) -> Optional[str]:
    prj_path = shp_path.with_suffix(".prj")
    if prj_path.exists():
        try:
            return prj_path.read_text(encoding="utf-8")
        except OSError:
            logger.warning("Failed to read PRJ file alongside %s", shp_path)
    return None


def _merge_category_shapefiles(category_root: Path, category: str) -> Optional[Path]:
    shp_files = sorted(category_root.rglob("*.shp"))
    if not shp_files:
        return None

    output_root = category_root.parent / "merged" / category
    output_root.mkdir(parents=True, exist_ok=True)
    base_name = output_root / f"{category}_merged"

    for suffix in (".shp", ".shx", ".dbf", ".cpg", ".prj"):
        target = base_name.with_suffix(suffix)
        if target.exists():
            try:
                target.unlink()
            except OSError:
                logger.warning("Failed to remove previous merged file %s", target)

    with shapefile.Reader(str(shp_files[0])) as prototype:
        fields = prototype.fields[1:]
        shape_type = prototype.shapeType
        prj_contents = _read_prj(shp_files[0])

    writer = shapefile.Writer(str(base_name))
    writer.fields = fields
    writer.shapeType = shape_type

    merged = 0
    for shp_path in shp_files:
        with shapefile.Reader(str(shp_path)) as reader:
            if reader.shapeType != shape_type or reader.fields[1:] != fields:
                logger.warning(
                    "Skipping incompatible shapefile %s for category %s", shp_path, category
                )
                continue
            for shape_record in reader.iterShapeRecords():
                writer.shape(shape_record.shape)
                writer.record(*shape_record.record)
                merged += 1

    writer.close()

    if merged == 0:
        logger.warning("No compatible records merged for %s", category)
    if prj_contents:
        try:
            base_name.with_suffix(".prj").write_text(prj_contents, encoding="utf-8")
        except OSError:
            logger.warning("Failed to write PRJ for merged shapefile %s", base_name)

    logger.info(
        "Merged %s shapefile records into %s", merged, base_name.with_suffix(".shp")
    )
    return base_name.with_suffix(".shp")


def _update_merged_shapefiles(blob_root: Path, progress: Optional[ProgressCallback]) -> None:
    for category in ("sgradi", "pozemleni_imoti"):
        _emit(progress, "merge:start", {"category": category})
        category_root = blob_root / category
        merged_path = None
        if category_root.exists():
            merged_path = _merge_category_shapefiles(category_root, category)
        _emit(
            progress,
            "merge:complete",
            {"category": category, "output": str(merged_path) if merged_path else None},
        )


def download_item(
    db: Database,
    item_id: int,
    file_url: str,
    observed_date: str,
    session: Optional[requests.Session] = None,
    progress: Optional[ProgressCallback] = None,
) -> Optional[int]:
    """Download the file for the monitored item and persist metadata."""

    session = session or requests.Session()
    try:
        response = session.get(file_url, timeout=60, stream=True)
    except requests.RequestException as exc:  # pragma: no cover - defensive
        raise DownloadError(f"Failed to download {file_url}: {exc}") from exc

    if response.status_code != 200:
        response.close()
        raise DownloadError(f"Failed to download {file_url}: {response.status_code}")

    item = db.get_item(item_id)
    if not item:
        response.close()
        return None

    item_data = dict(item)

    try:
        observed_dt = datetime.fromisoformat(observed_date)
        date_dir = observed_dt.strftime("%Y-%m-%d_%H-%M-%S")
    except ValueError:
        date_dir = observed_date.replace(":", "-")

    item_root, _, _archive_name = resolve_item_storage(
        db.base_path,
        raw_path=item_data.get("path"),
        title=item_data.get("title"),
        file_url=item_data.get("file_url") or file_url,
        item_id=item_id,
    )
    ensure_storage(str(item_root))

    timestamp_base = date_dir or utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    timestamp_name = f"{timestamp_base}.zip"
    file_path = item_root / timestamp_name
    counter = 1
    while file_path.exists():
        timestamp_name = f"{timestamp_base}-{counter}.zip"
        file_path = item_root / timestamp_name
        counter += 1

    total_size = int(response.headers.get("Content-Length") or 0)
    downloaded = 0
    last_percent = -1
    last_bytes_mark = 0
    _emit(
        progress,
        "download:start",
        {"total_bytes": total_size, "destination": str(file_path)},
    )

    try:
        with file_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                if not chunk:
                    continue
                handle.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    percent = int(downloaded * 100 / total_size)
                    if percent >= last_percent + DOWNLOAD_PROGRESS_STEP or downloaded == total_size:
                        last_percent = percent
                        _emit(
                            progress,
                            "download:chunk",
                            {
                                "downloaded_bytes": downloaded,
                                "total_bytes": total_size,
                                "percent": percent,
                                "destination": str(file_path),
                            },
                        )
                else:
                    if downloaded - last_bytes_mark >= DOWNLOAD_PROGRESS_BYTES:
                        last_bytes_mark = downloaded
                        _emit(
                            progress,
                            "download:chunk",
                            {
                                "downloaded_bytes": downloaded,
                                "total_bytes": 0,
                                "destination": str(file_path),
                            },
                        )
    except Exception:
        if file_path.exists():
            try:
                file_path.unlink()
            except OSError:
                pass
        raise
    finally:
        response.close()

    _emit(
        progress,
        "download:complete",
        {"downloaded_bytes": downloaded, "total_bytes": total_size, "destination": str(file_path)},
    )

    sha256 = compute_sha256(file_path)
    size = file_path.stat().st_size
    now = utcnow()
    download_id = db.record_download(item_id, str(file_path), sha256, size, now, observed_date)

    if zipfile.is_zipfile(file_path):
        extract_dir = item_root / "latest"
        if extract_dir.exists():
            shutil.rmtree(extract_dir)
        extract_dir.mkdir(parents=True, exist_ok=True)
        try:
            with zipfile.ZipFile(file_path) as archive:
                _safe_extract(archive, extract_dir, progress)
            _rename_to_latin(extract_dir)
            blob_root = _sync_blob_copy(db, item_id, extract_dir, progress)
            _update_merged_shapefiles(blob_root, progress)
        except zipfile.BadZipFile as exc:
            raise DownloadError(f"Downloaded archive is corrupt: {file_path}") from exc

    return download_id
