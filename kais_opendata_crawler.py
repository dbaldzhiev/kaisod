import argparse
import datetime as dt
import json
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

BASE_URL = "https://kais.cadastre.bg"
OPEN_DATA_URL = "https://kais.cadastre.bg/bg/OpenData"

STATE_FILE = "kais_opendata_state.json"
DATE_RE = re.compile(r"(\d{2}\.\d{2}\.\d{4})")  # dd.mm.yyyy
# File types you care about; extend if needed
DL_EXTS = (".zip", ".7z", ".gz", ".csv", ".json", ".xlsx", ".xls", ".xml", ".geojson")

def parse_date_in_text(text: str):
    if not text:
        return None
    m = DATE_RE.search(text)
    if not m:
        return None
    try:
        return dt.datetime.strptime(m.group(1), "%d.%m.%Y").date()
    except ValueError:
        return None

def load_state(path: Path):
    if not path.exists():
        return {"downloaded_urls": [], "last_seen_date": None}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except Exception:
        return {"downloaded_urls": [], "last_seen_date": None}

def save_state(path: Path, state: dict):
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

def ensure_abs(url: str):
    if not url:
        return None
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return urljoin(BASE_URL, url)

def should_consider_href(href: str):
    if not href:
        return False
    href_l = href.lower()
    return any(href_l.endswith(ext) for ext in DL_EXTS)

def gather_items(page):
    """
    Heuristic:
      - wait for main content to render
      - scan anchors; if they look like a downloadable resource (by extension),
        grab the nearest surrounding text to detect a dd.mm.yyyy date
    """
    # Give the SPA a little time to populate
    # (We also attempt a specific wait for anchors.)
    try:
        page.wait_for_selector("a", timeout=8000)
    except PWTimeout:
        pass
    time.sleep(1.0)

    anchors = page.query_selector_all("a")
    items = []
    for a in anchors:
        href = a.get_attribute("href")
        if not should_consider_href(href):
            continue
        url = ensure_abs(href)
        link_text = (a.inner_text() or "").strip()

        # Look for nearby context to find a date (closest list/table/card)
        context_text = link_text
        handle = a.evaluate_handle(
            """(el) => {
                function grabText(node) {
                  if (!node) return "";
                  return (node.innerText || "").trim();
                }
                const container = el.closest("li, tr, .card, .row, .item, .table, .list-group-item, div");
                return {
                  linkText: el.innerText || "",
                  containerText: grabText(container)
                };
            }"""
        )
        try:
            ctx = handle.json_value()
            context_text = (ctx.get("containerText") or link_text).strip()
        except Exception:
            pass
        finally:
            handle.dispose()

        date = parse_date_in_text(context_text)
        items.append({"url": url, "link_text": link_text, "context": context_text, "date": date})
    return items

def download_file(url: str, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    name = os.path.basename(urlparse(url).path) or "download"
    out_path = out_dir / name

    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with out_path.open("wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    return out_path

def main():
    ap = argparse.ArgumentParser(description="KAIS OpenData crawler (date-aware).")
    ap.add_argument("--download-dir", default="downloads", help="Where to save files.")
    ap.add_argument("--since", help="Only download items dated on/after this (YYYY-MM-DD).")
    ap.add_argument("--headful", action="store_true", help="Run with a visible browser.")
    ap.add_argument("--dry-run", action="store_true", help="List actions without downloading.")
    args = ap.parse_args()

    dl_dir = Path(args.download_dir)
    state_path = Path(STATE_FILE)
    state = load_state(state_path)
    downloaded = set(state.get("downloaded_urls") or [])
    last_seen_date = state.get("last_seen_date")
    if last_seen_date:
        try:
            last_seen_date = dt.date.fromisoformat(last_seen_date)
        except Exception:
            last_seen_date = None

    since_date = None
    if args.since:
        try:
            since_date = dt.date.fromisoformat(args.since)
        except ValueError:
            print("Invalid --since; expected YYYY-MM-DD", file=sys.stderr)
            sys.exit(2)

    effective_cutoff = since_date or last_seen_date

    new_downloads = []
    max_date_observed = last_seen_date

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headful)
        ctx = browser.new_context(accept_downloads=True, locale="bg-BG")
        page = ctx.new_page()
        page.goto(OPEN_DATA_URL, wait_until="domcontentloaded")
        # In case SPA navigation happens:
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except PWTimeout:
            pass

        items = gather_items(page)
        browser.close()

    # Decide which items are "new"
    candidates = []
    for it in items:
        url = it["url"]
        d = it["date"]
        # If the site shows no date near a link, we skip it to stay true to "according to the date shown"
        if d is None:
            continue
        if (effective_cutoff is None) or (d >= effective_cutoff):
            if url not in downloaded:
                candidates.append(it)
        if (max_date_observed is None) or (d > max_date_observed):
            max_date_observed = d

    # Stable order by date then link text
    candidates.sort(key=lambda x: (x["date"], x["link_text"] or ""))

    if not candidates:
        print("No new items found.")
    else:
        print("New items:")
        for c in candidates:
            print(f"- {c['date'].isoformat()}  {c['url']}  ({c['link_text']})")

    if args.dry_run:
        print("\nDry-run: no downloads performed.")
        return

    for c in candidates:
        try:
            path = download_file(c["url"], dl_dir)
            print(f"Downloaded: {path}")
            downloaded.add(c["url"])
            new_downloads.append(str(path))
        except Exception as e:
            print(f"Failed to download {c['url']}: {e}", file=sys.stderr)

    # Update state
    state["downloaded_urls"] = sorted(downloaded)
    state["last_seen_date"] = max_date_observed.isoformat() if max_date_observed else None
    save_state(state_path, state)

    if new_downloads:
        print("\nSaved files:")
        for p in new_downloads:
            print(f"  {p}")

if __name__ == "__main__":
    main()
