# AGENTS.md — Simple Internal Tool Brief

## 1) What this is

A tiny **internal** Ubuntu service + Web UI that **scrapes** `https://kais.cadastre.bg/bg/OpenData`, detects **new or changed items** using the **date column in the table**, and (optionally) **downloads** files for items you mark as “monitored”. No auth, no Docker, no scaling concerns.

---

## 2) Keep‑it‑simple stack

* **Language**: Python 3.10+
* **Web**: Flask (server‑rendered HTML + a dash of vanilla JS)
* **Crawler**: `requests` + `BeautifulSoup4`
* **Scheduler**: in‑process loop (thread) or `systemd` timer (pick one)
* **DB**: SQLite (single file)
* **OS**: Ubuntu 22.04+

Why: zero external deps, trivial deploy, easy to inspect.

---

## 3) Minimal architecture

```
+-------------------------------+
|  Flask Web UI + API (single)  |
|  - Items list / toggles       |
|  - Interval settings          |
|  - History & downloads        |
+---------------+---------------+
                |
                v
+---------------+---------------+
|  Scheduler (thread or timer)  |
|  - 6h / 1d / 6d / manual      |
+---------------+---------------+
                |
                v
+---------------+---------------+
|  Crawler + Parser (requests)  |
|  - fetch page                  |
|  - parse table + date column  |
|  - detect NEW/UPDATED         |
+---------------+---------------+
                |
                v
+---------------+---------------+
|  Download (optional per item) |
+---------------+---------------+
                |
                v
+---------------+---------------+
|  SQLite + Filesystem          |
+-------------------------------+
```

---

## 4) How it works (lifecycle)

1. **Initial scan** from UI → fetch page → parse rows → save to DB.
2. In UI, **mark items to monitor** (or ignore).
3. **Interval** (6h/1d/6d) triggers scan. If a row’s **date column** is newer than last seen, mark **UPDATED**. New rows become **NEW**.
4. For monitored items on NEW/UPDATED → **download** file to local storage.

---

## 5) Data we keep (small)

* `items(id, title, source_url, file_url, last_seen_date, first_seen_at, last_seen_at, monitored BOOL, ignored BOOL)`
* `events(id, item_id, kind[NEW|UPDATED], observed_date_in_table, observed_at)`
* `downloads(id, item_id, file_path, sha256, size, downloaded_at)`
* `settings(id, key, value)` — e.g., `scan_interval = 6h|1d|6d`

`observed_date_in_table` is the single source of truth for “changed”.

---

## 6) Web UI (dead simple)

* **Dashboard**: Last/Next scan times; counts (total/new/updated/monitored/ignored).
* **Items**: Table → Title | Date (from site) | Status | Monitored? (toggle) | Ignore (toggle) | Actions (Download, History).
* **Settings**: Interval select (**6h / 1d / 6d**), storage path.
* **History**: Per‑item events timeline.

All server‑rendered Jinja templates; a tiny bit of JS for toggles.

---

## 7) Interfaces (routes)

* `GET /` → dashboard
* `POST /scan` → manual scan now
* `GET /items` → list items (filters: `?monitored=1`, `?status=new|updated`)
* `POST /items/<id>/monitor` `{monitored: true|false}`
* `POST /items/<id>/ignore` `{ignored: true|false}`
* `GET /items/<id>/history`
* `GET /downloads/<download_id>` → serves file
* `GET /export.(csv|json)` → dump items & events
* `POST /settings/interval` `{value: "6h"|"1d"|"6d"}`

---

## 8) Change detection rules

* Identify a row by `(title, file link)`.
* Parse the **date column** (BG locale). If newer than `last_seen_date` → **UPDATED**.
* Unknown row → **NEW**.
* For monitored items on NEW/UPDATED → download.

---

## 9) Storage layout

* Base: `/var/lib/kais-monitor/`
* Files: `/var/lib/kais-monitor/<item-id>/<YYYY-MM-DD>/<original-filename>`
* DB: `/var/lib/kais-monitor/kais.sqlite3`

---

## 10) Setup & run (Ubuntu)

```bash
# 1) System deps
sudo apt update
sudo apt install -y python3 python3-venv

# 2) Project
git clone <repo>
cd kais-monitor
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt  # flask, requests, bs4, lxml

# 3) Run (dev)
python server/app.py  # serves UI + scheduler thread
```

**Optional systemd** (no Docker):

* `kais-monitor.service` → runs `python server/app.py`
* Or **systemd timer**: run `python server/scan_once.py` every 6h and keep Flask separate.

---

## 11) Project skeleton (tiny)

```
kais-monitor/
  AGENTS.md
  requirements.txt
  server/
    app.py              # Flask + scheduler thread
    scan_once.py        # one-off scan (for systemd timer mode)
    crawler.py          # fetch + parse page
    detector.py         # compare + events
    downloader.py       # download files
    models.py           # sqlite setup & helpers
    templates/
      base.html
      dashboard.html
      items.html
      history.html
      settings.html
    static/
      main.css
      main.js
```

---

## 12) Milestones

* **M1**: First scan & list items in UI; toggles saved.
* **M2**: Interval scanning (6h/1d/6d) + NEW/UPDATED detection via date column.
* **M3**: Auto‑download for monitored items + history + export.

Acceptance: When a monitored item’s table **date** changes, an **UPDATED** event appears and a new file is saved locally.

---

## 13) Risks (practical)

* **Page markup changes** → keep selectors simple; fallback to text search.
* **Date parsing (BG)** → unit test parsing; explicit formats.
* **Transient network** → retry with small backoff.

---

## 14) Way of use (operator)

1. Start the app.
2. Click **Scan now**.
3. In **Items**, mark what to **Monitor** and what to **Ignore**.
4. Set **Interval** to 6h/1d/6d.
5. Check **History** and **Downloads** when notified by counts.
