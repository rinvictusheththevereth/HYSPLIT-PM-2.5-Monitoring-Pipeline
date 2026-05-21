# -*- coding: utf-8 -*-
"""
READY GFS 0.25° downloader (MULTI-DAY PARALLEL + FORCE RE-DOWNLOAD)

- Downloads the 3 most recent complete GFS archives
- Runs downloads concurrently
- ALWAYS deletes existing files and re-downloads
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import re
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------- USER SETTINGS ----------------
OUT_DIR = Path(r"E:\GFS")
LOCAL_TZ = ZoneInfo("Asia/Bangkok")
TIMEOUT = 120
RETRIES = 3
RETRY_SLEEP = 5
CHUNK_MB = 16

MIN_VALID_BYTES = int(2.0 * 1024 * 1024 * 1024)
SCAN_BYTES = 32 * 1024 * 1024

N_DOWNLOADS = 3  # number of most recent days
# ------------------------------------------------

ROOT_URL = "https://www.ready.noaa.gov/data/archives/gfs0p25/"

FILE_RE = re.compile(r'href="(\d{8}_gfs0p25)/?"')

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) GFS-Downloader/1.0"
}


def log(msg: str):
    ts = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def fetch_text(url: str) -> str | None:
    try:
        import requests
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass
    return None


def valid_signature(head: bytes) -> tuple[bool, str]:
    if b"GRIB" in head:
        return True, "GRIB"
    if b"@INDX" in head[:2048]:
        return True, "READY-INDX"
    if head[:2] == b"\x1f\x8b":
        return True, "GZIP"
    if len(head) >= 262 and head[257:262] == b"ustar":
        return True, "TAR"
    return False, ""


def remote_size_bytes(url: str) -> int | None:
    try:
        import requests
        r = requests.head(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        if r.status_code >= 400:
            return None
        cl = r.headers.get("Content-Length")
        return int(cl) if cl and cl.isdigit() else None
    except Exception:
        return None


def download(url: str, dest: Path) -> bool:
    tmp = dest.with_suffix(".part")

    for attempt in range(1, RETRIES + 1):
        log(f"Downloading attempt {attempt}: {dest.name}")
        try:
            import requests
            with requests.get(url, headers=HEADERS, stream=True, timeout=TIMEOUT) as r:
                r.raise_for_status()

                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=CHUNK_MB * 1024 * 1024):
                        if chunk:
                            f.write(chunk)

            if tmp.stat().st_size < MIN_VALID_BYTES:
                raise RuntimeError(
                    f"File too small ({tmp.stat().st_size/1e9:.2f} GB)"
                )

            with open(tmp, "rb") as f:
                ok, kind = valid_signature(f.read(SCAN_BYTES))
            if not ok:
                raise RuntimeError("Invalid file signature")

            tmp.replace(dest)
            log(f"Saved {dest.name} [{kind}]")
            return True

        except Exception as e:
            log(f"Error: {e}")
            time.sleep(RETRY_SLEEP)

    if tmp.exists():
        try:
            tmp.unlink()
        except Exception:
            pass

    return False


def iter_candidate_urls():
    log("Scanning root archive...")
    root_html = fetch_text(ROOT_URL)
    if not root_html:
        return []

    files = sorted(set(FILE_RE.findall(root_html)), reverse=True)
    return [(f[:8], f"{ROOT_URL}{f}") for f in files]


def pick_latest_n_downloadable(candidates, n):
    selected = []

    for ymd, url in candidates:
        if len(selected) >= n:
            break

        sz = remote_size_bytes(url)
        if sz is not None and sz < MIN_VALID_BYTES:
            log(f"[SKIP] Too small: {ymd}")
            continue

        selected.append((ymd, url))

    return selected


def download_task(ymd, url):
    dest = OUT_DIR / f"{ymd}_gfs0p25.txt"

    # 🔥 ALWAYS delete existing file
    if dest.exists():
        try:
            dest.unlink()
            log(f"[DELETE] Removed existing: {dest.name}")
        except Exception as e:
            log(f"[WARN] Could not delete {dest.name}: {e}")

    # remove leftover partial file
    tmp = dest.with_suffix(".part")
    if tmp.exists():
        try:
            tmp.unlink()
        except Exception:
            pass

    download(url, dest)


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    candidates = iter_candidate_urls()
    if not candidates:
        log("No candidates found")
        sys.exit(2)

    selected = pick_latest_n_downloadable(candidates, N_DOWNLOADS)

    if not selected:
        log("No valid files found")
        sys.exit(2)

    log("Selected files:")
    for ymd, _ in selected:
        log(f"  {ymd}")

    # 🚀 PARALLEL DOWNLOADS
    with ThreadPoolExecutor(max_workers=N_DOWNLOADS) as executor:
        futures = [
            executor.submit(download_task, ymd, url)
            for ymd, url in selected
        ]

        for f in as_completed(futures):
            f.result()

    log("All downloads complete.")


if __name__ == "__main__":
    main()
