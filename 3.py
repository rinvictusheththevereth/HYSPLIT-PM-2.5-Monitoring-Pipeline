#!/usr/bin/env python3
"""
Download Thailand hotspot data from:
  http://35.187.240.74:3000/api/hotspot/thailand/date?start=YYYY-MM-DD&end=YYYY-MM-DD

This version finds the MOST RECENT date (by Asia/Bangkok local date) that returns
non-empty hotspot rows, then downloads ONLY that day and writes one CSV to E:\FIRMS.

It also maintains an NDJSON cache for that single-day run (useful if you Ctrl+C).

Python: 3.13.9
"""

from __future__ import annotations

import argparse
import csv
import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


DEFAULT_BASE_URL = "http://35.187.240.74:3000/api/hotspot/thailand/date"
DEFAULT_OUT_DIR = r"E:\FIRMS"
LOCAL_TZ = ZoneInfo("Asia/Bangkok")

# Preferred column order (adds any extras found in the JSON after these)
PREFERRED_COLUMNS = [
    "latitude", "longitude", "bright_ti4", "scan", "track",
    "acq_date", "acq_time", "satellite", "instrument",
    "confidence", "version", "bright_ti5", "frp",
    "daynight", "type", "tambon_id", "amphur_id", "province_i", "id"
]


@dataclass
class FetchResult:
    url: str
    status: int
    headers: dict[str, str]
    body: bytes


def ensure_out_dir(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    if not out_dir.is_dir():
        raise RuntimeError(f"Output path is not a directory: {out_dir}")


def build_url(base_url: str, start: str, end: str) -> str:
    params = {"start": start, "end": end}
    return f"{base_url}?{urllib.parse.urlencode(params)}"


def http_get(url: str, timeout_s: int = 60, retries: int = 5, backoff_s: float = 1.6) -> FetchResult:
    """GET with retries (exponential backoff)."""
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "Accept": "application/json, text/csv;q=0.9, */*;q=0.8",
                    "User-Agent": "python-urllib/3.13.9 hotspot-downloader",
                    "Connection": "close",
                },
                method="GET",
            )
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                status = getattr(resp, "status", 200)
                body = resp.read()  # <-- FIXED (was resp.read()a)
                headers = {k.lower(): v for k, v in resp.headers.items()}
                return FetchResult(url=url, status=status, headers=headers, body=body)
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff_s ** (attempt - 1))
            else:
                break
    raise RuntimeError(f"GET failed after {retries} attempts: {last_err}") from last_err


def looks_like_json(headers: dict[str, str], body: bytes) -> bool:
    ctype = headers.get("content-type", "").lower()
    if "application/json" in ctype or "text/json" in ctype:
        return True
    s = body.lstrip()
    return s.startswith(b"{") or s.startswith(b"[")


def is_record_dict(d: dict[str, Any]) -> bool:
    keys = {k.lower() for k in d.keys()}
    if not {"latitude", "longitude"}.issubset(keys):
        return False
    typical = {"acq_date", "acq_time", "frp", "bright_ti4", "confidence"}
    return len(keys.intersection(typical)) >= 1


def find_record_list(obj: Any) -> list[dict[str, Any]]:
    """Recursively search for a list[dict] that looks like hotspot records."""
    best: list[dict[str, Any]] = []
    best_score = 0

    def score_list(lst: list[dict[str, Any]]) -> int:
        return sum(1 for d in lst if is_record_dict(d))

    def walk(x: Any) -> None:
        nonlocal best, best_score
        if isinstance(x, list):
            if x and all(isinstance(i, dict) for i in x):
                lst = x  # type: ignore[assignment]
                sc = score_list(lst)
                if sc > 0:
                    if (len(lst) > len(best)) or (len(lst) == len(best) and sc > best_score):
                        best = lst
                        best_score = sc
            for i in x:
                walk(i)
        elif isinstance(x, dict):
            for v in x.values():
                walk(v)

    walk(obj)
    return best


def compute_fieldnames(rows: list[dict[str, Any]]) -> list[str]:
    extras: list[str] = []
    seen = {c.lower() for c in PREFERRED_COLUMNS}
    for r in rows:
        for k in r.keys():
            if k.lower() not in seen:
                extras.append(k)
                seen.add(k.lower())
    return PREFERRED_COLUMNS + extras


def normalize_row(row: dict[str, Any], fieldnames: list[str]) -> dict[str, Any]:
    lower_map = {k.lower(): k for k in row.keys()}
    out: dict[str, Any] = {}
    for col in fieldnames:
        src_key = lower_map.get(col.lower())
        v = row.get(src_key, "") if src_key else ""
        if isinstance(v, (dict, list)):
            out[col] = json.dumps(v, ensure_ascii=False)
        else:
            out[col] = v
    return out


def load_cached_days(cache_path: Path) -> set[str]:
    """Cache lines like: {"date":"YYYY-MM-DD","rows":[...]} or {"date":"YYYY-MM-DD","empty":true}"""
    done: set[str] = set()
    if not cache_path.exists():
        return done
    with cache_path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                d = obj.get("date")
                if isinstance(d, str):
                    done.add(d)
            except Exception:
                continue
    return done


def append_cache_line(cache_path: Path, obj: dict[str, Any]) -> None:
    with cache_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def fetch_day_rows(base_url: str, day: date, timeout: int, retries: int) -> tuple[list[dict[str, Any]], str | None]:
    """Return (rows, error_message). rows may be empty if no data."""
    day_str = day.isoformat()
    url = build_url(base_url, day_str, day_str)

    try:
        result = http_get(url, timeout_s=timeout, retries=retries)
    except Exception as e:
        return [], f"request failed: {e}"

    body = result.body.strip()
    if not body:
        return [], None  # empty body treated as "no rows"

    if not looks_like_json(result.headers, body):
        return [], f"non-JSON response (HTTP {result.status})"

    try:
        data = json.loads(body.decode("utf-8", errors="replace"))
    except Exception as e:
        return [], f"JSON parse failed: {e}"

    # Legit empty list
    if isinstance(data, list) and len(data) == 0:
        return [], None

    # Extract record list
    if isinstance(data, list) and all(isinstance(x, dict) for x in data):
        rows = data  # type: ignore[assignment]
    else:
        rows = find_record_list(data)

    if not rows:
        return [], "non-empty JSON but no record list found"

    return rows, None


def find_most_recent_date_with_rows(
    base_url: str,
    lookback_days: int,
    timeout: int,
    retries: int,
    sleep_s: float,
) -> tuple[date | None, list[dict[str, Any]]]:
    """
    Probe backward from Bangkok 'today' for up to lookback_days and return the first day with rows.
    """
    today_local = datetime.now(LOCAL_TZ).date()
    for i in range(0, lookback_days + 1):
        d = today_local - timedelta(days=i)
        rows, err = fetch_day_rows(base_url, d, timeout=timeout, retries=retries)
        if err:
            print(f"[probe] {d.isoformat()} -> ERROR ({err})")
        elif rows:
            print(f"[probe] {d.isoformat()} -> FOUND {len(rows)} rows")
            return d, rows
        else:
            print(f"[probe] {d.isoformat()} -> no rows")
        if sleep_s:
            time.sleep(sleep_s)
    return None, []


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Download Thailand hotspot data for the most recent day with available rows into one CSV."
    )
    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help=r'Output directory (default: E:\FIRMS)')
    ap.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Base API endpoint")
    ap.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds (default: 60)")
    ap.add_argument("--retries", type=int, default=5, help="Retry attempts (default: 5)")
    ap.add_argument("--sleep", type=float, default=0.10, help="Sleep seconds between requests (default: 0.10)")
    ap.add_argument("--lookback", type=int, default=21, help="How many days back to probe for data (default: 21)")
    ap.add_argument("--resume", action="store_true", help="Resume using the NDJSON cache (recommended)")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    ensure_out_dir(out_dir)

    print(f"Bangkok local now: {datetime.now(LOCAL_TZ).isoformat(timespec='seconds')}")
    print(f"API base: {args.base_url}")

    # Find latest day with rows
    latest_day, latest_rows = find_most_recent_date_with_rows(
        args.base_url,
        lookback_days=args.lookback,
        timeout=args.timeout,
        retries=args.retries,
        sleep_s=args.sleep,
    )

    if latest_day is None:
        print(f"[WARN] No hotspot rows found in the last {args.lookback} day(s). Writing header-only CSV.")
        # Write a header-only file with a timestamped name so you still get an artifact.
        stamp = datetime.now(LOCAL_TZ).strftime("%Y%m%d_%H%M%S")
        out_csv = out_dir / f"thailand_hotspot_NONE_FOUND_{stamp}.csv"
        with out_csv.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=PREFERRED_COLUMNS, extrasaction="ignore")
            w.writeheader()
        print(f"[OK] Wrote: {out_csv}")
        return 0

    # Output names tied to the discovered date
    d0 = latest_day
    d1 = latest_day
    stem = f"thailand_hotspot_{d0:%Y%m%d}_{d1:%Y%m%d}"
    out_csv = out_dir / f"{stem}.csv"
    cache_path = out_dir / f"{stem}_cache.ndjson"
    empty_dates_path = out_dir / f"{stem}_empty_dates.txt"
    errors_path = out_dir / f"{stem}_errors.txt"

    # Cache/resume logic (single day, but still useful)
    done_days: set[str] = load_cached_days(cache_path) if args.resume else set()
    day_str = latest_day.isoformat()

    empty_dates: list[str] = []
    errors: list[str] = []

    if args.resume and day_str in done_days:
        print(f"[resume] {day_str} already cached; rebuilding CSV from cache...")
    else:
        # Cache the rows we already fetched during probing
        append_cache_line(cache_path, {"date": day_str, "rows": latest_rows})
        print(f"[cache] {day_str} -> cached {len(latest_rows)} rows")

    # Build final CSV from cache
    all_rows: list[dict[str, Any]] = []
    if cache_path.exists():
        with cache_path.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue

                d = obj.get("date")
                if isinstance(obj, dict) and obj.get("empty") is True and isinstance(d, str):
                    empty_dates.append(d)
                elif isinstance(obj, dict) and "rows" in obj and isinstance(obj["rows"], list):
                    rows = [r for r in obj["rows"] if isinstance(r, dict)]
                    all_rows.extend(rows)
                elif isinstance(obj, dict) and "error" in obj and isinstance(d, str):
                    errors.append(f"{d} {obj['error']}")

    # Write CSV
    if all_rows:
        fieldnames = compute_fieldnames(all_rows)
        with out_csv.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            for r in all_rows:
                w.writerow(normalize_row(r, fieldnames))
        print(f"[OK] Wrote CSV: {out_csv}")
        print(f"     Date:       {day_str} (most recent with rows)")
        print(f"     Total rows: {len(all_rows)}")
    else:
        with out_csv.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=PREFERRED_COLUMNS, extrasaction="ignore")
            w.writeheader()
        print(f"[WARN] Cache had no rows for {day_str}. Wrote header-only CSV: {out_csv}")

    empty_dates_path.write_text("\n".join(sorted(set(empty_dates))) + ("\n" if empty_dates else ""), encoding="utf-8")
    errors_path.write_text("\n".join(errors) + ("\n" if errors else ""), encoding="utf-8")

    print(f"[OK] Empty dates list: {empty_dates_path} ({len(set(empty_dates))} day(s))")
    print(f"[OK] Errors list:      {errors_path} ({len(errors)} item(s))")
    print(f"[OK] Cache file:       {cache_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
