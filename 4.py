# -*- coding: utf-8 -*-
# Python 3.x

import os
import csv
import json
import shutil
import datetime as dt
import subprocess
from typing import List, Dict, Optional
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed

# ---------------- PARAMETERS ----------------
HYTS_EXE = r"C:\hysplit\exec\hyts_std.exe"
ASCDATA_CFG = r"C:\hysplit\working\ASCDATA.CFG"
GFS_DIR = r"E:\GFS"
GFS_SUFFIX = "_gfs0p25.txt"
TZ_OFFSET_HOURS = 7
TOP_OF_MODEL_M = 100000.0
VERTICAL_MOTION = 0

# ---------------- TASK SETTINGS ----------------
FIRMS_DIR = r"E:\FIRMS"

PROVINCE_IDS = [50, 51, 52, 54, 55, 56, 57, 58, 63]
RUNTIME_HOURS = 24
START_HGT_M = 200.0
START_HOURS_LOCAL = [0, 3, 6, 9, 12, 15, 18, 21]

RUNS_ROOT_BASE = r"D:\hysplit_runs_forward\FWT_PROVINCES"
OUT_ROOT = r"D:\FWT"

MAX_WORKERS = max(1, os.cpu_count() - 1)  # safe default

# ---------------- UTILS ----------------
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def yyyymmdd(d: dt.date) -> str:
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"

def local_to_utc(local_dt: dt.datetime) -> dt.datetime:
    return local_dt - dt.timedelta(hours=TZ_OFFSET_HOURS)

def sniff_dialect(csv_path: str) -> csv.Dialect:
    with open(csv_path, "r", encoding="utf-8-sig", errors="replace") as f:
        sample = f.read(8192)
    try:
        return csv.Sniffer().sniff(sample)
    except:
        return csv.get_dialect("excel")

def normalize_row_keys(row: Dict[str, str]) -> Dict[str, str]:
    return {str(k).strip().lower(): v for k, v in row.items() if k}

def _to_float(x):
    try:
        return float(str(x).replace(",", "."))
    except:
        return None

def _to_int(x):
    try:
        return int(float(str(x)))
    except:
        return None

def parse_acq_date(s: str) -> Optional[dt.date]:
    if not s:
        return None
    s = str(s).split("T")[0].split(" ")[0].replace("/", "-")
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except:
            pass
    return None

# ---------------- FIRMS ----------------
def get_latest_firms_csv(folder: str) -> str:
    files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".csv")]
    if not files:
        raise RuntimeError("No FIRMS CSV found")
    return max(files, key=os.path.getmtime)

def parse_firms_rows(csv_path: str) -> List[Dict]:
    dialect = sniff_dialect(csv_path)
    out = []
    with open(csv_path, "r", encoding="utf-8-sig", errors="replace") as f:
        r = csv.DictReader(f, dialect=dialect)
        for row_raw in r:
            row = normalize_row_keys(row_raw)
            lat = _to_float(row.get("latitude"))
            lon = _to_float(row.get("longitude"))
            d = parse_acq_date(row.get("acq_date"))
            prov = _to_int(row.get("province_id"))
            if None in (lat, lon, d, prov):
                continue
            out.append({
                "lat": lat,
                "lon": lon,
                "acq_date": d,
                "province_id": prov,
                "row": row
            })
    return out

# ---------------- MET CACHE ----------------
met_cache = {}

def met_file_path_for_date(d: dt.date) -> str:
    return os.path.join(GFS_DIR, yyyymmdd(d) + GFS_SUFFIX)

def met_files_for_run(start_utc: dt.datetime):
    key = start_utc.date()
    if key in met_cache:
        return met_cache[key]

    files = []
    for i in range(3):
        fp = met_file_path_for_date(start_utc.date() + dt.timedelta(days=i))
        if os.path.isfile(fp):
            files.append(fp)

    met_cache[key] = files
    return files

# ---------------- WORKER ----------------
def run_hysplit_task(task):

    workdir, control_text = task

    ensure_dir(workdir)

    with open(os.path.join(workdir, "CONTROL"), "w") as f:
        f.write(control_text)

    # Faster than copying (if supported)
    try:
        os.symlink(ASCDATA_CFG, os.path.join(workdir, "ASCDATA.CFG"))
    except:
        shutil.copy(ASCDATA_CFG, os.path.join(workdir, "ASCDATA.CFG"))

    subprocess.run([HYTS_EXE], cwd=workdir, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    tdump = os.path.join(workdir, "tdump")
    if not os.path.exists(tdump):
        return None

    coords = []
    with open(tdump) as f:
        for line in f:
            parts = line.split()
            if len(parts) > 12:
                try:
                    coords.append([float(parts[10]), float(parts[9])])
                except:
                    pass

    return coords if len(coords) > 1 else None

# ---------------- MAIN ----------------
def main():

    csv_path = get_latest_firms_csv(FIRMS_DIR)
    print("Using FIRMS:", csv_path)

    rows = parse_firms_rows(csv_path)
    if not rows:
        raise RuntimeError("No valid FIRMS rows")

    FIRMS_DATE = max(r["acq_date"] for r in rows)
    RUN_DATES = [FIRMS_DATE + dt.timedelta(days=i) for i in (1, 2, 3)]

    print("FIRMS_DATE:", FIRMS_DATE)
    print("RUN_DATES :", RUN_DATES)

    hs_all = [r for r in rows if r["acq_date"] == FIRMS_DATE]

    # GROUP BY PROVINCE (BIG SPEEDUP)
    prov_groups = defaultdict(list)
    for r in hs_all:
        prov_groups[r["province_id"]].append(r)

    for run_date in RUN_DATES:

        print(f"\n=== RUN DATE {run_date} ===")

        for prov in PROVINCE_IDS:

            hs = prov_groups.get(prov, [])
            if not hs:
                continue

            tasks = []
            meta = []

            for i, r in enumerate(hs, start=1):
                for hr in START_HOURS_LOCAL:

                    start_local = dt.datetime(run_date.year, run_date.month, run_date.day, hr)
                    start_utc = local_to_utc(start_local)

                    met_files = met_files_for_run(start_utc)
                    if not met_files:
                        continue

                    workdir = os.path.join(
                        RUNS_ROOT_BASE,
                        f"{prov}_{yyyymmdd(run_date)}_{i:05d}_{hr:02d}"
                    )

                    control = []
                    control.append(f"{start_utc.year} {start_utc.month:02d} {start_utc.day:02d} {start_utc.hour:02d}")
                    control.append("1")
                    control.append(f"{r['lat']} {r['lon']} {START_HGT_M}")
                    control.append(str(RUNTIME_HOURS))
                    control.append(str(VERTICAL_MOTION))
                    control.append(str(TOP_OF_MODEL_M))
                    control.append(str(len(met_files)))

                    for m in met_files:
                        d, n = os.path.split(m)
                        control.append(d + os.sep)
                        control.append(n)

                    control.append(workdir + os.sep)
                    control.append("tdump")

                    tasks.append((workdir, "\n".join(control) + "\n"))
                    meta.append((r, run_date))

            print(f"Submitting {len(tasks)} tasks (workers={MAX_WORKERS})...")

            traj_features = []

            with ProcessPoolExecutor(MAX_WORKERS) as exe:
                futures = [exe.submit(run_hysplit_task, t) for t in tasks]

                for fut, (r, run_date) in zip(as_completed(futures), meta):
                    coords = fut.result()
                    if not coords:
                        continue

                    props = dict(r["row"])
                    props.update({
                        "acq_date": FIRMS_DATE.isoformat(),
                        "run_date": run_date.isoformat()
                    })

                    traj_features.append({
                        "type": "Feature",
                        "geometry": {"type": "LineString", "coordinates": coords},
                        "properties": props
                    })

            out_dir = os.path.join(OUT_ROOT, str(prov), str(run_date.year))
            ensure_dir(out_dir)

            out_geojson = os.path.join(out_dir, f"FWT_{prov}_{yyyymmdd(run_date)}.geojson")

            with open(out_geojson, "w") as f:
                json.dump({
                    "type": "FeatureCollection",
                    "features": traj_features
                }, f)

            print("Wrote:", out_geojson)

    print("\n=== COMPLETE ===")

if __name__ == "__main__":
    main()
