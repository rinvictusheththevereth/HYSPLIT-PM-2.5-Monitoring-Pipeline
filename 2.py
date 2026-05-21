# -*- coding: utf-8 -*-
# Python 3.x
#
# HYSPLIT -24-hour BACKWARD trajectories (Bangkok local time, UTC+7)
#
# DAILY outputs for the THREE MOST RECENT GFS met-file DATES present in E:\GFS
# For EACH of those 3 days and EACH station:
#
#   D:\TRJ\<STATION>\2026\TRJ_<STATION>_YYYYMMDD.geojson
#   مثال: D:\TRJ\37T\2026\TRJ_37T_20260217.geojson
#
# IMPORTANT:
# - Always run ALL 8 Bangkok-local start hours per day: [0,3,6,9,12,15,18,21]
# - BACKWARD (-24h) runs require met data spanning start_utc-24h .. start_utc.
#   This can cross multiple UTC days => pass MULTIPLE daily GFS files.
#
# MET FILE SELECTION (EXPLICIT):
# - Required UTC window:
#       t0 = start_utc + runtime_h (runtime_h negative)
#       t1 = start_utc
# - Include ALL daily GFS files for each UTC date from t0.date()..t1.date() inclusive,
#   plus +/- MET_DAY_BUFFER days (recommended) to reduce metpos boundary errors.
#
# OUTPUT (CHANGED):
# - No per-day folder under TRJ_*. Writes a single GeoJSON per station+day directly:
#     D:\TRJ\<STATION>\2026\TRJ_<STATION>_YYYYMMDD.geojson
#
# GeoJSON only (NO shapefiles).

import os
import sys
import json
import shutil
import datetime as dt
import subprocess
import re
import time
from typing import List

# ---------------- USER SETTINGS ----------------
HYTS_EXE    = r"C:\hysplit\exec\hyts_std.exe"
ASCDATA_CFG = r"C:\hysplit\working\ASCDATA.CFG"

GFS_DIR     = r"E:\GFS"
GFS_SUFFIX  = "_gfs0p25.txt"  # YYYYMMDD_gfs0p25.txt

# Output root (FIXED)
OUT_ROOT = r"D:\TRJ"

# Scratch runs root (separate from output tree)
RUNS_ROOT = r"D:\hysplit_runs_backward"

# Bangkok timezone fixed offset (UTC+7)
TZ_OFFSET_HOURS = 7

# HYSPLIT trajectory configuration
TOP_OF_MODEL_M = 100000.0
RUNTIME_HOURS  = -24         # -24 BACKWARD

# Start hours in Bangkok LOCAL time (ALL 8 per day)
START_HOURS_LOCAL = [0, 3, 6, 9, 12, 15, 18, 21]

# Extra met buffer days on both sides of required UTC date range
MET_DAY_BUFFER = 1

# Stations (14 total)
STATIONS = {
    "76t": {"lat": 16.750102, "lon":  98.591312, "name": "Non-Formal Education Centre"},
    "67t": {"lat": 18.788878, "lon": 100.776359, "name": "Municipality Office"},
    "75t": {"lat": 19.322380, "lon": 101.025365, "name": "Chalermprakiet Hospital"},
    "70t": {"lat": 19.200226, "lon":  99.893048, "name": "Phayao Provincial"},
    "37t": {"lat": 18.278251, "lon":  99.506447, "name": "Meteorological stations"},
    "68t": {"lat": 18.567179, "lon":  99.038560, "name": "Meteorological Staions"},
    "57t": {"lat": 19.909242, "lon":  99.823357, "name": "Natural Resources and Environment Office"},
    "73t": {"lat": 20.427234, "lon":  99.883724, "name": "Maesai Health Office"},
    "69t": {"lat": 18.128928, "lon": 100.162345, "name": "Meteorology Center"},
    "35t": {"lat": 18.840633, "lon":  98.969661, "name": "City Hall"},
    "58t": {"lat": 19.304686, "lon":  97.970999, "name": "Natural Resources and Environment Office"},
    "o20": {"lat": 18.15917727, "lon": 97.93315927, "name": "Mae Sa Riang"},
    "o73": {"lat": 17.80241664, "lon": 98.95016385, "name": "Li"},
    "o75": {"lat": 19.35872200, "lon": 98.43939900, "name": "Pai North"},
}

# Behavior toggles
FORCE_RERUN_ALL          = True
CLEAN_PREV_TDUMPS_IN_RUN = True
OVERWRITE_GEOJSON        = True

# Segment definition: export full -24..0 path (relative index 0..24)
SEGMENT_REL_0_24 = (0, 24)


# ---------------- UTILS ----------------
def ensure_dir(path: str) -> None:
    if not path:
        return
    os.makedirs(path, exist_ok=True)

def add_trailing_sep(path: str) -> str:
    if not path.endswith("\\") and not path.endswith("/"):
        return path + os.sep
    return path

def station_code(station_id: str) -> str:
    return station_id.upper()

def trj_prefix(station_id: str) -> str:
    return "TRJ_{}".format(station_code(station_id))

def yyyymmdd(d: dt.date) -> str:
    return "{:04d}{:02d}{:02d}".format(d.year, d.month, d.day)

def parse_yyyymmdd(s: str) -> dt.date:
    return dt.date(int(s[0:4]), int(s[4:6]), int(s[6:8]))

def local_to_utc(local_dt: dt.datetime) -> dt.datetime:
    return local_dt - dt.timedelta(hours=TZ_OFFSET_HOURS)

def normalize_lon(lon: float) -> float:
    while lon > 180.0:
        lon -= 360.0
    while lon < -180.0:
        lon += 360.0
    return lon

def find_three_most_recent_gfs_dates(gfs_dir: str) -> List[dt.date]:
    pat = re.compile(r"^(\d{8})" + re.escape(GFS_SUFFIX) + r"$")
    dates: List[dt.date] = []
    try:
        names = os.listdir(gfs_dir)
    except Exception as e:
        raise RuntimeError(f"Cannot list GFS_DIR '{gfs_dir}': {e}")

    for name in names:
        m = pat.match(name)
        if not m:
            continue
        try:
            d = parse_yyyymmdd(m.group(1))
        except Exception:
            continue
        fp = os.path.join(gfs_dir, name)
        if os.path.isfile(fp):
            dates.append(d)

    if not dates:
        raise RuntimeError(f"No met files found in {gfs_dir} matching YYYYMMDD{GFS_SUFFIX}")

    dates = sorted(set(dates))
    return dates[-3:] if len(dates) >= 3 else dates[:]

def met_file_path_for_date(gfs_dir: str, d: dt.date) -> str:
    return os.path.join(gfs_dir, yyyymmdd(d) + GFS_SUFFIX)

def met_files_for_run(gfs_dir: str, start_utc: dt.datetime, runtime_h: int) -> List[str]:
    """
    Include ALL met files covering the UTC integration window, plus buffer days.

    t0 = start_utc + runtime_h  (runtime_h negative)
    t1 = start_utc
    dates = [t0.date() .. t1.date()] inclusive, plus +/- MET_DAY_BUFFER
    """
    t1 = start_utc
    t0 = start_utc + dt.timedelta(hours=int(runtime_h))

    d0 = t0.date()
    d1 = t1.date()
    if d1 < d0:
        d0, d1 = d1, d0

    d0 = d0 - dt.timedelta(days=MET_DAY_BUFFER)
    d1 = d1 + dt.timedelta(days=MET_DAY_BUFFER)

    met_files: List[str] = []
    d = d0
    while d <= d1:
        fp = met_file_path_for_date(gfs_dir, d)
        if os.path.isfile(fp):
            met_files.append(fp)
        d += dt.timedelta(days=1)

    return met_files

def write_control(control_path: str,
                  start_dt_utc: dt.datetime,
                  runtime_h: int,
                  top_m: float,
                  met_files: List[str],
                  out_dir: str,
                  out_name: str,
                  start_lat: float,
                  start_lon: float,
                  start_hgt_m: float) -> None:
    """
    CONTROL times MUST be UTC. runtime_h is NEGATIVE for backward trajectories.
    """
    y, m, d, h = start_dt_utc.year, start_dt_utc.month, start_dt_utc.day, start_dt_utc.hour
    lines = []
    lines.append("{:04d} {:02d} {:02d} {:02d}".format(y, m, d, h))  # 1
    lines.append("1")                                               # 2
    lines.append("{:.6f} {:.6f} {:.1f}".format(start_lat, start_lon, start_hgt_m))  # 3
    lines.append("{:d}".format(int(runtime_h)))                      # 4 (NEG = backward)
    lines.append("0")                                                # 5
    lines.append("{:.1f}".format(top_m))                             # 6
    lines.append("{:d}".format(len(met_files)))                      # 7
    for fp in met_files:                                             # 8
        dpath, fname = os.path.split(fp)
        dpath = add_trailing_sep(dpath if dpath else ".")
        lines.append(dpath)
        lines.append(fname)
    out_dir = add_trailing_sep(out_dir if out_dir else ".")          # 9
    lines.append(out_dir)
    lines.append(out_name)                                           # 10

    with open(control_path, "w", encoding="ascii", newline="\n") as f:
        f.write("\n".join(lines))

def run_hysplit(workdir: str) -> None:
    ascdst = os.path.join(workdir, "ASCDATA.CFG")
    if not os.path.exists(ascdst):
        shutil.copyfile(ASCDATA_CFG, ascdst)

    p = subprocess.Popen([HYTS_EXE], cwd=workdir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    rc = p.returncode
    if rc != 0:
        msg = [f"HYSPLIT returned non-zero exit code {rc}."]
        if out:
            msg.append("--- STDOUT ---\n" + out.decode("utf-8", "ignore"))
        if err:
            msg.append("--- STDERR ---\n" + err.decode("utf-8", "ignore"))
        raise RuntimeError("\n".join(msg))

def parse_tdump_to_coords(tdump_path: str) -> List[List[float]]:
    """
    Parse tdump -> list of [lon,lat] coordinates. Assumes lat/lon at cols 9/10.
    """
    coords: List[List[float]] = []
    if not os.path.exists(tdump_path):
        return coords

    with open(tdump_path, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.strip()
            if not line or line[0].isalpha():
                continue
            parts = line.split()
            if len(parts) < 12:
                continue
            try:
                lat = float(parts[9])
                lon = float(parts[10])
            except Exception:
                continue
            if not (-90.0 <= lat <= 90.0):
                continue
            coords.append([normalize_lon(lon), lat])
    return coords

def bulldoze_run_dir(run_dir: str) -> None:
    try:
        for name in os.listdir(run_dir):
            if name == "tdump" or name.startswith("tdump_") or name.upper() == "CONTROL":
                try:
                    os.remove(os.path.join(run_dir, name))
                except Exception:
                    pass
    except Exception:
        pass

def idx_for_hour_rel(hour_rel: int, npts: int, total_h: int) -> int:
    if npts <= 1:
        return 0
    if npts == total_h + 1:
        return max(0, min(npts - 1, hour_rel))
    frac = hour_rel / float(total_h) if total_h > 0 else 0.0
    idx = int(round(frac * (npts - 1)))
    return max(0, min(npts - 1, idx))

def slice_segment_rel(coords: List[List[float]], start_rel_h: int, end_rel_h: int, total_h: int) -> List[List[float]]:
    n = len(coords)
    if n < 2:
        return coords
    i0 = idx_for_hour_rel(start_rel_h, n, total_h)
    i1 = idx_for_hour_rel(end_rel_h,   n, total_h)
    if i1 < i0:
        i0, i1 = i1, i0
    return coords[i0:i1+1]

def feature_for_segment(station_id: str,
                        day_label_ymd: str,
                        start_hr_local: int,
                        start_dt_utc: dt.datetime,
                        start_lat: float,
                        start_lon: float,
                        start_hgt_m: float,
                        seg_coords: List[List[float]],
                        met_files: List[str]) -> dict:
    return {
        "type": "Feature",
        "geometry": {"type": "LineString", "coordinates": seg_coords},
        "properties": {
            "trj_id": trj_prefix(station_id),
            "station": station_code(station_id),
            "date": day_label_ymd,
            "start_hr_local": int(start_hr_local),
            "start_utc": start_dt_utc.strftime("%Y-%m-%d %H:00"),
            "seg_h0": -abs(int(RUNTIME_HOURS)),
            "seg_h1": 0,
            "lat0": float(start_lat),
            "lon0": float(start_lon),
            "hgt_m": float(start_hgt_m),
            "runtime_h": int(RUNTIME_HOURS),
            "top_m": float(TOP_OF_MODEL_M),
            # Explicit: show multiple met files used
            "met_files": [os.path.basename(fp) for fp in met_files],
        },
    }

def write_geojson(path: str, name: str, features: List[dict]) -> None:
    ensure_dir(os.path.dirname(path))
    if OVERWRITE_GEOJSON and os.path.exists(path):
        try:
            os.remove(path)
        except Exception:
            pass
    fc = {
        "type": "FeatureCollection",
        "name": name,
        "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
        "features": features,
    }
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(fc, ensure_ascii=False))


# ---------------- MAIN ----------------
def main() -> None:
    export_days = find_three_most_recent_gfs_dates(GFS_DIR)  # ascending
    total_h = abs(int(RUNTIME_HOURS))

    sys.stdout.write("=== HYSPLIT DAILY (-24h) BACKWARD (8 STARTS/DAY) MULTI-DAY MET ===\n")
    sys.stdout.write(f"GFS dir: {GFS_DIR}\n")
    sys.stdout.write("Export days (from newest files present): {}\n".format(", ".join([d.isoformat() for d in export_days])))
    sys.stdout.write("Start hours LOCAL: {}\n".format(START_HOURS_LOCAL))
    sys.stdout.write("Runtime: {}h (backward)\n".format(RUNTIME_HOURS))
    sys.stdout.write(f"MET_DAY_BUFFER: {MET_DAY_BUFFER} day(s)\n")
    sys.stdout.write(f"Output root: {OUT_ROOT}\n")
    sys.stdout.write(f"Runs root: {RUNS_ROOT}\n")
    sys.stdout.write("GeoJSON only (NO shapefiles)\n\n")

    total_runs_all = 0
    completed_runs_all = 0
    skipped_runs_all = 0

    for station_id, sinfo in STATIONS.items():
        START_LAT   = float(sinfo["lat"])
        START_LON   = float(sinfo["lon"])
        START_HGT_M = 200.0

        st = station_code(station_id)
        trj = trj_prefix(station_id)

        sys.stdout.write("\n=== Station {} ({:.6f},{:.6f}) ===\n".format(st, START_LAT, START_LON))

        for day in export_days:
            ymd_str = yyyymmdd(day)

            # OUTPUT PATH (NO per-day folder)
            # D:\TRJ\<STATION>\2026\TRJ_<STATION>_YYYYMMDD.geojson
            out_dir = os.path.join(OUT_ROOT, st, "2026")
            ensure_dir(out_dir)
            out_path = os.path.join(out_dir, "{}_{}.geojson".format(trj, ymd_str))

            feats: List[dict] = []

            day_total = 0
            day_done = 0
            day_skip = 0

            runs_day_base = os.path.join(RUNS_ROOT, st, "2026", ymd_str)
            ensure_dir(runs_day_base)

            for hr_local in START_HOURS_LOCAL:
                day_total += 1
                total_runs_all += 1

                start_local = dt.datetime(day.year, day.month, day.day, hr_local, 0, 0)
                start_utc   = local_to_utc(start_local)

                run_tag = "{}_{:02d}L_{:02d}Z".format(ymd_str, hr_local, start_utc.hour)
                workdir = os.path.join(runs_day_base, run_tag)
                ensure_dir(workdir)

                if CLEAN_PREV_TDUMPS_IN_RUN:
                    bulldoze_run_dir(workdir)

                met_files = met_files_for_run(GFS_DIR, start_utc, RUNTIME_HOURS)

                sys.stdout.write(f"[MET] {st} {run_tag} | {len(met_files)} file(s): "
                                 + ", ".join([os.path.basename(x) for x in met_files]) + "\n")

                if len(met_files) == 0:
                    sys.stdout.write("[Missing met] {} {} | no met files found for start_utc={}\n"
                                     .format(st, run_tag, start_utc.strftime("%Y-%m-%d %H:%M")))
                    day_skip += 1
                    skipped_runs_all += 1
                    continue

                control_path = os.path.join(workdir, "CONTROL")
                write_control(
                    control_path=control_path,
                    start_dt_utc=start_utc,
                    runtime_h=RUNTIME_HOURS,
                    top_m=TOP_OF_MODEL_M,
                    met_files=met_files,
                    out_dir=workdir,
                    out_name="tdump",
                    start_lat=START_LAT,
                    start_lon=START_LON,
                    start_hgt_m=START_HGT_M
                )

                tdump_path = os.path.join(workdir, "tdump")
                tdump_new  = os.path.join(workdir, "tdump_" + run_tag)

                need_run = FORCE_RERUN_ALL or (not os.path.exists(tdump_new) and not os.path.exists(tdump_path))

                if need_run:
                    if os.path.exists(tdump_path):
                        try:
                            os.remove(tdump_path)
                        except Exception:
                            pass

                    try:
                        run_hysplit(workdir)
                    except Exception as e:
                        sys.stdout.write("[Run failed] {} {} | {}\n".format(st, run_tag, e))
                        day_skip += 1
                        skipped_runs_all += 1
                        continue

                    if os.path.exists(tdump_path):
                        try:
                            if os.path.exists(tdump_new):
                                os.remove(tdump_new)
                            shutil.move(tdump_path, tdump_new)
                        except Exception:
                            tdump_new = tdump_path
                    else:
                        sys.stdout.write("[Warn] tdump not found after run: {} {}\n".format(st, run_tag))
                        day_skip += 1
                        skipped_runs_all += 1
                        continue

                use_path = tdump_new if os.path.exists(tdump_new) else tdump_path
                coords = parse_tdump_to_coords(use_path)
                if len(coords) < 2:
                    sys.stdout.write("[Warn] Trajectory has <2 points; skipping: {} {}\n".format(st, run_tag))
                    day_skip += 1
                    skipped_runs_all += 1
                    continue

                rel0, rel1 = SEGMENT_REL_0_24
                seg_coords = slice_segment_rel(coords, rel0, rel1, total_h)

                if len(seg_coords) >= 2:
                    feats.append(feature_for_segment(
                        station_id=station_id,
                        day_label_ymd=ymd_str,
                        start_hr_local=hr_local,
                        start_dt_utc=start_utc,
                        start_lat=START_LAT,
                        start_lon=START_LON,
                        start_hgt_m=START_HGT_M,
                        seg_coords=seg_coords,
                        met_files=met_files
                    ))

                day_done += 1
                completed_runs_all += 1

            write_geojson(out_path, "{}_{}".format(trj, ymd_str), feats)

            sys.stdout.write("\n--- [{} {}] ---\n".format(st, ymd_str))
            sys.stdout.write("Runs scheduled : {}\n".format(day_total))
            sys.stdout.write("Runs completed : {}\n".format(day_done))
            sys.stdout.write("Runs skipped   : {}\n".format(day_skip))
            sys.stdout.write("Wrote: {}\n".format(out_path))

    sys.stdout.write("\n=== ALL DONE ===\n")
    sys.stdout.write("Grand total scheduled runs : {}\n".format(total_runs_all))
    sys.stdout.write("Grand completed runs       : {}\n".format(completed_runs_all))
    sys.stdout.write("Grand skipped runs         : {}\n".format(skipped_runs_all))


if __name__ == "__main__":
    main()
