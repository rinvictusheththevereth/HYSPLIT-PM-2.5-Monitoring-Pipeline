# -*- coding: utf-8 -*-
"""
Daily PSCF - Single day (2026-03-08) - Endpoint counting method
- Only hotspots from provinces: 50, 51, 52, 54, 55, 56, 57, 58, 63
- One output GeoJSON per station (using combined hotspots from the 9 provinces)
- Output folder: D:\PSCF\PSCF_Overall\<station>\2026\...
"""

import os
import json
import re
import warnings
from datetime import date
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ========================= CONFIG =========================
TRAJ_ROOT = r"D:\TRJ"
HOTSPOTS = r"E:\FIRMS\thailand_hotspot_20260405_20260405.csv"          # ← changed to March 8
GRID_PATH = r"D:\PSCF_0\SEA_grid10km.shp"
PROV_POLY = r"D:\admin_polygons\L05_Province_ESRI_2559.shp"
OUT_ROOT = r"D:\PSCF\PSCF_Overall"  # ← changed he3e

GRID_CRS_EPSG = 4326

# Trajectory fields
TRAJ_ID_FLD = "trj_id"
TRAJ_DATE_FLD = "date"
TRAJ_HOUR_FLD = "start_hr_local"   # local time
TRAJ_ALT_FLD = "hgt_m"

# Hotspot fields
HS_LAT = "latitude"
HS_LON = "longitude"
HS_DATE = "acq_date"
HS_TIME = "acq_time"
HS_CONF = "confidence"
HS_PROV = "province_id"            # numeric, matches PV_IDN

CONF_ALLOW = {"h", "n"}
LOCAL_TZ = "Asia/Bangkok"
TARGET_DATE = date(2026, 4, 5)     # ← changed to March 8

# Only these provinces' hotspots are used (for ALL stations)
ALLOWED_PROVINCES = {"50", "51", "52", "54", "55", "56", "57", "58", "63"}

# PSCF settings
M_COUNT_PROV_STRICT = False        # count anywhere on hotspot days
MIN_N = None                       # no minimum
USE_WEIGHT = True                  # standard weight function

# ========================= HELPERS =========================
def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def clean_date(x):
    if pd.isna(x):
        return pd.NaT
    s = str(x).strip()
    if s.startswith('{') and s.endswith('}'):
        try:
            d = json.loads(s)
            if 'value' in d:
                return d['value']
        except:
            pass
        s = re.sub(r'^\{"value":\s*"', '', s)
        s = re.sub(r'"\s*\}$', '', s).strip()
    return s

def parse_hhmm(v):
    if pd.isna(v):
        return 0, 0
    try:
        s = str(int(float(v))).zfill(4)
        return int(s[:2]), int(s[2:])
    except:
        return 0, 0

def explode_to_points(gdf, id_fields):
    recs = []
    for _, row in gdf.iterrows():
        geom = row.geometry
        if geom is None:
            continue
        coords = list(geom.coords) if geom.geom_type == "LineString" else \
                 [pt for ls in geom.geoms for pt in ls.coords] if geom.geom_type == "MultiLineString" else []
        base = row[id_fields].to_dict()
        for x, y in coords:
            r = dict(base)
            r["geometry"] = Point(x, y)
            recs.append(r)
    return gpd.GeoDataFrame(recs, geometry="geometry", crs=gdf.crs)

def spatial_count(pts, grid, grid_key="GID"):
    if pts.empty:
        return pd.Series(dtype=int, name="count")
    j = gpd.sjoin(pts, grid[[grid_key, "geometry"]], how="left", predicate="intersects")
    return j[grid_key].value_counts()

def weight_func(n):
    if n >= 20: return 1.0
    if n >= 10: return 0.7
    if n >= 5:  return 0.4
    if n >= 3:  return 0.2
    return np.nan

def safe_join(gdf, prov, code_field):
    j = gpd.sjoin(gdf, prov[[code_field, "geometry"]], how="left", predicate="within")
    j = j.drop(columns=["index_right"], errors="ignore")
    if code_field in j.columns:
        j = j.rename(columns={code_field: "province_id"})
    return j.loc[:, ~j.columns.duplicated()].copy()

# ========================= MAIN =========================
def main():
    # Load grid
    grid = gpd.read_file(GRID_PATH)
    if grid.crs is None:
        grid.set_crs(epsg=GRID_CRS_EPSG, inplace=True)
    if "GID" not in grid.columns:
        grid["GID"] = grid.index.astype(int)
    print(f"[GRID] CRS={grid.crs}, cells={len(grid)}")

    # Load provinces
    prov = gpd.read_file(PROV_POLY)
    if prov.crs is None:
        prov.set_crs(epsg=4326, inplace=True)
    if str(prov.crs) != str(grid.crs):
        prov = prov.to_crs(grid.crs)
    prov["PV_IDN"] = prov["PV_IDN"].astype(str)

    # Hotspots - load and filter to ONLY the 9 provinces
    hs_raw = pd.read_csv(HOTSPOTS)
    print(f"[HOTSPOTS] Loaded {len(hs_raw)} rows")

    hs = gpd.GeoDataFrame(
        hs_raw,
        geometry=gpd.points_from_xy(hs_raw[HS_LON], hs_raw[HS_LAT]),
        crs="EPSG:4326"
    )
    if str(hs.crs) != str(grid.crs):
        hs = hs.to_crs(grid.crs)

    hs[HS_DATE] = hs[HS_DATE].apply(clean_date)
    hs["hs_date"] = pd.to_datetime(hs[HS_DATE], errors='coerce').dt.date
    hs = hs[hs["hs_date"].notna()]

    hh, mm = zip(*hs[HS_TIME].apply(parse_hhmm))
    hs["_h"] = hh
    hs["_m"] = mm
    hs["hs_dt_utc"] = pd.to_datetime(hs["hs_date"]) + pd.to_timedelta(hs["_h"], "h") + pd.to_timedelta(hs["_m"], "m")
    hs["hs_dt_local"] = hs["hs_dt_utc"].dt.tz_localize("UTC").dt.tz_convert(LOCAL_TZ)
    hs["hs_local_date"] = hs["hs_dt_local"].dt.date

    hs = hs[hs["hs_local_date"] == TARGET_DATE].copy()
    hs["_conf"] = hs[HS_CONF].astype(str).str.lower()
    hs = hs[hs["_conf"].isin(CONF_ALLOW)].copy()
    print(f"[HOTSPOTS] Date & confidence filtered: {len(hs)} points")

    hs = safe_join(hs, prov, "PV_IDN")
    hs = hs[hs["province_id"].notna()].copy()
    hs["province_id"] = hs["province_id"].astype(str)

    # FINAL FILTER: only keep hotspots from the 9 provinces
    hs = hs[hs["province_id"].isin(ALLOWED_PROVINCES)].copy()
    print(f"[HOTSPOTS] After province filter (9 provinces only): {len(hs)} points")

    if hs.empty:
        print("No hotspots in the selected 9 provinces → nothing to compute.")
        return

    # All allowed provinces are treated as one group (single m_ij)
    allowed_hot_days = {TARGET_DATE}  # only one day

    # Stations
    stations = [d for d in os.listdir(TRAJ_ROOT) if os.path.isdir(os.path.join(TRAJ_ROOT, d))]
    print(f"[STATIONS] Found {len(stations)} folders")

    for station in sorted(stations):
        year_dir = os.path.join(TRAJ_ROOT, station, "2026")
        if not os.path.isdir(year_dir):
            continue

        file_name = f"TRJ_{station}_{TARGET_DATE.strftime('%Y%m%d')}.geojson"
        traj_path = os.path.join(year_dir, file_name)

        if not os.path.exists(traj_path):
            print(f"[{station}] Missing: {file_name}")
            continue

        print(f"[{station}] Loading {file_name}")
        traj = gpd.read_file(traj_path)
        if traj.crs is None:
            traj.set_crs(epsg=4326, inplace=True)
        if str(traj.crs) != str(grid.crs):
            traj = traj.to_crs(grid.crs)

        traj["traj_date"] = pd.to_datetime(traj[TRAJ_DATE_FLD], errors="coerce").dt.date
        traj["_hr"] = pd.to_numeric(traj[TRAJ_HOUR_FLD], errors="coerce").fillna(0).astype(int)

        traj["traj_local_date"] = traj["traj_date"]  # already local

        day_traj = traj[traj["traj_local_date"] == TARGET_DATE].copy()
        if day_traj.empty:
            print(f"[{station}] No points on {TARGET_DATE}")
            continue

        day_traj["station"] = station
        traj_pts = explode_to_points(day_traj, [TRAJ_ID_FLD, "traj_local_date", "_hr", "station", TRAJ_ALT_FLD])
        if str(traj_pts.crs) != str(grid.crs):
            traj_pts = traj_pts.to_crs(grid.crs)

        traj_pts = safe_join(traj_pts, prov, "PV_IDN")
        traj_pts = traj_pts[traj_pts["province_id"].notna()].copy()
        print(f"[{station}] {len(traj_pts)} points")

        n_counts = spatial_count(traj_pts, grid)

        # m_ij: using hotspots from ALL 9 provinces combined
        pts_m = traj_pts  # no strict province filter - count anywhere
        m_counts = spatial_count(pts_m, grid)

        out = grid[["GID", "geometry"]].copy()
        out = out.merge(n_counts.rename("n_ij"), left_on="GID", right_index=True, how="left")
        out = out.merge(m_counts.rename("m_ij"), left_on="GID", right_index=True, how="left")
        out[["n_ij", "m_ij"]] = out[["n_ij", "m_ij"]].fillna(0).astype(int)

        with np.errstate(divide="ignore", invalid="ignore"):
            out["pscf_raw"] = out["m_ij"] / out["n_ij"].replace(0, np.nan)

        if USE_WEIGHT:
            out["weight"] = out["n_ij"].apply(weight_func)
            out["pscf_w"] = out["pscf_raw"] * out["weight"]
        else:
            out["pscf_w"] = out["pscf_raw"]

        out["valid"] = (out["n_ij"] >= 3).astype(int) if MIN_N == 3 else 1
        out["date"] = TARGET_DATE
        out["station"] = station
        out["hotspot_provinces"] = ", ".join(sorted(ALLOWED_PROVINCES))  # metadata

        n_ok = (out["n_ij"] >= 3).sum()
        m_pos = (out["m_ij"] > 0).sum()
        print(f"[{station} | {TARGET_DATE}] n>=3={n_ok}, m>0={m_pos}")

        # Output - one file per station
        out_dir = os.path.join(OUT_ROOT, station, "2026")
        ensure_dir(out_dir)
        fname = f"PSCF_{station}_9provinces_2026_{TARGET_DATE.strftime('%Y%m%d')}.geojson"
        out_path = os.path.join(out_dir, fname)

        out.to_file(out_path, driver="GeoJSON")
        print(f"[{station}] Saved: {out_path}")

    print("Done.")

if __name__ == "__main__":
    main()
