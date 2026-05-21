 
# Batch CWT exporter for multiple stations (GeoJSON trajectories)

# - Daily GeoJSON trajectory files
# - D:\TRJ\<STATION>\2026\TRJ_<STATION>_YYYYMMDD.geojson
# - Hardcoded stations
# - YYYYMMDD trajectory dates
# - start_hr_local field
# - Dynamic segment time weighting


import os
from datetime import datetime
from collections import defaultdict
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString
# ================== CONFIG ==================
PM_CSV_PATH = r"C:\Users\ryans\Downloads\PM25_11stations_2024_2025.csv"
GRID_FILE = r"C:\Users\ryans\Downloads\Grid_10km\Grid_10km\SEA_Grid10km.shp"
# INPUT TRAJECTORY ROOT
TRJ_ROOT = r"D:\TRJ"
# OUTPUT ROOT
EXPORT_ROOT = r"D:\CWT"
# ---------------- STATIONS ----------------
STATIONS = {
    "76t": {"lat": 16.750102, "lon": 98.591312, "name": "Non-Formal Education Centre"},
    "67t": {"lat": 18.788878, "lon": 100.776359, "name": "Municipality Office"},
    "75t": {"lat": 19.322380, "lon": 101.025365, "name": "Chalermprakiet Hospital"},
    "70t": {"lat": 19.200226, "lon": 99.893048, "name": "Phayao Provincial"},
    "37t": {"lat": 18.278251, "lon": 99.506447, "name": "Meteorological stations"},
    "68t": {"lat": 18.567179, "lon": 99.038560, "name": "Meteorological Staions"},
    "57t": {"lat": 19.909242, "lon": 99.823357, "name": "Natural Resources and Environment Office"},
    "73t": {"lat": 20.427234, "lon": 99.883724, "name": "Maesai Health Office"},
    "69t": {"lat": 18.128928, "lon": 100.162345, "name": "Meteorology Center"},
    "35t": {"lat": 18.840633, "lon": 98.969661, "name": "City Hall"},
    "58t": {"lat": 19.304686, "lon": 97.970999, "name": "Natural Resources and Environment Office"},
    "o20": {"lat": 18.15917727, "lon": 97.93315927, "name": "Mae Sa Riang"},
    "o73": {"lat": 17.80241664, "lon": 98.95016385, "name": "Li"},
    "o75": {"lat": 19.35872200, "lon": 98.43939900, "name": "Pai North"},
}
# ================== TIME WINDOW ==================
START_DATE = datetime(2026, 1, 1)
END_DATE = datetime(2026, 5, 31, 23, 59, 59)
# ================== CWT PARAMETERS ==================
MIN_RES_HOURS = 5.0
OVERWRITE = False
# ====================================================
# HELPERS
# ====================================================
def ensure_dir(path):
    if not os.path.isdir(path):
        os.makedirs(path)
def load_grid(path):
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    return gdf
# ====================================================
# PM DATA
# ====================================================
def read_pm_for_station(pm_csv, station_id):
    df = pd.read_csv(pm_csv)
    df["Date"] = pd.to_datetime(
        df["Date"].astype(str).str[:10],
        errors="coerce"
    )
    df = df[
        df["Station"].astype(str).str.upper()
        == station_id.upper()
    ]
    pm_col = (
        "PM25"
        if "PM25" in df.columns
        else next(c for c in df.columns if c.lower().startswith("pm"))
    )
    df = df.dropna(subset=["Date", pm_col])
    df = df[
        (df["Date"] >= pd.Timestamp(START_DATE.date())) &
        (df["Date"] <= pd.Timestamp(END_DATE.date()))
    ]
    # store as YYYYMMDD for exact match
    return dict(
        zip(
            df["Date"].dt.strftime("%Y%m%d"),
            df[pm_col].astype(float)
        )
    )
# ====================================================
# DAILY TRAJECTORY FILE DISCOVERY
# ====================================================
def iter_daily_geojsons(station_id):
    st = station_id.upper()
    root = os.path.join(TRJ_ROOT, st, "2026")
    if not os.path.isdir(root):
        return
    for fname in sorted(os.listdir(root)):
        if not fname.lower().endswith(".geojson"):
            continue
        # expected:
        # TRJ_76T_20260217.geojson
        parts = fname.replace(".geojson", "").split("_")
        if len(parts) < 3:
            continue
        ymd = parts[-1]
        try:
            dt_obj = datetime.strptime(ymd, "%Y%m%d")
        except:
            continue
        if dt_obj < START_DATE or dt_obj > END_DATE:
            continue
        path = os.path.join(root, fname)
        yield ymd, path
# ====================================================
# MONTH KEY
# ====================================================
def month_key_from_ymd(ymd):
    return ymd[:6]
# ====================================================
# CWT CORE
# ====================================================
def process_trajectory_file(
    grid_gdf,
    geojson_path,
    daily_pm,
    num,
    den
):
    gdf = gpd.read_file(geojson_path)
    if len(gdf) == 0:
        return
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326", allow_override=True)
    if gdf.crs != grid_gdf.crs:
        gdf = gdf.to_crs(grid_gdf.crs)
    cols_lower = {c.lower(): c for c in gdf.columns}
    # normalize field names
    rename_map = {}
    if "date" in cols_lower:
        rename_map[cols_lower["date"]] = "date"
    if "start_hr_local" in cols_lower:
        rename_map[cols_lower["start_hr_local"]] = "start_hr_local"
    gdf = gdf.rename(columns=rename_map)
    sindex = grid_gdf.sindex
    for _, row in gdf.iterrows():
        dstr = str(row["date"]).strip()
        # PM lookup
        pm = daily_pm.get(dstr)
        if pm is None:
            continue
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue
        lines = [geom] if isinstance(geom, LineString) else list(geom.geoms)
        for line in lines:
            coords = list(line.coords)
            if len(coords) < 2:
                continue
            # dynamic segment time
            segment_hours = 24.0 / (len(coords) - 1)
            for i in range(len(coords) - 1):
                seg = LineString([
                    coords[i],
                    coords[i + 1]
                ])
                seg_len = seg.length
                if seg_len <= 0:
                    continue
                for gi in sindex.intersection(seg.bounds):
                    cell = grid_gdf.geometry.iloc[gi]
                    if not seg.intersects(cell):
                        continue
                    inter = seg.intersection(cell)
                    if inter.is_empty:
                        continue
                    frac = inter.length / seg_len
                    hrs = segment_hours * frac
                    num[gi] += pm * hrs
                    den[gi] += hrs
# ====================================================
# EXPORT
# ====================================================
def export_geojson(
    grid,
    station_id,
    month_key,
    vals
):
    year = month_key[:4]
    out_dir = os.path.join(
        EXPORT_ROOT,
        f"{year}_{station_id.upper()}"
    )
    ensure_dir(out_dir)
    out_path = os.path.join(
        out_dir,
        f"CWT_{month_key}_{station_id.upper()}.geojson"
    )
    out = grid.copy()
    out["CWT"] = [
        vals.get(i, float("nan"))
        for i in range(len(out))
    ]
    out.to_file(out_path, driver="GeoJSON")
    print(
        f" -> {out_path} "
        f"| cells with CWT: {out['CWT'].notna().sum()}"
    )
def outputs_exist(month_key, station_id):
    year = month_key[:4]
    out_dir = os.path.join(
        EXPORT_ROOT,
        f"{year}_{station_id.upper()}"
    )
    path = os.path.join(
        out_dir,
        f"CWT_{month_key}_{station_id.upper()}.geojson"
    )
    return os.path.isfile(path)
# ====================================================
# MAIN
# ====================================================
def main():
    print("=== Batch CWT export (GeoJSON trajectories) ===")
    ensure_dir(EXPORT_ROOT)
    grid = load_grid(GRID_FILE)
    for st_key, meta in STATIONS.items():
        st = st_key.upper()
        print(f"\n--- Station {st} ({meta['name']}) ---")
        daily_pm = read_pm_for_station(
            PM_CSV_PATH,
            st
        )
        if not daily_pm:
            print(" No PM data — skipping.")
            continue
        # monthly accumulators
        monthly_num = defaultdict(lambda: defaultdict(float))
        monthly_den = defaultdict(lambda: defaultdict(float))
        found = False
        for ymd, geojson_path in iter_daily_geojsons(st):
            found = True
            mkey = month_key_from_ymd(ymd)
            print(f" {ymd}: processing")
            process_trajectory_file(
                grid,
                geojson_path,
                daily_pm,
                monthly_num[mkey],
                monthly_den[mkey]
            )
        if not found:
            print(" No GeoJSONs found.")
            continue
        # export monthly CWTs
        for mkey in sorted(monthly_num.keys()):
            if not OVERWRITE and outputs_exist(mkey, st):
                print(f" {mkey}: already exported")
                continue
            vals = {}
            for gi in monthly_den[mkey]:
                if monthly_den[mkey][gi] >= MIN_RES_HOURS:
                    vals[gi] = (
                        monthly_num[mkey][gi]
                        / monthly_den[mkey][gi]
                    )
            export_geojson(
                grid,
                st,
                mkey,
                vals
            )
    print("\nDone.")
if __name__ == "__main__":
    main()


