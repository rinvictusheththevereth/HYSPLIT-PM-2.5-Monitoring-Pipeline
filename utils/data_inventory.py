# -*- coding: utf-8 -*-
"""
Shared data discovery utilities for the HYSPLIT-PM-2.5 Pipeline
Reduces duplication while preserving robust standalone + pipeline behavior.
"""

from __future__ import annotations

import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Tuple

import json
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("Asia/Bangkok")


# ====================== GFS ======================

def find_three_most_recent_gfs_dates(gfs_dir: str | Path) -> List[date]:
    """Return up to the 3 most recent valid GFS dates (sorted ascending)."""
    gfs_dir = Path(gfs_dir)
    pat = re.compile(r"^(\d{8})_gfs0p25\.txt$")

    dates: List[date] = []
    for f in gfs_dir.iterdir():
        if not f.is_file():
            continue
        m = pat.match(f.name)
        if m:
            try:
                d = datetime.strptime(m.group(1), "%Y%m%d").date()
                dates.append(d)
            except Exception:
                continue

    dates = sorted(set(dates))
    return dates[-3:] if len(dates) >= 3 else dates


def met_file_path_for_date(gfs_dir: str | Path, d: date) -> Path:
    return Path(gfs_dir) / f"{d:%Y%m%d}_gfs0p25.txt"


# ====================== FIRMS / Hotspots ======================

def find_most_recent_firms_date(firms_dir: str | Path, lookback_days: int = 21) -> date | None:
    """Find the most recent date with a non-empty hotspot CSV in FIRMS folder."""
    firms_dir = Path(firms_dir)
    today = datetime.now(LOCAL_TZ).date()

    for i in range(lookback_days + 1):
        d = today - timedelta(days=i)
        pattern = f"*_{d:%Y%m%d}_*.csv"
        matches = list(firms_dir.glob(pattern))
        if matches:
            return d
    return None


# ====================== Pipeline State Integration ======================

class PipelineState:
    """Simple persistent state tracker (pipeline_state.json)"""

    def __init__(self, filepath: str | Path = "pipeline_state.json"):
        self.filepath = Path(filepath)
        self.data = self._load()

    def _load(self) -> dict:
        if self.filepath.exists():
            try:
                with open(self.filepath, encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        # Default structure
        return {
            "last_updated": datetime.now(LOCAL_TZ).isoformat(),
            "timezone": "Asia/Bangkok",
            "gfs": {"last_successful_date": None, "pending_dates": []},
            "firms": {"last_successful_date": None, "pending_dates": []},
            "daily_summary": {},
            "metadata": {"version": "1.1"}
        }

    def save(self):
        self.data["last_updated"] = datetime.now(LOCAL_TZ).isoformat()
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)

    def mark_gfs_success(self, ymd: str):
        self.data["gfs"]["last_successful_date"] = ymd
        self.save()

    def mark_firms_success(self, ymd: str):
        self.data["firms"]["last_successful_date"] = ymd
        self.save()

    def mark_day_completed(self, ymd: str, stage: str):
        if ymd not in self.data["daily_summary"]:
            self.data["daily_summary"][ymd] = {"status": "partial"}
        self.data["daily_summary"][ymd][f"{stage}_ok"] = True
        self.data["daily_summary"][ymd]["processed_at"] = datetime.now(LOCAL_TZ).isoformat()
        self.save()


# ====================== Helpers ======================

def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def get_pending_gfs_dates(gfs_dir: str | Path, state: PipelineState | None = None) -> List[date]:
    """Future extension point: combine filesystem + state for pending logic."""
    return find_three_most_recent_gfs_dates(gfs_dir)


# Example usage at bottom (for testing)
if __name__ == "__main__":
    print("Recent GFS dates:", [d.isoformat() for d in find_three_most_recent_gfs_dates(r"E:\GFS")])
