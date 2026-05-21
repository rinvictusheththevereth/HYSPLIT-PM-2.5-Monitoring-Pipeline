# -*- coding: utf-8 -*-
"""
HYSPLIT-PM-2.5 Daily Pipeline Orchestrator
Main entry point to run the full pipeline with safety, state tracking, and flexibility.
"""

import argparse
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from utils.data_inventory import (
    PipelineState,
    find_three_most_recent_gfs_dates,
    find_most_recent_firms_date,
    yyyymmdd,
)

LOCAL_TZ = ZoneInfo("Asia/Bangkok")


def run_script(script_name: str, args: list[str] | None = None, description: str = ""):
    """Run a numbered script with logging."""
    print(f"\n{'='*80}")
    print(f"🚀 Running {script_name} - {description}")
    print(f"{'='*80}\n")

    cmd = [sys.executable, script_name]
    if args:
        cmd.extend(args)

    try:
        result = subprocess.run(cmd, check=True, capture_output=False, text=True)
        print(f"✅ {script_name} completed successfully.\n")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {script_name} failed with exit code {e.returncode}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Run the full HYSPLIT PM2.5 Monitoring Pipeline"
    )
    parser.add_argument("--date", type=str, help="Target date in YYYYMMDD (default = today)")
    parser.add_argument("--force", action="store_true", help="Force re-run everything")
    parser.add_argument("--force-gfs", action="store_true", help="Force GFS download")
    parser.add_argument("--force-firms", action="store_true", help="Force FIRMS download")
    parser.add_argument("--skip-forward", action="store_true", help="Skip forward trajectories")
    parser.add_argument("--skip-receptors", action="store_true", help="Skip PSCF/CWT")
    parser.add_argument("--dry-run", action="store_true", help="Show what would run without executing")

    args = parser.parse_args()

    target_date = datetime.now(LOCAL_TZ).date()
    if args.date:
        target_date = datetime.strptime(args.date, "%Y%m%d").date()

    target_ymd = yyyymmdd(target_date)
    print(f"🎯 Running pipeline for date: {target_ymd} ({target_date})")
    print(f"   Current Bangkok time: {datetime.now(LOCAL_TZ)}")

    state = PipelineState()

    steps = []

    # 1. GFS Download
    if args.force or args.force_gfs or not state.data.get("gfs", {}).get("last_successful_date"):
        steps.append(("1.py", ["--force"], "Download GFS meteorological data"))

    # 2. Backward Trajectories
    steps.append(("2.py", ["--date", target_ymd], "Backward trajectories from receptors"))

    # 3. FIRMS Hotspots
    if args.force or args.force_firms:
        steps.append(("3.py", ["--force"], "Download latest FIRMS hotspots"))
    else:
        steps.append(("3.py", [], "Download latest FIRMS hotspots"))

    # 4. (Optional pre-processing if you have it)

    # 5. Forward Trajectories from hotspots
    if not args.skip_forward:
        steps.append(("6.py", ["--date", target_ymd], "Forward trajectories from fire hotspots"))

    # 6 & 7. Receptor Modeling
    if not args.skip_receptors:
        steps.append(("7.py", ["--date", target_ymd], "PSCF & CWT Receptor Modeling"))

    # Execute steps
    success_count = 0
    for script, script_args, desc in steps:
        if args.dry_run:
            print(f"   [DRY-RUN] Would run: {script} {' '.join(script_args)}")
            continue

        if run_script(script, script_args, desc):
            success_count += 1
            # Update state
            if "1.py" in script:
                state.mark_gfs_success(target_ymd)
            elif "3.py" in script:
                state.mark_firms_success(target_ymd)
            elif any(x in script for x in ["2.py", "6.py", "7.py"]):
                state.mark_day_completed(target_ymd, script.replace(".py", ""))
        else:
            print(f"⚠️  Pipeline continuing despite {script} failure...")

    print(f"\n{'='*80}")
    print(f"Pipeline finished! {success_count}/{len(steps)} steps completed.")
    print(f"State saved to pipeline_state.json")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()
