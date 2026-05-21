HAZEFREE PM 2.5 MONITORING PIPELINE


####################


SCRIPT 1: Ready GFS 0.25° downloader (MULTI-DAY PARALLEL + FORCE RE-DOWNLOAD)
This Python script automates the acquisition of the three most recent complete Global Forecast System (GFS) 0.25° resolution archive files from the NOAA READY archive (https://www.ready.noaa.gov/data/archives/gfs0p25/). It employs concurrent downloading, robust validation, forced re-download semantics, and defensive error handling.
The design prioritizes reliability and freshness over efficiency—suitable for operational meteorological data pipelines where stale or corrupted GRIB/TAR files must be avoided. It serves as the foundational data-ingestion component in a presumed six daily script workflow.
Key Metrics (as implemented):
Concurrency: Up to 3 parallel downloads
Minimum file size: 2 GB (to prevent GFSxxxxx.part)
Signature validation: GRIB, READY-INDX, GZIP, TAR
Retry policy: 3 attempts with exponential back-off potential
Force-delete + re-download behavior

2. Functional Requirements Fulfilled
The script satisfies the following requirements:
Discovery: HTML scraping of the root archive index to identify dated subdirectories (\d{8}_gfs0p25).
Selection: Most recent N=3 complete archives, with remote size pre-check.
Acquisition: Parallel HTTP streaming downloads with large (16 MiB) chunks.
Validation: Post-download size + binary signature checks.
Idempotency/Safety: Explicit deletion of prior files and partials before each run.
Logging: Timestamped, timezone-aware (Asia/Bangkok) console output.
Resilience: Timeouts, retries, exception isolation per download.

3. Architectural Breakdown
3.1 Core Modules & Dependencies
Standard Library: pathlib, datetime, zoneinfo, re, time, sys, concurrent.futures
Third-party: requests (imported lazily inside functions for cleaner error handling)
No external configuration files or heavy frameworks—lightweight and portable.
3.2 Major Functions
Function
Responsibility
Key Techniques
iter_candidate_urls()
Root page scraping & regex parsing
re.compile, sorted reverse
pick_latest_n_downloadable()
Size filtering + selection
HEAD requests for Content-Length
download()
Streaming download + validation
Chunked iter_content, signature scan
download_task()
Per-file orchestration (delete + download)
Atomic replace via .part
valid_signature()
Magic-byte detection
GRIB, @INDX, gzip, ustar checks
remote_size_bytes()
Pre-flight size check
HEAD request

3.3 Concurrency Model
Uses ThreadPoolExecutor(max_workers=N_DOWNLOADS) — appropriate for I/O-bound network tasks. as_completed() ensures logging order reflects completion rather than submission.

4. Strengths (Best Practices Demonstrated)
Defensive Programming: Extensive try/except blocks, fallback None returns, cleanup of partial files.
Resource Efficiency: 16 MiB chunks reduce syscall overhead; streaming avoids full in-memory buffering.
Data Integrity: Dual validation (size + magic bytes) prevents silent corruption.
Operational Robustness: Force-re-download policy eliminates “stale data” bugs common in incremental downloaders.
Timezone Awareness: Correct use of ZoneInfo("Asia/Bangkok") for logging in the user’s region.
Clean Separation: Configuration at top, pure functions, minimal global state.
User-Agent Politeness: Custom header identifies the script.

5. Limitations & Potential Issues
HTML Scraping Fragility Relies on regex against raw HTML. Any change to NOAA’s directory listing format (e.g., new CSS classes, JavaScript rendering) breaks discovery. In such a case, this issue would cascade through other scripts 2-6. Any change to NOAA’s directory listing format would require a script update to accommodate new format.
No Rate Limiting / Back-pressure Three simultaneous large (~2–10 GB) downloads may trigger server-side throttling or IP blocks if run too frequently. Currently, each GFS file is 3.1GB. 
Lazy Imports import requests inside functions is clever for error isolation but slightly hurts readability and static analysis. 
Missing Features:
No progress bars (e.g., tqdm).
No checksum verification (NOAA sometimes provides .md5).
No support for partial resume (always full re-download).
No logging to file or structured (JSON) output.
Hard-coded N_DOWNLOADS=3; could be CLI argument.
No asynchronous I/O (asyncio + aiohttp) for higher concurrency.
Windows-centric Path E:\GFS is hardcoded; Path(r"E:\GFS") works cross-platform but the drive letter is not. 
Security No certificate pinning or advanced TLS validation (acceptable for public NOAA data).

6. Quantitative Assessment
Code Quality Metrics (manual):
Cyclomatic complexity: Low–Medium
Error handling coverage: Excellent (~95% of network paths)
Testability: Moderate (pure functions easy to unit test; network side requires mocking)
Maintainability: High (clear structure, good comments)
Performance Characteristics:
Typical run: ~60–90 minutes depending on network and file sizes.
Memory footprint: < 100 MiB (streaming).
Disk I/O: Temporary .part files up to full size.

7. Recommendations (Script 1 → Future Versions)
Replace HTML scraping with official NOAA API or RSS feed if available, or switch to beautifulsoup4 + robust selectors.
Add CLI interface (argparse): --days, --output-dir, --no-force, --verbose.
Implement resume support using HTTP Range requests + temporary file tracking.
Add MD5/SHA verification when NOAA provides manifests.
Structured logging (logging module + JSON handler) for integration with orchestration tools (Airflow, Prefect, cron).
Configuration file (TOML/YAML) for settings.
Health checks post-download (e.g., try opening with cfgrib or xarray).
Containerization readiness: Dockerfile + volume mount for /data.

8. Role in the Six-Script Pipeline
As Script 1, this module establishes the raw data foundation. The force-re-download design suggests the pipeline values reproducibility and fresh daily runs over bandwidth conservation.


##########################


SCRIPT 2: BWT (TRJ) HYSPLIT -24-hour BACKWARD trajectories (Bangkok local time, UTC+7)
This script automates the generation of -24 hour backward air mass trajectories using the NOAA HYSPLIT model (hyts_std.exe). It processes the three most recent GFS 0.25° meteorological files produced by Script 1, runs trajectories for 14 air quality monitoring stations in northern Thailand, at 8 fixed local hours per day (every 3 hours), and exports results as GeoJSON FeatureCollections.The design is highly domain-specific, meteorologically accurate (proper UTC handling, multi-day met file selection with day buffer, backward runtime logic), and production-oriented. 
Key Operational Parameters:
3 most recent GFS days × 14 stations × 8 start hours = 336 trajectories per full run
Backward runtime: –24 h
Output: One GeoJSON per station per day containing 8 LineString features
Force-re-run capable, robust cleanup, detailed logging

2. Functional Requirements Fulfilled
Input Discovery: Automatically detects the 3 newest YYYYMMDD_gfs0p25.txt files from E:\GFS.
Temporal Handling: Correct Bangkok local → UTC conversion; handles trajectories that cross UTC day boundaries.
Meteorological File Selection: Intelligent multi-file selection with MET_DAY_BUFFER to avoid edge effects.
HYSPLIT Orchestration: Generates CONTROL file, runs hyts_std.exe, manages working directories.
Output: Clean GeoJSON with rich properties (metadata, used met files, segment slicing).
Robustness: Force rerun, tdump cleanup, error isolation per trajectory.

3. Architectural Breakdown
3.1 Core Dependencies
Standard Library: os, sys, json, shutil, datetime, subprocess, re, time
External: HYSPLIT executable (hyts_std.exe) + ASCDATA.CFG
No heavy Python scientific stack (deliberately lightweight)
3.2 Major Functions
Function
Responsibility
Key Techniques
find_three_most_recent_gfs_dates()
Discover latest GFS files
Regex + file system scan
met_files_for_run()
Select required + buffered met files
Date arithmetic with buffer
write_control()
Generate HYSPLIT CONTROL file
Precise ASCII formatting, path handling
run_hysplit()
Execute model via subprocess
Captures stdout/stderr, error reporting
parse_tdump_to_coords()
Extract trajectory points
Robust text parsing + validation
feature_for_segment()
Build GeoJSON Feature
Rich metadata embedding
write_geojson()
Export FeatureCollection
Overwrite control, CRS specification
main()
Orchestration loop
Station × Day × Hour nested loops

3.3 Data Flow
GFS files (Script 1) → Met file selection → CONTROL generation → HYSPLIT execution → tdump parsing → GeoJSON output tree (D:\TRJ\<station>\2026\TRJ_XX_YYYYMMDD.geojson)

4. Strengths (Best Practices Demonstrated)
Meteorological Correctness: Excellent handling of HYSPLIT backward trajectories, UTC/local conversion, and multi-day met data requirements.
Production Robustness: FORCE_RERUN_ALL, cleanup routines, detailed per-run logging, graceful degradation on missing files.
Output Quality: Rich GeoJSON properties (including which met files were used) — extremely valuable for traceability and debugging.
Modularity: Well-separated utility functions; clear constants at top.
Error Resilience: Per-trajectory exception handling; continues even if individual runs fail.
Reproducibility: Explicit working directories, tdump renaming with tags, consistent naming conventions.
Thai Context: Hard-coded stations with real names and coordinates for northern Thailand air quality network.

5. Limitations & Potential Issues
Hard-coded Paths — E:\GFS, D:\TRJ, D:\hysplit_runs_backward, C:\hysplit\... reduce portability.
No Parallelism — 336 sequential HYSPLIT runs in serial. (BWT does not take very long, ~20 minutes).  
HYSPLIT Dependency — Requires Windows + installed HYSPLIT; brittle if executable path changes. 
Limited Configuration — No CLI arguments (as discussed previously), everything is top-level constants.
File System Heavy — Creates many temporary directories/files; no automatic cleanup of old runs. Need a periodic deletion of ‘hysplit_runs_backward’.
Parsing Fragility — tdump parsing assumes fixed column positions (parts[9] and [10]).
No Progress Bar or ETA for long runs. 
Year Hard-coding — Output goes into 2026\ folder will need manual update yearly.

6. Quantitative Assessment
Code Quality Metrics:
Readability: Very Good
Maintainability: High (well-commented, logical structure)
Error Handling: Excellent
Domain Knowledge Embedded: Outstanding
Scale:
~336 model executions per run
Output: 42 GeoJSON files (14 stations × 3 days)
Typical runtime: 2–8 hours depending on hardware and HYSPLIT speed
Security / Safety: Safe (local execution), but large temporary disk usage on D:\.

7. Recommendations for Improvement
Add argparse CLI (as recommended in Script 1 report):
--days, --gfs-dir, --output-root, --force, --stations, --hours, etc.
Parallel Execution: Use concurrent.futures.ProcessPoolExecutor (HYSPLIT is CPU-bound).
Configuration File: TOML/YAML for stations, paths, and parameters.
Containerization / Environment Management: Docker with HYSPLIT or WSL2 support.
Post-processing: Optional conversion to shapefile, KML, or database ingestion.
Monitoring: Log summary statistics (average trajectory length, success rate) and send notifications.
Testing: Unit tests for date logic, met file selection, and tdump parsing.
Modernization: Consider Python bindings (pysplit or hysplit Python API) if available in future versions to eliminate subprocess calls.

8. Role in the Six-Script Pipeline
Script 2 is the computational core of the pipeline:
Consumes raw GFS data from Script 1
Produces geospatial trajectory products used by Scripts 5–6 
The choice of GeoJSON + rich metadata makes downstream integration (QGIS, Leaflet, Python geospatial stack, databases) very clean.


####################


SCRIPT 3: FIRMS-style daily downloader (most-recent-day focus)
This script retrieves active wildfire/hotspot detection data for Thailand from a custom REST API (http://35.187.240.74:3000/...). In addition to FIRMS data, it includes province, amphoe, and tambon administrative codes. Unlike bulk downloaders, it intelligently probes backward from the current Bangkok local date to locate the most recent day containing hotspot records, then downloads and converts that single day’s data into a well-structured CSV file in E:\FIRMS. It includes robust HTTP retry logic, NDJSON caching for resumability, flexible JSON-to-CSV normalization, and comprehensive sidecar files (empty dates, errors, cache). The design reflects a mature, production-oriented data ingestion component that complements the meteorological pipeline (Scripts 1–2). Key Features:
Automatic “latest non-empty day” discovery with configurable lookback
Full argparse CLI support (already implemented — excellent!)
Resilient parsing of variable JSON structures
Caching + resume capability
Rich output: main CSV + metadata files

2. Functional Requirements Fulfilled
Temporal Intelligence: Finds the newest Bangkok-local date with actual hotspot records.
Data Acquisition: Robust HTTP GET with retries and exponential backoff.
Data Transformation: JSON → normalized CSV with preferred column ordering and dynamic field support.
Resilience & Observability: NDJSON cache, empty-date tracking, error logging.
Output Consistency: BOM-encoded UTF-8 CSV suitable for Excel/QGIS, plus sidecar files.

3. Architectural Breakdown
3.1 Core Dependencies
Standard Library: argparse, csv, json, urllib.request, dataclasses, datetime, zoneinfo, pathlib
No third-party packages — highly portable and lightweight. 
3.2 Major Functions / Components
Component
Responsibility
Key Techniques
find_most_recent_date_with_rows()
Backward probing for latest valid day
Date arithmetic + early termination
http_get()
Reliable HTTP fetching
Retries, exponential backoff, custom headers
fetch_day_rows()
Day-specific fetch + JSON extraction
Content-type + heuristic list detection
find_record_list()
Recursive search for hotspot arrays in nested JSON
Scoring based on key presence
compute_fieldnames() / normalize_row()
CSV schema handling
Preferred column order + dynamic extras
load_cached_days() / append_cache_line()
NDJSON cache management
Line-oriented JSON for resumability
main()
CLI orchestration & final output
Comprehensive logging and sidecar files

3.3 Data Flow
Bangkok local “today” → probe backward → API → JSON parsing → cache → CSV + metadata

4. Strengths (Best Practices Demonstrated)
CLI Maturity: Full argparse implementation with sensible defaults and help text — directly addresses the recommendation from Scripts 1 & 2.
Robustness: Excellent error handling, retries, caching, and graceful degradation (header-only CSV when no data).
Data Quality: Smart column ordering, JSON-safe normalization, CRS-neutral geospatial readiness (lat/lon preserved).
Observability: Detailed console output + persistent sidecar files (_cache.ndjson, _empty_dates.txt, _errors.txt).
Modern Python: Type hints, dataclasses, zoneinfo, pathlib, f-strings, context managers.
Defensive Parsing: Handles malformed or nested JSON gracefully via recursive search.
Production Mindset: Resume support, timestamped fallback filenames, BOM for Excel compatibility.

5. Limitations & Potential Issues
Single-Day Focus — Only downloads the most recent valid day. This is intentional but may require wrapping scripts for multi-day historical backfills.
No Parallelism — Not an issue for single-day operation but could be extended for range downloads.
API Dependency — Relies on a non-public/custom endpoint (IP-based). No fallback to official NASA FIRMS if this service goes down.
Caching Scope — Cache is per-run (tied to date range). Long-term deduplication across runs could be improved.
Hard-coded Preferences — PREFERRED_COLUMNS is excellent but could be configurable via CLI or config file.
Minor Code Smell — One commented typo (# <-- FIXED (was resp.read()a)) indicates recent debugging.
No Validation of Coordinates — Does not sanity-check lat/lon ranges (though downstream GIS tools would catch this).

6. Quantitative Assessment
Code Quality Metrics:
Readability: Excellent
Maintainability: Very High
Error Handling / Resilience: Outstanding
Testability: High (most functions are pure or easily mockable)
Performance:
Typical runtime: < 30 seconds (single day + probing)
Memory: Minimal (streams response, processes in batches)
Output size: Highly variable (hundreds to tens of thousands of hotspots)
Overall Engineering Level: Professional-grade data ingestion script.

7. Limitations
Multi-day Mode — Add --days N or --start / --end flags to support historical bulk downloads while keeping single-day as default.
Official FIRMS Fallback — Optional mode to pull from NASA FIRMS (MODIS/VIIRS) public endpoints if the custom API is unavailable.
Database Integration — Option to upsert directly into PostgreSQL/PostGIS with spatial index.
Visualization Hook — Post-download option to generate a quick Leaflet HTML preview or GeoJSON conversion.
Configuration File — Support TOML for default column preferences, API keys (if added later), etc.
Monitoring — Optional --notify flag or integration with logging services for daily pipeline runs.
Unit Tests — Strong candidate for pytest with responses library to mock the API.

8. Role in the Six-Script Pipeline
Script 3 serves as the fire / biomass burning data ingestion layer. It pairs naturally with:
Scripts 1–2 (meteorology + backward trajectories) → correlation between air mass origins and active fires. Required for Script 4. 
The choice of daily “most recent” + rich CSV output makes it ideal for operational air quality monitoring in Thailand, especially during the burning season.



####################



SCRIPT 4: FWT (Forward HYSPLIT trajectories from FIRMS hotspots using provincial grouping + parallel processing)

This script generates forward 24-hour air mass trajectories starting from active wildfire/hotspot detections (FIRMS data produced by Script 3). It focuses on 9 key northern Thai provinces, launches trajectories at 8 local hours per day for up to 3 forecast days, and exports results as GeoJSON FeatureCollections.The design is optimized for performance through province-based grouping and multiprocessing, making it more scalable than the backward script (Script 2). It forms the forward modeling counterpart in the air quality attribution pipeline, linking biomass burning sources to potential downwind impact areas.
Key Operational Scale:
Input: Latest FIRMS CSV (Script 3)
Output: Multiple GeoJSON files (D:\FWT\<province>\YYYY\FWT_<prov>_<date>.geojson)
Parallelism: ProcessPoolExecutor using nearly all CPU cores
Typical volume: Hundreds to thousands of trajectories per run

2. Functional Requirements
Source Discovery: Automatically selects the newest FIRMS CSV and extracts valid hotspot records with province filtering. Filters FIRMS by administrative code already included in SCRIPT 3 FIRMS API download. Data joined from administrative (field) codes, not a spatial join).
Trajectory Generation: Forward +24h HYSPLIT runs from each hotspot at 8 daily start times.
Meteorological Support: Reuses GFS files (Script 1) with simple 3-day lookahead.
Performance: Province grouping + parallel execution.
Output: Clean GeoJSON with full original FIRMS attributes preserved.
Integration: Seamless link between fire detection (Script 3) and trajectory modeling.

3. Architectural Breakdown
3.1 Core Dependencies
Standard Library: os, csv, json, shutil, datetime, subprocess, concurrent.futures, collections
External: HYSPLIT (hyts_std.exe) — same as Script 2
3.2 Major Functions
Function / Component
Responsibility
Key Techniques
get_latest_firms_csv()
Find most recent FIRMS file
Modification time sorting
parse_firms_rows()
CSV parsing & validation
Dialect sniffing, robust type conversion
met_files_for_run()
GFS file selection (cached)
Simple lookahead caching
run_hysplit_task()
Single trajectory worker
CONTROL file generation, subprocess, tdump parsing
main()
Orchestration + parallel execution
Province grouping + ProcessPoolExecutor

3.3 Data Flow
Latest FIRMS CSV (Script 3) → Parse & group by province → Generate CONTROL files → Parallel HYSPLIT runs → GeoJSON output per province/year

4. Strengths (Best Practices Demonstrated)
Performance-Oriented: Excellent use of ProcessPoolExecutor and province-level grouping.
Memory & I/O Efficiency: Met file cache, dialect sniffing, minimal per-row processing.
Data Preservation: Carries through all original FIRMS attributes into GeoJSON properties.
Robust Parsing: Handles messy CSV (UTF-8 BOM, varying dialects, commas in numbers).
Clean Separation: Clear sections for parameters, utils, FIRMS, MET, worker, and main.
Scalability: Designed to handle large numbers of hotspots without excessive disk thrashing. 

5. Limitations & Potential Issues
No CLI Arguments — Hard-coded paths and parameters (regression compared to Script 3).
Path Duplication — Significant overlap with Script 2 (HYSPLIT setup, date utils, etc.). 
Limited Error Handling — Subprocess failures are silent (DEVNULL); worker exceptions may be swallowed. 
Resource Intensity — Running thousands of HYSPLIT instances in parallel can overwhelm disk I/O or memory on modest hardware.
Hard-coded Provinces & Logic — PROVINCE_IDS, 3-day lookahead, and 24h runtime are not configurable.
No Resume / Incremental — Always re-runs everything for the selected dates.
Year Handling — Output organized by year but no automatic rollover logic.
tdump Parsing — Simplified and slightly brittle (assumes column order).
File System Heavy — Creates many temporary directories/files; no automatic cleanup of old runs. Need a periodic deletion of ‘hysplit_runs_forward’.

6. Quantitative Assessment
Code Quality Metrics:
Readability: Good
Maintainability: Medium (due to duplication with Script 2)
Performance Design: Very Good
Error Handling: Moderate
Scale & Performance:
Can process thousands of trajectories in parallel
Typical runtime: Minutes to hours depending on hotspot count and hardware
Parallel efficiency: High (CPU-bound HYSPLIT)


7. Recommendations for Improvement
Introduce CLI (argparse) — Add flags for --firms-dir, --provinces, --runtime, --days, --workers, --force, etc.
Refactor into Shared Library — Extract common HYSPLIT utilities (CONTROL writer, tdump parser, date helpers) into a module usable by both Script 2 and 4.
Better Error Handling & Logging — Capture HYSPLIT output on failure, add progress bars (tqdm), and summary statistics.
Resume / Caching — Skip already-computed trajectories using tdump or GeoJSON existence checks.
Resource Management — Add worker limits, disk usage monitoring, or batching.
Output Enhancements — Optional KML export, daily aggregation, or direct database ingestion.
Testing — Unit tests for parsing and CONTROL generation; integration tests with mock HYSPLIT.

8. Role in the Six-Script Pipeline
Script 4 is the forward dispersion / source-to-receptor modeling component. Together with Script 2 (backward trajectories), it enables comprehensive air mass analysis:
Backward (Script 2): Where did the air at monitoring stations come from?
Forward (Script 4): Where is smoke from today’s fires going?
This pair, combined with hotspot data (Script 3) and GFS (Script 1), provides powerful evidence for PM2.5 source attribution during Thailand’s burning season. Likely feeds into Scripts 5–6 for visualization, statistical analysis, or reporting.


####################


SCRIPT 5: Daily PSCF – Single day (Endpoint counting method, provincial attribution)
This script implements a Potential Source Contribution Function (PSCF) analysis for a single target date. It combines backward trajectories (from Script 2) with same-day hotspot detections (from Script 3) to identify probable source regions of biomass burning smoke on a 10 km grid.
The analysis is performed per monitoring station and per province that had active fires on the target day. Output is a set of weighted PSCF GeoJSON grids suitable for visualization in QGIS or web maps.
This represents the core analytical / receptor-modeling layer of the pipeline — transforming raw trajectories and fire detections into quantitative source-apportionment products.
Key Characteristics:
Grid-based endpoint counting (not residence time)
Standard PSCF weighting function
Province-filtered analysis
Heavy reliance on GeoPandas + Shapely for spatial joins

2. Functional Requirements Fulfilled
Input Integration: Loads trajectories (GeoJSON), hotspots (CSV), administrative polygons, and a pre-defined analysis grid.
Temporal Alignment: Handles local vs. UTC dates and filters data to the exact target day.
Spatial Processing: Converts trajectories to points, performs spatial joins, and counts endpoints per grid cell.
PSCF Calculation: Computes raw PSCF (m_ij / n_ij), applies weighting, and masks low-count cells.
Output: Per-station, per-province GeoJSON grids with full PSCF metrics.

3. Architectural Breakdown
3.1 Core Dependencies
Scientific Stack: numpy, pandas, geopandas, shapely
Standard Library: os, json, re, warnings, datetime
No HYSPLIT execution — purely post-processing.
3.2 Major Functions
Function
Responsibility
Key Techniques
clean_date()
Robust date parsing from messy CSV/JSON
Regex + JSON fallback
parse_hhmm()
Convert FIRMS time strings
Padding and splitting
explode_to_points()
Convert LineStrings → individual endpoint points
Geometry iteration
spatial_count()
Count points per grid cell via spatial join
geopandas.sjoin + value_counts
safe_join()
Province attribution with cleanup
Spatial join + column deduplication
weight_func()
Standard PSCF weighting (1.0 / 0.7 / 0.4 / 0.2)
Piecewise thresholds
main()
Full orchestration
Station loop + province filtering

3.3 Data Flow
Trajectories (Script 2) + Hotspots (Script 3) → Filter by date/province/confidence → Explode to points → Spatial counts (n_ij, m_ij) → PSCF + weighting → GeoJSON output

4. Strengths (Best Practices Demonstrated)
Scientific Correctness: Proper PSCF formulation with endpoint counting, weighting function, and low-count masking.
Geospatial Robustness: Careful CRS handling, spatial joins, and geometry explosion.
Domain Specificity: Province-level filtering, confidence thresholding (h/n), and station-wise output — highly relevant for Thai air quality research.
Data Cleaning: Strong handling of messy input formats (JSON-wrapped dates, varying time strings).
Output Richness: Each grid cell contains n_ij, m_ij, pscf_raw, pscf_w, weight, etc. — excellent for downstream analysis and visualization.
Warnings Suppression: Practical for scientific scripts with noisy GeoPandas output.

5. Limitations & Potential Issues
Hard-coded Single Date — TARGET_DATE and specific hotspot filename make it non-automatic. Should be modified for Dynamic Most-Recent Input Resolution. However, should the GFS availability or the FWT processing time extend beyond the 24 hour interval, ‘the Most Recent’ method can cause the pipeline to ‘skip a day’. 
No CLI / Configurability — Paths, provinces, PSCF parameters, and grid are all hardcoded.
Performance — Exploding thousands of trajectory points and repeated spatial joins can be slow for many stations/days.
Memory Usage — Loads full GeoDataFrames repeatedly; could be optimized with Dask or chunking for larger datasets.
Limited Flexibility:
Only endpoint counting (no residence time or concentration weighting).
No multi-day PSCF (e.g., 3-day backward).
Province filtering is strict.
Error Resilience — Moderate; missing files are skipped but major errors (e.g., CRS mismatch) may crash.
Reproducibility — Hard-coded date and file paths reduce portability.

6. Quantitative Assessment
Code Quality Metrics:
Readability: Good (with clear sections)
Maintainability: Medium (heavy hard-coding)
Scientific Rigor: High
Performance: Acceptable for daily use, but scales poorly
Typical Scale:
Grid: ~10 km resolution over Southeast Asia
Per station/province: Thousands of trajectory endpoints
Output: 14 stations × 9 provinces = up to ~126 GeoJSON files per day
Dependencies Risk: Relies on specific Shapefile paths (SEA_grid10km.shp, province polygons).

7. Recommendations for Improvement
Add Full argparse CLI — Support --date, --hotspots, --traj-root, --provinces, --grid, PSCF parameters (--min-n, --weight, etc.).
Make Multi-Day Capable — Accept a range or list of dates and aggregate trajectories/hotspots.
Performance Optimization:
Pre-compute or index the grid.
Vectorize more operations.
Optional parallel processing per station using concurrent.futures.
Shared Configuration — Central config file (TOML) shared across Scripts 1–5.
Enhanced Outputs:
Combined PSCF across all stations.
Uncertainty metrics or bootstrap confidence intervals.
Raster (GeoTIFF) export alongside GeoJSON.
Validation & Visualization — Automatic summary statistics and quick PNG/QGIS style preview.
Modularization — Extract PSCF core logic into a reusable function/class for use in Script 6 or Jupyter notebooks.
File size – The province PSCF outputs are large. Plan a storage budget–the exFAT folder size for a single month is 80-100GB.

8. Role in the Six-Script Pipeline
Script 5 is the statistical receptor modeling engine. It synthesizes outputs from:
Script 1 (GFS)
Script 2 (TRJ –Backward trajectories (AKA BWT))
Script 3 (Hotspots)
(Indirectly Script 4 – forward trajectories)
It produces the key analytical product (PSCF grids).This script transforms the pipeline from “data generation” to “actionable insight.”


##########################


SCRIPT 6: Daily PSCF – Overall (aggregated 9-province hotspots, per-station output)

This script computes an aggregated Potential Source Contribution Function (PSCF) for a single target date, treating hotspots from 9 key northern Thai provinces as a single source group. Unlike Script 5 (per-province analysis), it produces one GeoJSON grid per monitoring station, representing the combined contribution probability from all selected provinces. Script 6 serves as the final analytical synthesis layer of the pipeline. It delivers a streamlined, station-specific view of probable fire-smoke source regions across northern Thailand.
Core Differences from Script 5:
Single m_ij calculation using all 9 provinces combined (no per-province loop)
Output: D:\PSCF\PSCF_Overall\<station>\2026\PSCF_<station>_9provinces_...geojson
Simpler, higher-level attribution product

2. Functional Requirements Fulfilled
Data Integration: Loads backward trajectories (Script 2), filtered FIRMS hotspots (Script 3), analysis grid, and province polygons.
Filtering Logic: Date, confidence (h/n), and province-restricted hotspots.
PSCF Computation: Endpoint counting, n_ij / m_ij, standard weighting function.
Aggregation: All 9 provinces treated as one collective source.
Output: Clean per-station GeoJSON grids with rich metadata.

3. Architectural Breakdown
3.1 Core Dependencies
Same as Script 5: numpy, pandas, geopandas, shapely + standard library.
3.2 Major Functions
The script is structurally almost identical to Script 5. Key shared/reused components:
Function
Responsibility
Notes
clean_date(), parse_hhmm()
Input sanitization
Robust handling of messy FIRMS data
explode_to_points()
Trajectory → endpoint points
Identical to Script 5
spatial_count()
Grid cell counting via spatial join
Reused
weight_func()
PSCF weighting (1.0 / 0.7 / 0.4 / 0.2)
Standard
safe_join()
Province attribution
Reused

The main difference is in main(): no inner province loop and unified m_counts.

4. Strengths (Best Practices Demonstrated)
Scientific Clarity: Clean separation between per-province (Script 5) and overall (Script 6) analyses — both valid and complementary PSCF approaches.
Code Reuse Potential: High similarity to Script 5 makes modularization straightforward.
Metadata Richness: Output includes hotspot_provinces field listing all 9 provinces.
Practical Output: One file per station is easier for quick station-specific visualization and comparison.
Consistent Methodology: Same weighting, grid, and filtering logic as Script 5 — ensures comparability.

5. Limitations & Potential Issues
Code Duplication — ~90% of the code is copied from Script 5. While this does violate DRY (Don’t Repeat Yourself) principles and creates maintenance burden, the idea for this duplication assumes that SCRIPT 5 or 6 may be run without the other.  
Hard-coded Everything — Date, paths, provinces, PSCF parameters — same issue as Script 5.
Single-Day Only — Not automated for daily pipeline runs. Repeat of SCRIPT 5 NOTES:
TARGET_DATE and specific hotspot filename make it non-automatic. Should be modified for Dynamic Most-Recent Input Resolution. However, should the GFS availability or the FWT processing time extend beyond the 24 hour interval, ‘the Most Recent’ method can cause the pipeline to ‘skip a day’. 
Performance — Same bottlenecks as Script 5 (repeated geometry explosion and spatial joins).
Limited Flexibility — Cannot easily switch weighting functions, grid resolutions, or time windows.
No Aggregation Across Stations — No overall regional PSCF map.

6. Quantitative Assessment
Code Quality Metrics:
Readability: Good (High duplication)
Maintainability: Medium-Low (high duplication)
Scientific Rigor: High
Reusability: Low (needs to be refactored)
Comparison to Script 5:
Nearly identical performance and output volume.
Fewer output files (14 vs. ~126), making it lighter for quick reviews.

7. Recommendations for Improvement
Immediate Priority: Refactoring
Python
Other enhancements:
Full CLI + Config — Merge both scripts into one flexible tool with --mode overall|per_province, --date, --provinces, etc.
Multi-day Support — Aggregate over several days for more robust statistics.
Combined Pipeline Runner — Script that calls 1→2→3→4→5→6 automatically.
Advanced PSCF Variants — Residence time, concentration-weighted (CWT), or kernel density options.
Visualization — Automatic PNG/QGIS project generation or Leaflet HTML export.
Performance — Use dask-geopandas or spatial indexing for larger grids.

8. Role in the Six-Script Pipeline
Script 6 completes the analytical chain:
Script
Role
Output Type
1
GFS Meteorology
Raw archive files
2
Backward Trajectories
Station GeoJSONs
3
Hotspot Detection
FIRMS+admin_* codes CSV
4
Forward Trajectories
Fire-origin GeoJSONs
5
Per-Province PSCF
Detailed source maps
6
Overall PSCF
Station-level summary maps

Together they form a complete operational air quality source-apportionment system tailored for northern Thailand’s biomass burning season.


#############################



SCRIPT 7: CWT MONTH_AGG - Batch CWT (Concentration Weighted Trajectory) for multiple stations using daily GeoJSON trajectories  

This script implements Concentration Weighted Trajectory (CWT) analysis — a more advanced receptor model than PSCF. It combines daily backward trajectories (from Script 2) with observed PM2.5 concentrations to produce concentration-weighted source contribution maps on a 10 km grid. It uses archival data from air4thai.com. Unlike the daily single-date PSCF scripts (5 & 6), this is a batch processor that handles an entire time window (Jan–May 2026 in the example), aggregates results monthly per station, and exports one GeoJSON per month per station. It represents the highest-tier statistical analysis layer in the suite, providing quantitative estimates of how much PM2.5 is associated with air masses passing over each grid cell.
Key Characteristics:
Trajectory segment length weighting (dynamic time allocation)
Monthly accumulation and export for study period
Handles 14 stations sourced with archival PM2.5 data from air4thai.com 
Robust trajectory discovery from the standardized GeoJSON structure

2. Functional Requirements Fulfilled
PM2.5 Integration: Reads station-specific daily PM2.5 from CSV and matches by date.
Trajectory Ingestion: Discovers and loads daily GeoJSON files from Script 2 output structure.
CWT Computation: Weights trajectory segments by residence time in each grid cell and multiplies by observed PM2.5.
Aggregation: Monthly accumulators (num / den) with minimum residence time threshold.
Output: Clean monthly CWT GeoJSON files per station.

3. Architectural Breakdown
3.1 Core Dependencies
pandas, geopandas, shapely (same stack as Scripts 5 & 6)
Standard library only otherwise
3.2 Major Functions
Function
Responsibility
Key Techniques
read_pm_for_station()
Load & filter PM2.5 data per station
Flexible column detection, date normalization
iter_daily_geojsons()
Discover daily trajectory files
Filename parsing + date filtering
process_trajectory_file()
Core CWT accumulation
Segment-wise intersection + fractional time weighting
export_geojson()
Write monthly CWT grid
Selective export with NaN handling
main()
Orchestration across stations and months
Monthly accumulators via defaultdict

3.3 Data Flow
PM2.5 CSV + Daily Trajectories (Script 2) → Per-station monthly accumulators → CWT = Σ(PM × residence_time) / Σ(residence_time) → Monthly GeoJSON grids

4. Strengths (Best Practices Demonstrated)
Scientific Sophistication: Correct CWT implementation with dynamic segment time weighting (much better than simple endpoint counting).
Batch Efficiency: Processes entire seasons efficiently with monthly aggregation.
Robustness: Flexible field name normalization, graceful skipping of missing data, overwrite protection.
Output Organization: Logical folder structure (D:\CWT\2026_STATION\CWT_202601_XX.geojson).
Reusability Potential: Well-structured helpers that could be shared with PSCF scripts.
Performance Awareness: Uses spatial index (sindex) for faster intersections.

5. Limitations & Potential Issues
Hard-coded Paths & Dates — Same issue as previous scripts; not easily configurable.
No CLI — Parameters (date range, min hours, paths) are all top-level constants.
Memory Usage — Accumulators for all months/stations stay in memory (acceptable for this scale but could grow).
Code Style — Mix of older patterns (datetime instead of zoneinfo, some global-like variables).
Error Handling — Moderate; many operations silently skip bad data.
Duplication — Trajectory loading and grid handling overlap with Scripts 5 & 6.
No Parallelism — Single-threaded across stations (could be accelerated).

6. Quantitative Assessment
Code Quality Metrics:
Readability: Good
Maintainability: Medium (some duplication with 5/6)
Scientific Rigor: Excellent (proper residence-time weighting)
Scalability: Good for seasonal batches
Typical Scale:
14 stations × ~150 days × 8 trajectories/day = thousands of trajectories
Output: ~14 stations × 5 months = ~70 GeoJSON files

7. Recommendations for Improvement
Unify with PSCF Core — Extract shared geometry/grid/trajectory loading logic into a common module (receptor_utils.py).
Add Full CLI — Support --start-date, --end-date, --pm-csv, --stations, --min-hours, --overwrite, etc.
Parallel Processing — Use ProcessPoolExecutor for stations or months.
Enhanced Features:
Uncertainty estimation (bootstrap)
Seasonal / custom period aggregation
Combined multi-station CWT
Raster (GeoTIFF) export option
Visualization Integration — Automatic style generation or HTML map output.
Modernization — Use pathlib, datetime with timezone awareness, and configuration file (TOML).

8. Role in the Overall Pipeline (Scripts 1–7)
Script
Purpose
Analysis Type
Frequency
1
GFS Meteorology
Data acquisition
Daily
2
Backward Trajectories
Transport modeling
Daily
3
Hotspots (FIRMS)
Source detection
Daily
4
Forward Trajectories
Source dispersion
Daily
5
Per-Province PSCF
Qualitative source ID
Daily
6
Overall PSCF (9 provinces)
Aggregated source ID
Daily
7
CWT (PM-weighted)
Quantitative source strength
Batch-run

Script 7 completes the suite by adding concentration weighting, turning binary “likely source” maps (PSCF) into quantitative “expected contribution” maps (CWT). Together, Scripts 1–7 form a comprehensive operational air quality source-apportionment system.


############################




SUMMARY

FULL PIPELINE: Full Northern Thailand PM2.5 Receptor Modeling System (GFS → HYSPLIT → PSCF/CWT)

This seven-script Python pipeline forms a comprehensive, operational daily air quality source-apportionment system plus monthly CWT batch run designed for northern Thailand’s biomass burning season. It seamlessly integrates meteorological data acquisition, air mass trajectory modeling, FIRMS satellite fire detection, and advanced receptor modeling (PSCF and CWT) to identify and quantify the contribution of wildfire smoke to PM2.5 concentrations at 14 monitoring stations.
The system supports daily automated runs for Scripts 1–6 and periodic batch processing for Script 7, delivering consistent, rich geospatial outputs (primarily GeoJSON) suitable for visualization, research, and decision support.

2. Overall Workflow Architecture
Daily Data Flow:
Script 1  ──►  GFS 0.25° Meteorology (3 most recent days)
       │
      ▼
Script 2  ──►  Backward Trajectories (-24h, 8 start hours/day, 14 stations)
      │
      ├─► Script 5 ──► Per-Province PSCF Grids
      └─► Script 6 ──► Overall PSCF (9 key provinces)
      │
Script 3  ──►  Thailand Hotspot Data (most recent day)
       │
      ▼
Script 4  ──►  Forward Trajectories (+24h from active hotspots)
       │
Script 7 (Batch) ──► CWT Analysis (PM2.5-weighted, monthly per station)
Key Outputs:
Raw GFS meteorological archives
Daily backward and forward trajectory GeoJSON files
Daily hotspot CSV
Per-station and per-province PSCF grids
Monthly CWT concentration-weighted grids
All geospatial products include rich metadata for traceability

3. Component Summary
Script
Purpose
Frequency
Core Technology
1
GFS 0.25° downloader (multi-day parallel)
Daily
requests + ThreadPoolExecutor
2
Backward HYSPLIT trajectories
Daily
hyts_std.exe + GeoJSON export
3
Thailand hotspot API downloader
Daily
argparse + NDJSON caching
4
Forward HYSPLIT trajectories from hotspots
Daily
ProcessPoolExecutor + GeoJSON
5
Per-province PSCF (endpoint counting)
Daily
GeoPandas + spatial joins
6
Overall PSCF (aggregated 9 provinces)
Daily
GeoPandas + spatial joins
7
Batch CWT (concentration-weighted)
Periodic
Residence-time segment weighting


4. Strengths of the Complete Pipeline
Scientific Integration: Combines meteorological forcing, Lagrangian transport modeling, satellite-derived fire data, and receptor modeling techniques into a cohesive analytical framework.
Operational Flow: Clear daily progression from raw data ingestion to advanced statistical products, with consistent file naming and directory structures.
Geospatial Consistency: All analytical outputs use standardized GeoJSON format with comprehensive properties, enabling seamless use in GIS software and web applications.
Domain Relevance: Specifically tailored to northern Thailand with real monitoring stations, key provinces, local time handling, and biomass burning focus.
Complementary Methods: Provides multiple perspectives through backward/forward trajectories, per-province and overall PSCF, and quantitative CWT analysis.
Resilience and Traceability: Built-in validation, metadata embedding (e.g., used meteorological files, confidence levels), and support for force re-processing.

5. Future Improvements
Develop shared utility modules (hysplit_utils.py, receptor_core.py, config.toml) to eliminate duplication and optimize performance. 
Implement full CLI support across all scripts for flexible date, path, and parameter control.
Create a master orchestrator script for one-command daily execution.
Add progress tracking, structured logging, and optional parallel processing.
Support configuration-driven paths and parameters.
Enable multi-day aggregation options for PSCF and CWT.
Containerization for easier deployment and reproducibility.
Integration with a web dashboard for daily result visualization.
Extension into a reusable framework for similar regional air quality studies.

6. Conclusion
This pipeline represents a well-integrated, scientifically robust end-to-end system for daily PM2.5 source apportionment. It effectively transforms raw meteorological and satellite data into actionable geospatial intelligence, supporting both operational monitoring and research needs in northern Thailand. The modular design and consistent outputs make it a strong foundation for ongoing air quality analysis.



