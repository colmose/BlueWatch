# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Lint
python -m ruff check .

# Type check
python -m mypy bluewatch scripts/build_climatology.py run_pipeline.py

# Run all tests
python -m pytest tests/ -v

# Run a single test
python -m pytest tests/test_ingest.py::test_good_pixels_retained -v

# Build the climatological baseline (one-time, requires CMEMS credentials)
CMEMS_USERNAME=xxx CMEMS_PASSWORD=yyy python scripts/build_climatology.py
```

## Architecture

BlueWatch is a daily satellite HAB early-warning pipeline for the west coast of Ireland. The flow is:

1. **Ingest** (`bluewatch/ingest.py`) — downloads the CMEMS L3 NRT Atlantic Chl-a product (`cmems_obs-oc_atl_bgc-plankton_nrt_l3-olci-300m_P1D`) for the WCI bounding box via `copernicusmarine.subset()`. Quality filter: pixels with `CHL_flags != 1` are set to NaN. Requires `CMEMS_USERNAME` + `CMEMS_PASSWORD` env vars; exits loudly if missing. Accepts an optional `run_date` for historical replay.

2. **Climatology baseline** (`scripts/build_climatology.py`) — a one-time offline script that downloads the CMEMS L3 MY reprocessed product (2016–2024), groups by ISO calendar week, and saves per-pixel mean CHL to `data/climatology/wci_chl_climatology_wk.nc`. Outputs a `(week, lat, lon)` NetCDF with variable `CHL_mean`. ISO week 53 is conditionally included when present in the source data.

3. **Anomaly engine** (`bluewatch/anomaly_engine.py`) — loads the climatology baseline, applies the turbid pixel mask (`data/masks/wci_turbid_mask.geojson`), and computes zone-averaged anomaly ratios (current CHL / climatological mean for that ISO week). Returns a `ZoneResult` per zone with status `DATA_AVAILABLE` or `CLOUD_GAP`.

4. **Alert dispatcher** (`bluewatch/alert_dispatcher.py`) — sends Resend emails when a zone's anomaly ratio meets/exceeds its `threshold_multiplier`. Deduplicates alerts via an `alert_log` table (SQLite default; Postgres supported via `DATABASE_URL`). Sends a gap notification after ≥3 consecutive cloud-gap days. Requires `RESEND_API_KEY` + `BLUEWATCH_FROM_EMAIL` env vars.

5. **Entry point** (`run_pipeline.py`) — orchestrates steps 1–4. Supports `--date YYYY-MM-DD` for historical replay/backtest, `--config`, `--log-dir`, and `--database-url` CLI flags. Emits one JSON-lines log entry per zone to stdout and to `logs/pipeline_YYYY-MM-DD.jsonl`.

### Zone configuration (`config/zones.yaml`)

Loaded and validated by `bluewatch/config.py`. Required fields per zone: `name`, `description`, `polygon` (GeoJSON, ≥1 km²), `threshold_multiplier` (positive float), `alert_email`. Limits: 1–10 zones. Any validation error calls `sys.exit(1)` — fail loud, never fake.

### Static data files

- `data/climatology/wci_chl_climatology_wk.nc` — pre-computed weekly baseline (built by `scripts/build_climatology.py`)
- `data/masks/wci_turbid_mask.geojson` — FeatureCollection of 4 turbid exclusion polygons: Shannon Estuary, Inner Galway Bay, Clew Bay Narrows, Bantry Bay Inner Reaches

### Testing conventions

- `scripts/build_climatology.py` is not a package, so tests load it with `importlib.util.spec_from_file_location`.
- Network calls (`copernicusmarine.subset`) are kept out of tests by patching `ingest._download_subset` via `monkeypatch.setattr`.
- For `build_climatology.py`, `copernicusmarine.open_dataset` is patched with `unittest.mock.patch.object`.
- Cloud/credential failures must propagate as non-zero `SystemExit` — tests assert `exc_info.value.code != 0`.

## Don't touch
- `.env` files
- `pyproject.toml` make suggestions only