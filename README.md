# BlueWatch

Satellite chlorophyll-a anomaly alerts for coastal HAB early warning.

BlueWatch ingests daily Copernicus Marine Service (CMEMS) Sentinel-3 OLCI data, computes chlorophyll-a anomalies against a climatological baseline, and sends email alerts when phytoplankton concentrations in a configured coastal zone exceed the seasonal norm. The goal is to give aquaculture operators a 24–72 hour lead time before a harmful algal bloom becomes visible — enough time to delay harvest rather than discover toxin contamination after the fact.

Current scope: **west coast of Ireland MVP** (Donegal Bay to Bantry Bay). See [`docs/prd-west-coast-ireland-mvp.md`](docs/prd-west-coast-ireland-mvp.md) for the full spec.

---

## How it works

1. **Ingest** — downloads the CMEMS L3 NRT Atlantic Chl-a product (`cmems_obs-oc_atl_bgc-plankton_nrt_l3-olci-300m_P1D`) for the WCI bounding box
2. **Mask** — excludes turbid coastal pixels (Shannon Estuary, inner Galway Bay, etc.) via a static GeoJSON mask
3. **Compute** — calculates a zone-averaged anomaly ratio (`current Chl-a / climatological mean for that ISO week`) for each configured polygon
4. **Alert** — sends a Resend email when a zone's ratio meets or exceeds its configured threshold (default 3×); logs a gap notification if cloud cover persists for ≥3 days

Cloud gaps are first-class status — the pipeline never silently fails or substitutes interpolated data without disclosure.

---

## Setup

### Prerequisites

- Python 3.11+
- A [CMEMS Marine Data Store](https://marine.copernicus.eu/) account (free)
- A [Resend](https://resend.com/) account with an API key and verified sender

### Install

```bash
pip install -r requirements.txt
```

### Environment variables

Copy `.env.example` to `.env` and fill in:

```bash
CMEMS_USERNAME=your_cmems_username
CMEMS_PASSWORD=your_cmems_password
RESEND_API_KEY=your_resend_api_key
BLUEWATCH_FROM_EMAIL=alerts@yourdomain.com
```

`run_pipeline.py` and `scripts/build_climatology.py` will read values from `.env`
automatically when exported environment variables are missing. Exported variables still
take precedence.

If you want shell variables available to every child process, `source .env` alone is not
enough in `zsh` unless the file contains `export ...` lines. Use `set -a; source .env; set +a`
if you specifically want the shell to export them.

### Alert log backend

BlueWatch keeps SQLite as the default alert deduplication backend. If `DATABASE_URL` is unset, the pipeline creates and uses `data/alert_log.db` locally on first run.

You can override that with:

```bash
# Explicit local SQLite file
DATABASE_URL=sqlite:////absolute/path/to/alert_log.db

# Managed Postgres, for example Supabase or Neon
DATABASE_URL=postgres://user:password@host:5432/database
# or
DATABASE_URL=postgresql://user:password@host:5432/database
```

Notes:

- SQLite remains the default and requires no extra configuration beyond the bundled dependencies.
- Postgres uses the same `alert_log(zone_name, alert_date, alert_type)` schema and deduplicates with `ON CONFLICT DO NOTHING`.
- Unsupported `DATABASE_URL` schemes are rejected at startup.

### Build the climatology baseline (one-time)

Before running the pipeline, compute the per-pixel ISO-week climatological mean from the CMEMS L3 MY reprocessed product (2016–2024). This downloads a large dataset and may take a while.

```bash
python scripts/build_climatology.py
```

Output: `data/climatology/wci_chl_climatology_wk.nc`

### Configure zones

Edit `config/zones.yaml` to define your monitoring polygons:

```yaml
zones:
  - name: Outer Clew Bay
    description: Atlantic-facing mouth of Clew Bay, Co. Mayo
    alert_email: operator@example.com
    threshold_multiplier: 3.0
    polygon:
      type: Polygon
      coordinates:
        - [[-10.05, 53.78], [-9.85, 53.78], [-9.85, 53.88], [-10.05, 53.88], [-10.05, 53.78]]
```

Polygons must be at least 1 km² (300m pixel resolution floor). The pipeline will exit with an error at startup if any zone is too small.

### Run

```bash
python run_pipeline.py
```

Historical replay and local verification are supported via CLI flags:

```bash
python run_pipeline.py \
  --date 2026-04-13 \
  --config config/zones.yaml \
  --log-dir logs \
  --database-url sqlite:////absolute/path/to/alert_log.db
```

Logs are written to `logs/pipeline_YYYY-MM-DD.jsonl` (JSON lines, one entry per zone per run).

### Schedule (cron)

```cron
# Run daily at 10:00 UTC (CMEMS NRT data typically available by 09:00 UTC)
0 10 * * * /path/to/venv/bin/python /path/to/BlueWatch/run_pipeline.py >> /var/log/bluewatch.log 2>&1
```

### Manual Live Verification

Use this when you want to verify the real CMEMS and Resend path outside CI:

```bash
CMEMS_USERNAME=your_cmems_username \
CMEMS_PASSWORD=your_cmems_password \
RESEND_API_KEY=your_resend_api_key \
BLUEWATCH_FROM_EMAIL=alerts@yourdomain.com \
python run_pipeline.py --date 2026-04-13
```

Verification checklist:

- Confirm `logs/pipeline_2026-04-13.jsonl` was created.
- Inspect the JSON line for `status`, `anomaly_ratio`, `zone_avg_chl`, `climatology_mean_chl`, `valid_pixel_count`, and `email_sent`.
- If the anomaly ratio meets the configured threshold, confirm one alert email arrives with the zone name, observed date, Chl-a values, and ratio in the body.
- Re-run the same command for the same date and confirm no duplicate anomaly email is sent for the same zone.
- For gap-notification verification, run three consecutive historical dates with cloud-limited coverage and confirm the third day sends a gap notification.

---

## Project structure

```
BlueWatch/
├── config/
│   └── zones.yaml                    # Zone definitions and thresholds
├── data/
│   ├── climatology/
│   │   └── wci_chl_climatology_wk.nc # Pre-computed baseline (built once offline)
│   ├── masks/
│   │   └── wci_turbid_mask.geojson   # Turbid pixel exclusion polygons
│   └── alert_log.db                  # Default local SQLite dedup log (auto-created)
├── logs/                             # Daily pipeline log files (auto-created)
├── bluewatch/
│   ├── ingest.py                     # CMEMS API download
│   ├── anomaly_engine.py             # Climatology load, masking, anomaly calc
│   ├── alert_dispatcher.py           # Threshold check, Resend dispatch, dedup
│   └── config.py                     # Zone config loader and validator
├── scripts/
│   └── build_climatology.py          # One-time offline baseline builder
├── run_pipeline.py                   # Entry point
├── .env.example
└── requirements.txt
```

---

## Caveats

- **Chl-a is a proxy for total phytoplankton biomass, not species identification.** Satellite data cannot distinguish toxic from non-toxic species. BlueWatch is a precautionary signal tool, not a regulatory trigger.
- **Cloud cover creates data gaps.** Sentinel-3 OLCI is optical. Persistent cloud cover over Irish Atlantic waters is common, particularly in winter. The pipeline reports gap status explicitly; it does not substitute interpolated data silently.
- **This is not a harvest closure system.** Shellfish harvest decisions are made by government food safety agencies (FSAI) based on toxin testing. BlueWatch alerts are an early warning input, not a regulatory substitute.

---

## Status

Early MVP — west coast of Ireland only. See the [PRD](docs/prd-west-coast-ireland-mvp.md) for implementation tasks and acceptance criteria.
