# PRD: BlueWatch MVP — West Coast of Ireland

**Status:** Draft
**Date:** 2026-04-13

---

## 1. Overview

BlueWatch West Coast Ireland MVP is a minimal server-side pipeline and alert system that monitors chlorophyll-a anomalies along the west coast of Ireland using free Copernicus Marine Service (CMEMS) satellite data. It ingests daily L3 near-real-time Chl-a data, computes anomaly ratios against a climatological baseline, and sends email alerts when configured zones exceed a threshold. This is a focused proof-of-concept targeting the Atlantic-facing aquaculture coastline from Donegal Bay to Bantry Bay — one of the cleaner, lower-turbidity candidate geographies identified in the idea document — before any broader product build. The market thesis is deliberately narrower than "no HAB alerting systems exist": Atlantic-European forecasting services have existed in institutional form, but BlueWatch is testing whether a simpler, lower-overhead, zone-configurable product can be useful in Ireland without the full expert-curated modelling stack.

---

## 2. Problem Statement

1. Aquaculture operators on Ireland's west coast (mussels, oysters in Clew Bay, Killary Harbour, Bantry Bay, Carlingford Lough) do not appear to have access to a lightweight, self-serve alert product for user-defined monitoring zones, despite the existence of institutional HAB forecasting and bulletin services in Atlantic Europe.
2. Existing precedents such as ASIMUTH (2010-2013), HABreports/SAMS, and the Marine Institute / PRIMROSE forecasting work demonstrate that early warning demand is real, but those systems depend on regional modelling, expert interpretation, or public-service delivery rather than a simple product workflow.
3. CMEMS publishes daily Chl-a data for Irish coastal waters but there is no obvious lightweight product wrapping it into straightforward anomaly alerts for Irish operators using preset zones and simple delivery channels such as email.
4. Without a working pipeline against a real geography, the core feasibility assumptions (anomaly signal precedes known bloom events by 24–72 hours, false positive rate is manageable in open Atlantic waters) remain unvalidated.

### 2.1 Market Context Note

- ASIMUTH was an EU FP7 project coordinated from Ireland that ran from 1 December 2010 to 30 November 2013. CORDIS describes it as having developed short-term HAB alert systems for Atlantic Europe using satellite data, modelling, a web portal, SMS alerts, and a smartphone app.
- CORDIS also reports that ASIMUTH surveyed aquaculture users, found 2-7 days of warning materially useful, and developed a business plan for a financially self-sustaining service.
- The Marine Institute later described the PRIMROSE web portal in 2021 as providing Atlantic-coast early warning for shellfish producers and authorities, alongside weekly HAB bulletins in Ireland.
- The Marine Institute's weekly Irish HAB bulletin is currently operational. On 13 April 2026, the bulletin page listed `2026 Week 15 Irish HAB Bulletin`.
- Therefore the correct framing for BlueWatch is not "first HAB early warning in Ireland" but "potentially a simpler, self-serve anomaly-alert product for Irish zones, with a narrower technical scope than expert-curated forecast systems."

---

## 3. Goals

- Validate that the CMEMS L3 NRT Chl-a anomaly pipeline can run daily for Irish coastal coordinates with acceptable latency and data availability.
- Validate that the anomaly signal in open Atlantic coastal waters (west coast of Ireland) has a sufficiently low false positive rate to be useful.
- Deliver email alerts to at least one real or simulated aquaculture site polygon when the Chl-a anomaly threshold is crossed.
- Build the minimum reusable infrastructure (data ingestion, anomaly engine, alert dispatch) on which a broader product can be grown.

---

## 4. Non-Goals

- Does **not** build a user-facing web application, map UI, or self-serve zone editor in v1.
- Does **not** implement mobile push notifications.
- Does **not** cover Ireland's east coast, inland bays with high turbidity, or any other geography.
- Does **not** implement the SST or wind context layers.
- Does **not** implement paid tiers, accounts, or billing.
- Does **not** ingest or use L4 gap-filled CMEMS product (v1 uses L3 only; gap status is reported but not filled).
- Does **not** include a species context layer or HAB species identification.
- Does **not** provide historical archive access beyond the climatological baseline required for anomaly computation.
- Does **not** implement a public-facing API or GeoJSON export.

---

## 5. Users

**Primary:** The developer/operator running the system (Colm / BlueWatch team). There is no external user interface in this MVP — alerts go to a hardcoded or config-file-defined email address for one or more preset Irish aquaculture zones.

**Secondary:** Up to 5 beta testers (aquaculture operators or researchers with Irish coastal sites) who agree to receive alert emails and give feedback on signal quality.

---

## 6. User Stories

| # | Story |
|---|-------|
| U1 | As the system operator, I want a daily automated job that ingests CMEMS L3 Chl-a data for the west coast of Ireland so that the anomaly pipeline runs without manual intervention. |
| U2 | As the system operator, I want the pipeline to compute a zone-averaged Chl-a anomaly ratio for each configured polygon so that I can determine whether the signal exceeds threshold. |
| U3 | As the system operator, I want an email sent when a zone's anomaly ratio exceeds the configured threshold so that I (or a beta tester) receive a timely alert. |
| U4 | As the system operator, I want the pipeline to explicitly record and log a "cloud gap" status when no valid Chl-a pixels are available for a zone so that I know the system is not silently failing. |
| U5 | As a beta tester, I want the alert email to tell me the current Chl-a value, the seasonal mean, and the anomaly ratio for my zone so that I can decide whether to act on it. |
| U6 | As the system operator, I want a simple daily log of pipeline run status (data available, gap, anomaly triggered, no anomaly) so that I can monitor system health without reading raw outputs. |

---

## 7. Functional Requirements

### 7.1 Data Ingestion

- **FR-01** The system SHALL query the CMEMS Marine Data Store API daily to download the most recent L3 NRT (near-real-time) Chl-a product (`cmems_obs-oc_atl_bgc-plankton_nrt_l3-olci-300m_P1D`) for a bounding box covering the west coast of Ireland: approximately lon −11°W to −7°W, lat 51°N to 55.5°N.
- **FR-02** The system SHALL authenticate with CMEMS using a service account credential stored as an environment variable (not hardcoded).
- **FR-03** The system SHALL log a `DATA_AVAILABLE` or `CLOUD_GAP` status per zone after each ingestion attempt. A zone is considered a cloud gap if fewer than 20% of its pixels have valid (non-NaN) Chl-a values.
- **FR-04** The system SHALL NOT silently proceed with a gap run as if data were available. If a zone is in gap status, no anomaly computation is performed and no alert is sent — a gap notification email SHALL be sent instead if the zone has been in gap status for ≥3 consecutive days.

### 7.2 Climatological Baseline

- **FR-05** The system SHALL use a pre-computed per-pixel climatological mean Chl-a derived from the CMEMS L3 multi-year (MY) reprocessed product for the same geographic bounding box, aggregated by ISO calendar week (weeks 1–52). *(TODO: experiment with a rolling 30-day window in a later iteration to assess whether it reduces week-boundary artefacts.)*
- **FR-06** The climatological baseline SHALL be computed once offline and stored as a static NetCDF file (`data/climatology/wci_chl_climatology_wk.nc`) prior to first production run. It need not be recomputed automatically.
- **FR-07** The system SHALL match the current date to the correct climatological week and load only that week's baseline layer for each daily run.

### 7.3 Anomaly Computation

- **FR-08** The system SHALL compute a pixel-wise anomaly ratio as `anomaly = current_chl / climatological_mean_chl` for all valid pixels within each zone polygon.
- **FR-09** The system SHALL compute a zone-averaged anomaly ratio as the mean of all valid pixel anomaly ratios within the polygon.
- **FR-10** The system SHALL apply a turbidity mask to exclude pixels within estuaries and inner bays where the standard OC5 algorithm is unreliable. The mask SHALL be a static GeoJSON polygon layer (`data/masks/wci_turbid_mask.geojson`) applied before anomaly computation.
- **FR-11** The system SHALL log the zone-averaged anomaly ratio, pixel count, valid pixel count, and cloud gap status for each zone on each run.

### 7.4 Zone Configuration

- **FR-12** Zones SHALL be defined in a static YAML config file (`config/zones.yaml`) with fields: `name`, `description`, `polygon` (GeoJSON coordinates), `threshold_multiplier` (default: 3.0), `alert_email`.
- **FR-13** The system SHALL support between 1 and 10 zones in the config file.
- **FR-14** Zone polygons SHALL be validated at startup against a minimum area of 1 km² (to avoid sub-pixel queries at 300m resolution). The system SHALL exit with a clear error if any zone fails validation.

### 7.5 Alert Dispatch

- **FR-15** The system SHALL send an alert email when a zone's averaged anomaly ratio meets or exceeds its configured `threshold_multiplier`.
- **FR-16** Alert emails SHALL include: zone name, alert date, current zone-averaged Chl-a (mg/m³), climatological mean for that calendar week, anomaly ratio, valid pixel count, and a plain-language summary sentence.
- **FR-17** The system SHALL NOT send more than one alert email per zone per calendar day, regardless of how many times the pipeline is run.
- **FR-18** Gap notification emails (FR-04) SHALL be clearly distinguished from anomaly alert emails in subject line and body.
- **FR-19** Email dispatch SHALL use the SendGrid API (`sendgrid` Python SDK). The SendGrid API key SHALL be stored as an environment variable (`SENDGRID_API_KEY`). The system SHALL log a clear error and exit non-zero if dispatch fails — it SHALL NOT silently drop alerts.

### 7.6 Scheduling & Operations

- **FR-20** The pipeline SHALL be runnable as a single command: `python run_pipeline.py` with no required arguments.
- **FR-21** The pipeline SHALL be designed to run as a daily cron job or scheduled cloud function. A sample cron entry SHALL be included in the README.
- **FR-22** The system SHALL produce a structured daily log entry (JSON lines format) to stdout and to `logs/pipeline_YYYY-MM-DD.jsonl` capturing: run timestamp, each zone's status, anomaly values, email sent (bool), errors.

---

## 8. Non-Functional Requirements

| Requirement | Detail |
|---|---|
| **Performance** | Full pipeline run (ingest + compute + alert) SHALL complete in under 10 minutes for up to 10 zones on a single-core machine. |
| **Reliability** | Pipeline MUST fail loudly on CMEMS API errors, credential failures, or missing baseline files. No silent fallbacks. |
| **Security** | No credentials in source code or config files. All secrets via environment variables. |
| **Privacy** | No personal data is stored beyond alert email addresses in config. No user tracking. |
| **Platform** | Runs on Python 3.11+, Linux/macOS. Docker container is acceptable but not required for MVP. |
| **Dependencies** | Must use only open-source libraries (xarray, netCDF4, shapely, geopandas, pyyaml, copernicusmarine, smtplib). No proprietary data sources. |

---

## 9. Technical Approach

### Stack

- **Language:** Python 3.11+
- **Core libraries:** `copernicusmarine` (official CMEMS Python toolbox), `xarray`, `netCDF4`, `shapely 2.x`, `geopandas`, `numpy`, `pyyaml`, `sendgrid`
- **Alerting:** `sendgrid` Python SDK (free tier, 100 emails/day — sufficient for MVP alert volume)
- **Scheduling:** OS cron or GitHub Actions scheduled workflow

### Architecture

```
[CMEMS API] ──► ingest.py ──► raw NetCDF (temp file)
                                    │
                    ┌───────────────▼───────────────┐
                    │  anomaly_engine.py             │
                    │  - load climatology baseline   │
                    │  - apply turbidity mask        │
                    │  - compute pixel anomaly       │
                    │  - compute zone average        │
                    └───────────────┬───────────────┘
                                    │
                    ┌───────────────▼───────────────┐
                    │  alert_dispatcher.py           │
                    │  - threshold check             │
                    │  - dedup (1 per zone per day)  │
                    │  - send email via SMTP         │
                    └───────────────┬───────────────┘
                                    │
                             logs/pipeline_YYYY-MM-DD.jsonl
```

### Key Implementation Notes

- Use `copernicusmarine.subset()` to download only the required bounding box and date range, minimising bandwidth.
- The CMEMS L3 NRT product variable is `CHL` (chlorophyll_a), quality flag variable `CHL_flags`. Apply `CHL_flags == 1` (good data) filter before computing zone averages.
- Zone polygon masking: use `regionmask` or `shapely` to create a boolean mask array aligned to the NetCDF grid. Pre-compute and cache the mask per zone at startup to avoid re-computing each run.
- Climatology: compute from `cmems_obs-oc_atl_bgc-plankton_my_l3-olci-300m_P1D` offline using a separate `scripts/build_climatology.py` script. Group by `time.isocalendar().week`, take mean per pixel. Store as `wci_chl_climatology_wk.nc` with dimensions `(week, lat, lon)`.
- Turbid mask: hand-draw conservative GeoJSON polygons covering Shannon Estuary, inner Galway Bay, Clew Bay narrows, and Bantry Bay inner reaches. Err on the side of exclusion for v1.
- Deduplication: write a small SQLite file (`data/alert_log.db`) with schema `(zone_name TEXT, alert_date DATE, PRIMARY KEY (zone_name, alert_date))`. Check before sending; insert after successful send.

### File/Module Map

```
BlueWatch/
├── config/
│   └── zones.yaml                    # Zone definitions and thresholds
├── data/
│   ├── climatology/
│   │   └── wci_chl_climatology_wk.nc # Pre-computed baseline (built once offline)
│   ├── masks/
│   │   └── wci_turbid_mask.geojson   # Turbid pixel exclusion polygons
│   └── alert_log.db                  # SQLite dedup log (auto-created)
├── logs/                             # Daily pipeline log files (auto-created)
├── bluewatch/
│   ├── __init__.py
│   ├── ingest.py                     # CMEMS API download
│   ├── anomaly_engine.py             # Climatology load, masking, anomaly calc
│   ├── alert_dispatcher.py           # Threshold check, email send, dedup
│   └── config.py                     # Zone config loader and validator
├── scripts/
│   └── build_climatology.py          # One-time offline baseline builder
├── run_pipeline.py                   # Entry point: orchestrates ingest → compute → alert
├── .env.example                      # Template for required env vars
├── requirements.txt
└── README.md
```

---

## 10. Out of Scope (v1)

- Web UI, map visualisation, or user-facing dashboard.
- User account management, self-serve zone creation, or authentication.
- Mobile push notifications.
- SST or wind context overlays.
- L4 gap-filled product ingestion.
- Any geography outside the defined west coast of Ireland bounding box.
- East coast of Ireland, Northern Irish Lough systems, or inland waterbodies.
- Species context layer.
- Historical archive access or trend charts in alert emails.
- CSV/GeoJSON export or API endpoints.
- Paid tier or billing infrastructure.
- Docker containerisation (cron or GitHub Actions scheduler is sufficient).

---

## 11. Acceptance Criteria

| # | Criteria | Pass condition |
|---|---|---|
| AC-01 | Pipeline ingests CMEMS data | `python run_pipeline.py` completes without error and logs `DATA_AVAILABLE` or `CLOUD_GAP` for at least one zone |
| AC-02 | Climatology baseline loads | Pipeline loads the correct calendar-week slice from `wci_chl_climatology_wk.nc` without error |
| AC-03 | Turbidity mask applied | Pixels within `wci_turbid_mask.geojson` are excluded from zone averages; logged pixel count is lower than unmasked count |
| AC-04 | Anomaly ratio computed | Log entry for each zone contains numeric `anomaly_ratio`, `valid_pixel_count`, and `zone_avg_chl` fields |
| AC-05 | Alert email sent on threshold breach | When anomaly ratio ≥ threshold, an email is delivered to `alert_email` in zones.yaml with zone name, date, Chl-a values, and ratio in body |
| AC-06 | No duplicate alerts | Running the pipeline twice in one calendar day does not send two alert emails for the same zone |
| AC-07 | Cloud gap handling | When <20% valid pixels exist for a zone, log shows `CLOUD_GAP` and no anomaly email is sent |
| AC-08 | Gap notification after 3 days | A gap notification email is sent when a zone records 3 consecutive days of `CLOUD_GAP` |
| AC-09 | Loud failure on credential error | If `CMEMS_USERNAME` or `CMEMS_PASSWORD` env vars are missing, pipeline exits with non-zero code and a clear error message |
| AC-10 | Loud failure on email error | If SMTP dispatch fails, pipeline exits non-zero and logs the failure — does not silently continue |
| AC-11 | Zone validation | A zone polygon smaller than 1 km² in zones.yaml causes pipeline to exit at startup with a descriptive error |
| AC-12 | Daily log written | `logs/pipeline_YYYY-MM-DD.jsonl` is created with at least one JSON line containing `run_timestamp`, `zone_name`, `status`, `anomaly_ratio` |

---

## 12. Implementation Tasks

- [ ] **T01** Set up Python project: create directory structure, `requirements.txt`, `.env.example`, and basic `README.md` with cron setup instructions. (FR-20, FR-21)
- [ ] **T02** Implement `bluewatch/config.py`: load and validate `config/zones.yaml`; raise on missing fields or polygons below 1 km² minimum area. (FR-12, FR-13, FR-14)
- [ ] **T03** Write `scripts/build_climatology.py`: authenticate with CMEMS, download CMEMS L3 MY product for WCI bounding box (2016–2024), group by ISO calendar week, compute per-pixel mean, save as `data/climatology/wci_chl_climatology_wk.nc`. Run this script once offline. Add a `TODO` comment flagging the rolling 30-day window as a future experiment. (FR-05, FR-06)
- [ ] **T04** Create `data/masks/wci_turbid_mask.geojson`: hand-draw GeoJSON exclusion polygons for Shannon Estuary, inner Galway Bay, Clew Bay narrows, and Bantry Bay inner reaches using a GeoJSON editor. (FR-10)
- [ ] **T05** Implement `bluewatch/ingest.py`: use `copernicusmarine.subset()` to download the most recent L3 NRT Chl-a product for the WCI bounding box; apply `CHL_flags == 1` quality filter; return an `xarray.Dataset`. Use env vars for credentials. (FR-01, FR-02)
- [ ] **T06** Implement `bluewatch/anomaly_engine.py`: load climatology week slice, apply turbidity mask, compute per-pixel anomaly ratio, compute zone-averaged anomaly and valid pixel count per configured zone, determine `DATA_AVAILABLE` vs `CLOUD_GAP` status. (FR-03, FR-07, FR-08, FR-09, FR-10, FR-11)
- [ ] **T07** Create `data/alert_log.db` initialisation logic in `bluewatch/alert_dispatcher.py`: SQLite table `alert_log(zone_name TEXT, alert_date DATE, alert_type TEXT, PRIMARY KEY(zone_name, alert_date, alert_type))`. (FR-17)
- [ ] **T08** Implement `bluewatch/alert_dispatcher.py`: threshold check, deduplication query, SendGrid API send for anomaly alerts and gap notifications. Raise on SendGrid API failure. (FR-15, FR-16, FR-17, FR-18, FR-19)
- [ ] **T09** Implement `run_pipeline.py`: orchestrate ingest → anomaly engine → alert dispatcher; write structured JSON lines to stdout and to `logs/pipeline_YYYY-MM-DD.jsonl`; exit non-zero on any unhandled error. (FR-20, FR-22)
- [ ] **T10** Write `config/zones.yaml` with at least two real west coast of Ireland aquaculture zones (e.g. outer Clew Bay, outer Killary Harbour) using coordinates from public aquaculture licensing maps. (FR-12)
- [ ] **T11** End-to-end test: run `python run_pipeline.py` against a historical date with known CMEMS data availability; verify log output, anomaly values, and (if threshold met) email receipt. Confirm AC-01 through AC-12.

---

## 13. Success Metrics

1. Pipeline runs daily for 14 consecutive days without unhandled errors or silent failures.
2. At least one anomaly alert or gap notification is successfully delivered to the configured email address during the 14-day run.
3. The logged anomaly ratio for at least one zone on at least one day is cross-checked against a manual CMEMS portal query for the same zone and date — values agree within 10%.
4. False positive rate (anomaly alert sent during a week with no publicly reported HAB activity in Irish coastal waters) is below 30% over the 14-day pilot window.

---

## 14. Open Questions

*All current questions resolved. See below.*

### Resolved

- Market framing corrected: prior Irish / Atlantic-European HAB forecasting work exists and should be treated as precedent, not ignored.
- Differentiation clarified: the MVP competes on simplicity, zone configurability, and lower operational complexity rather than claiming no predecessor systems exist.

### Open

- Is the product advantage strong enough if institutional Irish or Atlantic-European bulletins already satisfy the highest-value users?
- Should BlueWatch be positioned primarily as a complementary operational layer on top of the currently operational Marine Institute bulletin and other public monitoring systems rather than as a replacement?

| # | Question | Decision |
|---|---|---|
| ~~OQ-0~~ | Should MVP cover all of Ireland or a subset? | West coast only (Atlantic-facing, lower turbidity, better OC5 algorithm performance per idea doc). East coast, estuaries, and inner bays deferred. |
| ~~OQ-1~~ | Which CMEMS L3 NRT product — Atlantic regional or global? | Atlantic regional: `cmems_obs-oc_atl_bgc-plankton_nrt_l3-olci-300m_P1D`. Better coastal resolution for the WCI bounding box. |
| ~~OQ-2~~ | ISO calendar-week climatology vs. rolling 30-day window? | ISO-week for v1 (simpler). Flag a TODO in `build_climatology.py` to experiment with rolling window in a later iteration. |
| ~~OQ-3~~ | SMTP (personal account) vs. transactional email service? | SendGrid API (free tier, 100 emails/day). Sufficient for MVP volume; avoids Gmail/Fastmail deliverability issues. |
| ~~OQ-4~~ | Beta testers from real west coast of Ireland aquaculture operators? | Multiple operators available; willingness to be named testers to be confirmed. |
