#!/usr/bin/env python3
"""
Build the per-pixel weekly climatological Chl-a baseline for the west coast of Ireland.

Downloads CMEMS L3 MY reprocessed Chl-a product (2016–2024) for the west coast of
Ireland bounding box, applies quality filtering (CHL_flags == 1), groups by ISO
calendar week, and saves the 52-week per-pixel mean as a NetCDF file.

Output: data/climatology/wci_chl_climatology_wk.nc
  Dimensions: (week, lat, lon)
  Variable:   CHL_mean — climatological mean Chl-a (mg m-3)

Run once offline before starting the daily pipeline:
  CMEMS_USERNAME=xxx CMEMS_PASSWORD=yyy python scripts/build_climatology.py
"""

# TODO: experiment with a rolling 30-day window climatology in a later iteration
# to assess whether it reduces week-boundary artefacts near ISO week transitions
# (e.g., late December / early January). Compare false-positive rates against the
# ISO-week baseline over the same 14-day pilot window before switching.

import os
import sys
from pathlib import Path

import numpy as np
import xarray as xr
import copernicusmarine

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DATASET_ID = "cmems_obs-oc_atl_bgc-plankton_my_l3-olci-300m_P1D"

# West coast of Ireland bounding box
MIN_LON = -11.0
MAX_LON = -7.0
MIN_LAT = 51.0
MAX_LAT = 55.5

START_DATE = "2016-01-01"
END_DATE = "2024-12-31"

OUTPUT_PATH = (
    Path(__file__).parent.parent / "data" / "climatology" / "wci_chl_climatology_wk.nc"
)

N_WEEKS = 52  # ISO calendar weeks; week 53 (rare) is absorbed into week 52


# ---------------------------------------------------------------------------
# Core computation (extracted for testability)
# ---------------------------------------------------------------------------


def apply_quality_mask(ds: xr.Dataset) -> xr.DataArray:
    """Return CHL DataArray with non-flag-1 pixels masked to NaN."""
    return ds["CHL"].where(ds["CHL_flags"] == 1)


def compute_weekly_climatology(chl: xr.DataArray, n_weeks: int = N_WEEKS) -> xr.DataArray:
    """Compute per-pixel climatological mean Chl-a grouped by ISO calendar week.

    ISO week 53 (occurs in some years) is absorbed into week 52 to keep a
    fixed 52-week output regardless of year.

    Args:
        chl: DataArray with dimension 'time' and CHL_flags already applied.
        n_weeks: Number of output weeks (default 52).

    Returns:
        DataArray with dimension 'week' (1-indexed, length n_weeks).
    """
    # time.dt.isocalendar().week is fast: the time coordinate is not dask-backed.
    iso_weeks_raw = chl.time.dt.isocalendar().week.values  # numpy int64 array
    iso_weeks = np.minimum(iso_weeks_raw, n_weeks).astype(int)  # cap week 53 → 52

    chl_with_week = chl.assign_coords(week=("time", iso_weeks))

    # groupby triggers a single pass over the data (dask-compatible)
    weekly_clim = chl_with_week.groupby("week").mean(dim="time", skipna=True)

    # Ensure every week 1–n_weeks is present; fill any gap with NaN
    all_weeks = np.arange(1, n_weeks + 1)
    if len(weekly_clim.week) != n_weeks or not np.array_equal(weekly_clim.week.values, all_weeks):
        weekly_clim = weekly_clim.reindex(week=all_weeks)

    return weekly_clim


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    username = os.environ.get("CMEMS_USERNAME")
    password = os.environ.get("CMEMS_PASSWORD")
    if not username or not password:
        sys.exit(
            "ERROR: CMEMS_USERNAME and CMEMS_PASSWORD environment variables must be set.\n"
            "       Register for free at https://marine.copernicus.eu"
        )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    print(f"Opening dataset:  {DATASET_ID}")
    print(f"  Bounding box:   lon [{MIN_LON}, {MAX_LON}], lat [{MIN_LAT}, {MAX_LAT}]")
    print(f"  Time range:     {START_DATE} to {END_DATE}")

    ds = copernicusmarine.open_dataset(
        dataset_id=DATASET_ID,
        variables=["CHL", "CHL_flags"],
        minimum_longitude=MIN_LON,
        maximum_longitude=MAX_LON,
        minimum_latitude=MIN_LAT,
        maximum_latitude=MAX_LAT,
        start_datetime=START_DATE,
        end_datetime=END_DATE,
        username=username,
        password=password,
    )

    print(f"Dataset opened.   Dimensions: {dict(ds.sizes)}")
    n_steps = ds.sizes["time"]
    print(f"  Time steps:     {n_steps} days")
    if n_steps == 0:
        sys.exit(
            f"ERROR: CMEMS returned 0 time steps for {DATASET_ID} "
            f"over {START_DATE} to {END_DATE}. "
            "Check product availability and date range."
        )

    chl_masked = apply_quality_mask(ds)
    weekly_clim = compute_weekly_climatology(chl_masked)

    weekly_clim.name = "CHL_mean"
    weekly_clim.attrs.update(
        {
            "long_name": "Climatological mean chlorophyll-a by ISO calendar week",
            "units": "mg m-3",
            "source_dataset": DATASET_ID,
            "time_range": f"{START_DATE} to {END_DATE}",
            "quality_filter": "CHL_flags == 1 (good data only)",
            "week_note": "ISO week 53 (occurs in some years) is merged into week 52",
        }
    )

    ds_out = weekly_clim.to_dataset(name="CHL_mean")
    ds_out.attrs["description"] = (
        "Per-pixel weekly climatological mean Chl-a for the west coast of Ireland. "
        f"Derived from {DATASET_ID}, good-quality pixels only (CHL_flags == 1). "
        f"Time range: {START_DATE} to {END_DATE}. "
        "Grouped by ISO calendar week (1–52); week 53 merged into week 52."
    )

    print(f"Saving to {OUTPUT_PATH} ...")
    ds_out.to_netcdf(OUTPUT_PATH)
    print(f"Done.  Output: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
