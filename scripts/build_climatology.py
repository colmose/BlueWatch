#!/usr/bin/env python3
"""
Build the per-pixel weekly climatological Chl-a baseline for the west coast of Ireland.

Downloads CMEMS L3 MY reprocessed Chl-a product (2016–2024) for the west coast of
Ireland bounding box, applies quality filtering (CHL_flags == 1), groups by ISO
calendar week, and saves the per-pixel mean as a NetCDF file.

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

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import copernicusmarine  # noqa: E402
import numpy as np  # noqa: E402
import xarray as xr  # noqa: E402

from bluewatch.cmems import (  # noqa: E402
    build_valid_chl_mask,
    is_missing_quality_variable_error,
    variable_request_candidates,
)
from bluewatch.env import get_env  # noqa: E402

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
    REPO_ROOT / "data" / "climatology" / "wci_chl_climatology_wk.nc"
)

DEFAULT_N_WEEKS = 52
MAX_ISO_WEEK = 53


# ---------------------------------------------------------------------------
# Core computation (extracted for testability)
# ---------------------------------------------------------------------------


def apply_quality_mask(ds: xr.Dataset) -> xr.DataArray:
    """Return CHL DataArray with non-flag-1 pixels masked to NaN."""
    return ds["CHL"].where(build_valid_chl_mask(ds))


def compute_weekly_climatology(
    chl: xr.DataArray,
    n_weeks: int | None = None,
) -> xr.DataArray:
    """Compute per-pixel climatological mean Chl-a grouped by ISO calendar week.

    Args:
        chl: DataArray with dimension 'time' and CHL_flags already applied.
        n_weeks: Minimum number of output weeks. Defaults to 52, but expands to
            53 automatically if the input contains ISO week 53.

    Returns:
        DataArray with dimension 'week' (1-indexed, length n_weeks).
    """
    # time.dt.isocalendar().week is fast: the time coordinate is not dask-backed.
    iso_weeks = chl.time.dt.isocalendar().week.values.astype(int)
    max_observed_week = int(iso_weeks.max()) if iso_weeks.size else DEFAULT_N_WEEKS
    effective_n_weeks = max(n_weeks or DEFAULT_N_WEEKS, max_observed_week)

    chl_with_week = chl.assign_coords(week=("time", iso_weeks))

    # groupby triggers a single pass over the data (dask-compatible)
    weekly_clim = chl_with_week.groupby("week").mean(dim="time", skipna=True)

    # Ensure every week 1–effective_n_weeks is present; fill any gap with NaN.
    all_weeks = np.arange(1, effective_n_weeks + 1)
    if len(weekly_clim.week) != effective_n_weeks or not np.array_equal(
        weekly_clim.week.values,
        all_weeks,
    ):
        weekly_clim = weekly_clim.reindex(week=all_weeks)

    return weekly_clim


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    username = get_env("CMEMS_USERNAME")
    password = get_env("CMEMS_PASSWORD")
    if not username or not password:
        sys.exit(
            "ERROR: CMEMS_USERNAME and CMEMS_PASSWORD environment variables must be set.\n"
            "       Register for free at https://marine.copernicus.eu"
        )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    print(f"Opening dataset:  {DATASET_ID}")
    print(f"  Bounding box:   lon [{MIN_LON}, {MAX_LON}], lat [{MIN_LAT}, {MAX_LAT}]")
    print(f"  Time range:     {START_DATE} to {END_DATE}")

    ds: xr.Dataset | None = None
    compatibility_errors: list[str] = []
    for variables in variable_request_candidates():
        try:
            ds = copernicusmarine.open_dataset(
                dataset_id=DATASET_ID,
                variables=list(variables),
                minimum_longitude=MIN_LON,
                maximum_longitude=MAX_LON,
                minimum_latitude=MIN_LAT,
                maximum_latitude=MAX_LAT,
                start_datetime=START_DATE,
                end_datetime=END_DATE,
                username=username,
                password=password,
            )
            break
        except Exception as exc:
            if is_missing_quality_variable_error(exc, variables[1]):
                compatibility_errors.append(str(exc))
                continue
            raise

    if ds is None:
        compatibility_detail = "; ".join(compatibility_errors)
        sys.exit(
            "ERROR: CMEMS dataset does not expose a supported quality variable. "
            f"{compatibility_detail}"
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
            "week_note": "Grouped by ISO calendar week; output expands to week 53 when present.",
        }
    )

    ds_out = weekly_clim.to_dataset(name="CHL_mean")
    ds_out.attrs["description"] = (
        "Per-pixel weekly climatological mean Chl-a for the west coast of Ireland. "
        f"Derived from {DATASET_ID}, good-quality pixels only (CHL_flags == 1). "
        f"Time range: {START_DATE} to {END_DATE}. "
        "Grouped by ISO calendar week (1–52 by default, including week 53 when present)."
    )

    print(f"Saving to {OUTPUT_PATH} ...")
    ds_out.to_netcdf(OUTPUT_PATH)
    print(f"Done.  Output: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
