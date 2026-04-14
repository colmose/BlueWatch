"""CMEMS L3 NRT Chl-a ingestion (FR-01, FR-02)."""

from __future__ import annotations

import datetime
import sys
import tempfile
from pathlib import Path
from typing import Any, cast

import xarray as xr

from bluewatch.cmems import (
    build_valid_chl_mask,
    is_missing_quality_variable_error,
    variable_request_candidates,
)
from bluewatch.env import get_env

try:
    import copernicusmarine as _copernicusmarine
except ImportError:  # pragma: no cover - exercised via failure path
    class _MissingCopernicusMarine:
        def __getattr__(self, name: str) -> object:
            raise ImportError(
                "copernicusmarine is required for ingestion. Install dependencies from "
                "requirements.txt before running the pipeline."
            )

    _copernicusmarine = _MissingCopernicusMarine()

copernicusmarine = cast(Any, _copernicusmarine)


DATASET_ID = "cmems_obs-oc_atl_bgc-plankton_nrt_l3-olci-300m_P1D"

# West coast of Ireland bounding box
MIN_LON = -11.0
MAX_LON = -7.0
MIN_LAT = 51.0
MAX_LAT = 55.5

# NRT product has a typical 1-2 day publication lag.
NRT_LAG_DAYS = 1
NRT_FALLBACK_DAYS = 1


class CMEMSDownloadError(RuntimeError):
    """Raised when CMEMS cannot provide the requested subset."""


def apply_quality_filter(ds: xr.Dataset) -> xr.Dataset:
    """Return a copy of ds with non-flag-1 CHL pixels masked to NaN."""
    filtered = ds.copy()
    filtered["CHL"] = ds["CHL"].where(build_valid_chl_mask(ds))
    return filtered


def fetch_latest_chl(date: datetime.date | None = None) -> xr.Dataset:
    """Download and quality-filter the L3 NRT Chl-a product for WCI."""
    username, password = _require_credentials()
    candidate_dates = _candidate_dates(date)

    with tempfile.TemporaryDirectory() as tmpdir:
        errors: list[str] = []
        for target_date in candidate_dates:
            date_str = target_date.isoformat()
            out_file = Path(tmpdir) / f"chl_nrt_{date_str}.nc"

            try:
                _download_subset(username, password, date_str, out_file)
                dataset = xr.open_dataset(out_file, engine="h5netcdf").load()
            except CMEMSDownloadError as exc:
                errors.append(str(exc))
                continue
            except SystemExit:
                raise
            except Exception as exc:  # pragma: no cover - defensive runtime path
                sys.exit(f"ERROR: failed to open downloaded CHL file: {exc}")

            return apply_quality_filter(dataset)

    sys.exit("\n".join(errors))


def _require_credentials() -> tuple[str, str]:
    username = get_env("CMEMS_USERNAME")
    password = get_env("CMEMS_PASSWORD")

    if not username or not password:
        sys.exit(
            "ERROR: CMEMS_USERNAME and CMEMS_PASSWORD environment variables must be set.\n"
            "       Register for free at https://marine.copernicus.eu"
        )

    return username, password


def _candidate_dates(date: datetime.date | None) -> list[datetime.date]:
    if date is not None:
        return [date]

    latest_expected = datetime.date.today() - datetime.timedelta(days=NRT_LAG_DAYS)
    return [
        latest_expected - datetime.timedelta(days=offset)
        for offset in range(NRT_FALLBACK_DAYS + 1)
    ]


def _download_subset(username: str, password: str, date_str: str, out_file: Path) -> None:
    compatibility_errors: list[str] = []

    for variables in variable_request_candidates():
        try:
            out_file.unlink(missing_ok=True)
            copernicusmarine.subset(
                dataset_id=DATASET_ID,
                variables=list(variables),
                minimum_longitude=MIN_LON,
                maximum_longitude=MAX_LON,
                minimum_latitude=MIN_LAT,
                maximum_latitude=MAX_LAT,
                start_datetime=date_str,
                end_datetime=date_str,
                output_filename=out_file.name,
                output_directory=str(out_file.parent),
                username=username,
                password=password,
            )
        except Exception as exc:
            if is_missing_quality_variable_error(exc, variables[1]):
                compatibility_errors.append(str(exc))
                continue
            raise CMEMSDownloadError(f"ERROR: CMEMS download failed for {date_str}: {exc}") from exc

        if not out_file.exists():
            raise CMEMSDownloadError(
                f"ERROR: copernicusmarine.subset() did not create expected output file "
                f"{out_file} - check CMEMS product availability for {date_str}."
            )

        return

    compatibility_detail = "; ".join(compatibility_errors)
    raise CMEMSDownloadError(
        f"ERROR: CMEMS download failed for {date_str}: no supported quality variable "
        f"was available ({compatibility_detail})"
    )
