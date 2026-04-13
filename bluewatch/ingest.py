"""CMEMS L3 NRT Chl-a ingestion (FR-01, FR-02)."""

import datetime
import os
import sys
import tempfile
from pathlib import Path

import copernicusmarine
import xarray as xr

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DATASET_ID = "cmems_obs-oc_atl_bgc-plankton_nrt_l3-olci-300m_P1D"

# West coast of Ireland bounding box
MIN_LON = -11.0
MAX_LON = -7.0
MIN_LAT = 51.0
MAX_LAT = 55.5

# NRT product has a ~1-2 day processing lag; download yesterday by default
NRT_LAG_DAYS = 1
NRT_FALLBACK_DAYS = 1


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def apply_quality_filter(ds: xr.Dataset) -> xr.Dataset:
    """Return a copy of ds with non-flag-1 CHL pixels masked to NaN (FR-02)."""
    filtered = ds.copy()
    filtered["CHL"] = ds["CHL"].where(ds["CHL_flags"] == 1)
    return filtered


def fetch_latest_chl(date: datetime.date | None = None) -> xr.Dataset:
    """Download and quality-filter the L3 NRT Chl-a product for WCI.

    Uses ``copernicusmarine.subset()`` to download the specified (or most
    recent available) daily L3 NRT Chl-a for the west coast of Ireland
    bounding box, then applies the ``CHL_flags == 1`` quality filter.

    Args:
        date: Date to download. Defaults to the newest published NRT slice,
            starting with today minus NRT_LAG_DAYS and falling back one extra
            day to cover the documented publication lag.

    Returns:
        xr.Dataset with ``CHL`` (quality-filtered, bad pixels → NaN) and
        ``CHL_flags`` variables, with lat, lon, time coordinates.

    Exits non-zero if:
        - ``CMEMS_USERNAME`` or ``CMEMS_PASSWORD`` env vars are missing (AC-09).
        - The download fails or the output file cannot be opened.
    """
    username, password = _require_credentials()
    target_dates = _candidate_dates(date)

    with tempfile.TemporaryDirectory() as tmpdir:
        errors: list[str] = []
        for target_date in target_dates:
            date_str = target_date.isoformat()
            out_file = Path(tmpdir) / f"chl_nrt_{date_str}.nc"
            try:
                _download_subset(username, password, date_str, out_file)
                ds = xr.open_dataset(out_file, engine="h5netcdf").load()
            except CMEMSDownloadError as exc:
                errors.append(str(exc))
                continue
            except Exception as exc:
                sys.exit(f"ERROR: failed to open downloaded CHL file: {exc}")
            break
        else:
            sys.exit("\n".join(errors))

    return apply_quality_filter(ds)


# ---------------------------------------------------------------------------
# Internal helpers (separated for testability)
# ---------------------------------------------------------------------------


def _require_credentials() -> tuple[str, str]:
    """Return (username, password) from environment; sys.exit on missing (AC-09)."""
    username = os.environ.get("CMEMS_USERNAME")
    password = os.environ.get("CMEMS_PASSWORD")
    if not username or not password:
        sys.exit(
            "ERROR: CMEMS_USERNAME and CMEMS_PASSWORD environment variables must be set.\n"
            "       Register for free at https://marine.copernicus.eu"
        )
    return username, password


def _candidate_dates(date: datetime.date | None) -> list[datetime.date]:
    """Return explicit date or default NRT date(s) to try in order."""
    if date is not None:
        return [date]

    latest_expected = datetime.date.today() - datetime.timedelta(days=NRT_LAG_DAYS)
    return [
        latest_expected - datetime.timedelta(days=offset)
        for offset in range(NRT_FALLBACK_DAYS + 1)
    ]


class CMEMSDownloadError(RuntimeError):
    """Raised when a CMEMS subset download cannot provide the requested file."""


def _download_subset(username: str, password: str, date_str: str, out_file: Path) -> None:
    """Download a single-day CHL subset to out_file via copernicusmarine.subset()."""
    try:
        copernicusmarine.subset(
            dataset_id=DATASET_ID,
            variables=["CHL", "CHL_flags"],
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
        raise CMEMSDownloadError(f"ERROR: CMEMS download failed for {date_str}: {exc}") from exc
    if not out_file.exists():
        raise CMEMSDownloadError(
            f"ERROR: copernicusmarine.subset() did not create expected output file "
            f"{out_file} — check CMEMS product availability for {date_str}."
        )
