"""Synthetic fixture builder for historical HAB bloom tests (T12).

Turns a bloom catalog entry into an xr.Dataset usable by compute_zone_results().
Keeps tests fully offline — no CMEMS credentials needed.
"""

from __future__ import annotations

import datetime
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import xarray as xr
import yaml
from shapely.geometry import Point

# WCI bounding box (matches ingest defaults)
_WCI_LAT_MIN = 51.0
_WCI_LAT_MAX = 55.5
_WCI_LON_MIN = -11.0
_WCI_LON_MAX = -7.0

# Pixel resolution matching CMEMS L3 OLCI 300m product (approx 0.00278° ~= 300m)
_PIXEL_STEP = 0.05  # coarser for test speed but sufficient for zone coverage

# Baseline climatology CHL value used for all pixels outside the bloom zone.
_BACKGROUND_CHL = 1.0  # mg m-3

# Minimum valid pixel fraction (must match anomaly_engine.MIN_VALID_FRACTION = 0.20)
_MIN_VALID_FRACTION = 0.20

CATALOG_PATH = Path(__file__).parent / "historical_blooms.yaml"


@dataclass(frozen=True)
class BloomEvent:
    event_id: str
    date: datetime.date
    zone_name: str
    species: str
    source: str
    synthetic_anomaly_ratio: float
    should_alert: bool
    notes: str


_REQUIRED_BLOOM_FIELDS = frozenset(
    {"event_id", "date", "zone_name", "species", "source",
     "synthetic_anomaly_ratio", "should_alert"}
)


def load_bloom_catalog(catalog_path: Path = CATALOG_PATH) -> list[BloomEvent]:
    """Load and return all bloom events from the catalog YAML.

    Raises:
        ValueError: If any required field is missing from a catalog entry.
    """
    with catalog_path.open() as fh:
        raw: dict[str, Any] = yaml.safe_load(fh)

    events = []
    for i, entry in enumerate(raw["events"]):
        missing = _REQUIRED_BLOOM_FIELDS - set(entry.keys())
        if missing:
            raise ValueError(
                f"Bloom catalog entry {i} (event_id={entry.get('event_id', '?')!r}) "
                f"is missing required fields: {', '.join(sorted(missing))}"
            )
        events.append(
            BloomEvent(
                event_id=entry["event_id"],
                date=datetime.date.fromisoformat(str(entry["date"])),
                zone_name=entry["zone_name"],
                species=entry.get("species") or "",
                source=entry["source"],
                synthetic_anomaly_ratio=float(entry["synthetic_anomaly_ratio"]),
                should_alert=bool(entry["should_alert"]),
                notes=entry.get("notes", ""),
            )
        )
    return events


def build_climatology_fixture(
    event: BloomEvent,
    zone_polygon: Any,  # shapely geometry
    tmp_path: Path,
    background_chl: float = _BACKGROUND_CHL,
) -> Path:
    """Write a synthetic climatology NetCDF that the anomaly engine can load.

    All pixel climatology values are set to ``background_chl``.  The bloom
    is represented as a CHL elevation in the CHL dataset (see
    ``build_bloom_fixture``), not in the climatology.

    Args:
        event: The bloom event entry.
        zone_polygon: The shapely polygon for the target zone.
        tmp_path: Directory to write the NetCDF into.
        background_chl: Climatological mean CHL (mg m-3) used for all pixels.

    Returns:
        Path to the written climatology NetCDF.
    """
    lats = np.arange(_WCI_LAT_MIN, _WCI_LAT_MAX, _PIXEL_STEP)
    lons = np.arange(_WCI_LON_MIN, _WCI_LON_MAX, _PIXEL_STEP)
    n_weeks = 52

    clim_data = np.full(
        (n_weeks, len(lats), len(lons)), background_chl, dtype=np.float32
    )
    da = xr.DataArray(
        clim_data,
        dims=["week", "lat", "lon"],
        coords={
            "week": np.arange(1, n_weeks + 1),
            "lat": lats,
            "lon": lons,
        },
        name="CHL_mean",
    )
    clim_path = tmp_path / f"clim_{event.event_id}.nc"
    da.to_dataset(name="CHL_mean").to_netcdf(clim_path)
    return clim_path


def build_bloom_fixture(
    event: BloomEvent,
    zone_polygon: Any,  # shapely geometry
    background_chl: float = _BACKGROUND_CHL,
) -> xr.Dataset:
    """Build a synthetic CHL Dataset that simulates the given bloom event.

    Pixels *inside* the target zone are set to
    ``background_chl * event.synthetic_anomaly_ratio``.
    All other pixels retain the background level.

    The fraction of valid (non-NaN) pixels inside the zone is guaranteed to
    exceed ``MIN_VALID_FRACTION`` (0.20) so that compute_zone_results() yields
    DATA_AVAILABLE rather than CLOUD_GAP.

    Args:
        event: The bloom event to simulate.
        zone_polygon: The shapely polygon for the zone named in ``event.zone_name``.
        background_chl: Climatological mean CHL (mg m-3).

    Returns:
        xr.Dataset with variable ``CHL`` and coords ``time``, ``lat``, ``lon``.
    """
    lats = np.arange(_WCI_LAT_MIN, _WCI_LAT_MAX, _PIXEL_STEP)
    lons = np.arange(_WCI_LON_MIN, _WCI_LON_MAX, _PIXEL_STEP)

    bloom_chl = float(background_chl * event.synthetic_anomaly_ratio)

    chl_values = np.full(
        (1, len(lats), len(lons)), background_chl, dtype=np.float32
    )

    # Set bloom CHL inside the zone polygon
    lon_grid, lat_grid = np.meshgrid(lons, lats)
    for i in range(len(lats)):
        for j in range(len(lons)):
            if zone_polygon.contains(Point(lon_grid[i, j], lat_grid[i, j])):
                chl_values[0, i, j] = bloom_chl

    return xr.Dataset(
        {"CHL": (["time", "lat", "lon"], chl_values)},
        coords={
            "time": np.array([str(event.date)], dtype="datetime64[ns]"),
            "lat": lats,
            "lon": lons,
        },
    )


def build_synthetic_climatology(
    tmp_path: Path,
    zone_polygon: Any,  # shapely geometry
    n_weeks: int = 52,
    base_chl: float = 2.0,
) -> Path:
    """Build a minimal synthetic climatology NetCDF covering a zone polygon.

    Creates a ``(week, lat, lon)`` NetCDF where all ``CHL_mean`` values equal
    ``base_chl``.  The grid covers the zone's bounding box padded by 0.15°
    at 0.1° spacing, guaranteeing at least a 3×3 grid.

    Args:
        tmp_path: Directory to write the file into.
        zone_polygon: Shapely geometry used to derive the bounding box.
        n_weeks: Number of ISO weeks to include (52 or 53).
        base_chl: Uniform climatological CHL value (mg m-3).

    Returns:
        Path to the written ``wci_chl_climatology_wk.nc`` file.
    """
    pad = 0.15
    step = 0.1

    min_lon, min_lat, max_lon, max_lat = zone_polygon.bounds
    lats = np.arange(min_lat - pad, max_lat + pad + step, step)
    lons = np.arange(min_lon - pad, max_lon + pad + step, step)

    # Guarantee at least a 3×3 grid
    while len(lats) < 3:
        lats = np.append(lats, lats[-1] + step)
    while len(lons) < 3:
        lons = np.append(lons, lons[-1] + step)

    clim_data = np.full(
        (n_weeks, len(lats), len(lons)), base_chl, dtype=np.float32
    )
    da = xr.DataArray(
        clim_data,
        dims=["week", "lat", "lon"],
        coords={
            "week": np.arange(1, n_weeks + 1),
            "lat": lats,
            "lon": lons,
        },
        name="CHL_mean",
    )
    clim_path = tmp_path / "wci_chl_climatology_wk.nc"
    da.to_dataset(name="CHL_mean").to_netcdf(clim_path)
    return clim_path


def write_empty_turbid_mask(tmp_path: Path) -> Path:
    """Write a turbid mask GeoJSON with no features (excludes nothing)."""
    mask_path = tmp_path / "turbid_mask.geojson"
    mask_path.write_text(
        json.dumps({"type": "FeatureCollection", "features": []}),
        encoding="utf-8",
    )
    return mask_path
