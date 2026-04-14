"""Anomaly computation engine (FR-03, FR-07–FR-11)."""

import datetime
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import numpy as np
import shapely
import xarray as xr
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry

from bluewatch.config import Zone

CLIMATOLOGY_PATH = (
    Path(__file__).parent.parent / "data" / "climatology" / "wci_chl_climatology_wk.nc"
)
TURBID_MASK_PATH = Path(__file__).parent.parent / "data" / "masks" / "wci_turbid_mask.geojson"

# Zones with fewer than this fraction of valid pixels are CLOUD_GAP (FR-03)
MIN_VALID_FRACTION = 0.20


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ZoneResult:
    zone_name: str
    status: Literal["DATA_AVAILABLE", "CLOUD_GAP"]
    anomaly_ratio: float | None      # None when CLOUD_GAP
    zone_avg_chl: float | None       # None when CLOUD_GAP (mg m-3)
    climatology_mean_chl: float | None  # None when CLOUD_GAP (mg m-3)
    valid_pixel_count: int
    total_pixel_count: int


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_climatology_week(week: int, clim_path: Path = CLIMATOLOGY_PATH) -> xr.DataArray:
    """Return the per-pixel Chl-a climatological mean for a given ISO calendar week (FR-07).

    Args:
        week: ISO calendar week number (1–52, or 53 when present).
        clim_path: Path to ``wci_chl_climatology_wk.nc``.

    Returns:
        2-D DataArray with lat/lon dims and the week's mean CHL values.

    Exits non-zero if the file is missing, malformed, or the week is absent.
    """
    if not clim_path.exists():
        sys.exit(f"ERROR: climatology file not found: {clim_path}")
    ds = xr.open_dataset(clim_path)
    if "CHL_mean" not in ds.data_vars:
        sys.exit(f"ERROR: variable 'CHL_mean' not found in {clim_path}")
    available = ds.week.values
    if week not in available:
        sys.exit(
            f"ERROR: week {week} not found in climatology {clim_path}. "
            f"Available: {int(available[0])}–{int(available[-1])}"
        )
    return ds["CHL_mean"].sel(week=week)


def load_turbid_polygons(mask_path: Path = TURBID_MASK_PATH) -> list[BaseGeometry]:
    """Return a list of shapely Polygon objects from the turbid mask GeoJSON (FR-10)."""
    if not mask_path.exists():
        sys.exit(f"ERROR: turbid mask file not found: {mask_path}")
    gj = json.loads(mask_path.read_text())
    return [shape(f["geometry"]) for f in gj["features"]]


def build_polygon_mask(
    lats: np.ndarray,
    lons: np.ndarray,
    polygon: BaseGeometry,
) -> np.ndarray:
    """Return boolean (lat, lon) array — True if pixel centre is inside polygon.

    Args:
        lats: 1-D array of latitude values.
        lons: 1-D array of longitude values.
        polygon: A shapely geometry to test containment against.

    Returns:
        Boolean ndarray of shape (len(lats), len(lons)).
    """
    lon_grid, lat_grid = np.meshgrid(lons, lats)
    inside: np.ndarray = np.asarray(
        shapely.contains_xy(polygon, lon_grid.ravel(), lat_grid.ravel())
    )
    return inside.reshape(lon_grid.shape)


def compute_zone_results(
    chl_ds: xr.Dataset,
    zones: list[Zone],
    run_date: datetime.date,
    *,
    clim_path: Path = CLIMATOLOGY_PATH,
    mask_path: Path = TURBID_MASK_PATH,
) -> list[ZoneResult]:
    """Compute anomaly ratios and gap status for all configured zones (FR-08–FR-11).

    Args:
        chl_ds: Quality-filtered Dataset from ``ingest.fetch_latest_chl()``.
                Must contain variable ``CHL`` (bad pixels → NaN) with lat/lon coords.
        zones: Zone list from ``config.load_zones()``.
        run_date: Pipeline run date; used to select the climatology week.
        clim_path: Path to climatology NetCDF.
        mask_path: Path to turbid mask GeoJSON.

    Returns:
        List of ZoneResult, one per zone, in the same order as ``zones``.
    """
    week = run_date.isocalendar().week
    clim_week = load_climatology_week(week, clim_path)

    turbid_polygons = load_turbid_polygons(mask_path)
    turbid_union = shapely.unary_union(turbid_polygons)

    chl = _normalize_spatial_axes(chl_ds["CHL"])
    if "time" in chl.dims:
        chl = chl.isel(time=0)

    clim_week = _normalize_spatial_axes(clim_week)
    lats = chl.lat.values
    lons = chl.lon.values

    # Align climatology grid to the CHL grid (nearest-neighbour, no scipy required)
    clim_aligned = clim_week.sel(lat=chl.lat, lon=chl.lon, method="nearest")

    # Turbidity exclusion mask: True = turbid pixel (excluded from zone averages)
    turbid_mask = build_polygon_mask(lats, lons, turbid_union)

    # Apply turbidity mask — NaN-out excluded pixels
    chl_clean = chl.values.copy().astype(float)
    chl_clean[turbid_mask] = np.nan

    results: list[ZoneResult] = []
    for zone in zones:
        zone_mask = build_polygon_mask(lats, lons, zone.polygon)
        total_pixels = int(zone_mask.sum())

        if total_pixels == 0:
            results.append(
                ZoneResult(
                    zone_name=zone.name,
                    status="CLOUD_GAP",
                    anomaly_ratio=None,
                    zone_avg_chl=None,
                    climatology_mean_chl=None,
                    valid_pixel_count=0,
                    total_pixel_count=0,
                )
            )
            continue

        zone_chl = chl_clean[zone_mask]
        zone_clim = clim_aligned.values[zone_mask]

        # Valid pixel: non-NaN CHL, non-NaN climatology, positive climatology (FR-08)
        valid = ~np.isnan(zone_chl) & ~np.isnan(zone_clim) & (zone_clim > 0)
        valid_count = int(valid.sum())
        valid_fraction = valid_count / total_pixels

        if valid_fraction < MIN_VALID_FRACTION:
            results.append(
                ZoneResult(
                    zone_name=zone.name,
                    status="CLOUD_GAP",
                    anomaly_ratio=None,
                    zone_avg_chl=None,
                    climatology_mean_chl=None,
                    valid_pixel_count=valid_count,
                    total_pixel_count=total_pixels,
                )
            )
        else:
            pixel_ratios = zone_chl[valid] / zone_clim[valid]
            results.append(
                ZoneResult(
                    zone_name=zone.name,
                    status="DATA_AVAILABLE",
                    anomaly_ratio=float(np.mean(pixel_ratios)),
                    zone_avg_chl=float(np.mean(zone_chl[valid])),
                    climatology_mean_chl=float(np.mean(zone_clim[valid])),
                    valid_pixel_count=valid_count,
                    total_pixel_count=total_pixels,
                )
            )

    return results


def _normalize_spatial_axes(data: xr.DataArray) -> xr.DataArray:
    """Return a view of data with spatial axes normalized to lat/lon names."""
    lat_name, lon_name = _resolve_spatial_axis_names(data)

    normalized = data.transpose(..., lat_name, lon_name)
    rename_map = {}
    if lat_name != "lat":
        rename_map[lat_name] = "lat"
    if lon_name != "lon":
        rename_map[lon_name] = "lon"

    if rename_map:
        normalized = normalized.rename(rename_map)

    return normalized


def _resolve_spatial_axis_names(data: xr.DataArray) -> tuple[str, str]:
    for lat_name, lon_name in (("lat", "lon"), ("latitude", "longitude")):
        if lat_name in data.coords and lon_name in data.coords:
            return lat_name, lon_name
        if lat_name in data.dims and lon_name in data.dims:
            return lat_name, lon_name

    raise RuntimeError(
        "CHL data does not expose supported spatial coordinates. "
        f"Found dims={tuple(data.dims)!r}, coords={tuple(data.coords)!r}"
    )
