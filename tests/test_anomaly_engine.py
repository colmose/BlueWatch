"""Tests for bluewatch/anomaly_engine.py (T06)."""

import datetime
import json
from pathlib import Path

import numpy as np
import pytest
import xarray as xr
from shapely.geometry import box

from bluewatch.anomaly_engine import (
    build_polygon_mask,
    compute_zone_results,
    load_climatology_week,
    load_turbid_polygons,
)
from bluewatch.config import Zone

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_chl_dataset(
    chl_values,
    lats=(53.0, 53.1, 53.2),
    lons=(-10.0, -9.9, -9.8),
    date: str = "2024-04-01",
) -> xr.Dataset:
    """Build a synthetic quality-filtered CHL Dataset (bad pixels already NaN)."""
    return xr.Dataset(
        {"CHL": (["time", "lat", "lon"], np.array(chl_values, dtype=np.float32))},
        coords={
            "time": np.array([date], dtype="datetime64[ns]"),
            "lat": np.array(lats),
            "lon": np.array(lons),
        },
    )


def _write_climatology(tmp_path: Path, chl_mean_values, lats, lons, n_weeks=52) -> Path:
    """Write a synthetic climatology NetCDF and return its path."""
    weeks = np.arange(1, n_weeks + 1)
    clim_data = np.zeros((n_weeks, len(lats), len(lons)), dtype=np.float32)
    # Fill all weeks with the provided values
    clim_data[:] = np.array(chl_mean_values, dtype=np.float32)

    da = xr.DataArray(
        clim_data,
        dims=["week", "lat", "lon"],
        coords={"week": weeks, "lat": np.array(lats), "lon": np.array(lons)},
        name="CHL_mean",
    )
    path = tmp_path / "wci_chl_climatology_wk.nc"
    da.to_dataset(name="CHL_mean").to_netcdf(path)
    return path


def _write_empty_mask(tmp_path: Path) -> Path:
    """Write a turbid mask GeoJSON with no features (excludes nothing)."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = tmp_path / "turbid_mask.geojson"
    path.write_text(json.dumps({"type": "FeatureCollection", "features": []}))
    return path


def _make_zone(
    name: str = "Test Zone",
    polygon=None,
    threshold_multiplier: float = 3.0,
    alert_email: str = "test@example.com",
) -> Zone:
    if polygon is None:
        # Small box centred in the synthetic grid
        polygon = box(-10.05, 52.95, -9.75, 53.25)
    return Zone(
        name=name,
        description="Test zone",
        polygon=polygon,
        threshold_multiplier=threshold_multiplier,
        alert_email=alert_email,
    )


# ---------------------------------------------------------------------------
# load_climatology_week
# ---------------------------------------------------------------------------


def test_load_climatology_week_returns_correct_slice(tmp_path):
    lats = [53.0, 53.1]
    lons = [-10.0, -9.9]
    n_weeks = 52
    clim_data = np.zeros((n_weeks, 2, 2), dtype=np.float32)
    clim_data[0] = 1.0   # week 1
    clim_data[9] = 5.0   # week 10

    da = xr.DataArray(
        clim_data,
        dims=["week", "lat", "lon"],
        coords={"week": np.arange(1, 53), "lat": lats, "lon": lons},
        name="CHL_mean",
    )
    clim_path = tmp_path / "clim.nc"
    da.to_dataset(name="CHL_mean").to_netcdf(clim_path)

    slice_1 = load_climatology_week(1, clim_path)
    assert float(slice_1.isel(lat=0, lon=0)) == pytest.approx(1.0)

    slice_10 = load_climatology_week(10, clim_path)
    assert float(slice_10.isel(lat=0, lon=0)) == pytest.approx(5.0)


def test_load_climatology_week_exits_if_file_missing(tmp_path):
    with pytest.raises(SystemExit) as exc_info:
        load_climatology_week(1, tmp_path / "no_such_file.nc")
    assert exc_info.value.code != 0


def test_load_climatology_week_exits_if_week_absent(tmp_path):
    lats = [53.0]
    lons = [-10.0]
    da = xr.DataArray(
        np.ones((1, 1, 1), dtype=np.float32),
        dims=["week", "lat", "lon"],
        coords={"week": [5], "lat": lats, "lon": lons},
        name="CHL_mean",
    )
    clim_path = tmp_path / "clim.nc"
    da.to_dataset(name="CHL_mean").to_netcdf(clim_path)

    with pytest.raises(SystemExit) as exc_info:
        load_climatology_week(99, clim_path)
    assert exc_info.value.code != 0


# ---------------------------------------------------------------------------
# load_turbid_polygons
# ---------------------------------------------------------------------------


def test_load_turbid_polygons_exits_if_missing(tmp_path):
    with pytest.raises(SystemExit) as exc_info:
        load_turbid_polygons(tmp_path / "no_mask.geojson")
    assert exc_info.value.code != 0


def test_load_turbid_polygons_returns_polygon_list(tmp_path):
    mask_path = tmp_path / "mask.geojson"
    mask_path.write_text(
        json.dumps({
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[
                            [-10.0, 52.0], [-9.0, 52.0],
                            [-9.0, 53.0], [-10.0, 53.0], [-10.0, 52.0],
                        ]],
                    },
                }
            ],
        })
    )
    polygons = load_turbid_polygons(mask_path)
    assert len(polygons) == 1


# ---------------------------------------------------------------------------
# build_polygon_mask
# ---------------------------------------------------------------------------


def test_build_polygon_mask_inside_pixels_true():
    lats = np.array([53.0, 53.1, 53.2])
    lons = np.array([-10.0, -9.9, -9.8])
    # polygon that exactly covers the centre pixel (53.1, -9.9)
    poly = box(-9.95, 53.05, -9.85, 53.15)
    mask = build_polygon_mask(lats, lons, poly)
    assert mask.shape == (3, 3)
    assert mask[1, 1]          # centre pixel inside
    assert not mask[0, 0]      # corner pixel outside


def test_build_polygon_mask_outside_polygon_all_false():
    lats = np.array([53.0, 53.1])
    lons = np.array([-10.0, -9.9])
    poly = box(-8.0, 50.0, -7.0, 51.0)  # far away
    mask = build_polygon_mask(lats, lons, poly)
    assert not mask.any()


# ---------------------------------------------------------------------------
# compute_zone_results — DATA_AVAILABLE path
# ---------------------------------------------------------------------------


def test_data_available_when_sufficient_valid_pixels(tmp_path):
    lats = (53.0, 53.1, 53.2)
    lons = (-10.0, -9.9, -9.8)

    # CHL = 6.0 everywhere, clim = 2.0 → expected anomaly_ratio = 3.0
    chl_ds = _make_chl_dataset(
        chl_values=[np.full((3, 3), 6.0, dtype=np.float32)],
        lats=lats,
        lons=lons,
    )
    clim_path = _write_climatology(tmp_path, np.full((3, 3), 2.0, dtype=np.float32), lats, lons)
    mask_path = _write_empty_mask(tmp_path)
    zone = _make_zone()

    results = compute_zone_results(
        chl_ds,
        [zone],
        datetime.date(2024, 4, 1),
        clim_path=clim_path,
        mask_path=mask_path,
    )

    assert len(results) == 1
    r = results[0]
    assert r.status == "DATA_AVAILABLE"
    assert r.anomaly_ratio == pytest.approx(3.0, rel=0.01)
    assert r.zone_avg_chl == pytest.approx(6.0, rel=0.01)
    assert r.valid_pixel_count > 0
    assert r.total_pixel_count > 0


def test_zone_result_has_required_fields(tmp_path):
    """AC-04: zone result contains anomaly_ratio, valid_pixel_count, zone_avg_chl."""
    lats = (53.0, 53.1, 53.2)
    lons = (-10.0, -9.9, -9.8)
    chl_ds = _make_chl_dataset(
        chl_values=[np.full((3, 3), 4.0, dtype=np.float32)],
        lats=lats, lons=lons,
    )
    clim_path = _write_climatology(tmp_path, np.full((3, 3), 2.0, dtype=np.float32), lats, lons)
    mask_path = _write_empty_mask(tmp_path)

    results = compute_zone_results(
        chl_ds, [_make_zone()], datetime.date(2024, 4, 1),
        clim_path=clim_path, mask_path=mask_path,
    )
    r = results[0]
    assert isinstance(r.anomaly_ratio, float)
    assert isinstance(r.valid_pixel_count, int)
    assert isinstance(r.zone_avg_chl, float)


# ---------------------------------------------------------------------------
# compute_zone_results — CLOUD_GAP path (AC-07)
# ---------------------------------------------------------------------------


def test_cloud_gap_when_all_chl_nan(tmp_path):
    """AC-07: zone with all-NaN CHL should return CLOUD_GAP."""
    lats = (53.0, 53.1, 53.2)
    lons = (-10.0, -9.9, -9.8)
    chl_data = np.full((1, 3, 3), np.nan, dtype=np.float32)
    chl_ds = _make_chl_dataset(chl_values=chl_data, lats=lats, lons=lons)
    clim_path = _write_climatology(tmp_path, np.full((3, 3), 2.0, dtype=np.float32), lats, lons)
    mask_path = _write_empty_mask(tmp_path)

    results = compute_zone_results(
        chl_ds, [_make_zone()], datetime.date(2024, 4, 1),
        clim_path=clim_path, mask_path=mask_path,
    )
    r = results[0]
    assert r.status == "CLOUD_GAP"
    assert r.anomaly_ratio is None
    assert r.zone_avg_chl is None


def test_cloud_gap_when_valid_fraction_below_threshold(tmp_path):
    """AC-07: fewer than 20% valid pixels → CLOUD_GAP."""
    lats = (53.0, 53.1, 53.2)
    lons = (-10.0, -9.9, -9.8)
    # 9 pixels in zone; only 1 valid (= 11%, below 20%)
    chl_data = np.full((1, 3, 3), np.nan, dtype=np.float32)
    chl_data[0, 1, 1] = 4.0  # one valid pixel
    chl_ds = _make_chl_dataset(chl_values=chl_data, lats=lats, lons=lons)
    clim_path = _write_climatology(tmp_path, np.full((3, 3), 2.0, dtype=np.float32), lats, lons)
    mask_path = _write_empty_mask(tmp_path)

    # Zone covers all 9 pixels
    zone = _make_zone(polygon=box(-10.05, 52.95, -9.75, 53.25))
    results = compute_zone_results(
        chl_ds, [zone], datetime.date(2024, 4, 1),
        clim_path=clim_path, mask_path=mask_path,
    )
    r = results[0]
    assert r.status == "CLOUD_GAP"
    assert r.valid_pixel_count == 1


def test_data_available_at_exact_threshold(tmp_path):
    """Exactly 20% valid pixels should be DATA_AVAILABLE (boundary condition)."""
    lats = (53.0, 53.1, 53.2, 53.3, 53.4)
    lons = (-10.0,)
    # 5 pixels total; 1 valid = exactly 20%
    chl_data = np.full((1, 5, 1), np.nan, dtype=np.float32)
    chl_data[0, 2, 0] = 4.0  # one valid pixel
    chl_ds = _make_chl_dataset(chl_values=chl_data, lats=lats, lons=lons)
    clim_path = _write_climatology(tmp_path, np.full((5, 1), 2.0, dtype=np.float32), lats, lons)
    mask_path = _write_empty_mask(tmp_path)

    zone = _make_zone(polygon=box(-10.05, 52.95, -9.95, 53.45))
    results = compute_zone_results(
        chl_ds, [zone], datetime.date(2024, 4, 1),
        clim_path=clim_path, mask_path=mask_path,
    )
    r = results[0]
    assert r.status == "DATA_AVAILABLE"


# ---------------------------------------------------------------------------
# Turbidity mask exclusion (AC-03)
# ---------------------------------------------------------------------------


def test_turbid_mask_reduces_valid_pixel_count(tmp_path):
    """AC-03: pixels inside turbid mask should be excluded from zone averages."""
    lats = (53.0, 53.1, 53.2)
    lons = (-10.0, -9.9, -9.8)
    chl_ds = _make_chl_dataset(
        chl_values=[np.full((3, 3), 4.0, dtype=np.float32)],
        lats=lats, lons=lons,
    )
    clim_path = _write_climatology(tmp_path, np.full((3, 3), 2.0, dtype=np.float32), lats, lons)

    # Turbid mask covers the left column (lon=-10.0), excluding 3 pixels
    turbid_mask_path = tmp_path / "turbid.geojson"
    turbid_mask_path.write_text(
        json.dumps({
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "properties": {},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [-10.05, 52.95], [-9.85, 52.95],
                        [-9.85, 53.25], [-10.05, 53.25], [-10.05, 52.95],
                    ]],
                },
            }],
        })
    )

    zone = _make_zone(polygon=box(-10.05, 52.95, -9.75, 53.25))
    no_mask_path = _write_empty_mask(tmp_path / "empty")
    no_mask_path.parent.mkdir(exist_ok=True)

    results_with_mask = compute_zone_results(
        chl_ds, [zone], datetime.date(2024, 4, 1),
        clim_path=clim_path, mask_path=turbid_mask_path,
    )
    results_no_mask = compute_zone_results(
        chl_ds, [zone], datetime.date(2024, 4, 1),
        clim_path=clim_path, mask_path=no_mask_path,
    )

    assert results_with_mask[0].valid_pixel_count < results_no_mask[0].valid_pixel_count


# ---------------------------------------------------------------------------
# Climatology week selection (AC-02)
# ---------------------------------------------------------------------------


def test_correct_week_selected_from_run_date(tmp_path):
    """AC-02: run_date determines which climatology week slice is loaded."""
    lats = (53.0, 53.1)
    lons = (-10.0, -9.9)
    n_weeks = 52
    clim_data = np.ones((n_weeks, 2, 2), dtype=np.float32)
    # week 14 (2024-04-01 is ISO week 14) → clim = 3.0, CHL = 9.0 → ratio 3.0
    # week 20 → clim = 1.0, CHL = 9.0 → ratio 9.0
    clim_data[13] = 3.0  # week 14 (0-indexed: 13)
    clim_data[19] = 1.0  # week 20

    da = xr.DataArray(
        clim_data,
        dims=["week", "lat", "lon"],
        coords={"week": np.arange(1, 53), "lat": np.array(lats), "lon": np.array(lons)},
        name="CHL_mean",
    )
    clim_path = tmp_path / "clim.nc"
    da.to_dataset(name="CHL_mean").to_netcdf(clim_path)
    mask_path = _write_empty_mask(tmp_path)

    chl_ds = _make_chl_dataset(
        chl_values=[np.full((2, 2), 9.0, dtype=np.float32)],
        lats=lats, lons=lons,
    )
    zone = _make_zone(polygon=box(-10.05, 52.95, -9.85, 53.15))

    # 2024-04-01 is ISO week 14
    assert datetime.date(2024, 4, 1).isocalendar().week == 14
    results_w14 = compute_zone_results(
        chl_ds, [zone], datetime.date(2024, 4, 1),
        clim_path=clim_path, mask_path=mask_path,
    )
    assert results_w14[0].anomaly_ratio == pytest.approx(3.0, rel=0.01)

    # 2024-05-13 is ISO week 20
    assert datetime.date(2024, 5, 13).isocalendar().week == 20
    results_w20 = compute_zone_results(
        chl_ds, [zone], datetime.date(2024, 5, 13),
        clim_path=clim_path, mask_path=mask_path,
    )
    assert results_w20[0].anomaly_ratio == pytest.approx(9.0, rel=0.01)


# ---------------------------------------------------------------------------
# Multiple zones
# ---------------------------------------------------------------------------


def test_multiple_zones_all_returned(tmp_path):
    lats = (53.0, 53.1, 53.2)
    lons = (-10.0, -9.9, -9.8)
    chl_ds = _make_chl_dataset(
        chl_values=[np.full((3, 3), 4.0, dtype=np.float32)],
        lats=lats, lons=lons,
    )
    clim_path = _write_climatology(tmp_path, np.full((3, 3), 2.0, dtype=np.float32), lats, lons)
    mask_path = _write_empty_mask(tmp_path)

    zones = [
        _make_zone("Zone A", polygon=box(-10.05, 52.95, -9.85, 53.15)),
        _make_zone("Zone B", polygon=box(-9.95, 53.05, -9.75, 53.25)),
    ]
    results = compute_zone_results(
        chl_ds, zones, datetime.date(2024, 4, 1),
        clim_path=clim_path, mask_path=mask_path,
    )
    assert len(results) == 2
    assert results[0].zone_name == "Zone A"
    assert results[1].zone_name == "Zone B"
