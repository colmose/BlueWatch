"""Tests for scripts/build_climatology.py (T03)."""

import importlib.util
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import xarray as xr

# ---------------------------------------------------------------------------
# Import the script module (scripts/ is not a package, so load explicitly)
# ---------------------------------------------------------------------------

_SCRIPT_PATH = Path(__file__).parent.parent / "scripts" / "build_climatology.py"
_spec = importlib.util.spec_from_file_location("build_climatology", _SCRIPT_PATH)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

apply_quality_mask = _mod.apply_quality_mask
compute_weekly_climatology = _mod.compute_weekly_climatology
N_WEEKS = _mod.N_WEEKS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_dataset(times, chl_values, flag_values):
    """Build a minimal synthetic xarray Dataset mimicking CMEMS L3 output.

    Args:
        times: list of numpy datetime64 strings, e.g. ["2018-01-01", ...]
        chl_values: array of shape (time, lat, lon)
        flag_values: array of shape (time, lat, lon)
    """
    time_coord = np.array(times, dtype="datetime64[ns]")
    lat_coord = np.array([53.0, 53.1])
    lon_coord = np.array([-10.0, -9.9])
    return xr.Dataset(
        {
            "CHL": (["time", "lat", "lon"], np.array(chl_values, dtype=np.float32)),
            "CHL_flags": (["time", "lat", "lon"], np.array(flag_values, dtype=np.int8)),
        },
        coords={"time": time_coord, "lat": lat_coord, "lon": lon_coord},
    )


def _week_of(date_str: str) -> int:
    """Return ISO calendar week number for a date string."""
    import datetime
    d = datetime.date.fromisoformat(date_str)
    return d.isocalendar().week


# ---------------------------------------------------------------------------
# apply_quality_mask
# ---------------------------------------------------------------------------


def test_good_flag_pixels_kept():
    ds = _make_dataset(
        times=["2018-01-08"],
        chl_values=[[[1.0, 2.0], [3.0, 4.0]]],
        flag_values=[[[1, 1], [1, 1]]],
    )
    masked = apply_quality_mask(ds)
    assert float(masked.isel(time=0, lat=0, lon=0)) == pytest.approx(1.0)
    assert not np.isnan(float(masked.isel(time=0, lat=1, lon=1)))


def test_bad_flag_pixels_become_nan():
    ds = _make_dataset(
        times=["2018-01-08"],
        chl_values=[[[1.0, 2.0], [3.0, 4.0]]],
        flag_values=[[[1, 0], [2, 1]]],  # pixels (0,1) and (1,0) are bad
    )
    masked = apply_quality_mask(ds)
    assert float(masked.isel(time=0, lat=0, lon=0)) == pytest.approx(1.0)
    assert np.isnan(float(masked.isel(time=0, lat=0, lon=1)))
    assert np.isnan(float(masked.isel(time=0, lat=1, lon=0)))
    assert float(masked.isel(time=0, lat=1, lon=1)) == pytest.approx(4.0)


def test_all_bad_flags_all_nan():
    ds = _make_dataset(
        times=["2018-01-08"],
        chl_values=[[[5.0, 6.0], [7.0, 8.0]]],
        flag_values=[[[0, 0], [0, 0]]],
    )
    masked = apply_quality_mask(ds)
    assert np.all(np.isnan(masked.values))


# ---------------------------------------------------------------------------
# compute_weekly_climatology — output shape and week coordinate
# ---------------------------------------------------------------------------


def test_output_has_52_weeks():
    # 3 days each in week 1, 10, 52
    dates = ["2018-01-01", "2018-01-02", "2018-01-03",   # week 1
             "2018-03-05", "2018-03-06", "2018-03-07",   # week 10
             "2018-12-24", "2018-12-25", "2018-12-26"]   # week 52
    n = len(dates)
    ds = _make_dataset(
        times=dates,
        chl_values=np.ones((n, 2, 2), dtype=np.float32),
        flag_values=np.ones((n, 2, 2), dtype=np.int8),
    )
    chl = apply_quality_mask(ds)
    clim = compute_weekly_climatology(chl)
    assert clim.sizes["week"] == 52
    assert int(clim.week.values[0]) == 1
    assert int(clim.week.values[-1]) == 52


def test_weekly_mean_values_correct():
    # Week 1: two days with CHL values 2.0 and 4.0 → mean 3.0
    ds = _make_dataset(
        times=["2018-01-01", "2018-01-02"],
        chl_values=[[[2.0, 2.0], [2.0, 2.0]],
                    [[4.0, 4.0], [4.0, 4.0]]],
        flag_values=[[[1, 1], [1, 1]],
                     [[1, 1], [1, 1]]],
    )
    chl = apply_quality_mask(ds)
    clim = compute_weekly_climatology(chl)
    week1_mean = float(clim.sel(week=1).isel(lat=0, lon=0))
    assert week1_mean == pytest.approx(3.0)


def test_bad_pixels_excluded_from_mean():
    # Week 1: day 1 has CHL=2.0 (good), day 2 has CHL=10.0 but flag=0 (bad)
    # Expected mean: 2.0 (bad pixel masked before groupby)
    ds = _make_dataset(
        times=["2018-01-01", "2018-01-02"],
        chl_values=[[[2.0, 2.0], [2.0, 2.0]],
                    [[10.0, 10.0], [10.0, 10.0]]],
        flag_values=[[[1, 1], [1, 1]],
                     [[0, 0], [0, 0]]],
    )
    chl = apply_quality_mask(ds)
    clim = compute_weekly_climatology(chl)
    week1_mean = float(clim.sel(week=1).isel(lat=0, lon=0))
    assert week1_mean == pytest.approx(2.0)


def test_weeks_with_no_data_are_nan():
    # Only provide data for week 10; all other weeks should be NaN
    ds = _make_dataset(
        times=["2018-03-05"],
        chl_values=[[[5.0, 5.0], [5.0, 5.0]]],
        flag_values=[[[1, 1], [1, 1]]],
    )
    chl = apply_quality_mask(ds)
    clim = compute_weekly_climatology(chl)
    assert float(clim.sel(week=10).isel(lat=0, lon=0)) == pytest.approx(5.0)
    # Week 1 had no data → NaN
    assert np.isnan(float(clim.sel(week=1).isel(lat=0, lon=0)))


# ---------------------------------------------------------------------------
# ISO week 53 absorption
# ---------------------------------------------------------------------------


def test_week_53_absorbed_into_week_52():
    """Days in ISO week 53 must contribute to the week-52 climatology."""
    # 2020-12-28 is in ISO week 53 of 2020
    date_w53 = "2020-12-28"
    assert _week_of(date_w53) == 53, "Test assumption: date is in week 53"

    # Provide one week-52 day (value 2.0) and one week-53 day (value 4.0)
    # After absorption the week-52 mean should be (2+4)/2 = 3.0
    date_w52 = "2020-12-21"  # ISO week 52
    assert _week_of(date_w52) == 52, "Test assumption: date is in week 52"

    ds = _make_dataset(
        times=[date_w52, date_w53],
        chl_values=[[[2.0, 2.0], [2.0, 2.0]],
                    [[4.0, 4.0], [4.0, 4.0]]],
        flag_values=[[[1, 1], [1, 1]],
                     [[1, 1], [1, 1]]],
    )
    chl = apply_quality_mask(ds)
    clim = compute_weekly_climatology(chl)

    assert clim.sizes["week"] == 52, "Output must still have exactly 52 weeks"
    week52_mean = float(clim.sel(week=52).isel(lat=0, lon=0))
    assert week52_mean == pytest.approx(3.0), (
        f"Week-53 data should be merged into week 52; expected mean 3.0, got {week52_mean}"
    )


# ---------------------------------------------------------------------------
# main() — credential check
# ---------------------------------------------------------------------------


def test_main_exits_on_missing_credentials(monkeypatch):
    monkeypatch.delenv("CMEMS_USERNAME", raising=False)
    monkeypatch.delenv("CMEMS_PASSWORD", raising=False)
    with pytest.raises(SystemExit) as exc_info:
        _mod.main()
    assert exc_info.value.code != 0


def test_main_exits_if_only_username_set(monkeypatch):
    monkeypatch.setenv("CMEMS_USERNAME", "user")
    monkeypatch.delenv("CMEMS_PASSWORD", raising=False)
    with pytest.raises(SystemExit):
        _mod.main()


def test_main_exits_if_only_password_set(monkeypatch):
    monkeypatch.delenv("CMEMS_USERNAME", raising=False)
    monkeypatch.setenv("CMEMS_PASSWORD", "pass")
    with pytest.raises(SystemExit):
        _mod.main()


# ---------------------------------------------------------------------------
# main() — end-to-end with mocked CMEMS (verifies output NetCDF structure)
# ---------------------------------------------------------------------------


def test_main_writes_netcdf_with_correct_structure(tmp_path, monkeypatch):
    """main() should produce a NetCDF with dimension 'week' (52) and var 'CHL_mean'."""
    monkeypatch.setenv("CMEMS_USERNAME", "test_user")
    monkeypatch.setenv("CMEMS_PASSWORD", "test_pass")

    # Redirect output path to a temp directory
    out_path = tmp_path / "climatology" / "wci_chl_climatology_wk.nc"
    monkeypatch.setattr(_mod, "OUTPUT_PATH", out_path)

    # Build a minimal synthetic dataset covering a few weeks
    dates = (
        ["2018-01-01", "2018-01-02"]  # week 1
        + ["2018-03-05"]              # week 10
        + ["2018-12-24"]              # week 52
    )
    n = len(dates)
    fake_ds = _make_dataset(
        times=dates,
        chl_values=np.full((n, 2, 2), 2.5, dtype=np.float32),
        flag_values=np.ones((n, 2, 2), dtype=np.int8),
    )

    with patch.object(_mod.copernicusmarine, "open_dataset", return_value=fake_ds):
        _mod.main()

    assert out_path.exists(), "Output NetCDF was not created"

    ds_out = xr.open_dataset(out_path)
    assert "CHL_mean" in ds_out, "Variable CHL_mean missing from output"
    assert "week" in ds_out.dims, "Dimension 'week' missing from output"
    assert ds_out.sizes["week"] == 52, f"Expected 52 weeks, got {ds_out.sizes['week']}"
    assert "lat" in ds_out.dims
    assert "lon" in ds_out.dims
    ds_out.close()
