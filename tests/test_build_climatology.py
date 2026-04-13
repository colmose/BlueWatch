"""Tests for scripts/build_climatology.py (T03)."""

import importlib.util
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Protocol, cast
from unittest.mock import patch

import numpy as np
import numpy.typing as npt
import pytest
import xarray as xr

# ---------------------------------------------------------------------------
# Import the script module (scripts/ is not a package, so load explicitly)
# ---------------------------------------------------------------------------

_SCRIPT_PATH = Path(__file__).parent.parent / "scripts" / "build_climatology.py"
_spec = importlib.util.spec_from_file_location("build_climatology", _SCRIPT_PATH)
assert _spec is not None
assert _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)


class _ComputeWeeklyClimatology(Protocol):
    def __call__(self, chl: xr.DataArray, n_weeks: int | None = None) -> xr.DataArray: ...

apply_quality_mask = cast(Callable[[xr.Dataset], xr.DataArray], _mod.apply_quality_mask)
compute_weekly_climatology = cast(_ComputeWeeklyClimatology, _mod.compute_weekly_climatology)
main = cast(Callable[[], None], _mod.main)
DEFAULT_N_WEEKS = cast(int, _mod.DEFAULT_N_WEEKS)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_dataset(
    times: Sequence[str],
    chl_values: npt.ArrayLike,
    flag_values: npt.ArrayLike,
) -> xr.Dataset:
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


def test_good_flag_pixels_kept() -> None:
    ds = _make_dataset(
        times=["2018-01-08"],
        chl_values=[[[1.0, 2.0], [3.0, 4.0]]],
        flag_values=[[[1, 1], [1, 1]]],
    )
    masked = apply_quality_mask(ds)
    assert float(masked.isel(time=0, lat=0, lon=0)) == pytest.approx(1.0)
    assert not np.isnan(float(masked.isel(time=0, lat=1, lon=1)))


def test_bad_flag_pixels_become_nan() -> None:
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


def test_all_bad_flags_all_nan() -> None:
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


def test_output_has_52_weeks_by_default() -> None:
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
    assert clim.sizes["week"] == DEFAULT_N_WEEKS
    assert int(clim.week.values[0]) == 1
    assert int(clim.week.values[-1]) == DEFAULT_N_WEEKS


def test_weekly_mean_values_correct() -> None:
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


def test_bad_pixels_excluded_from_mean() -> None:
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


def test_weeks_with_no_data_are_nan() -> None:
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
# ISO week 53 preservation
# ---------------------------------------------------------------------------


def test_week_53_preserved_as_separate_slice() -> None:
    """Days in ISO week 53 must remain in a distinct climatology slice."""
    # 2020-12-28 is in ISO week 53 of 2020
    date_w53 = "2020-12-28"
    assert _week_of(date_w53) == 53, "Test assumption: date is in week 53"

    # Provide one week-52 day (value 2.0) and one week-53 day (value 4.0).
    # They must remain in separate weekly bins.
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

    assert clim.sizes["week"] == 53, "Output must expand to include week 53 when present"
    week52_mean = float(clim.sel(week=52).isel(lat=0, lon=0))
    week53_mean = float(clim.sel(week=53).isel(lat=0, lon=0))
    assert week52_mean == pytest.approx(2.0)
    assert week53_mean == pytest.approx(4.0)


# ---------------------------------------------------------------------------
# main() — credential check
# ---------------------------------------------------------------------------


def test_main_exits_on_missing_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CMEMS_USERNAME", raising=False)
    monkeypatch.delenv("CMEMS_PASSWORD", raising=False)
    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code != 0


def test_main_exits_if_only_username_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CMEMS_USERNAME", "user")
    monkeypatch.delenv("CMEMS_PASSWORD", raising=False)
    with pytest.raises(SystemExit):
        main()


def test_main_exits_if_only_password_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CMEMS_USERNAME", raising=False)
    monkeypatch.setenv("CMEMS_PASSWORD", "pass")
    with pytest.raises(SystemExit):
        main()


# ---------------------------------------------------------------------------
# main() — end-to-end with mocked CMEMS (verifies output NetCDF structure)
# ---------------------------------------------------------------------------


def test_main_writes_netcdf_with_correct_structure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """main() should produce a NetCDF with 52 weeks when no ISO week 53 is present."""
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
        main()

    assert out_path.exists(), "Output NetCDF was not created"
    out_ds = xr.open_dataset(out_path)
    assert out_ds.sizes["week"] == DEFAULT_N_WEEKS
    assert int(out_ds.week.values[-1]) == DEFAULT_N_WEEKS
    assert "CHL_mean" in out_ds.data_vars


def test_main_writes_week_53_slice_when_present(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """main() should preserve week 53 when the source data contains it."""
    monkeypatch.setenv("CMEMS_USERNAME", "test_user")
    monkeypatch.setenv("CMEMS_PASSWORD", "test_pass")
    out_path = tmp_path / "climatology" / "wci_chl_climatology_wk.nc"
    monkeypatch.setattr(_mod, "OUTPUT_PATH", out_path)

    fake_ds = _make_dataset(
        times=["2020-12-21", "2020-12-28"],
        chl_values=[[[2.0, 2.0], [2.0, 2.0]], [[4.0, 4.0], [4.0, 4.0]]],
        flag_values=[[[1, 1], [1, 1]], [[1, 1], [1, 1]]],
    )

    with patch.object(_mod.copernicusmarine, "open_dataset", return_value=fake_ds):
        main()

    out_ds = xr.open_dataset(out_path)
    assert out_ds.sizes["week"] == 53
    assert float(out_ds["CHL_mean"].sel(week=52).isel(lat=0, lon=0)) == pytest.approx(2.0)
    assert float(out_ds["CHL_mean"].sel(week=53).isel(lat=0, lon=0)) == pytest.approx(4.0)


def test_main_exits_on_empty_dataset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """main() must exit loudly when CMEMS returns 0 time steps."""
    monkeypatch.setenv("CMEMS_USERNAME", "test_user")
    monkeypatch.setenv("CMEMS_PASSWORD", "test_pass")
    monkeypatch.setattr(_mod, "OUTPUT_PATH", tmp_path / "clim.nc")

    empty_ds = _make_dataset(
        times=[],
        chl_values=np.zeros((0, 2, 2)),
        flag_values=np.zeros((0, 2, 2)),
    )
    with patch.object(_mod.copernicusmarine, "open_dataset", return_value=empty_ds):
        with pytest.raises(SystemExit) as exc_info:
            main()
    assert exc_info.value.code != 0
