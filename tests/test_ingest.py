"""Tests for bluewatch/ingest.py (T05)."""

import datetime
from pathlib import Path

import numpy as np
import numpy.typing as npt
import pytest
import xarray as xr

import bluewatch.ingest as ingest_mod
from bluewatch.ingest import apply_quality_filter, fetch_latest_chl

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_chl_dataset(
    chl_values: npt.ArrayLike,
    flag_values: npt.ArrayLike,
    date: str = "2024-01-15",
) -> xr.Dataset:
    """Build a minimal synthetic Dataset mimicking CMEMS L3 NRT output."""
    return xr.Dataset(
        {
            "CHL": (["time", "lat", "lon"], np.array(chl_values, dtype=np.float32)),
            "CHL_flags": (
                ["time", "lat", "lon"],
                np.array(flag_values, dtype=np.int8),
            ),
        },
        coords={
            "time": np.array([date], dtype="datetime64[ns]"),
            "lat": np.array([53.0, 53.1]),
            "lon": np.array([-10.0, -9.9]),
        },
    )


def _write_fake_nc(out_file: Path, date: str = "2024-01-15") -> None:
    """Write a minimal CHL NetCDF to out_file for download mocking."""
    ds = _make_chl_dataset(
        chl_values=[[[1.0, 2.0], [3.0, 4.0]]],
        flag_values=[[[1, 0], [1, 1]]],
        date=date,
    )
    out_file.parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(out_file, engine="h5netcdf")


# ---------------------------------------------------------------------------
# apply_quality_filter
# ---------------------------------------------------------------------------


def test_good_pixels_retained() -> None:
    ds = _make_chl_dataset(
        chl_values=[[[1.0, 2.0], [3.0, 4.0]]],
        flag_values=[[[1, 1], [1, 1]]],
    )
    result = apply_quality_filter(ds)
    assert not np.any(np.isnan(result["CHL"].values))
    assert float(result["CHL"].isel(time=0, lat=0, lon=0)) == pytest.approx(1.0)


def test_bad_pixels_become_nan() -> None:
    ds = _make_chl_dataset(
        chl_values=[[[1.0, 2.0], [3.0, 4.0]]],
        flag_values=[[[1, 0], [2, 1]]],
    )
    result = apply_quality_filter(ds)
    assert float(result["CHL"].isel(time=0, lat=0, lon=0)) == pytest.approx(1.0)
    assert np.isnan(float(result["CHL"].isel(time=0, lat=0, lon=1)))
    assert np.isnan(float(result["CHL"].isel(time=0, lat=1, lon=0)))
    assert float(result["CHL"].isel(time=0, lat=1, lon=1)) == pytest.approx(4.0)


def test_all_bad_flags_produce_all_nan() -> None:
    ds = _make_chl_dataset(
        chl_values=[[[5.0, 6.0], [7.0, 8.0]]],
        flag_values=[[[0, 2], [3, 4]]],
    )
    result = apply_quality_filter(ds)
    assert np.all(np.isnan(result["CHL"].values))


def test_quality_filter_does_not_mutate_input() -> None:
    ds = _make_chl_dataset(
        chl_values=[[[1.0, 2.0], [3.0, 4.0]]],
        flag_values=[[[1, 0], [1, 1]]],
    )
    original_val = float(ds["CHL"].isel(time=0, lat=0, lon=1))
    apply_quality_filter(ds)
    assert float(ds["CHL"].isel(time=0, lat=0, lon=1)) == pytest.approx(original_val)


def test_chl_flags_variable_preserved_after_filter() -> None:
    ds = _make_chl_dataset(
        chl_values=[[[1.0, 2.0], [3.0, 4.0]]],
        flag_values=[[[1, 0], [1, 1]]],
    )
    result = apply_quality_filter(ds)
    assert "CHL_flags" in result.data_vars


# ---------------------------------------------------------------------------
# fetch_latest_chl — credential checks (AC-09)
# ---------------------------------------------------------------------------


def test_fetch_exits_on_missing_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CMEMS_USERNAME", raising=False)
    monkeypatch.delenv("CMEMS_PASSWORD", raising=False)
    with pytest.raises(SystemExit) as exc_info:
        fetch_latest_chl()
    assert exc_info.value.code != 0


def test_fetch_exits_if_only_username_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CMEMS_USERNAME", "user")
    monkeypatch.delenv("CMEMS_PASSWORD", raising=False)
    with pytest.raises(SystemExit) as exc_info:
        fetch_latest_chl()
    assert exc_info.value.code != 0


def test_fetch_exits_if_only_password_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CMEMS_USERNAME", raising=False)
    monkeypatch.setenv("CMEMS_PASSWORD", "pass")
    with pytest.raises(SystemExit) as exc_info:
        fetch_latest_chl()
    assert exc_info.value.code != 0


# ---------------------------------------------------------------------------
# fetch_latest_chl — end-to-end with mocked download
# ---------------------------------------------------------------------------


def test_fetch_returns_quality_filtered_dataset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CMEMS_USERNAME", "user")
    monkeypatch.setenv("CMEMS_PASSWORD", "pass")

    def fake_download(username: str, password: str, date_str: str, out_file: Path) -> None:
        del username, password
        _write_fake_nc(out_file, date=date_str)

    monkeypatch.setattr(ingest_mod, "_download_subset", fake_download)
    result = fetch_latest_chl(date=datetime.date(2024, 1, 15))

    assert "CHL" in result.data_vars
    assert "CHL_flags" in result.data_vars
    # pixel (lat=0, lon=1) has flag=0 → should be NaN
    assert np.isnan(float(result["CHL"].isel(time=0, lat=0, lon=1)))
    # pixel (lat=0, lon=0) has flag=1 → should be present
    assert float(result["CHL"].isel(time=0, lat=0, lon=0)) == pytest.approx(1.0)


def test_fetch_passes_correct_date_to_download(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CMEMS_USERNAME", "user")
    monkeypatch.setenv("CMEMS_PASSWORD", "pass")

    captured: dict[str, str] = {}

    def fake_download(username: str, password: str, date_str: str, out_file: Path) -> None:
        del username, password
        captured["date_str"] = date_str
        _write_fake_nc(out_file, date=date_str)

    monkeypatch.setattr(ingest_mod, "_download_subset", fake_download)
    fetch_latest_chl(date=datetime.date(2024, 3, 20))

    assert captured["date_str"] == "2024-03-20"


def test_fetch_exits_when_download_fails_to_create_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If _download_subset exits loudly (as it should), fetch_latest_chl propagates it."""
    monkeypatch.setenv("CMEMS_USERNAME", "user")
    monkeypatch.setenv("CMEMS_PASSWORD", "pass")

    def failing_download(
        username: str,
        password: str,
        date_str: str,
        out_file: Path,
    ) -> None:
        # Real _download_subset calls sys.exit on failure; simulate that.
        del username, password, date_str, out_file
        raise SystemExit("ERROR: CMEMS download failed")

    monkeypatch.setattr(ingest_mod, "_download_subset", failing_download)
    with pytest.raises(SystemExit) as exc_info:
        fetch_latest_chl(date=datetime.date(2024, 1, 15))
    assert exc_info.value.code != 0
