"""Tests for bluewatch/ingest.py (T05)."""

from __future__ import annotations

import datetime
from pathlib import Path
from typing import cast

import numpy as np
import numpy.typing as npt
import pytest
import xarray as xr

import bluewatch.env as env_mod
import bluewatch.ingest as ingest_mod
from bluewatch.env import get_env
from bluewatch.ingest import CMEMSDownloadError, apply_quality_filter, fetch_latest_chl


def make_dataset(
    chl_values: npt.ArrayLike,
    flag_values: npt.ArrayLike,
    date: str = "2024-01-15",
) -> xr.Dataset:
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


def write_fake_nc(out_file: Path, date: str = "2024-01-15") -> None:
    dataset = make_dataset(
        chl_values=[[[1.0, 2.0], [3.0, 4.0]]],
        flag_values=[[[1, 0], [1, 1]]],
        date=date,
    )
    out_file.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_netcdf(out_file, engine="h5netcdf")


def test_apply_quality_filter_retains_good_pixels() -> None:
    dataset = make_dataset(
        chl_values=[[[1.0, 2.0], [3.0, 4.0]]],
        flag_values=[[[1, 1], [1, 1]]],
    )

    result = apply_quality_filter(dataset)

    assert float(result["CHL"].isel(time=0, lat=0, lon=0)) == pytest.approx(1.0)
    assert not np.any(np.isnan(result["CHL"].values))


def test_apply_quality_filter_masks_bad_pixels() -> None:
    dataset = make_dataset(
        chl_values=[[[1.0, 2.0], [3.0, 4.0]]],
        flag_values=[[[1, 0], [2, 1]]],
    )

    result = apply_quality_filter(dataset)

    assert np.isnan(float(result["CHL"].isel(time=0, lat=0, lon=1)))
    assert np.isnan(float(result["CHL"].isel(time=0, lat=1, lon=0)))


def test_apply_quality_filter_supports_current_flags_schema() -> None:
    dataset = xr.Dataset(
        {
            "CHL": (["time", "lat", "lon"], np.array([[[1.0, 2.0], [3.0, 4.0]]], dtype=np.float32)),
            "flags": (["time", "lat", "lon"], np.array([[[0, 1], [0, 1]]], dtype=np.int8)),
        },
        coords={
            "time": np.array(["2024-01-15"], dtype="datetime64[ns]"),
            "lat": np.array([53.0, 53.1]),
            "lon": np.array([-10.0, -9.9]),
        },
    )
    dataset["flags"].attrs.update({"flag_masks": 1, "flag_meanings": "LAND"})

    result = apply_quality_filter(dataset)

    assert float(result["CHL"].isel(time=0, lat=0, lon=0)) == pytest.approx(1.0)
    assert np.isnan(float(result["CHL"].isel(time=0, lat=0, lon=1)))


def test_fetch_latest_chl_exits_on_missing_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CMEMS_USERNAME", "")
    monkeypatch.setenv("CMEMS_PASSWORD", "")

    with pytest.raises(SystemExit, match="CMEMS_USERNAME"):
        fetch_latest_chl()


def test_fetch_latest_chl_exits_when_no_env_or_dotenv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CMEMS_USERNAME", raising=False)
    monkeypatch.delenv("CMEMS_PASSWORD", raising=False)
    monkeypatch.setattr(env_mod, "find_dotenv", lambda: None)

    with pytest.raises(SystemExit, match="CMEMS_USERNAME"):
        fetch_latest_chl()


def test_get_env_reads_plain_dotenv_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CMEMS_USERNAME", raising=False)
    monkeypatch.delenv("CMEMS_PASSWORD", raising=False)
    (tmp_path / ".env").write_text(
        "CMEMS_USERNAME=dotenv-user\nCMEMS_PASSWORD=dotenv-pass\n",
        encoding="utf-8",
    )

    assert get_env("CMEMS_USERNAME") == "dotenv-user"
    assert get_env("CMEMS_PASSWORD") == "dotenv-pass"


def test_get_env_reads_export_prefixed_dotenv_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CMEMS_USERNAME", raising=False)
    monkeypatch.delenv("CMEMS_PASSWORD", raising=False)
    (tmp_path / ".env").write_text(
        "export CMEMS_USERNAME=dotenv-user\nexport CMEMS_PASSWORD='dotenv-pass'\n",
        encoding="utf-8",
    )

    assert get_env("CMEMS_USERNAME") == "dotenv-user"
    assert get_env("CMEMS_PASSWORD") == "dotenv-pass"


def test_get_env_prefers_exported_env_over_dotenv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CMEMS_USERNAME", "exported-user")
    monkeypatch.setenv("CMEMS_PASSWORD", "exported-pass")
    (tmp_path / ".env").write_text(
        "CMEMS_USERNAME=dotenv-user\nCMEMS_PASSWORD=dotenv-pass\n",
        encoding="utf-8",
    )

    assert get_env("CMEMS_USERNAME") == "exported-user"
    assert get_env("CMEMS_PASSWORD") == "exported-pass"


def test_get_env_prefers_empty_exported_env_over_dotenv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CMEMS_USERNAME", "")
    monkeypatch.setenv("CMEMS_PASSWORD", "")
    (tmp_path / ".env").write_text(
        "CMEMS_USERNAME=dotenv-user\nCMEMS_PASSWORD=dotenv-pass\n",
        encoding="utf-8",
    )

    assert get_env("CMEMS_USERNAME") == ""
    assert get_env("CMEMS_PASSWORD") == ""


def test_fetch_latest_chl_uses_dotenv_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CMEMS_USERNAME", raising=False)
    monkeypatch.delenv("CMEMS_PASSWORD", raising=False)
    (tmp_path / ".env").write_text(
        "CMEMS_USERNAME=dotenv-user\nCMEMS_PASSWORD=dotenv-pass\n",
        encoding="utf-8",
    )

    captured: dict[str, str] = {}

    def fake_download(username: str, password: str, date_str: str, out_file: Path) -> None:
        captured["username"] = username
        captured["password"] = password
        write_fake_nc(out_file, date_str)

    monkeypatch.setattr(ingest_mod, "_download_subset", fake_download)

    fetch_latest_chl(date=datetime.date(2024, 1, 15))

    assert captured == {"username": "dotenv-user", "password": "dotenv-pass"}


def test_fetch_latest_chl_returns_filtered_dataset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CMEMS_USERNAME", "user")
    monkeypatch.setenv("CMEMS_PASSWORD", "pass")

    def fake_download(username: str, password: str, date_str: str, out_file: Path) -> None:
        del username, password
        write_fake_nc(out_file, date_str)

    monkeypatch.setattr(ingest_mod, "_download_subset", fake_download)

    result = fetch_latest_chl(date=datetime.date(2024, 1, 15))

    assert np.isnan(float(result["CHL"].isel(time=0, lat=0, lon=1)))
    assert float(result["CHL"].isel(time=0, lat=1, lon=0)) == pytest.approx(3.0)


def test_fetch_latest_chl_falls_back_one_extra_day(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CMEMS_USERNAME", "user")
    monkeypatch.setenv("CMEMS_PASSWORD", "pass")
    attempted_dates: list[str] = []

    class FrozenDate(datetime.date):
        @classmethod
        def today(cls) -> "FrozenDate":
            return cls(2024, 1, 17)

    def fake_download(username: str, password: str, date_str: str, out_file: Path) -> None:
        del username, password
        attempted_dates.append(date_str)
        if date_str == "2024-01-16":
            raise CMEMSDownloadError("ERROR: CMEMS download failed for 2024-01-16: unavailable")
        write_fake_nc(out_file, date_str)

    monkeypatch.setattr("bluewatch.ingest.datetime.date", FrozenDate)
    monkeypatch.setattr(ingest_mod, "_download_subset", fake_download)

    result = fetch_latest_chl()

    assert attempted_dates == ["2024-01-16", "2024-01-15"]
    assert str(result["time"].values[0]) == "2024-01-15T00:00:00.000000000"


def test_download_subset_does_not_pass_force_download(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    def fake_subset(**kwargs: object) -> None:
        captured.update(kwargs)
        output_path = Path(str(kwargs["output_directory"])) / str(kwargs["output_filename"])
        write_fake_nc(output_path, date=str(kwargs["start_datetime"]))

    monkeypatch.setattr(ingest_mod.copernicusmarine, "subset", fake_subset)

    ingest_mod._download_subset("user", "pass", "2024-01-15", tmp_path / "chl_nrt.nc")

    assert "force_download" not in captured


def test_download_subset_falls_back_to_current_quality_variable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    attempted_variables: list[list[str]] = []

    def fake_subset(**kwargs: object) -> None:
        variables = list(cast(list[str], kwargs["variables"]))
        attempted_variables.append(variables)
        if variables == ["CHL", "CHL_flags"]:
            raise RuntimeError(
                "The variable 'CHL_flags' is neither a variable or a standard "
                "name in the dataset."
            )
        output_path = Path(str(kwargs["output_directory"])) / str(kwargs["output_filename"])
        write_fake_nc(output_path, date=str(kwargs["start_datetime"]))

    monkeypatch.setattr(ingest_mod.copernicusmarine, "subset", fake_subset)

    ingest_mod._download_subset("user", "pass", "2024-01-15", tmp_path / "chl_nrt.nc")

    assert attempted_variables == [["CHL", "CHL_flags"], ["CHL", "flags"]]
