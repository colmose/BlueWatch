"""Tests for run_pipeline.py (T09)."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest
import xarray as xr
from shapely.geometry import box

import run_pipeline as pipeline
from bluewatch.alert_dispatcher import record_alert
from bluewatch.anomaly_engine import ZoneResult
from bluewatch.config import Zone


def make_zone(name: str = "Outer Clew Bay") -> Zone:
    return Zone(
        name=name,
        description="Atlantic-facing monitoring zone",
        polygon=box(-10.0, 53.7, -9.6, 53.9),
        threshold_multiplier=3.0,
        alert_email="ops@example.com",
    )


def make_result(
    name: str = "Outer Clew Bay",
    *,
    status: str = "DATA_AVAILABLE",
    anomaly_ratio: float | None = 3.5,
    zone_avg_chl: float | None = 2.1,
    climatology_mean_chl: float | None = 0.6,
) -> ZoneResult:
    return ZoneResult(
        zone_name=name,
        status=status,  # type: ignore[arg-type]
        anomaly_ratio=anomaly_ratio,
        zone_avg_chl=zone_avg_chl,
        climatology_mean_chl=climatology_mean_chl,
        valid_pixel_count=9,
        total_pixel_count=12,
    )


def make_dataset(observed_date: str = "2026-04-12") -> xr.Dataset:
    return xr.Dataset(coords={"time": [observed_date]})


def test_run_pipeline_logs_to_stdout_and_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    zone = make_zone()
    monkeypatch.setattr(pipeline, "load_zones", lambda config_path: [zone])
    monkeypatch.setattr(pipeline, "fetch_latest_chl", lambda run_date=None: make_dataset())
    monkeypatch.setattr(
        pipeline,
        "compute_zone_results",
        lambda ds, zones, run_date: [make_result()],
    )
    monkeypatch.setattr(
        pipeline,
        "dispatch_anomaly_alert",
        lambda zone, result, **kwargs: True,
    )

    exit_code = pipeline.run_pipeline(run_date=date(2026, 4, 13), log_dir=tmp_path / "logs")

    assert exit_code == 0
    stdout = capsys.readouterr().out.strip()
    entry = json.loads(stdout)
    assert entry["zone_name"] == "Outer Clew Bay"
    assert entry["status"] == "DATA_AVAILABLE"
    assert entry["email_sent"] is True
    assert entry["observed_date"] == "2026-04-12"
    assert entry["climatology_mean_chl"] == pytest.approx(0.6)

    log_path = tmp_path / "logs" / "pipeline_2026-04-13.jsonl"
    file_entry = json.loads(log_path.read_text().strip())
    assert file_entry == entry


def test_run_pipeline_passes_explicit_date_to_ingest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    zone = make_zone()
    captured: dict[str, date | None] = {}

    monkeypatch.setattr(pipeline, "load_zones", lambda config_path: [zone])

    def fake_fetch_latest_chl(run_date: date | None) -> xr.Dataset:
        captured["run_date"] = run_date
        return make_dataset("2026-04-13")

    monkeypatch.setattr(pipeline, "fetch_latest_chl", fake_fetch_latest_chl)
    monkeypatch.setattr(
        pipeline,
        "compute_zone_results",
        lambda ds, zones, run_date: [make_result()],
    )
    monkeypatch.setattr(
        pipeline,
        "dispatch_anomaly_alert",
        lambda zone, result, **kwargs: False,
    )

    pipeline.run_pipeline(run_date=date(2026, 4, 13), log_dir=tmp_path / "logs")

    assert captured["run_date"] == date(2026, 4, 13)


def test_run_pipeline_uses_previous_logs_for_gap_streak(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    zone = make_zone()
    log_dir = tmp_path / "logs"
    log_dir.mkdir(parents=True)

    previous_gap_entries = (
        ("2026-04-12", "2026-04-11"),
        ("2026-04-13", "2026-04-12"),
    )
    for execution_date, observed_date in previous_gap_entries:
        (log_dir / f"pipeline_{execution_date}.jsonl").write_text(
            json.dumps(
                {
                    "zone_name": zone.name,
                    "status": "CLOUD_GAP",
                    "observed_date": observed_date,
                }
            )
            + "\n",
            encoding="utf-8",
        )

    monkeypatch.setattr(pipeline, "load_zones", lambda config_path: [zone])
    monkeypatch.setattr(
        pipeline,
        "fetch_latest_chl",
        lambda run_date=None: make_dataset("2026-04-13"),
    )
    monkeypatch.setattr(
        pipeline,
        "compute_zone_results",
        lambda ds, zones, run_date: [
            make_result(status="CLOUD_GAP", anomaly_ratio=None, zone_avg_chl=None)
        ],
    )

    captured: dict[str, int] = {}

    def fake_gap_dispatch(
        zone: Zone,
        *,
        observed_date: date,
        consecutive_gap_days: int,
        **kwargs: object,
    ) -> bool:
        captured["observed_date"] = observed_date.toordinal()
        captured["consecutive_gap_days"] = consecutive_gap_days
        return True

    monkeypatch.setattr(pipeline, "dispatch_gap_notification", fake_gap_dispatch)

    pipeline.run_pipeline(run_date=date(2026, 4, 13), log_dir=log_dir)

    entry = json.loads(capsys.readouterr().out.strip())
    assert captured["observed_date"] == date(2026, 4, 13).toordinal()
    assert captured["consecutive_gap_days"] == 3
    assert entry["consecutive_gap_days"] == 3
    assert entry["email_sent"] is True


def test_resolve_observed_zone_entry_prefers_live_run_log(tmp_path: Path) -> None:
    zone = make_zone()
    log_dir = tmp_path / "logs"
    log_dir.mkdir(parents=True)

    observed_date = date(2026, 4, 12)
    replay_entry = {
        "zone_name": zone.name,
        "status": "CLOUD_GAP",
        "observed_date": observed_date.isoformat(),
        "run_date": observed_date.isoformat(),
    }
    live_entry = {
        "zone_name": zone.name,
        "status": "CLOUD_GAP",
        "observed_date": observed_date.isoformat(),
        "run_date": date(2026, 4, 13).isoformat(),
    }

    (log_dir / "pipeline_2026-04-12.jsonl").write_text(
        json.dumps(replay_entry) + "\n",
        encoding="utf-8",
    )
    (log_dir / "pipeline_2026-04-13.jsonl").write_text(
        json.dumps(live_entry) + "\n",
        encoding="utf-8",
    )

    resolved = pipeline.resolve_observed_zone_entry(log_dir, zone.name, observed_date)

    assert resolved == live_entry


def test_run_pipeline_uses_observed_date_for_replay_dedup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    zone = make_zone()
    database_url = f"sqlite:///{tmp_path / 'alert_log.db'}"

    monkeypatch.setattr(pipeline, "load_zones", lambda config_path: [zone])
    monkeypatch.setattr(
        pipeline,
        "fetch_latest_chl",
        lambda run_date=None: make_dataset("2026-04-12"),
    )
    monkeypatch.setattr(
        pipeline,
        "compute_zone_results",
        lambda ds, zones, run_date: [make_result()],
    )

    record_alert(zone.name, date(2026, 4, 12), "anomaly", database_url=database_url)

    pipeline.run_pipeline(
        run_date=date(2026, 4, 13),
        log_dir=tmp_path / "logs",
        database_url=database_url,
    )

    entry = json.loads(capsys.readouterr().out.strip())
    assert entry["observed_date"] == "2026-04-12"
    assert entry["email_sent"] is False


def test_run_pipeline_logs_error_then_reraises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    zone = make_zone()
    monkeypatch.setattr(pipeline, "load_zones", lambda config_path: [zone])
    monkeypatch.setattr(pipeline, "fetch_latest_chl", lambda run_date=None: make_dataset())
    monkeypatch.setattr(
        pipeline,
        "compute_zone_results",
        lambda ds, zones, run_date: [make_result()],
    )
    monkeypatch.setattr(
        pipeline,
        "dispatch_anomaly_alert",
        lambda zone, result, **kwargs: (_ for _ in ()).throw(RuntimeError("resend down")),
    )

    with pytest.raises(RuntimeError, match="resend down"):
        pipeline.run_pipeline(run_date=date(2026, 4, 13), log_dir=tmp_path / "logs")

    entry = json.loads(capsys.readouterr().out.strip())
    assert entry["email_sent"] is False
    assert entry["error"] == "resend down"


def test_main_returns_one_on_unhandled_exception(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        pipeline,
        "parse_args",
        lambda argv=None: type(
            "Args",
            (),
            {
                "date": None,
                "config": Path("config/zones.yaml"),
                "log_dir": Path("logs"),
                "database_url": None,
            },
        )(),
    )
    monkeypatch.setattr(
        pipeline,
        "run_pipeline",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    assert pipeline.main() == 1
    assert "boom" in capsys.readouterr().err


def test_parse_args_accepts_cli_overrides(tmp_path: Path) -> None:
    config_path = tmp_path / "zones.yaml"
    log_dir = tmp_path / "logs"
    database_url = "sqlite:////tmp/bluewatch-alerts.db"

    args = pipeline.parse_args(
        [
            "--date",
            "2026-04-13",
            "--config",
            str(config_path),
            "--log-dir",
            str(log_dir),
            "--database-url",
            database_url,
        ]
    )

    assert args.date == date(2026, 4, 13)
    assert args.config == config_path
    assert args.log_dir == log_dir
    assert args.database_url == database_url
