"""Subprocess end-to-end tests for the BlueWatch CLI."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "e2e"
CONFIG_PATH = FIXTURE_DIR / "zones.yaml"
INVALID_CONFIG_PATH = FIXTURE_DIR / "zones_too_small.yaml"


def _python_path_with_sitecustomize() -> str:
    existing = os.environ.get("PYTHONPATH")
    parts = [str(FIXTURE_DIR), str(REPO_ROOT)]
    if existing:
        parts.append(existing)
    return os.pathsep.join(parts)


def _base_env(tmp_path: Path, *, chl_fixture: str, email_mode: str = "success") -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "PYTHONPATH": _python_path_with_sitecustomize(),
            "BLUEWATCH_E2E_CHL_FIXTURE": str(FIXTURE_DIR / chl_fixture),
            "BLUEWATCH_E2E_CLIM_FIXTURE": str(FIXTURE_DIR / "wci_chl_climatology_wk.nc"),
            "BLUEWATCH_E2E_MASK_FIXTURE": str(FIXTURE_DIR / "turbid_mask.geojson"),
            "BLUEWATCH_E2E_EMAIL_CAPTURE": str(tmp_path / "captured_emails.jsonl"),
            "BLUEWATCH_E2E_EMAIL_MODE": email_mode,
            "CMEMS_USERNAME": "fixture-user",
            "CMEMS_PASSWORD": "fixture-pass",
            "RESEND_API_KEY": "re_test",
        }
    )
    return env


def _database_url(db_path: Path) -> str:
    return f"sqlite://{db_path}"


def _run_pipeline(
    args: list[str],
    *,
    env: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "run_pipeline.py", *args],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def _read_email_capture(capture_path: Path) -> list[dict[str, object]]:
    if not capture_path.exists():
        return []

    return [
        json.loads(line)
        for line in capture_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_cli_e2e_anomaly_alert_writes_expected_log_and_email(tmp_path: Path) -> None:
    log_dir = tmp_path / "logs"
    capture_path = tmp_path / "captured_emails.jsonl"
    database_url = _database_url(tmp_path / "alert_log.db")
    env = _base_env(tmp_path, chl_fixture="chl_threshold_breach.nc")

    result = _run_pipeline(
        [
            "--date",
            "2026-04-13",
            "--config",
            str(CONFIG_PATH),
            "--log-dir",
            str(log_dir),
            "--database-url",
            database_url,
        ],
        env=env,
    )

    assert result.returncode == 0, result.stderr
    stdout_lines = result.stdout.strip().splitlines()
    assert len(stdout_lines) == 1

    entry = json.loads(stdout_lines[0])
    assert entry["status"] == "DATA_AVAILABLE"
    assert entry["observed_date"] == "2026-04-13"
    assert entry["anomaly_ratio"] == pytest.approx(4.0)
    assert entry["zone_avg_chl"] == pytest.approx(8.0)
    assert entry["climatology_mean_chl"] == pytest.approx(2.0)
    assert entry["valid_pixel_count"] == 8
    assert entry["total_pixel_count"] == 9
    assert entry["email_sent"] is True

    log_path = log_dir / "pipeline_2026-04-13.jsonl"
    assert log_path.exists()
    assert json.loads(log_path.read_text(encoding="utf-8").strip()) == entry

    emails = _read_email_capture(capture_path)
    assert len(emails) == 1
    payload = emails[0]["payload"]
    assert isinstance(payload, dict)
    assert payload["subject"] == "[BlueWatch] Chl-a anomaly alert - Fixture Bay - 2026-04-13"
    assert "Fixture Bay" in payload["text"]
    assert "2026-04-13" in payload["text"]
    assert "8.0000" in payload["text"]
    assert "4.00x" in payload["text"]


def test_cli_e2e_deduplicates_same_day_alert(tmp_path: Path) -> None:
    log_dir = tmp_path / "logs"
    capture_path = tmp_path / "captured_emails.jsonl"
    database_url = _database_url(tmp_path / "alert_log.db")
    env = _base_env(tmp_path, chl_fixture="chl_threshold_breach.nc")
    args = [
        "--date",
        "2026-04-13",
        "--config",
        str(CONFIG_PATH),
        "--log-dir",
        str(log_dir),
        "--database-url",
        database_url,
    ]

    first = _run_pipeline(args, env=env)
    second = _run_pipeline(args, env=env)

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert json.loads(first.stdout.strip())["email_sent"] is True
    assert json.loads(second.stdout.strip())["email_sent"] is False

    emails = _read_email_capture(capture_path)
    assert len(emails) == 1

    log_path = log_dir / "pipeline_2026-04-13.jsonl"
    log_entries = [
        json.loads(line)
        for line in log_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(log_entries) == 2


def test_cli_e2e_gap_notification_after_three_days(tmp_path: Path) -> None:
    log_dir = tmp_path / "logs"
    capture_path = tmp_path / "captured_emails.jsonl"
    database_url = _database_url(tmp_path / "alert_log.db")
    env = _base_env(tmp_path, chl_fixture="chl_cloud_gap.nc")

    entries: list[dict[str, object]] = []
    for run_date in ("2026-04-11", "2026-04-12", "2026-04-13"):
        result = _run_pipeline(
            [
                "--date",
                run_date,
                "--config",
                str(CONFIG_PATH),
                "--log-dir",
                str(log_dir),
                "--database-url",
                database_url,
            ],
            env=env,
        )
        assert result.returncode == 0, result.stderr
        entries.append(json.loads(result.stdout.strip()))

    assert [entry["consecutive_gap_days"] for entry in entries] == [1, 2, 3]
    assert [entry["email_sent"] for entry in entries] == [False, False, True]
    assert all(entry["status"] == "CLOUD_GAP" for entry in entries)

    emails = _read_email_capture(capture_path)
    assert len(emails) == 1
    payload = emails[0]["payload"]
    assert isinstance(payload, dict)
    assert payload["subject"] == "[BlueWatch] Data gap notification - Fixture Bay - 2026-04-13"
    assert "Consecutive gaps:  3 days" in payload["text"]


def test_cli_e2e_fails_loudly_without_cmems_credentials(tmp_path: Path) -> None:
    env = _base_env(tmp_path, chl_fixture="chl_threshold_breach.nc")
    env["CMEMS_USERNAME"] = ""
    env["CMEMS_PASSWORD"] = ""

    result = _run_pipeline(
        [
            "--date",
            "2026-04-13",
            "--config",
            str(CONFIG_PATH),
            "--log-dir",
            str(tmp_path / "logs"),
        ],
        env=env,
    )

    assert result.returncode != 0
    assert "CMEMS_USERNAME and CMEMS_PASSWORD" in result.stderr
    assert result.stdout == ""


def test_cli_e2e_logs_and_exits_non_zero_on_email_failure(tmp_path: Path) -> None:
    log_dir = tmp_path / "logs"
    env = _base_env(tmp_path, chl_fixture="chl_threshold_breach.nc", email_mode="fail")

    result = _run_pipeline(
        [
            "--date",
            "2026-04-13",
            "--config",
            str(CONFIG_PATH),
            "--log-dir",
            str(log_dir),
            "--database-url",
            _database_url(tmp_path / "alert_log.db"),
        ],
        env=env,
    )

    assert result.returncode == 1
    assert "ERROR: pipeline failed: Resend API failure" in result.stderr

    entry = json.loads(result.stdout.strip())
    assert entry["email_sent"] is False
    assert "Resend API failure" in entry["error"]

    log_path = log_dir / "pipeline_2026-04-13.jsonl"
    assert json.loads(log_path.read_text(encoding="utf-8").strip()) == entry


def test_cli_e2e_rejects_zone_config_below_minimum_area(tmp_path: Path) -> None:
    env = _base_env(tmp_path, chl_fixture="chl_threshold_breach.nc")

    result = _run_pipeline(
        [
            "--date",
            "2026-04-13",
            "--config",
            str(INVALID_CONFIG_PATH),
            "--log-dir",
            str(tmp_path / "logs"),
        ],
        env=env,
    )

    assert result.returncode != 0
    assert "below the 1 km² minimum required" in result.stderr
    assert result.stdout == ""
