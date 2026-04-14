#!/usr/bin/env python3
"""BlueWatch pipeline entry point (T09)."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import xarray as xr

from bluewatch.alert_dispatcher import (
    GAP_DAYS_THRESHOLD,
    dispatch_anomaly_alert,
    dispatch_gap_notification,
)
from bluewatch.anomaly_engine import ZoneResult, compute_zone_results
from bluewatch.config import CONFIG_PATH, Zone, load_zones
from bluewatch.ingest import fetch_latest_chl

LOG_DIR = Path(__file__).parent / "logs"


def run_pipeline(
    run_date: dt.date | None = None,
    *,
    config_path: Path = CONFIG_PATH,
    log_dir: Path = LOG_DIR,
    database_url: str | None = None,
) -> int:
    """Run the daily BlueWatch pipeline and emit one JSON line per zone."""
    execution_date = run_date or dt.date.today()
    run_timestamp = dt.datetime.now(dt.UTC).isoformat()

    zones = load_zones(config_path)
    chl_ds = fetch_latest_chl(run_date)
    observation_date = extract_dataset_date(chl_ds)
    results = compute_zone_results(chl_ds, zones, observation_date)
    ensure_zone_results_match(zones, results)

    log_path = build_log_path(log_dir, execution_date)

    for zone, result in zip(zones, results):
        consecutive_gap_days: int | None = None
        email_sent = False
        error: str | None = None

        try:
            if result.status == "DATA_AVAILABLE":
                email_sent = dispatch_anomaly_alert(
                    zone,
                    result,
                    observed_date=observation_date,
                    database_url=database_url,
                )
            else:
                consecutive_gap_days = (
                    count_previous_gap_days(zone.name, observation_date, log_dir) + 1
                )
                email_sent = dispatch_gap_notification(
                    zone,
                    observed_date=observation_date,
                    consecutive_gap_days=consecutive_gap_days,
                    database_url=database_url,
                )
        except Exception as exc:
            error = str(exc)
            emit_log_entry(
                build_log_entry(
                    zone,
                    result,
                    run_timestamp=run_timestamp,
                    run_date=execution_date,
                    observed_date=observation_date,
                    email_sent=False,
                    consecutive_gap_days=consecutive_gap_days,
                    error=error,
                ),
                log_path,
            )
            raise

        emit_log_entry(
            build_log_entry(
                zone,
                result,
                run_timestamp=run_timestamp,
                run_date=execution_date,
                observed_date=observation_date,
                email_sent=email_sent,
                consecutive_gap_days=consecutive_gap_days,
                error=error,
            ),
            log_path,
        )

    return 0


def build_log_path(log_dir: Path, run_date: dt.date) -> Path:
    return log_dir / f"pipeline_{run_date.isoformat()}.jsonl"


def extract_dataset_date(chl_ds: xr.Dataset) -> dt.date:
    if "time" not in chl_ds.coords or chl_ds.sizes.get("time", 0) == 0:
        raise RuntimeError("Downloaded CHL dataset does not contain a time coordinate")

    value = np.asarray(chl_ds["time"].values[0], dtype="datetime64[D]")
    return dt.date.fromisoformat(str(value))


def ensure_zone_results_match(zones: list[Zone], results: list[ZoneResult]) -> None:
    if len(zones) != len(results):
        raise RuntimeError(
            f"Anomaly engine returned {len(results)} results for {len(zones)} configured zones"
        )


def count_previous_gap_days(zone_name: str, observed_date: dt.date, log_dir: Path) -> int:
    count = 0
    current_date = observed_date - dt.timedelta(days=1)

    while True:
        entry = resolve_observed_zone_entry(log_dir, zone_name, current_date)
        if entry is None or entry.get("status") != "CLOUD_GAP":
            return count

        count += 1
        current_date -= dt.timedelta(days=1)


def resolve_observed_zone_entry(
    log_dir: Path,
    zone_name: str,
    observed_date: dt.date,
) -> dict[str, Any] | None:
    observed_date_iso = observed_date.isoformat()
    fallback_entry: dict[str, Any] | None = None

    # Scheduled runs usually write yesterday's observation into today's logfile.
    for execution_date in (observed_date + dt.timedelta(days=1), observed_date):
        entry = read_zone_entry(build_log_path(log_dir, execution_date), zone_name)
        if entry is None:
            continue

        entry_observed_date = entry.get("observed_date")
        if entry_observed_date == observed_date_iso:
            return entry

        if entry_observed_date is None and fallback_entry is None:
            fallback_entry = entry

    return fallback_entry


def read_zone_entry(log_path: Path, zone_name: str) -> dict[str, Any] | None:
    if not log_path.exists():
        return None

    matched_entry: dict[str, Any] | None = None
    with log_path.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue

            if entry.get("zone_name") == zone_name:
                matched_entry = entry

    return matched_entry


def build_log_entry(
    zone: Zone,
    result: ZoneResult,
    *,
    run_timestamp: str,
    run_date: dt.date,
    observed_date: dt.date,
    email_sent: bool,
    consecutive_gap_days: int | None,
    error: str | None,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "run_timestamp": run_timestamp,
        "run_date": run_date.isoformat(),
        "observed_date": observed_date.isoformat(),
        "zone_name": zone.name,
        "status": result.status,
        "anomaly_ratio": result.anomaly_ratio,
        "zone_avg_chl": result.zone_avg_chl,
        "climatology_mean_chl": result.climatology_mean_chl,
        "valid_pixel_count": result.valid_pixel_count,
        "total_pixel_count": result.total_pixel_count,
        "threshold_multiplier": zone.threshold_multiplier,
        "alert_email": zone.alert_email,
        "email_sent": email_sent,
        "error": error,
    }

    if result.status == "CLOUD_GAP":
        entry["consecutive_gap_days"] = consecutive_gap_days
        entry["gap_notification_threshold_days"] = GAP_DAYS_THRESHOLD

    return entry


def emit_log_entry(entry: dict[str, Any], log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(entry, sort_keys=True)

    print(line)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"{line}\n")


def _parse_date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date {value!r}. Expected YYYY-MM-DD."
        ) from exc


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the BlueWatch daily pipeline.")
    parser.add_argument(
        "--date",
        type=_parse_date,
        help="Historical run date to fetch and process (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=CONFIG_PATH,
        help="Path to a zones.yaml config file.",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=LOG_DIR,
        help="Directory for pipeline_YYYY-MM-DD.jsonl output.",
    )
    parser.add_argument(
        "--database-url",
        help="Optional DATABASE_URL override for alert deduplication storage.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        return run_pipeline(
            run_date=args.date,
            config_path=args.config,
            log_dir=args.log_dir,
            database_url=args.database_url,
        )
    except Exception as exc:
        print(f"ERROR: pipeline failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
