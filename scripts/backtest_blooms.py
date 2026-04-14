#!/usr/bin/env python3
"""BlueWatch — Historical HAB Bloom Backtest Report (T12).

Runs the anomaly engine against each event in the bloom catalog using
synthetic fixtures and prints a human-readable (or JSON) pass/fail report.

Usage:
    python scripts/backtest_blooms.py
    python scripts/backtest_blooms.py --catalog tests/fixtures/historical_blooms.yaml
    python scripts/backtest_blooms.py --config config/zones.yaml
    python scripts/backtest_blooms.py --json

Flags:
    --catalog PATH   Path to bloom catalog YAML (default: tests/fixtures/historical_blooms.yaml)
    --config PATH    Path to zones.yaml (default: config/zones.yaml)
    --json           Emit machine-readable JSON instead of a table

Exit codes:
    0  All events correctly classified
    1  One or more events mis-classified, or a runtime error occurred
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

# Ensure the project root is importable
sys.path.insert(0, str(Path(__file__).parent.parent))

from bluewatch.anomaly_engine import compute_zone_results
from bluewatch.config import load_zones
from tests.fixtures.bloom_fixtures import (
    build_bloom_fixture,
    build_climatology_fixture,
    load_bloom_catalog,
    write_empty_turbid_mask,
)


@dataclass
class BacktestRow:
    event_id: str
    date: str
    zone_name: str
    species: str
    anomaly_ratio: float | None
    threshold: float | None
    expected_alert: bool
    actual_alert: bool | None
    result: str  # "PASS", "FAIL", "SKIP"
    notes: str


def _zone_by_name(zones: list[Any], name: str) -> Any | None:
    for z in zones:
        if z.name == name:
            return z
    return None


def run_backtest(
    catalog_path: Path,
    config_path: Path,
) -> list[BacktestRow]:
    """Run the engine against each catalog event and return result rows."""
    events = load_bloom_catalog(catalog_path)
    zones = load_zones(config_path)

    rows: list[BacktestRow] = []

    for event in events:
        zone = _zone_by_name(zones, event.zone_name)

        if zone is None:
            rows.append(
                BacktestRow(
                    event_id=event.event_id,
                    date=str(event.date),
                    zone_name=event.zone_name,
                    species=event.species,
                    anomaly_ratio=None,
                    threshold=None,
                    expected_alert=event.should_alert,
                    actual_alert=None,
                    result="SKIP",
                    notes=f"Zone '{event.zone_name}' not in {config_path}",
                )
            )
            continue

        # Run the engine with a temporary directory for fixture files
        with tempfile.TemporaryDirectory(prefix="backtest_") as tmpdir:
            tmp_path = Path(tmpdir)
            try:
                clim_path = build_climatology_fixture(event, zone.polygon, tmp_path)
                mask_path = write_empty_turbid_mask(tmp_path)
                chl_ds = build_bloom_fixture(event, zone.polygon)

                results = compute_zone_results(
                    chl_ds,
                    zones,
                    run_date=event.date,
                    clim_path=clim_path,
                    mask_path=mask_path,
                )
            except Exception as exc:
                rows.append(
                    BacktestRow(
                        event_id=event.event_id,
                        date=str(event.date),
                        zone_name=event.zone_name,
                        species=event.species,
                        anomaly_ratio=None,
                        threshold=zone.threshold_multiplier,
                        expected_alert=event.should_alert,
                        actual_alert=None,
                        result="FAIL",
                        notes=f"Engine error: {exc}",
                    )
                )
                continue

        zone_result = next(
            (r for r in results if r.zone_name == event.zone_name), None
        )

        if zone_result is None or zone_result.status != "DATA_AVAILABLE":
            rows.append(
                BacktestRow(
                    event_id=event.event_id,
                    date=str(event.date),
                    zone_name=event.zone_name,
                    species=event.species,
                    anomaly_ratio=None,
                    threshold=zone.threshold_multiplier,
                    expected_alert=event.should_alert,
                    actual_alert=None,
                    result="FAIL",
                    notes=(
                        f"Engine returned CLOUD_GAP or no result "
                        f"(valid_pixels={zone_result.valid_pixel_count if zone_result else 'N/A'})"
                    ),
                )
            )
            continue

        actual_alert = zone_result.anomaly_ratio >= zone.threshold_multiplier
        passed = actual_alert == event.should_alert

        rows.append(
            BacktestRow(
                event_id=event.event_id,
                date=str(event.date),
                zone_name=event.zone_name,
                species=event.species,
                anomaly_ratio=round(zone_result.anomaly_ratio, 4),
                threshold=zone.threshold_multiplier,
                expected_alert=event.should_alert,
                actual_alert=actual_alert,
                result="PASS" if passed else "FAIL",
                notes="",
            )
        )

    return rows


def _fmt_bool(value: bool | None) -> str:
    if value is None:
        return "N/A"
    return "ALERT" if value else "NO ALERT"


def _print_table(rows: list[BacktestRow]) -> None:
    """Print a human-readable fixed-width report to stdout."""
    col_widths = {
        "date": 10,
        "zone": 22,
        "event_id": 26,
        "species": 24,
        "ratio": 10,
        "threshold": 9,
        "expected": 10,
        "actual": 10,
        "result": 6,
    }

    header = (
        f"{'Date':{col_widths['date']}}  "
        f"{'Zone':{col_widths['zone']}}  "
        f"{'Event ID':{col_widths['event_id']}}  "
        f"{'Species':{col_widths['species']}}  "
        f"{'Anom Ratio':{col_widths['ratio']}}  "
        f"{'Threshold':{col_widths['threshold']}}  "
        f"{'Expected':{col_widths['expected']}}  "
        f"{'Actual':{col_widths['actual']}}  "
        f"Result"
    )
    sep = "-" * len(header)

    print()
    print("BlueWatch -- Historical HAB Bloom Backtest")
    print("=" * len(header))
    print(header)
    print(sep)

    for row in rows:
        ratio_str = f"{row.anomaly_ratio:.2f}" if row.anomaly_ratio is not None else "N/A"
        thresh_str = f"{row.threshold:.2f}" if row.threshold is not None else "N/A"
        print(
            f"{row.date:{col_widths['date']}}  "
            f"{row.zone_name:{col_widths['zone']}}  "
            f"{row.event_id:{col_widths['event_id']}}  "
            f"{(row.species or 'N/A'):{col_widths['species']}}  "
            f"{ratio_str:{col_widths['ratio']}}  "
            f"{thresh_str:{col_widths['threshold']}}  "
            f"{_fmt_bool(row.expected_alert):{col_widths['expected']}}  "
            f"{_fmt_bool(row.actual_alert):{col_widths['actual']}}  "
            f"{row.result}"
            + (f"  [{row.notes}]" if row.notes else "")
        )

    print(sep)
    passed = sum(1 for r in rows if r.result == "PASS")
    failed = sum(1 for r in rows if r.result == "FAIL")
    skipped = sum(1 for r in rows if r.result == "SKIP")
    print(f"\nSummary: {passed} passed, {failed} failed, {skipped} skipped")


def _print_json(rows: list[BacktestRow]) -> None:
    output = [
        {
            **asdict(row),
            "expected_alert_label": _fmt_bool(row.expected_alert),
            "actual_alert_label": _fmt_bool(row.actual_alert),
        }
        for row in rows
    ]
    print(json.dumps(output, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="BlueWatch historical HAB bloom backtest report"
    )
    parser.add_argument(
        "--catalog",
        type=Path,
        default=Path(__file__).parent.parent / "tests" / "fixtures" / "historical_blooms.yaml",
        help="Path to bloom catalog YAML",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).parent.parent / "config" / "zones.yaml",
        help="Path to zones.yaml",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Emit machine-readable JSON output",
    )
    args = parser.parse_args()

    if not args.catalog.exists():
        print(f"ERROR: catalog not found: {args.catalog}", file=sys.stderr)
        return 1
    if not args.config.exists():
        print(f"ERROR: zones config not found: {args.config}", file=sys.stderr)
        return 1

    try:
        rows = run_backtest(catalog_path=args.catalog, config_path=args.config)
    except Exception as exc:
        print(f"ERROR: backtest failed: {exc}", file=sys.stderr)
        return 1

    if args.as_json:
        _print_json(rows)
    else:
        _print_table(rows)

    # Exit 1 if any event was mis-classified (FAIL result)
    any_failed = any(r.result == "FAIL" for r in rows)
    return 1 if any_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
