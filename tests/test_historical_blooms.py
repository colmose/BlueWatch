# mypy: disable-error-code=no-untyped-def
"""Parametrized test suite for historical HAB bloom detection (T12).

Tests validate that the anomaly engine would have correctly classified
each documented event in the bloom catalog — ALERT or NO ALERT —
matching the 'should_alert' field derived from primary sources.

All tests run offline: no CMEMS credentials are required.
Fixtures are synthetic datasets that reproduce the documented CHL anomaly ratio.
"""

from __future__ import annotations

import datetime
import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from bluewatch.anomaly_engine import compute_zone_results
from bluewatch.config import load_zones

from tests.fixtures.bloom_fixtures import (
    CATALOG_PATH,
    BloomEvent,
    build_bloom_fixture,
    build_climatology_fixture,
    build_synthetic_climatology,
    load_bloom_catalog,
    write_empty_turbid_mask,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ZONES_CONFIG_PATH = Path(__file__).parent.parent / "config" / "zones.yaml"
_BACKTEST_SCRIPT = Path(__file__).parent.parent / "scripts" / "backtest_blooms.py"


@pytest.fixture(name="backtest_mod")
def _backtest_mod_fixture():
    """Load scripts/backtest_blooms.py as a module, registered in sys.modules.

    Registration is required so @dataclass can resolve string annotations via
    typing.get_type_hints() (which calls sys.modules.get(cls.__module__)).
    """
    module_name = "backtest_blooms"
    spec = importlib.util.spec_from_file_location(module_name, _BACKTEST_SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    try:
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        yield mod
    finally:
        sys.modules.pop(module_name, None)


def _zone_by_name(zones: list[Any], name: str) -> Any | None:
    for z in zones:
        if z.name == name:
            return z
    return None


# ---------------------------------------------------------------------------
# Catalog unit tests
# ---------------------------------------------------------------------------


def test_catalog_loads_at_least_five_events():
    """Catalog must contain >= 5 documented events."""
    events = load_bloom_catalog()
    assert len(events) >= 5, f"Expected >=5 events, got {len(events)}"


def test_catalog_has_at_least_one_non_alert_event():
    """At least one sub-threshold (should_alert=False) event must be present."""
    events = load_bloom_catalog()
    non_alert = [e for e in events if not e.should_alert]
    assert len(non_alert) >= 1, "Catalog must include at least one non-alert boundary event"


def test_catalog_has_at_least_one_alert_event():
    """At least one above-threshold (should_alert=True) event must be present."""
    events = load_bloom_catalog()
    alert = [e for e in events if e.should_alert]
    assert len(alert) >= 1, "Catalog must include at least one alerting event"


def test_catalog_all_events_have_required_fields():
    """Every catalog entry must have all required fields populated."""
    required = {"event_id", "date", "zone_name", "species", "source",
                "synthetic_anomaly_ratio", "should_alert", "notes"}
    with CATALOG_PATH.open() as fh:
        raw = yaml.safe_load(fh)
    for entry in raw["events"]:
        missing = required - set(entry.keys())
        assert not missing, f"Event {entry.get('event_id', '?')} is missing fields: {missing}"


def test_catalog_event_ids_are_unique():
    events = load_bloom_catalog()
    ids = [e.event_id for e in events]
    assert len(ids) == len(set(ids)), "Duplicate event_id values found in catalog"


def test_catalog_dates_are_valid_iso():
    """All date fields must parse to valid datetime.date objects."""
    events = load_bloom_catalog()
    for event in events:
        assert isinstance(event.date, datetime.date), (
            f"Event {event.event_id}: 'date' did not parse to datetime.date"
        )


def test_catalog_alert_events_above_threshold():
    """Events with should_alert=True must have synthetic_anomaly_ratio >= 3.0 (default threshold)."""
    DEFAULT_THRESHOLD = 3.0
    events = load_bloom_catalog()
    for event in events:
        if event.should_alert:
            assert event.synthetic_anomaly_ratio >= DEFAULT_THRESHOLD, (
                f"Event {event.event_id}: should_alert=True but "
                f"ratio {event.synthetic_anomaly_ratio} < {DEFAULT_THRESHOLD}"
            )


def test_catalog_non_alert_events_below_threshold():
    """Events with should_alert=False must have synthetic_anomaly_ratio < 3.0."""
    DEFAULT_THRESHOLD = 3.0
    events = load_bloom_catalog()
    for event in events:
        if not event.should_alert:
            assert event.synthetic_anomaly_ratio < DEFAULT_THRESHOLD, (
                f"Event {event.event_id}: should_alert=False but "
                f"ratio {event.synthetic_anomaly_ratio} >= {DEFAULT_THRESHOLD}"
            )


def test_catalog_sources_are_cited():
    """Every event must have a non-empty source citation."""
    events = load_bloom_catalog()
    for event in events:
        assert event.source and event.source.strip(), (
            f"Event {event.event_id} has no source citation"
        )


# ---------------------------------------------------------------------------
# Bloom fixture builder unit tests
# ---------------------------------------------------------------------------


def test_build_bloom_fixture_has_correct_structure(tmp_path):
    """build_bloom_fixture returns a Dataset with CHL, lat, lon, time coords."""
    events = load_bloom_catalog()
    event = events[0]
    zones = load_zones(ZONES_CONFIG_PATH)
    zone = _zone_by_name(zones, event.zone_name)
    if zone is None:
        pytest.skip(f"Zone '{event.zone_name}' not found in zones.yaml")

    ds = build_bloom_fixture(event, zone.polygon)
    assert "CHL" in ds.data_vars
    assert "lat" in ds.coords
    assert "lon" in ds.coords
    assert "time" in ds.coords


def test_build_bloom_fixture_zone_pixels_at_bloom_chl(tmp_path):
    """Pixels inside the zone polygon should have CHL = background * synthetic_anomaly_ratio."""
    events = load_bloom_catalog()
    event = events[0]
    zones = load_zones(ZONES_CONFIG_PATH)
    zone = _zone_by_name(zones, event.zone_name)
    if zone is None:
        pytest.skip(f"Zone '{event.zone_name}' not found in zones.yaml")

    background = 1.0
    ds = build_bloom_fixture(event, zone.polygon, background_chl=background)
    chl = ds["CHL"].isel(time=0)
    lats = chl.lat.values
    lons = chl.lon.values

    expected_bloom = background * event.synthetic_anomaly_ratio
    import numpy as np
    from shapely.geometry import Point

    # Check at least one interior pixel has bloom CHL
    found_bloom = False
    for i, lat in enumerate(lats):
        for j, lon in enumerate(lons):
            if zone.polygon.contains(Point(lon, lat)):
                val = float(chl.values[i, j])
                assert abs(val - expected_bloom) < 1e-3, (
                    f"Expected bloom CHL {expected_bloom}, got {val}"
                )
                found_bloom = True
                break
        if found_bloom:
            break
    assert found_bloom, "No pixels found inside zone polygon — zone may be too small for grid step"


def test_build_climatology_fixture_creates_file(tmp_path):
    """build_climatology_fixture must create a valid .nc file at the expected path."""
    events = load_bloom_catalog()
    event = events[0]
    zones = load_zones(ZONES_CONFIG_PATH)
    zone = _zone_by_name(zones, event.zone_name)
    if zone is None:
        pytest.skip(f"Zone '{event.zone_name}' not found in zones.yaml")

    clim_path = build_climatology_fixture(event, zone.polygon, tmp_path)
    assert clim_path.exists(), "Climatology file was not created"

    import xarray as xr
    ds = xr.open_dataset(clim_path)
    assert "CHL_mean" in ds.data_vars
    assert "week" in ds.dims


# ---------------------------------------------------------------------------
# Core parametrized bloom detection tests
# ---------------------------------------------------------------------------


def _load_event_params() -> list[tuple[str, BloomEvent]]:
    """Return list of (event_id, BloomEvent) pairs for parametrize."""
    try:
        events = load_bloom_catalog()
        return [(e.event_id, e) for e in events]
    except Exception:
        return []


@pytest.mark.parametrize("event_id,event", _load_event_params())
def test_bloom_detection(event_id: str, event: BloomEvent, tmp_path: Path) -> None:
    """Engine must correctly classify each historical bloom event.

    For each catalog entry:
    - Build a synthetic CHL dataset with pixels at event.synthetic_anomaly_ratio
    - Run compute_zone_results() with a matching climatology
    - Assert alert_would_fire == event.should_alert
    """
    zones = load_zones(ZONES_CONFIG_PATH)
    zone = _zone_by_name(zones, event.zone_name)

    if zone is None:
        pytest.skip(
            f"Zone '{event.zone_name}' (from event {event_id}) is not configured "
            f"in {ZONES_CONFIG_PATH} — skipping."
        )

    # Build synthetic fixtures
    clim_path = build_climatology_fixture(event, zone.polygon, tmp_path)
    mask_path = write_empty_turbid_mask(tmp_path)
    chl_ds = build_bloom_fixture(event, zone.polygon)

    # Run the engine
    results = compute_zone_results(
        chl_ds,
        zones,
        run_date=event.date,
        clim_path=clim_path,
        mask_path=mask_path,
    )

    # Find the result for the target zone
    zone_result = next(
        (r for r in results if r.zone_name == event.zone_name), None
    )
    assert zone_result is not None, (
        f"compute_zone_results did not return a result for zone '{event.zone_name}'"
    )

    # Engine must not return CLOUD_GAP for a synthetic fixture with valid pixels
    assert zone_result.status == "DATA_AVAILABLE", (
        f"Event {event_id}: expected DATA_AVAILABLE, got CLOUD_GAP "
        f"(valid_pixel_count={zone_result.valid_pixel_count}, "
        f"total_pixel_count={zone_result.total_pixel_count})"
    )

    assert zone_result.anomaly_ratio is not None, (
        f"Event {event_id}: anomaly_ratio is None despite DATA_AVAILABLE status"
    )

    alert_would_fire = zone_result.anomaly_ratio >= zone.threshold_multiplier

    assert alert_would_fire == event.should_alert, (
        f"{event_id}: expected alert={event.should_alert}, "
        f"got anomaly_ratio={zone_result.anomaly_ratio:.4f}, "
        f"threshold={zone.threshold_multiplier:.2f}"
    )


# ---------------------------------------------------------------------------
# Threshold boundary test
# ---------------------------------------------------------------------------


def test_sub_threshold_event_does_not_alert(tmp_path):
    """The non-alert event must produce anomaly_ratio < threshold_multiplier."""
    events = load_bloom_catalog()
    non_alert_events = [e for e in events if not e.should_alert]
    assert non_alert_events, "No non-alert events in catalog"

    event = non_alert_events[0]
    zones = load_zones(ZONES_CONFIG_PATH)
    zone = _zone_by_name(zones, event.zone_name)

    if zone is None:
        pytest.skip(f"Zone '{event.zone_name}' not configured — skipping boundary test")

    clim_path = build_climatology_fixture(event, zone.polygon, tmp_path)
    mask_path = write_empty_turbid_mask(tmp_path)
    chl_ds = build_bloom_fixture(event, zone.polygon)

    results = compute_zone_results(
        chl_ds, zones, run_date=event.date,
        clim_path=clim_path, mask_path=mask_path,
    )

    zone_result = next(r for r in results if r.zone_name == event.zone_name)
    assert zone_result.status == "DATA_AVAILABLE"
    assert zone_result.anomaly_ratio is not None
    assert zone_result.anomaly_ratio < zone.threshold_multiplier, (
        f"Sub-threshold event {event.event_id} yielded ratio "
        f"{zone_result.anomaly_ratio:.4f} >= threshold {zone.threshold_multiplier}"
    )


# ---------------------------------------------------------------------------
# Zone name matching / skip behaviour
# ---------------------------------------------------------------------------


def test_unknown_zone_name_is_skipped_cleanly():
    """A catalog event referencing an unconfigured zone must be gracefully skipped."""
    zones = load_zones(ZONES_CONFIG_PATH)
    zone_names = {z.name for z in zones}
    zone = _zone_by_name(zones, "Zone That Does Not Exist")
    assert zone is None, "Expected None for unknown zone name"


def test_catalog_zone_names_match_config():
    """Warn (don't fail) if any catalog zone name is absent from zones.yaml."""
    events = load_bloom_catalog()
    zones = load_zones(ZONES_CONFIG_PATH)
    zone_names = {z.name for z in zones}
    unconfigured = [e.event_id for e in events if e.zone_name not in zone_names]
    # This is non-blocking: events with unknown zones are skipped in parametrized tests.
    # This test documents which events (if any) would be skipped in this environment.
    if unconfigured:
        import warnings
        warnings.warn(
            f"These catalog events reference zones not in zones.yaml and will be "
            f"skipped: {unconfigured}",
            stacklevel=2,
        )
    # Always passes — just documents coverage gap
    assert True


# ---------------------------------------------------------------------------
# Backtest script integration tests (scripts/backtest_blooms.py)
# ---------------------------------------------------------------------------


def test_backtest_script_exists():
    """scripts/backtest_blooms.py must exist."""
    script = Path(__file__).parent.parent / "scripts" / "backtest_blooms.py"
    assert script.exists(), f"Backtest script not found: {script}"


def test_backtest_script_is_importable(backtest_mod):
    """scripts/backtest_blooms.py must be importable without side effects."""
    assert hasattr(backtest_mod, "run_backtest"), "backtest_blooms.py must expose run_backtest()"
    assert hasattr(backtest_mod, "main"), "backtest_blooms.py must expose main()"


def test_backtest_run_backtest_returns_rows(backtest_mod):
    """run_backtest() must return a non-empty list of BacktestRow objects."""
    rows = backtest_mod.run_backtest(
        catalog_path=CATALOG_PATH,
        config_path=ZONES_CONFIG_PATH,
    )
    assert isinstance(rows, list), "run_backtest() must return a list"
    assert len(rows) > 0, "run_backtest() must return at least one row"

    row = rows[0]
    assert hasattr(row, "event_id")
    assert hasattr(row, "result")
    assert row.result in ("PASS", "FAIL", "SKIP"), f"Unexpected result value: {row.result!r}"


def test_backtest_all_events_pass(backtest_mod):
    """All correctly classified catalog events must have result='PASS' or 'SKIP'."""
    rows = backtest_mod.run_backtest(
        catalog_path=CATALOG_PATH,
        config_path=ZONES_CONFIG_PATH,
    )

    failed = [r for r in rows if r.result == "FAIL"]
    assert not failed, (
        f"Backtest failed for events: {[r.event_id for r in failed]}\n"
        + "\n".join(
            f"  {r.event_id}: anomaly_ratio={r.anomaly_ratio}, "
            f"threshold={r.threshold}, expected_alert={r.expected_alert}, "
            f"actual_alert={r.actual_alert}, notes={r.notes}"
            for r in failed
        )
    )


def test_backtest_main_exits_zero_on_success(backtest_mod, monkeypatch, capsys):
    """main() must return 0 when all events pass."""
    monkeypatch.setattr(
        "sys.argv",
        [
            "backtest_blooms.py",
            "--catalog", str(CATALOG_PATH),
            "--config", str(ZONES_CONFIG_PATH),
        ],
    )
    exit_code = backtest_mod.main()
    assert exit_code == 0, f"main() returned {exit_code}, expected 0"


def test_backtest_main_json_flag_outputs_json(backtest_mod, monkeypatch, capsys):
    """--json flag must produce valid JSON output."""
    import json as json_mod

    monkeypatch.setattr(
        "sys.argv",
        [
            "backtest_blooms.py",
            "--catalog", str(CATALOG_PATH),
            "--config", str(ZONES_CONFIG_PATH),
            "--json",
        ],
    )
    exit_code = backtest_mod.main()
    output = capsys.readouterr().out
    parsed = json_mod.loads(output)
    assert isinstance(parsed, list)
    assert len(parsed) > 0
    assert "event_id" in parsed[0]
    assert "result" in parsed[0]
    assert exit_code == 0


def test_backtest_main_exits_one_on_missing_catalog(backtest_mod, monkeypatch, tmp_path):
    """main() must return 1 if catalog file does not exist."""
    monkeypatch.setattr(
        "sys.argv",
        [
            "backtest_blooms.py",
            "--catalog", str(tmp_path / "no_such_catalog.yaml"),
            "--config", str(ZONES_CONFIG_PATH),
        ],
    )
    exit_code = backtest_mod.main()
    assert exit_code == 1, f"main() should return 1 on missing catalog, got {exit_code}"


# ---------------------------------------------------------------------------
# Spec-required tests (items 11-12 from the T12 spec)
# ---------------------------------------------------------------------------


def test_load_bloom_catalog_raises_on_missing_field(tmp_path):
    """load_bloom_catalog must raise ValueError if a required field is missing."""
    bad_catalog = tmp_path / "bad_catalog.yaml"
    bad_catalog.write_text(
        "events:\n"
        "  - date: \"2012-08-15\"\n"
        "    zone_name: Outer Clew Bay\n"
        "    species: Karenia mikimotoi\n"
        "    source: IMI bulletin\n"
        "    synthetic_anomaly_ratio: 4.2\n"
        "    should_alert: true\n"
        "    notes: missing event_id field\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="event_id"):
        load_bloom_catalog(bad_catalog)


def test_build_synthetic_climatology_covers_zone(tmp_path):
    """build_synthetic_climatology must create a ≥3×3 grid overlapping the zone polygon."""
    import numpy as np
    import xarray as xr
    from shapely.geometry import box

    zone_polygon = box(-9.74, 53.785, -9.54, 53.9)
    clim_path = build_synthetic_climatology(tmp_path, zone_polygon)

    assert clim_path.exists(), "Climatology file was not created"
    ds = xr.open_dataset(clim_path)
    assert "CHL_mean" in ds.data_vars
    assert "week" in ds.dims

    lats = ds.lat.values
    lons = ds.lon.values
    assert len(lats) >= 3, f"Expected ≥3 lat values, got {len(lats)}"
    assert len(lons) >= 3, f"Expected ≥3 lon values, got {len(lons)}"

    # At least one grid point must be inside the zone polygon
    from shapely.geometry import Point
    found = any(
        zone_polygon.contains(Point(lon, lat))
        for lat in lats
        for lon in lons
    )
    assert found, "No grid point inside the zone polygon"

    # All CHL_mean values must equal the default base_chl
    chl_vals = ds["CHL_mean"].values
    assert np.all(~np.isnan(chl_vals)), "CHL_mean contains NaN values"
    assert np.allclose(chl_vals, 2.0, atol=1e-4), f"Expected all CHL_mean=2.0, got {np.unique(chl_vals)}"


def test_backtest_script_exits_one_on_failure(tmp_path, monkeypatch, capsys):
    """main() must return 1 if a bloom event is mis-classified.

    Spec test 14: create a catalog where should_alert=True but the synthetic ratio
    is below the threshold (2.0 < 3.0).  The engine computes anomaly_ratio=2.0 and
    alert_would_fire=False, but should_alert=True — a mis-classification.
    """
    import importlib.util

    bad_catalog = tmp_path / "bad_catalog.yaml"
    bad_catalog.write_text(
        "events:\n"
        "  - event_id: misclassified_event\n"
        "    date: \"2012-08-15\"\n"
        "    zone_name: Outer Clew Bay\n"
        "    species: Karenia mikimotoi\n"
        "    source: IMI bulletin\n"
        "    synthetic_anomaly_ratio: 2.0\n"
        "    should_alert: true\n"
        "    notes: deliberately mis-classified for testing\n",
        encoding="utf-8",
    )

    script = Path(__file__).parent.parent / "scripts" / "backtest_blooms.py"
    spec_obj = importlib.util.spec_from_file_location("backtest_blooms_fail", script)
    mod = importlib.util.module_from_spec(spec_obj)  # type: ignore[arg-type]
    sys.modules["backtest_blooms_fail"] = mod
    spec_obj.loader.exec_module(mod)  # type: ignore[union-attr]

    monkeypatch.setattr(
        "sys.argv",
        [
            "backtest_blooms.py",
            "--catalog", str(bad_catalog),
            "--config", str(ZONES_CONFIG_PATH),
        ],
    )
    try:
        exit_code = mod.main()
    finally:
        sys.modules.pop("backtest_blooms_fail", None)
    assert exit_code == 1, (
        f"main() should return 1 when an event is mis-classified, got {exit_code}"
    )


def test_backtest_script_skips_unknown_zone(tmp_path, monkeypatch, capsys):
    """main() with a catalog entry referencing an unknown zone must SKIP it and exit 0.

    Spec test 16: unknown zone_name must produce SKIP result, not FAIL.
    """
    import importlib.util
    import json as json_mod

    catalog_with_unknown = tmp_path / "catalog_unknown_zone.yaml"
    catalog_with_unknown.write_text(
        "events:\n"
        "  - event_id: unknown_zone_event\n"
        "    date: \"2012-08-15\"\n"
        "    zone_name: Zone That Does Not Exist\n"
        "    species: Karenia mikimotoi\n"
        "    source: IMI bulletin\n"
        "    synthetic_anomaly_ratio: 4.2\n"
        "    should_alert: true\n"
        "    notes: zone not in config\n",
        encoding="utf-8",
    )

    script = Path(__file__).parent.parent / "scripts" / "backtest_blooms.py"
    spec_obj = importlib.util.spec_from_file_location("backtest_blooms_skip", script)
    mod = importlib.util.module_from_spec(spec_obj)  # type: ignore[arg-type]
    sys.modules["backtest_blooms_skip"] = mod
    spec_obj.loader.exec_module(mod)  # type: ignore[union-attr]

    monkeypatch.setattr(
        "sys.argv",
        [
            "backtest_blooms.py",
            "--catalog", str(catalog_with_unknown),
            "--config", str(ZONES_CONFIG_PATH),
            "--json",
        ],
    )
    try:
        exit_code = mod.main()
        output = capsys.readouterr().out
    finally:
        sys.modules.pop("backtest_blooms_skip", None)
    parsed = json_mod.loads(output)
    assert isinstance(parsed, list) and len(parsed) == 1
    assert parsed[0]["event_id"] == "unknown_zone_event"
    assert parsed[0]["result"] == "SKIP", (
        f"Expected SKIP for unknown zone, got {parsed[0]['result']!r}"
    )
    assert exit_code == 0, (
        f"SKIP-only run should exit 0, got {exit_code}"
    )
