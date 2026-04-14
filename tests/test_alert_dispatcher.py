"""Tests for bluewatch/alert_dispatcher.py (T07/T08)."""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path
from typing import Any, Literal
from urllib import error

import pytest
from shapely.geometry import box

import bluewatch.alert_dispatcher as alert_dispatcher
from bluewatch.alert_dispatcher import (
    GAP_DAYS_THRESHOLD,
    PostgresAlertLogStore,
    SQLiteAlertLogStore,
    _format_anomaly_body,
    _format_gap_body,
    _send_email,
    dispatch_anomaly_alert,
    dispatch_gap_notification,
    get_alert_log_store,
    has_alert_been_logged,
    initialize_alert_log,
    record_alert,
)
from bluewatch.anomaly_engine import ZoneResult
from bluewatch.config import Zone


def make_zone(
    name: str = "Outer Clew Bay",
    *,
    threshold_multiplier: float = 3.0,
    alert_email: str = "ops@example.com",
) -> Zone:
    return Zone(
        name=name,
        description="Atlantic-facing monitoring zone",
        polygon=box(-10.0, 53.7, -9.6, 53.9),
        threshold_multiplier=threshold_multiplier,
        alert_email=alert_email,
    )


def make_result(
    zone_name: str = "Outer Clew Bay",
    *,
    status: str = "DATA_AVAILABLE",
    anomaly_ratio: float | None = 4.0,
    zone_avg_chl: float | None = 2.0,
    climatology_mean_chl: float | None = 0.5,
    valid_pixel_count: int = 10,
    total_pixel_count: int = 12,
) -> ZoneResult:
    return ZoneResult(
        zone_name=zone_name,
        status=status,  # type: ignore[arg-type]
        anomaly_ratio=anomaly_ratio,
        zone_avg_chl=zone_avg_chl,
        climatology_mean_chl=climatology_mean_chl,
        valid_pixel_count=valid_pixel_count,
        total_pixel_count=total_pixel_count,
    )


def test_initialize_alert_log_creates_database_and_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "alert_log.db"

    created_path = initialize_alert_log(db_path)

    assert created_path == db_path
    assert db_path.exists()

    with sqlite3.connect(db_path) as conn:
        table_sql = conn.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table' AND name = 'alert_log'
            """
        ).fetchone()

    assert table_sql is not None
    assert "zone_name TEXT NOT NULL" in table_sql[0]
    assert "alert_date DATE NOT NULL" in table_sql[0]
    assert "alert_type TEXT NOT NULL" in table_sql[0]
    assert "PRIMARY KEY(zone_name, alert_date, alert_type)" in table_sql[0]


def test_get_alert_log_store_defaults_to_sqlite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "data" / "alert_log.db"
    monkeypatch.delenv("DATABASE_URL", raising=False)

    store = get_alert_log_store(db_path=db_path)

    assert isinstance(store, SQLiteAlertLogStore)
    assert store.db_path == db_path


def test_get_alert_log_store_supports_sqlite_url(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "explicit-alert-log.db"

    store = get_alert_log_store(database_url=f"sqlite://{db_path}")

    assert isinstance(store, SQLiteAlertLogStore)
    assert store.db_path == db_path


def test_get_alert_log_store_supports_postgres_url(monkeypatch: pytest.MonkeyPatch) -> None:
    database_url = "postgres://bluewatch:secret@example.com/alerts"
    monkeypatch.setattr(alert_dispatcher, "_load_psycopg", lambda: object())

    store = get_alert_log_store(database_url=database_url)

    assert isinstance(store, PostgresAlertLogStore)
    assert store.database_url == database_url


def test_get_alert_log_store_rejects_unsupported_url_scheme() -> None:
    with pytest.raises(ValueError, match="Unsupported DATABASE_URL scheme"):
        get_alert_log_store(database_url="mysql://bluewatch:secret@example.com/alerts")


def test_get_alert_log_store_raises_helpful_error_when_psycopg_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_import_module(name: str) -> Any:
        if name == "psycopg":
            raise ImportError("No module named 'psycopg'")
        return __import__(name)

    monkeypatch.setattr("bluewatch.alert_dispatcher.importlib.import_module", fake_import_module)

    with pytest.raises(ImportError, match="psycopg is required for postgres alert logs"):
        get_alert_log_store(database_url="postgres://bluewatch:secret@example.com/alerts")


def test_has_alert_been_logged_false_for_missing_record(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "alert_log.db"

    assert not has_alert_been_logged(
        "Outer Clew Bay",
        date(2026, 4, 13),
        "anomaly",
        db_path=db_path,
    )


def test_record_alert_inserts_new_record(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "alert_log.db"

    inserted = record_alert(
        "Outer Clew Bay",
        date(2026, 4, 13),
        "anomaly",
        db_path=db_path,
    )

    assert inserted is True
    assert has_alert_been_logged(
        "Outer Clew Bay",
        date(2026, 4, 13),
        "anomaly",
        db_path=db_path,
    )


def test_record_alert_returns_false_for_duplicate_composite_key(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "alert_log.db"

    first_insert = record_alert(
        "Outer Clew Bay",
        date(2026, 4, 13),
        "anomaly",
        db_path=db_path,
    )
    second_insert = record_alert(
        "Outer Clew Bay",
        date(2026, 4, 13),
        "anomaly",
        db_path=db_path,
    )

    assert first_insert is True
    assert second_insert is False

    with sqlite3.connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM alert_log").fetchone()

    assert count is not None
    assert count[0] == 1


def test_record_alert_allows_different_alert_type_and_date(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "alert_log.db"

    assert record_alert("Outer Clew Bay", date(2026, 4, 13), "anomaly", db_path=db_path)
    assert record_alert("Outer Clew Bay", date(2026, 4, 13), "gap", db_path=db_path)
    assert record_alert("Outer Clew Bay", date(2026, 4, 14), "anomaly", db_path=db_path)

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT zone_name, alert_date, alert_type
            FROM alert_log
            ORDER BY alert_date, alert_type
            """
        ).fetchall()

    assert rows == [
        ("Outer Clew Bay", "2026-04-13", "anomaly"),
        ("Outer Clew Bay", "2026-04-13", "gap"),
        ("Outer Clew Bay", "2026-04-14", "anomaly"),
    ]


def test_send_email_posts_to_resend(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class FakeResponse:
        status = 202

        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> Literal[False]:
            return False

    def fake_urlopen(req: Any) -> FakeResponse:
        captured["url"] = req.full_url
        captured["headers"] = dict(req.header_items())
        captured["body"] = req.data.decode("utf-8")
        return FakeResponse()

    monkeypatch.setattr("bluewatch.alert_dispatcher.request.urlopen", fake_urlopen)
    _send_email(
        api_key="re_test",
        to="ops@example.com",
        subject="Alert",
        body="payload",
        from_email="alerts@example.com",
    )

    assert captured["url"] == alert_dispatcher.RESEND_API_URL
    assert "Authorization" in captured["headers"]
    assert '"subject": "Alert"' in str(captured["body"])


def test_send_email_raises_runtime_error_on_network_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_urlopen(req: Any) -> None:
        raise error.URLError("network down")

    monkeypatch.setattr("bluewatch.alert_dispatcher.request.urlopen", fake_urlopen)

    with pytest.raises(RuntimeError, match="Resend API failure"):
        _send_email(api_key="re_test", to="ops@example.com", subject="Alert", body="payload")


def test_dispatch_anomaly_alert_sends_when_threshold_met(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    sent: list[tuple[str, str, str]] = []

    monkeypatch.setattr(
        alert_dispatcher,
        "_send_email",
        lambda *, api_key, to, subject, body, from_email=None: sent.append((to, subject, body)),
    )

    zone = make_zone()
    result = make_result()

    assert dispatch_anomaly_alert(
        zone,
        result,
        alert_date=date(2026, 4, 13),
        observed_date=date(2026, 4, 12),
        db_path=tmp_path / "alert_log.db",
    )
    assert len(sent) == 1


def test_dispatch_anomaly_alert_skips_below_threshold(tmp_path: Path) -> None:
    zone = make_zone(threshold_multiplier=3.0)
    result = make_result(anomaly_ratio=2.9)

    assert not dispatch_anomaly_alert(
        zone,
        result,
        alert_date=date(2026, 4, 13),
        observed_date=date(2026, 4, 13),
        db_path=tmp_path / "alert_log.db",
    )


def test_dispatch_anomaly_alert_deduplicates_same_day(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    sent: list[str] = []

    monkeypatch.setattr(
        alert_dispatcher,
        "_send_email",
        lambda *, api_key, to, subject, body, from_email=None: sent.append(subject),
    )

    zone = make_zone()
    result = make_result()
    db_path = tmp_path / "alert_log.db"

    assert dispatch_anomaly_alert(
        zone,
        result,
        alert_date=date(2026, 4, 13),
        observed_date=date(2026, 4, 13),
        db_path=db_path,
    )
    assert not dispatch_anomaly_alert(
        zone,
        result,
        alert_date=date(2026, 4, 13),
        observed_date=date(2026, 4, 13),
        db_path=db_path,
    )
    assert len(sent) == 1


def test_dispatch_anomaly_alert_does_not_record_on_send_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    monkeypatch.setattr(
        alert_dispatcher,
        "_send_email",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("resend down")),
    )

    zone = make_zone()
    result = make_result()
    db_path = tmp_path / "alert_log.db"

    with pytest.raises(RuntimeError, match="resend down"):
        dispatch_anomaly_alert(
            zone,
            result,
            alert_date=date(2026, 4, 13),
            observed_date=date(2026, 4, 13),
            db_path=db_path,
        )

    assert not has_alert_been_logged(
        "Outer Clew Bay",
        date(2026, 4, 13),
        "anomaly",
        db_path=db_path,
    )


def test_dispatch_gap_notification_sends_at_threshold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    sent: list[str] = []
    monkeypatch.setattr(
        alert_dispatcher,
        "_send_email",
        lambda *, api_key, to, subject, body, from_email=None: sent.append(subject),
    )

    assert dispatch_gap_notification(
        make_zone(),
        alert_date=date(2026, 4, 13),
        consecutive_gap_days=GAP_DAYS_THRESHOLD,
        db_path=tmp_path / "alert_log.db",
    )
    assert len(sent) == 1


def test_dispatch_gap_notification_skips_below_threshold(tmp_path: Path) -> None:
    assert not dispatch_gap_notification(
        make_zone(),
        alert_date=date(2026, 4, 13),
        consecutive_gap_days=GAP_DAYS_THRESHOLD - 1,
        db_path=tmp_path / "alert_log.db",
    )


def test_dispatch_gap_notification_deduplicates_same_day(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    sent: list[str] = []
    monkeypatch.setattr(
        alert_dispatcher,
        "_send_email",
        lambda *, api_key, to, subject, body, from_email=None: sent.append(subject),
    )

    db_path = tmp_path / "alert_log.db"

    assert dispatch_gap_notification(
        make_zone(),
        alert_date=date(2026, 4, 13),
        consecutive_gap_days=GAP_DAYS_THRESHOLD,
        db_path=db_path,
    )
    assert not dispatch_gap_notification(
        make_zone(),
        alert_date=date(2026, 4, 13),
        consecutive_gap_days=GAP_DAYS_THRESHOLD,
        db_path=db_path,
    )
    assert len(sent) == 1


def test_dispatch_gap_notification_does_not_record_on_send_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    monkeypatch.setattr(
        alert_dispatcher,
        "_send_email",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("resend down")),
    )

    zone = make_zone()
    db_path = tmp_path / "alert_log.db"

    with pytest.raises(RuntimeError, match="resend down"):
        dispatch_gap_notification(
            zone,
            alert_date=date(2026, 4, 13),
            consecutive_gap_days=GAP_DAYS_THRESHOLD,
            db_path=db_path,
        )

    assert not has_alert_been_logged(zone.name, date(2026, 4, 13), "gap", db_path=db_path)


def test_format_anomaly_body_contains_required_fields() -> None:
    body = _format_anomaly_body(
        make_zone(),
        make_result(),
        alert_date=date(2026, 4, 13),
        observed_date=date(2026, 4, 12),
    )

    assert "Outer Clew Bay" in body
    assert "2026-04-13" in body
    assert "2026-04-12" in body
    assert "2.0000" in body
    assert "4.00" in body


def test_format_gap_body_contains_required_fields() -> None:
    body = _format_gap_body(
        make_zone(),
        alert_date=date(2026, 4, 13),
        consecutive_gap_days=3,
    )

    assert "Outer Clew Bay" in body
    assert "2026-04-13" in body
    assert "3" in body
