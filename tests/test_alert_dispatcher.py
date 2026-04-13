"""Tests for bluewatch/alert_dispatcher.py (T07)."""

import sqlite3
from datetime import date

import pytest

from bluewatch import alert_dispatcher
from bluewatch.alert_dispatcher import (
    PostgresAlertLogStore,
    SQLiteAlertLogStore,
    get_alert_log_store,
    has_alert_been_logged,
    initialize_alert_log,
    record_alert,
)


def test_initialize_alert_log_creates_database_and_schema(tmp_path):
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


def test_get_alert_log_store_defaults_to_sqlite(tmp_path, monkeypatch):
    db_path = tmp_path / "data" / "alert_log.db"
    monkeypatch.delenv("DATABASE_URL", raising=False)

    store = get_alert_log_store(db_path=db_path)

    assert isinstance(store, SQLiteAlertLogStore)
    assert store.db_path == db_path


def test_get_alert_log_store_supports_sqlite_url(tmp_path):
    db_path = tmp_path / "data" / "explicit-alert-log.db"

    store = get_alert_log_store(database_url=f"sqlite://{db_path}")

    assert isinstance(store, SQLiteAlertLogStore)
    assert store.db_path == db_path


def test_get_alert_log_store_supports_postgres_url(monkeypatch):
    database_url = "postgres://bluewatch:secret@example.com/alerts"
    monkeypatch.setattr(alert_dispatcher, "_load_psycopg", lambda: object())

    store = get_alert_log_store(database_url=database_url)

    assert isinstance(store, PostgresAlertLogStore)
    assert store.database_url == database_url


def test_get_alert_log_store_rejects_unsupported_url_scheme():
    with pytest.raises(ValueError, match="Unsupported DATABASE_URL scheme"):
        get_alert_log_store(database_url="mysql://bluewatch:secret@example.com/alerts")


def test_get_alert_log_store_raises_helpful_error_when_psycopg_missing(monkeypatch):
    def fake_import_module(name):
        if name == "psycopg":
            raise ImportError("No module named 'psycopg'")
        return __import__(name)

    monkeypatch.setattr(alert_dispatcher.importlib, "import_module", fake_import_module)

    with pytest.raises(ImportError, match="psycopg is required for postgres alert logs"):
        get_alert_log_store(database_url="postgres://bluewatch:secret@example.com/alerts")


def test_has_alert_been_logged_false_for_missing_record(tmp_path):
    db_path = tmp_path / "data" / "alert_log.db"

    assert not has_alert_been_logged(
        "Outer Clew Bay",
        date(2026, 4, 13),
        "anomaly",
        db_path=db_path,
    )


def test_record_alert_inserts_new_record(tmp_path):
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


def test_record_alert_returns_false_for_duplicate_composite_key(tmp_path):
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


def test_record_alert_allows_different_alert_type_and_date(tmp_path):
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
