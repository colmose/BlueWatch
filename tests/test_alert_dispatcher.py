"""Tests for bluewatch/alert_dispatcher.py (T07)."""

import sqlite3
from datetime import date

from bluewatch.alert_dispatcher import (
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
