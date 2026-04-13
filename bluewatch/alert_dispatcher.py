"""SQLite-backed alert log helpers for alert deduplication (FR-17)."""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

ALERT_LOG_PATH = Path(__file__).parent.parent / "data" / "alert_log.db"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS alert_log (
    zone_name TEXT NOT NULL,
    alert_date DATE NOT NULL,
    alert_type TEXT NOT NULL,
    PRIMARY KEY(zone_name, alert_date, alert_type)
)
"""


def initialize_alert_log(db_path: Path = ALERT_LOG_PATH) -> Path:
    """Create the alert log database and schema if they do not already exist."""
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        conn.execute(SCHEMA_SQL)
        conn.commit()

    return db_path


def has_alert_been_logged(
    zone_name: str,
    alert_date: date,
    alert_type: str,
    *,
    db_path: Path = ALERT_LOG_PATH,
) -> bool:
    """Return whether an alert record already exists for the composite key."""
    initialize_alert_log(db_path)

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM alert_log
            WHERE zone_name = ? AND alert_date = ? AND alert_type = ?
            LIMIT 1
            """,
            (zone_name, alert_date.isoformat(), alert_type),
        ).fetchone()

    return row is not None


def record_alert(
    zone_name: str,
    alert_date: date,
    alert_type: str,
    *,
    db_path: Path = ALERT_LOG_PATH,
) -> bool:
    """Insert a deduplication record. Returns False when the record already exists."""
    initialize_alert_log(db_path)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO alert_log(zone_name, alert_date, alert_type)
            VALUES(?, ?, ?)
            """,
            (zone_name, alert_date.isoformat(), alert_type),
        )
        conn.commit()

    return cursor.rowcount == 1
