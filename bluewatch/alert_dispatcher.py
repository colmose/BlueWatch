"""Alert log helpers for alert deduplication (FR-17)."""

from __future__ import annotations

import importlib
import os
import sqlite3
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from urllib.parse import unquote, urlparse

ALERT_LOG_PATH = Path(__file__).parent.parent / "data" / "alert_log.db"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS alert_log (
    zone_name TEXT NOT NULL,
    alert_date DATE NOT NULL,
    alert_type TEXT NOT NULL,
    PRIMARY KEY(zone_name, alert_date, alert_type)
)
"""

SQLITE_INSERT_SQL = """
INSERT OR IGNORE INTO alert_log(zone_name, alert_date, alert_type)
VALUES(?, ?, ?)
"""

POSTGRES_INSERT_SQL = """
INSERT INTO alert_log(zone_name, alert_date, alert_type)
VALUES(%s, %s, %s)
ON CONFLICT DO NOTHING
"""

SELECT_EXISTS_SQL = """
SELECT 1
FROM alert_log
WHERE zone_name = {placeholder} AND alert_date = {placeholder} AND alert_type = {placeholder}
LIMIT 1
"""


class AlertLogStore(ABC):
    """Backend interface for alert deduplication storage."""

    @abstractmethod
    def initialize(self) -> Path | str:
        """Create the backing store if needed and return its location."""

    @abstractmethod
    def has_alert_been_logged(self, zone_name: str, alert_date: date, alert_type: str) -> bool:
        """Return whether an alert record already exists for the composite key."""

    @abstractmethod
    def record_alert(self, zone_name: str, alert_date: date, alert_type: str) -> bool:
        """Insert a deduplication record and return whether it was newly created."""


class SQLiteAlertLogStore(AlertLogStore):
    """SQLite-backed alert log store."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def initialize(self) -> Path:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(SCHEMA_SQL)
            conn.commit()

        return self.db_path

    def has_alert_been_logged(self, zone_name: str, alert_date: date, alert_type: str) -> bool:
        self.initialize()

        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                SELECT_EXISTS_SQL.format(placeholder="?"),
                (zone_name, alert_date.isoformat(), alert_type),
            ).fetchone()

        return row is not None

    def record_alert(self, zone_name: str, alert_date: date, alert_type: str) -> bool:
        self.initialize()

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                SQLITE_INSERT_SQL,
                (zone_name, alert_date.isoformat(), alert_type),
            )
            conn.commit()

        return cursor.rowcount == 1


def _load_psycopg():
    try:
        return importlib.import_module("psycopg")
    except ImportError as exc:
        raise ImportError(
            "psycopg is required for postgres alert logs. Install psycopg[binary] "
            "or unset DATABASE_URL to keep using SQLite."
        ) from exc


class PostgresAlertLogStore(AlertLogStore):
    """Postgres-backed alert log store."""

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._psycopg = _load_psycopg()

    def initialize(self) -> str:
        with self._psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute(SCHEMA_SQL)
            conn.commit()

        return self.database_url

    def has_alert_been_logged(self, zone_name: str, alert_date: date, alert_type: str) -> bool:
        self.initialize()

        with self._psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    SELECT_EXISTS_SQL.format(placeholder="%s"),
                    (zone_name, alert_date.isoformat(), alert_type),
                )
                row = cursor.fetchone()

        return row is not None

    def record_alert(self, zone_name: str, alert_date: date, alert_type: str) -> bool:
        self.initialize()

        with self._psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    POSTGRES_INSERT_SQL,
                    (zone_name, alert_date.isoformat(), alert_type),
                )
                inserted = int(cursor.rowcount) == 1
            conn.commit()

        return inserted


def _resolve_sqlite_path(database_url: str) -> Path:
    parsed = urlparse(database_url)
    if parsed.netloc:
        raise ValueError("Unsupported sqlite DATABASE_URL. Use sqlite:///absolute/path/to.db")

    return Path(unquote(parsed.path))


def get_alert_log_store(
    database_url: str | None = None,
    *,
    db_path: Path = ALERT_LOG_PATH,
) -> AlertLogStore:
    """Resolve the configured alert log backend."""
    resolved_url = database_url if database_url is not None else os.getenv("DATABASE_URL")

    if not resolved_url:
        return SQLiteAlertLogStore(db_path)

    if resolved_url.startswith("sqlite:///"):
        return SQLiteAlertLogStore(_resolve_sqlite_path(resolved_url))

    if resolved_url.startswith(("postgres://", "postgresql://")):
        return PostgresAlertLogStore(resolved_url)

    raise ValueError(
        "Unsupported DATABASE_URL scheme. Supported values are sqlite:///, "
        "postgres://, and postgresql://."
    )


def initialize_alert_log(
    db_path: Path = ALERT_LOG_PATH,
    *,
    database_url: str | None = None,
) -> Path | str:
    """Create the alert log backend if it does not already exist."""
    return get_alert_log_store(database_url=database_url, db_path=db_path).initialize()


def has_alert_been_logged(
    zone_name: str,
    alert_date: date,
    alert_type: str,
    *,
    db_path: Path = ALERT_LOG_PATH,
    database_url: str | None = None,
) -> bool:
    """Return whether an alert record already exists for the composite key."""
    return get_alert_log_store(
        database_url=database_url,
        db_path=db_path,
    ).has_alert_been_logged(zone_name, alert_date, alert_type)


def record_alert(
    zone_name: str,
    alert_date: date,
    alert_type: str,
    *,
    db_path: Path = ALERT_LOG_PATH,
    database_url: str | None = None,
) -> bool:
    """Insert a deduplication record. Returns False when the record already exists."""
    return get_alert_log_store(
        database_url=database_url,
        db_path=db_path,
    ).record_alert(zone_name, alert_date, alert_type)
