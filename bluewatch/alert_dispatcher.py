"""Alert dispatch and deduplication helpers (FR-15 through FR-19)."""

from __future__ import annotations

import importlib
import json
import os
import sqlite3
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from typing import Any
from urllib import error, request
from urllib.parse import unquote, urlparse

from bluewatch.anomaly_engine import ZoneResult
from bluewatch.config import Zone

ALERT_LOG_PATH = Path(__file__).parent.parent / "data" / "alert_log.db"
DEFAULT_FROM_EMAIL = "BlueWatch Alerts <alerts@bluewatch.io>"
RESEND_API_URL = "https://api.resend.com/emails"
GAP_DAYS_THRESHOLD = 3

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


def _load_psycopg() -> Any:
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


def dispatch_anomaly_alert(
    zone: Zone,
    result: ZoneResult,
    *,
    alert_date: date,
    observed_date: date,
    db_path: Path = ALERT_LOG_PATH,
    database_url: str | None = None,
) -> bool:
    """Send an anomaly alert when the zone breaches threshold and was not already sent."""
    if result.status != "DATA_AVAILABLE" or result.anomaly_ratio is None:
        return False

    if result.anomaly_ratio < zone.threshold_multiplier:
        return False

    if has_alert_been_logged(
        zone.name,
        alert_date,
        "anomaly",
        db_path=db_path,
        database_url=database_url,
    ):
        return False

    api_key = require_resend_api_key()
    _send_email(
        api_key=api_key,
        to=zone.alert_email,
        subject=f"[BlueWatch] Chl-a anomaly alert - {zone.name} - {alert_date.isoformat()}",
        body=_format_anomaly_body(zone, result, alert_date=alert_date, observed_date=observed_date),
    )
    record_alert(
        zone.name,
        alert_date,
        "anomaly",
        db_path=db_path,
        database_url=database_url,
    )
    return True


def dispatch_gap_notification(
    zone: Zone,
    *,
    alert_date: date,
    consecutive_gap_days: int,
    db_path: Path = ALERT_LOG_PATH,
    database_url: str | None = None,
) -> bool:
    """Send a gap notification when the zone reaches the configured gap streak."""
    if consecutive_gap_days < GAP_DAYS_THRESHOLD:
        return False

    if has_alert_been_logged(
        zone.name,
        alert_date,
        "gap",
        db_path=db_path,
        database_url=database_url,
    ):
        return False

    api_key = require_resend_api_key()
    _send_email(
        api_key=api_key,
        to=zone.alert_email,
        subject=f"[BlueWatch] Data gap notification - {zone.name} - {alert_date.isoformat()}",
        body=_format_gap_body(
            zone,
            alert_date=alert_date,
            consecutive_gap_days=consecutive_gap_days,
        ),
    )
    record_alert(
        zone.name,
        alert_date,
        "gap",
        db_path=db_path,
        database_url=database_url,
    )
    return True


def require_resend_api_key() -> str:
    """Return the configured Resend API key or exit loudly if absent."""
    api_key = os.getenv("RESEND_API_KEY")
    if not api_key:
        raise SystemExit("ERROR: RESEND_API_KEY environment variable must be set.")

    return api_key


def _send_email(
    *,
    api_key: str,
    to: str,
    subject: str,
    body: str,
    from_email: str | None = None,
) -> None:
    payload: dict[str, object] = {
        "from": from_email or os.getenv("BLUEWATCH_FROM_EMAIL", DEFAULT_FROM_EMAIL),
        "to": [to],
        "subject": subject,
        "text": body,
    }
    req = request.Request(
        RESEND_API_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with request.urlopen(req) as response:
            status = getattr(response, "status", 200)
            if status >= 400:
                raise RuntimeError(f"Resend API failure sending to {to!r}: HTTP {status}")
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Resend API failure sending to {to!r}: HTTP {exc.code} {details}"
        ) from exc
    except error.URLError as exc:
        raise RuntimeError(f"Resend API failure sending to {to!r}: {exc.reason}") from exc


def _format_anomaly_body(
    zone: Zone,
    result: ZoneResult,
    *,
    alert_date: date,
    observed_date: date,
) -> str:
    zone_avg = f"{result.zone_avg_chl:.4f}" if result.zone_avg_chl is not None else "N/A"
    ratio = f"{result.anomaly_ratio:.2f}" if result.anomaly_ratio is not None else "N/A"
    climatology_mean = (
        f"{result.climatology_mean_chl:.4f}"
        if result.climatology_mean_chl is not None
        else "N/A"
    )

    return (
        "BlueWatch Anomaly Alert\n"
        "=======================\n\n"
        f"Zone:                {zone.name}\n"
        f"Alert date:          {alert_date.isoformat()}\n"
        f"Observed data date:  {observed_date.isoformat()}\n"
        f"Zone avg Chl-a:      {zone_avg} mg/m3\n"
        f"Climatology mean:    {climatology_mean} mg/m3\n"
        f"Anomaly ratio:       {ratio}x\n"
        f"Valid pixels:        {result.valid_pixel_count}\n\n"
        f"The chlorophyll-a concentration in {zone.name} meets or exceeds the configured "
        f"alert threshold of {zone.threshold_multiplier:.1f}x seasonal normal.\n"
    )


def _format_gap_body(zone: Zone, *, alert_date: date, consecutive_gap_days: int) -> str:
    return (
        "BlueWatch Data Gap Notification\n"
        "===============================\n\n"
        f"Zone:              {zone.name}\n"
        f"Alert date:        {alert_date.isoformat()}\n"
        f"Consecutive gaps:  {consecutive_gap_days} days\n\n"
        f"{zone.name} has had insufficient valid satellite Chl-a coverage for "
        f"{consecutive_gap_days} consecutive days, so no anomaly assessment can be made.\n"
    )
