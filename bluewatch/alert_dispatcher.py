"""Alert dispatcher: threshold check, deduplication, Resend API email send.

Implements T07 (SQLite alert log) and T08 (alert dispatch) per FR-15 through FR-19.
"""

import datetime
import os
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import resend

from bluewatch.config import Zone

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).parent.parent / "data" / "alert_log.db"
GAP_DAYS_THRESHOLD = 3  # consecutive CLOUD_GAP days before gap notification (FR-04)

AlertType = Literal["anomaly", "gap"]


# ---------------------------------------------------------------------------
# Data transfer object
# ---------------------------------------------------------------------------


@dataclass
class ZoneResult:
    """Output of the anomaly engine for a single zone on a single day."""

    zone: Zone
    date: datetime.date
    status: Literal["DATA_AVAILABLE", "CLOUD_GAP"]
    anomaly_ratio: float | None       # None when status == CLOUD_GAP
    zone_avg_chl: float | None        # mg/m³; None when status == CLOUD_GAP
    climatology_mean_chl: float | None  # mg/m³; None when status == CLOUD_GAP
    valid_pixel_count: int


# ---------------------------------------------------------------------------
# SQLite alert log (T07)
# ---------------------------------------------------------------------------


def init_db(db_path: Path = DB_PATH) -> None:
    """Create data directory and alert_log table if absent (T07)."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_log (
                zone_name  TEXT NOT NULL,
                alert_date DATE NOT NULL,
                alert_type TEXT NOT NULL,
                PRIMARY KEY (zone_name, alert_date, alert_type)
            )
            """
        )


def already_sent(
    zone_name: str,
    alert_date: datetime.date,
    alert_type: AlertType,
    *,
    db_path: Path = DB_PATH,
) -> bool:
    """Return True if an alert was already logged for this zone/date/type (AC-06)."""
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM alert_log WHERE zone_name = ? AND alert_date = ? AND alert_type = ?",
            (zone_name, alert_date.isoformat(), alert_type),
        ).fetchone()
    return row is not None


def record_sent(
    zone_name: str,
    alert_date: datetime.date,
    alert_type: AlertType,
    *,
    db_path: Path = DB_PATH,
) -> None:
    """Insert a deduplication record; silently no-ops if already present."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO alert_log (zone_name, alert_date, alert_type) VALUES (?, ?, ?)",
            (zone_name, alert_date.isoformat(), alert_type),
        )


# ---------------------------------------------------------------------------
# Resend API helpers
# ---------------------------------------------------------------------------


def _require_resend_key() -> str:
    """Return RESEND_API_KEY from env; sys.exit on missing (mirrors AC-09 pattern)."""
    key = os.environ.get("RESEND_API_KEY")
    if not key:
        sys.exit("ERROR: RESEND_API_KEY environment variable must be set.")
    return key


def _send_email(
    *,
    api_key: str,
    to: str,
    subject: str,
    body: str,
) -> None:
    """Send a plain-text email via the Resend API.

    Raises RuntimeError on any Resend API failure (AC-10: do not silently drop alerts).
    """
    from_addr = os.environ.get("BLUEWATCH_FROM_EMAIL", "BlueWatch Alerts <alerts@bluewatch.io>")
    resend.api_key = api_key
    try:
        resend.Emails.send(
            {
                "from": from_addr,
                "to": [to],
                "subject": subject,
                "text": body,
            }
        )
    except Exception as exc:
        raise RuntimeError(f"Resend API failure sending to {to!r}: {exc}") from exc


# ---------------------------------------------------------------------------
# Public dispatch functions
# ---------------------------------------------------------------------------


def dispatch_anomaly_alert(
    result: ZoneResult,
    *,
    db_path: Path = DB_PATH,
) -> bool:
    """Send an anomaly alert email if threshold is met and not already sent today.

    Returns True if an email was sent, False if the alert was skipped.
    Raises RuntimeError on Resend API failure (AC-10).
    """
    if result.status != "DATA_AVAILABLE":
        return False
    if result.anomaly_ratio is None:
        return False
    if result.anomaly_ratio < result.zone.threshold_multiplier:
        return False

    init_db(db_path)
    if already_sent(result.zone.name, result.date, "anomaly", db_path=db_path):
        return False

    api_key = _require_resend_key()
    subject = f"[BlueWatch] Chl-a anomaly alert — {result.zone.name} — {result.date}"
    _send_email(
        api_key=api_key,
        to=result.zone.alert_email,
        subject=subject,
        body=_format_anomaly_body(result),
    )
    record_sent(result.zone.name, result.date, "anomaly", db_path=db_path)
    return True


def dispatch_gap_notification(
    zone: Zone,
    date: datetime.date,
    consecutive_gap_days: int,
    *,
    db_path: Path = DB_PATH,
) -> bool:
    """Send a gap notification if consecutive_gap_days >= threshold and not sent today.

    Returns True if an email was sent, False if skipped.
    Raises RuntimeError on Resend API failure (AC-10).
    """
    if consecutive_gap_days < GAP_DAYS_THRESHOLD:
        return False

    init_db(db_path)
    if already_sent(zone.name, date, "gap", db_path=db_path):
        return False

    api_key = _require_resend_key()
    subject = (
        f"[BlueWatch] Data gap notification — {zone.name} — {date} "
        f"({consecutive_gap_days} consecutive days)"
    )
    _send_email(
        api_key=api_key,
        to=zone.alert_email,
        subject=subject,
        body=_format_gap_body(zone, date, consecutive_gap_days),
    )
    record_sent(zone.name, date, "gap", db_path=db_path)
    return True


# ---------------------------------------------------------------------------
# Email body formatters
# ---------------------------------------------------------------------------


def _format_anomaly_body(result: ZoneResult) -> str:
    chl = f"{result.zone_avg_chl:.4f}" if result.zone_avg_chl is not None else "N/A"
    clim = (
        f"{result.climatology_mean_chl:.4f}" if result.climatology_mean_chl is not None else "N/A"
    )
    ratio = f"{result.anomaly_ratio:.2f}" if result.anomaly_ratio is not None else "N/A"
    return (
        f"BlueWatch Anomaly Alert\n"
        f"=======================\n\n"
        f"Zone:                {result.zone.name}\n"
        f"Date:                {result.date}\n"
        f"Zone avg Chl-a:      {chl} mg/m\u00b3\n"
        f"Climatological mean: {clim} mg/m\u00b3\n"
        f"Anomaly ratio:       {ratio}x\n"
        f"Valid pixels:        {result.valid_pixel_count}\n\n"
        f"The chlorophyll-a concentration in {result.zone.name} is "
        f"{result.anomaly_ratio:.1f}x the seasonal climatological mean for this calendar week, "
        f"which meets or exceeds the configured alert threshold of "
        f"{result.zone.threshold_multiplier:.1f}x.\n\n"
        f"This is an automated alert from the BlueWatch monitoring system."
    )


def _format_gap_body(zone: Zone, date: datetime.date, consecutive_gap_days: int) -> str:
    return (
        f"BlueWatch Data Gap Notification\n"
        f"================================\n\n"
        f"Zone:               {zone.name}\n"
        f"Date:               {date}\n"
        f"Consecutive gaps:   {consecutive_gap_days} days\n\n"
        f"{zone.name} has had insufficient valid satellite Chl-a data (cloud cover or "
        f"sensor gap) for {consecutive_gap_days} consecutive days. No anomaly assessment "
        f"can be made during this period.\n\n"
        f"This is an automated gap notification from the BlueWatch monitoring system."
    )
