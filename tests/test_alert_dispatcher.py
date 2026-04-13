"""Tests for bluewatch/alert_dispatcher.py (T07/T08)."""

import datetime
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from shapely.geometry import box

import bluewatch.alert_dispatcher as disp_mod
from bluewatch.alert_dispatcher import (
    GAP_DAYS_THRESHOLD,
    ZoneResult,
    _format_anomaly_body,
    _format_gap_body,
    _send_email,
    already_sent,
    dispatch_anomaly_alert,
    dispatch_gap_notification,
    init_db,
    record_sent,
)
from bluewatch.config import Zone

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TODAY = datetime.date(2024, 6, 10)


def _zone(
    name: str = "Test Zone",
    threshold: float = 3.0,
    email: str = "ops@example.com",
) -> Zone:
    return Zone(
        name=name,
        description="A test zone",
        polygon=box(-10.0, 53.0, -9.5, 53.5),
        threshold_multiplier=threshold,
        alert_email=email,
    )


def _result(
    zone: Zone | None = None,
    status: str = "DATA_AVAILABLE",
    anomaly_ratio: float | None = 4.0,
    zone_avg_chl: float | None = 2.0,
    climatology_mean_chl: float | None = 0.5,
    valid_pixel_count: int = 100,
    date: datetime.date = TODAY,
) -> ZoneResult:
    return ZoneResult(
        zone=zone or _zone(),
        date=date,
        status=status,  # type: ignore[arg-type]
        anomaly_ratio=anomaly_ratio,
        zone_avg_chl=zone_avg_chl,
        climatology_mean_chl=climatology_mean_chl,
        valid_pixel_count=valid_pixel_count,
    )


# ---------------------------------------------------------------------------
# init_db
# ---------------------------------------------------------------------------


def test_init_db_creates_table(tmp_path: Path) -> None:
    db = tmp_path / "alert_log.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='alert_log'"
        ).fetchall()
    assert len(rows) == 1


def test_init_db_creates_parent_dirs(tmp_path: Path) -> None:
    db = tmp_path / "nested" / "dir" / "alert_log.db"
    init_db(db)
    assert db.exists()


def test_init_db_idempotent(tmp_path: Path) -> None:
    db = tmp_path / "alert_log.db"
    init_db(db)
    init_db(db)  # second call must not raise


# ---------------------------------------------------------------------------
# already_sent / record_sent
# ---------------------------------------------------------------------------


def test_not_sent_by_default(tmp_path: Path) -> None:
    db = tmp_path / "alert_log.db"
    init_db(db)
    assert not already_sent("Zone A", TODAY, "anomaly", db_path=db)


def test_record_then_already_sent(tmp_path: Path) -> None:
    db = tmp_path / "alert_log.db"
    init_db(db)
    record_sent("Zone A", TODAY, "anomaly", db_path=db)
    assert already_sent("Zone A", TODAY, "anomaly", db_path=db)


def test_different_type_not_deduped(tmp_path: Path) -> None:
    db = tmp_path / "alert_log.db"
    init_db(db)
    record_sent("Zone A", TODAY, "anomaly", db_path=db)
    assert not already_sent("Zone A", TODAY, "gap", db_path=db)


def test_different_zone_not_deduped(tmp_path: Path) -> None:
    db = tmp_path / "alert_log.db"
    init_db(db)
    record_sent("Zone A", TODAY, "anomaly", db_path=db)
    assert not already_sent("Zone B", TODAY, "anomaly", db_path=db)


def test_different_date_not_deduped(tmp_path: Path) -> None:
    db = tmp_path / "alert_log.db"
    init_db(db)
    record_sent("Zone A", TODAY, "anomaly", db_path=db)
    tomorrow = TODAY + datetime.timedelta(days=1)
    assert not already_sent("Zone A", tomorrow, "anomaly", db_path=db)


def test_record_sent_idempotent(tmp_path: Path) -> None:
    db = tmp_path / "alert_log.db"
    init_db(db)
    record_sent("Zone A", TODAY, "anomaly", db_path=db)
    record_sent("Zone A", TODAY, "anomaly", db_path=db)  # INSERT OR IGNORE — must not raise
    assert already_sent("Zone A", TODAY, "anomaly", db_path=db)


# ---------------------------------------------------------------------------
# _send_email
# ---------------------------------------------------------------------------


def test_send_email_calls_resend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BLUEWATCH_FROM_EMAIL", "test@bluewatch.io")
    sent: list[dict[str, Any]] = []

    def fake_send(params: dict[str, Any]) -> dict[str, str]:
        sent.append(params)
        return {"id": "abc123"}

    monkeypatch.setattr(disp_mod.resend.Emails, "send", fake_send)
    _send_email(api_key="re_test", to="user@example.com", subject="Hi", body="Body text")
    assert len(sent) == 1
    assert sent[0]["to"] == ["user@example.com"]
    assert sent[0]["subject"] == "Hi"
    assert sent[0]["text"] == "Body text"
    assert sent[0]["from"] == "test@bluewatch.io"


def test_send_email_raises_on_resend_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(params: dict[str, Any]) -> None:
        raise Exception("network error")

    monkeypatch.setattr(disp_mod.resend.Emails, "send", boom)
    with pytest.raises(RuntimeError, match="Resend API failure"):
        _send_email(api_key="re_test", to="user@example.com", subject="Hi", body="Body")


# ---------------------------------------------------------------------------
# dispatch_anomaly_alert — threshold checks (AC-05)
# ---------------------------------------------------------------------------


def test_anomaly_alert_sent_when_threshold_met(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    sent: list[dict[str, Any]] = []
    monkeypatch.setattr(disp_mod.resend.Emails, "send", lambda p: sent.append(p) or {"id": "x"})

    db = tmp_path / "alert_log.db"
    result = _result(anomaly_ratio=3.5, zone=_zone(threshold=3.0))
    sent_flag = dispatch_anomaly_alert(result, db_path=db)

    assert sent_flag is True
    assert len(sent) == 1
    assert "anomaly" in sent[0]["subject"].lower()


def test_anomaly_alert_not_sent_below_threshold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    sent: list[dict[str, Any]] = []
    monkeypatch.setattr(disp_mod.resend.Emails, "send", lambda p: sent.append(p) or {"id": "x"})

    db = tmp_path / "alert_log.db"
    result = _result(anomaly_ratio=2.9, zone=_zone(threshold=3.0))
    sent_flag = dispatch_anomaly_alert(result, db_path=db)

    assert sent_flag is False
    assert len(sent) == 0


def test_anomaly_alert_sent_at_exact_threshold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    sent: list[dict[str, Any]] = []
    monkeypatch.setattr(disp_mod.resend.Emails, "send", lambda p: sent.append(p) or {"id": "x"})

    db = tmp_path / "alert_log.db"
    result = _result(anomaly_ratio=3.0, zone=_zone(threshold=3.0))
    assert dispatch_anomaly_alert(result, db_path=db) is True
    assert len(sent) == 1


def test_anomaly_alert_skipped_for_cloud_gap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    sent: list[dict[str, Any]] = []
    monkeypatch.setattr(disp_mod.resend.Emails, "send", lambda p: sent.append(p) or {"id": "x"})

    db = tmp_path / "alert_log.db"
    result = _result(status="CLOUD_GAP", anomaly_ratio=None, zone_avg_chl=None)
    assert dispatch_anomaly_alert(result, db_path=db) is False
    assert len(sent) == 0


def test_anomaly_alert_skipped_when_none_ratio(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    sent: list[dict[str, Any]] = []
    monkeypatch.setattr(disp_mod.resend.Emails, "send", lambda p: sent.append(p) or {"id": "x"})

    db = tmp_path / "alert_log.db"
    result = _result(status="DATA_AVAILABLE", anomaly_ratio=None)
    assert dispatch_anomaly_alert(result, db_path=db) is False
    assert len(sent) == 0


# ---------------------------------------------------------------------------
# dispatch_anomaly_alert — deduplication (AC-06)
# ---------------------------------------------------------------------------


def test_anomaly_alert_not_sent_twice_same_day(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    sent: list[dict[str, Any]] = []
    monkeypatch.setattr(disp_mod.resend.Emails, "send", lambda p: sent.append(p) or {"id": "x"})

    db = tmp_path / "alert_log.db"
    result = _result(anomaly_ratio=5.0)
    assert dispatch_anomaly_alert(result, db_path=db) is True
    assert dispatch_anomaly_alert(result, db_path=db) is False
    assert len(sent) == 1


def test_anomaly_alert_sent_again_next_day(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    sent: list[dict[str, Any]] = []
    monkeypatch.setattr(disp_mod.resend.Emails, "send", lambda p: sent.append(p) or {"id": "x"})

    db = tmp_path / "alert_log.db"
    result_today = _result(anomaly_ratio=5.0, date=TODAY)
    result_tomorrow = _result(anomaly_ratio=5.0, date=TODAY + datetime.timedelta(days=1))

    assert dispatch_anomaly_alert(result_today, db_path=db) is True
    assert dispatch_anomaly_alert(result_tomorrow, db_path=db) is True
    assert len(sent) == 2


# ---------------------------------------------------------------------------
# dispatch_anomaly_alert — Resend failure (AC-10)
# ---------------------------------------------------------------------------


def test_anomaly_alert_raises_on_resend_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    monkeypatch.setattr(
        disp_mod.resend.Emails,
        "send",
        lambda p: (_ for _ in ()).throw(Exception("timeout")),
    )

    db = tmp_path / "alert_log.db"
    result = _result(anomaly_ratio=5.0)
    with pytest.raises(RuntimeError, match="Resend API failure"):
        dispatch_anomaly_alert(result, db_path=db)


def test_anomaly_alert_not_recorded_on_resend_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If Resend fails, the dedup record must NOT be written (so next run can retry)."""
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    monkeypatch.setattr(
        disp_mod.resend.Emails,
        "send",
        lambda p: (_ for _ in ()).throw(Exception("timeout")),
    )

    db = tmp_path / "alert_log.db"
    result = _result(anomaly_ratio=5.0)
    with pytest.raises(RuntimeError):
        dispatch_anomaly_alert(result, db_path=db)

    assert not already_sent(result.zone.name, result.date, "anomaly", db_path=db)


def test_anomaly_alert_exits_on_missing_api_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("RESEND_API_KEY", raising=False)

    db = tmp_path / "alert_log.db"
    result = _result(anomaly_ratio=5.0)
    with pytest.raises(SystemExit):
        dispatch_anomaly_alert(result, db_path=db)


# ---------------------------------------------------------------------------
# dispatch_gap_notification (AC-08)
# ---------------------------------------------------------------------------


def test_gap_notification_sent_at_threshold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    sent: list[dict[str, Any]] = []
    monkeypatch.setattr(disp_mod.resend.Emails, "send", lambda p: sent.append(p) or {"id": "x"})

    db = tmp_path / "alert_log.db"
    zone = _zone()
    sent_flag = dispatch_gap_notification(zone, TODAY, GAP_DAYS_THRESHOLD, db_path=db)

    assert sent_flag is True
    assert len(sent) == 1
    assert "gap" in sent[0]["subject"].lower()


def test_gap_notification_not_sent_below_threshold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    sent: list[dict[str, Any]] = []
    monkeypatch.setattr(disp_mod.resend.Emails, "send", lambda p: sent.append(p) or {"id": "x"})

    db = tmp_path / "alert_log.db"
    zone = _zone()
    assert dispatch_gap_notification(zone, TODAY, GAP_DAYS_THRESHOLD - 1, db_path=db) is False
    assert len(sent) == 0


def test_gap_notification_not_sent_twice_same_day(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    sent: list[dict[str, Any]] = []
    monkeypatch.setattr(disp_mod.resend.Emails, "send", lambda p: sent.append(p) or {"id": "x"})

    db = tmp_path / "alert_log.db"
    zone = _zone()
    assert dispatch_gap_notification(zone, TODAY, GAP_DAYS_THRESHOLD, db_path=db) is True
    assert dispatch_gap_notification(zone, TODAY, GAP_DAYS_THRESHOLD, db_path=db) is False
    assert len(sent) == 1


def test_gap_notification_raises_on_resend_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    monkeypatch.setattr(
        disp_mod.resend.Emails,
        "send",
        lambda p: (_ for _ in ()).throw(Exception("timeout")),
    )

    db = tmp_path / "alert_log.db"
    zone = _zone()
    with pytest.raises(RuntimeError, match="Resend API failure"):
        dispatch_gap_notification(zone, TODAY, GAP_DAYS_THRESHOLD, db_path=db)


def test_gap_not_recorded_on_resend_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    monkeypatch.setattr(
        disp_mod.resend.Emails,
        "send",
        lambda p: (_ for _ in ()).throw(Exception("timeout")),
    )

    db = tmp_path / "alert_log.db"
    zone = _zone()
    with pytest.raises(RuntimeError):
        dispatch_gap_notification(zone, TODAY, GAP_DAYS_THRESHOLD, db_path=db)

    assert not already_sent(zone.name, TODAY, "gap", db_path=db)


def test_gap_notification_subject_distinct_from_anomaly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FR-18: gap emails must be distinguishable from anomaly emails (AC-08)."""
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    sent: list[dict[str, Any]] = []
    monkeypatch.setattr(disp_mod.resend.Emails, "send", lambda p: sent.append(p) or {"id": "x"})

    db = tmp_path / "alert_log.db"
    zone = _zone()
    dispatch_gap_notification(zone, TODAY, GAP_DAYS_THRESHOLD, db_path=db)
    subject = sent[0]["subject"]
    assert "gap" in subject.lower()
    assert "anomaly" not in subject.lower()


# ---------------------------------------------------------------------------
# Email body formatters
# ---------------------------------------------------------------------------


def test_anomaly_body_contains_required_fields() -> None:
    result = _result()
    body = _format_anomaly_body(result)
    assert result.zone.name in body
    assert str(result.date) in body
    assert "2.0000" in body  # zone_avg_chl
    assert "0.5000" in body  # climatology_mean_chl
    assert "4.00" in body    # anomaly_ratio
    assert "100" in body     # valid_pixel_count
    assert "3.0" in body     # threshold_multiplier


def test_gap_body_contains_required_fields() -> None:
    zone = _zone()
    body = _format_gap_body(zone, TODAY, 4)
    assert zone.name in body
    assert str(TODAY) in body
    assert "4" in body
    assert "gap" in body.lower()


def test_gap_body_does_not_mention_anomaly() -> None:
    """FR-18: gap body must not read like an anomaly alert."""
    zone = _zone()
    body = _format_gap_body(zone, TODAY, 3)
    assert "anomaly alert" not in body.lower()
