# mypy: disable-error-code=no-untyped-def
"""Subprocess-only patches for offline BlueWatch CLI end-to-end tests."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Literal
from urllib import error

import bluewatch.alert_dispatcher as alert_dispatcher
import bluewatch.anomaly_engine as anomaly_engine
import bluewatch.ingest as ingest

_CHL_FIXTURE = Path(os.environ["BLUEWATCH_E2E_CHL_FIXTURE"])
_CLIM_FIXTURE = Path(os.environ["BLUEWATCH_E2E_CLIM_FIXTURE"])
_MASK_FIXTURE = Path(os.environ["BLUEWATCH_E2E_MASK_FIXTURE"])
_EMAIL_CAPTURE = Path(os.environ["BLUEWATCH_E2E_EMAIL_CAPTURE"])
_EMAIL_MODE = os.environ.get("BLUEWATCH_E2E_EMAIL_MODE", "success")

_ORIGINAL_COMPUTE_ZONE_RESULTS = anomaly_engine.compute_zone_results
_ORIGINAL_FETCH_LATEST_CHL = ingest.fetch_latest_chl


def _fake_download_subset(username: str, password: str, date_str: str, out_file: Path) -> None:
    shutil.copyfile(_CHL_FIXTURE, out_file)


def _fetch_latest_chl_with_fixture_date(run_date=None):
    dataset = _ORIGINAL_FETCH_LATEST_CHL(run_date)
    if run_date is None:
        return dataset
    return dataset.assign_coords(time=[run_date.isoformat()])


def _compute_zone_results_with_fixtures(chl_ds, zones, run_date, *, clim_path=None, mask_path=None):
    return _ORIGINAL_COMPUTE_ZONE_RESULTS(
        chl_ds,
        zones,
        run_date,
        clim_path=_CLIM_FIXTURE,
        mask_path=_MASK_FIXTURE,
    )


class _FakeResponse:
    status = 202

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> Literal[False]:
        return False


def _fake_urlopen(req):
    if _EMAIL_MODE == "fail":
        raise error.URLError("simulated resend outage")

    payload = json.loads(req.data.decode("utf-8"))
    _EMAIL_CAPTURE.parent.mkdir(parents=True, exist_ok=True)
    with _EMAIL_CAPTURE.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "url": req.full_url,
                    "headers": dict(req.header_items()),
                    "payload": payload,
                }
            )
            + "\n"
        )
    return _FakeResponse()


ingest._download_subset = _fake_download_subset
ingest.fetch_latest_chl = _fetch_latest_chl_with_fixture_date
anomaly_engine.compute_zone_results = _compute_zone_results_with_fixtures
alert_dispatcher.request.urlopen = _fake_urlopen  # type: ignore[attr-defined]
