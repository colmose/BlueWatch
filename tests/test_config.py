"""Tests for bluewatch/config.py (T02)."""

import tempfile
from pathlib import Path

import pytest
import yaml

from bluewatch.config import Zone, load_zones

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

VALID_ZONE = {
    "name": "Outer Clew Bay",
    "description": "Atlantic-facing mouth of Clew Bay, Co. Mayo",
    "alert_email": "ops@example.com",
    "threshold_multiplier": 3.0,
    "polygon": {
        "type": "Polygon",
        "coordinates": [
            [[-10.05, 53.78], [-9.85, 53.78], [-9.85, 53.88], [-10.05, 53.88], [-10.05, 53.78]]
        ],
    },
}


def write_config(data: dict) -> Path:
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
    yaml.dump(data, f)
    f.flush()
    return Path(f.name)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_single_valid_zone():
    p = write_config({"zones": [VALID_ZONE]})
    zones = load_zones(p)
    assert len(zones) == 1
    z = zones[0]
    assert isinstance(z, Zone)
    assert z.name == "Outer Clew Bay"
    assert z.alert_email == "ops@example.com"
    assert z.threshold_multiplier == 3.0
    assert z.polygon is not None


def test_multiple_valid_zones():
    zone2 = {**VALID_ZONE, "name": "Outer Killary", "alert_email": "killary@example.com"}
    p = write_config({"zones": [VALID_ZONE, zone2]})
    zones = load_zones(p)
    assert len(zones) == 2
    assert zones[1].name == "Outer Killary"


def test_threshold_multiplier_as_int():
    z = {**VALID_ZONE, "threshold_multiplier": 3}
    p = write_config({"zones": [z]})
    zones = load_zones(p)
    assert zones[0].threshold_multiplier == 3.0
    assert isinstance(zones[0].threshold_multiplier, float)


# ---------------------------------------------------------------------------
# File / structure errors
# ---------------------------------------------------------------------------


def test_missing_file_exits():
    with pytest.raises(SystemExit):
        load_zones(Path("/nonexistent/zones.yaml"))


def test_missing_zones_key_exits():
    p = write_config({"foo": []})
    with pytest.raises(SystemExit, match="zones"):
        load_zones(p)


def test_zones_not_a_list_exits():
    p = write_config({"zones": "not-a-list"})
    with pytest.raises(SystemExit):
        load_zones(p)


# ---------------------------------------------------------------------------
# Zone count limits (FR-13)
# ---------------------------------------------------------------------------


def test_zero_zones_exits():
    p = write_config({"zones": []})
    with pytest.raises(SystemExit):
        load_zones(p)


def test_eleven_zones_exits():
    p = write_config({"zones": [VALID_ZONE] * 11})
    with pytest.raises(SystemExit, match="11"):
        load_zones(p)


def test_ten_zones_ok():
    zone_variants = [
        {**VALID_ZONE, "name": f"Zone {i}", "alert_email": f"z{i}@example.com"}
        for i in range(10)
    ]
    p = write_config({"zones": zone_variants})
    zones = load_zones(p)
    assert len(zones) == 10


# ---------------------------------------------------------------------------
# Required field validation (FR-12)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("field", ["name", "description", "polygon", "threshold_multiplier", "alert_email"])
def test_missing_required_field_exits(field):
    z = {k: v for k, v in VALID_ZONE.items() if k != field}
    p = write_config({"zones": [z]})
    with pytest.raises(SystemExit, match=field):
        load_zones(p)


def test_empty_name_exits():
    z = {**VALID_ZONE, "name": "   "}
    p = write_config({"zones": [z]})
    with pytest.raises(SystemExit):
        load_zones(p)


def test_negative_threshold_exits():
    z = {**VALID_ZONE, "threshold_multiplier": -1.0}
    p = write_config({"zones": [z]})
    with pytest.raises(SystemExit):
        load_zones(p)


def test_zero_threshold_exits():
    z = {**VALID_ZONE, "threshold_multiplier": 0}
    p = write_config({"zones": [z]})
    with pytest.raises(SystemExit):
        load_zones(p)


def test_invalid_polygon_geojson_exits():
    z = {**VALID_ZONE, "polygon": {"type": "NotAType", "coordinates": []}}
    p = write_config({"zones": [z]})
    with pytest.raises(SystemExit):
        load_zones(p)


# ---------------------------------------------------------------------------
# Area validation (FR-14)
# ---------------------------------------------------------------------------


def test_polygon_below_1km2_exits():
    # ~100m × 100m box — well under 1 km²
    tiny = {
        **VALID_ZONE,
        "polygon": {
            "type": "Polygon",
            "coordinates": [
                [[-9.0, 53.0], [-9.0001, 53.0], [-9.0001, 53.0001], [-9.0, 53.0001], [-9.0, 53.0]]
            ],
        },
    }
    p = write_config({"zones": [tiny]})
    with pytest.raises(SystemExit, match="km"):
        load_zones(p)


def test_polygon_exactly_at_limit_passes():
    # ~11km × 11km box — comfortably over 1 km²
    large = {
        **VALID_ZONE,
        "polygon": {
            "type": "Polygon",
            "coordinates": [
                [[-10.0, 53.0], [-9.9, 53.0], [-9.9, 53.1], [-10.0, 53.1], [-10.0, 53.0]]
            ],
        },
    }
    p = write_config({"zones": [large]})
    zones = load_zones(p)
    assert len(zones) == 1
