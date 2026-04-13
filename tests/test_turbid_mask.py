"""Structural tests for the static west-coast Ireland turbidity mask."""

import json
from pathlib import Path

MASK_PATH = Path(__file__).parent.parent / "data" / "masks" / "wci_turbid_mask.geojson"
EXPECTED_REGIONS = {
    "Shannon Estuary",
    "Inner Galway Bay",
    "Clew Bay Narrows",
    "Bantry Bay Inner Reaches",
}


def test_mask_file_exists_and_is_feature_collection():
    assert MASK_PATH.exists(), "Expected static turbidity mask GeoJSON to exist"

    payload = json.loads(MASK_PATH.read_text())

    assert payload["type"] == "FeatureCollection"
    assert isinstance(payload["features"], list)
    assert len(payload["features"]) == 4


def test_mask_contains_required_named_polygons_with_closed_rings():
    payload = json.loads(MASK_PATH.read_text())
    observed_regions = set()

    for feature in payload["features"]:
        assert feature["type"] == "Feature"
        observed_regions.add(feature["properties"]["name"])

        geometry = feature["geometry"]
        assert geometry["type"] == "Polygon"
        assert len(geometry["coordinates"]) == 1

        ring = geometry["coordinates"][0]
        assert len(ring) >= 4
        assert ring[0] == ring[-1], "GeoJSON polygon ring must be closed"

        for lon, lat in ring:
            assert isinstance(lon, (int, float))
            assert isinstance(lat, (int, float))
            assert -11.5 <= lon <= -7.0
            assert 51.0 <= lat <= 56.0

    assert observed_regions == EXPECTED_REGIONS
