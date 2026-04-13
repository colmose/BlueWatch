"""Zone config loader and validator (FR-12, FR-13, FR-14)."""

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from shapely.geometry import shape
from shapely.ops import transform
import pyproj

CONFIG_PATH = Path(__file__).parent.parent / "config" / "zones.yaml"
MIN_ZONE_AREA_KM2 = 1.0
MIN_ZONES = 1
MAX_ZONES = 10

REQUIRED_FIELDS = {"name", "description", "polygon", "threshold_multiplier", "alert_email"}


@dataclass(frozen=True)
class Zone:
    name: str
    description: str
    polygon: Any          # shapely geometry
    threshold_multiplier: float
    alert_email: str


def _area_km2(geom) -> float:
    """Return the area of a shapely geometry in km²."""
    wgs84 = pyproj.CRS("EPSG:4326")
    # Use an equal-area projection centred on the geometry centroid
    lon, lat = geom.centroid.x, geom.centroid.y
    laea = pyproj.CRS(
        proj="laea", lat_0=lat, lon_0=lon, datum="WGS84", units="m"
    )
    project = pyproj.Transformer.from_crs(wgs84, laea, always_xy=True).transform
    projected = transform(project, geom)
    return projected.area / 1e6


def load_zones(config_path: Path = CONFIG_PATH) -> list[Zone]:
    """Load and validate zones from YAML.  Calls sys.exit(1) on any error."""

    if not config_path.exists():
        sys.exit(f"ERROR: zone config not found: {config_path}")

    with config_path.open() as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict) or "zones" not in raw:
        sys.exit(f"ERROR: {config_path} must contain a top-level 'zones' key")

    entries = raw["zones"]
    if not isinstance(entries, list):
        sys.exit(f"ERROR: 'zones' in {config_path} must be a list")

    if not (MIN_ZONES <= len(entries) <= MAX_ZONES):
        sys.exit(
            f"ERROR: expected between {MIN_ZONES} and {MAX_ZONES} zones, "
            f"got {len(entries)}"
        )

    zones: list[Zone] = []
    for i, entry in enumerate(entries, start=1):
        _validate_zone(entry, index=i, config_path=config_path)
        zones.append(_build_zone(entry, index=i))

    return zones


def _validate_zone(entry: dict, *, index: int, config_path: Path) -> None:
    tag = f"zones[{index}]"

    missing = REQUIRED_FIELDS - set(entry.keys())
    if missing:
        sys.exit(
            f"ERROR: {tag} in {config_path} is missing required fields: "
            + ", ".join(sorted(missing))
        )

    for field in ("name", "description", "alert_email"):
        if not isinstance(entry[field], str) or not entry[field].strip():
            sys.exit(f"ERROR: {tag}.{field} must be a non-empty string")

    tm = entry["threshold_multiplier"]
    if not isinstance(tm, (int, float)) or tm <= 0:
        sys.exit(f"ERROR: {tag}.threshold_multiplier must be a positive number, got {tm!r}")

    polygon_raw = entry["polygon"]
    try:
        geom = shape(polygon_raw)
    except Exception as exc:
        sys.exit(f"ERROR: {tag}.polygon is not valid GeoJSON geometry: {exc}")

    if not geom.is_valid:
        sys.exit(f"ERROR: {tag}.polygon is an invalid geometry (self-intersecting?)")

    area = _area_km2(geom)
    if area < MIN_ZONE_AREA_KM2:
        sys.exit(
            f"ERROR: {tag} ('{entry['name']}') polygon area is {area:.4f} km², "
            f"which is below the 1 km² minimum required at 300m pixel resolution"
        )


def _build_zone(entry: dict, *, index: int) -> Zone:
    return Zone(
        name=entry["name"],
        description=entry["description"],
        polygon=shape(entry["polygon"]),
        threshold_multiplier=float(entry["threshold_multiplier"]),
        alert_email=entry["alert_email"],
    )
