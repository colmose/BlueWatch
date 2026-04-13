"""Zone config loader and validator (FR-12, FR-13, FR-14)."""

import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from shapely.geometry import shape

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


def _area_km2(geom: Any) -> float:
    """Return the approximate area of a WGS-84 geometry in km².

    Uses the spherical approximation: scale degrees² by (111.32 km/°)²
    and cos(lat) for longitude compression. Accurate to ~1% for small
    polygons at mid-latitudes — sufficient for the 1 km² threshold check.
    """
    lat_rad = math.radians(geom.centroid.y)
    km_per_deg_lat = 111.32
    km_per_deg_lon = 111.32 * math.cos(lat_rad)
    # geom.area is in degrees²
    return float(geom.area) * km_per_deg_lat * km_per_deg_lon


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
        zones.append(_build_zone(entry))

    return zones


def _validate_zone(entry: dict[str, Any], *, index: int, config_path: Path) -> None:
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


def _build_zone(entry: dict[str, Any]) -> Zone:
    return Zone(
        name=entry["name"],
        description=entry["description"],
        polygon=shape(entry["polygon"]),
        threshold_multiplier=float(entry["threshold_multiplier"]),
        alert_email=entry["alert_email"],
    )
