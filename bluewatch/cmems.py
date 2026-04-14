"""Shared CMEMS schema compatibility helpers."""

from __future__ import annotations

from collections.abc import Iterable

import xarray as xr

LEGACY_QUALITY_VARIABLE = "CHL_flags"
CURRENT_QUALITY_VARIABLE = "flags"

_VARIABLE_CANDIDATES: tuple[tuple[str, str], ...] = (
    ("CHL", LEGACY_QUALITY_VARIABLE),
    ("CHL", CURRENT_QUALITY_VARIABLE),
)


def variable_request_candidates() -> tuple[tuple[str, str], ...]:
    """Return ordered variable sets to try against CMEMS products."""
    return _VARIABLE_CANDIDATES


def is_missing_quality_variable_error(exc: Exception, quality_variable: str) -> bool:
    """Return True when CMEMS rejects a request because a quality variable is absent."""
    message = str(exc)
    return quality_variable in message and "variable" in message.lower()


def build_valid_chl_mask(ds: xr.Dataset) -> xr.DataArray:
    """Return a boolean mask for valid CHL pixels across supported CMEMS schemas."""
    if LEGACY_QUALITY_VARIABLE in ds:
        return ds[LEGACY_QUALITY_VARIABLE] == 1

    if CURRENT_QUALITY_VARIABLE in ds:
        # Current NRT schema exposes a LAND status flag only; keep non-land ocean pixels.
        return ds[CURRENT_QUALITY_VARIABLE] == 0

    available_variables = ", ".join(sorted(_data_variable_names(ds)))
    raise KeyError(
        "CMEMS dataset did not expose a supported quality variable. "
        f"Available data variables: {available_variables}"
    )


def _data_variable_names(ds: xr.Dataset) -> Iterable[str]:
    return (str(name) for name in ds.data_vars.keys())
