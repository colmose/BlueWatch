"""Environment helpers for local CLI entrypoints."""

from __future__ import annotations

import ast
import os
import re
from pathlib import Path

_DOTENV_LINE_RE = re.compile(r"^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$")
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def get_env(name: str) -> str | None:
    """Return an exported env var, falling back to a local .env file."""
    if name in os.environ:
        return os.environ.get(name)

    dotenv_path = find_dotenv()
    if dotenv_path is None:
        return None

    dotenv_values = parse_dotenv(dotenv_path)
    dotenv_value = dotenv_values.get(name)
    if dotenv_value:
        return dotenv_value

    return None


def find_dotenv() -> Path | None:
    """Find the nearest project-level .env file for CLI use."""
    candidates = [Path.cwd() / ".env", _PROJECT_ROOT / ".env"]
    seen: set[Path] = set()

    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if candidate.is_file():
            return candidate

    return None


def parse_dotenv(path: Path) -> dict[str, str]:
    """Parse a minimal .env file without introducing a runtime dependency."""
    values: dict[str, str] = {}

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        match = _DOTENV_LINE_RE.match(line)
        if match is None:
            continue

        key, raw_value = match.groups()
        values[key] = _parse_dotenv_value(raw_value.strip())

    return values


def _parse_dotenv_value(raw_value: str) -> str:
    if not raw_value:
        return ""

    if len(raw_value) >= 2 and raw_value[0] == raw_value[-1] and raw_value[0] in {'"', "'"}:
        try:
            parsed = ast.literal_eval(raw_value)
        except (SyntaxError, ValueError):
            return raw_value[1:-1]
        return parsed if isinstance(parsed, str) else str(parsed)

    comment_index = raw_value.find(" #")
    if comment_index != -1:
        return raw_value[:comment_index].rstrip()

    return raw_value
