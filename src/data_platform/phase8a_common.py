"""Shared, local-only Phase 8A report helpers."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def utc_now() -> str:
    """Return a timezone-aware UTC timestamp in a stable representation."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def load_json_object(path: Path) -> dict[str, Any]:
    """Load a JSON object and reject any other top-level shape."""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read JSON object {path}: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"JSON input must be an object: {path}")
    return value


def write_json(path: Path, value: dict[str, Any]) -> None:
    """Write a stable machine-readable JSON artifact."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def report_header(
    report_type: str,
    *,
    git_sha: str | None,
    evidence_level: str,
    provider: str = "local",
    cloud_execution_status: str = "not_executed",
) -> dict[str, Any]:
    """Build the mandatory Phase 8A report envelope."""
    return {
        "report_type": report_type,
        "schema_version": 1,
        "generated_at": utc_now(),
        "git_sha": git_sha,
        "phase_identifier": "phase_8a",
        "validation_mode": "local_static",
        "evidence_level": evidence_level,
        "provider": provider,
        "cloud_execution_status": cloud_execution_status,
    }
