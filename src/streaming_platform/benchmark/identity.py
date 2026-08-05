"""UTC run identifiers and collision-resistant report filenames."""

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4


def create_run_id(now: datetime | None = None) -> str:
    """Create a sortable UTC timestamp plus UUID run identifier."""
    value = (now or datetime.now(UTC)).astimezone(UTC)
    timestamp = value.strftime("%Y%m%dT%H%M%S%fZ")
    return f"{timestamp}-{uuid4()}"


def report_path(directory: Path, run_id: str, profile: str) -> Path:
    """Return a unique report path without overwriting earlier runs."""
    return directory / f"benchmark-{profile}-{run_id}.json"
