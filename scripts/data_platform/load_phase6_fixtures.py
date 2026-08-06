"""Load deterministic Phase 6 fixtures through the Kafka Core."""

import json
import os
from datetime import UTC, datetime
from pathlib import Path

from data_platform.fixtures import load_fixtures
from streaming_platform.config import get_settings


def main() -> None:
    """Load a repeatable fixture run and print its machine-readable summary."""
    default_run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M")
    run_id = os.environ.get("DATA_PLATFORM_FIXTURE_RUN_ID") or default_run_id
    summary = load_fixtures(
        get_settings(),
        run_id,
        Path("reports/data-quality/phase6-fixtures-latest.json"),
    )
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
