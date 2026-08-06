"""Validate deterministic dbt model conventions from manifest.json."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from data_platform.conventions import (
    ConventionInputError,
    validate_manifest,
    write_convention_report,
)


def main() -> int:
    """Run convention validation and return non-zero for blocking findings."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path("dbt/target/manifest.json"))
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("reports/data-quality/phase7-conventions.json"),
    )
    args = parser.parse_args()
    try:
        report = validate_manifest(args.manifest)
        write_convention_report(report, args.report)
    except ConventionInputError as error:
        print(json.dumps({"status": "error", "error": str(error)}, sort_keys=True))
        return 2
    print(json.dumps(report.to_dict(), sort_keys=True))
    return 1 if report.error_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
