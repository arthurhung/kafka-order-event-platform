"""Validate Phase 8A compatibility metadata from an explicit fresh manifest."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from data_platform.bigquery_compatibility import (
    CompatibilityInputError,
    validate_compatibility_manifest,
    write_compatibility_report,
)


def main() -> int:
    """Write a report and return deterministic policy exit codes."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--git-sha")
    args = parser.parse_args()
    try:
        report = validate_compatibility_manifest(args.manifest, git_sha=args.git_sha)
        write_compatibility_report(report, args.report)
    except CompatibilityInputError as error:
        print(json.dumps({"status": "invalid", "error": str(error)}, sort_keys=True))
        return 2
    print(json.dumps(report.to_dict(), sort_keys=True))
    return 1 if report.error_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
