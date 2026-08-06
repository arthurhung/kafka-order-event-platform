"""Validate the local Phase 8A orchestration contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from data_platform.phase8a_common import write_json
from data_platform.phase8a_orchestration import (
    OrchestrationInputError,
    validate_orchestration_contract,
)


def main() -> int:
    """Validate the contract without an Airflow runtime claim."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--git-sha")
    args = parser.parse_args()
    try:
        report = validate_orchestration_contract(args.contract, git_sha=args.git_sha)
        write_json(args.report, report)
    except OrchestrationInputError as error:
        print(json.dumps({"status": "invalid", "error": str(error)}, sort_keys=True))
        return 2
    print(json.dumps(report, sort_keys=True))
    return 1 if report["status"] == "blocked" else 0


if __name__ == "__main__":
    raise SystemExit(main())
