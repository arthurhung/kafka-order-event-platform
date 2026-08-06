"""Run Phase 7 local Slim CI with explicit state or full fallback."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from data_platform.slim_ci import SlimCIError, existing_artifacts, run_slim_ci


def main() -> int:
    """Execute Slim CI and print its machine-readable summary."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--base-ref", default="HEAD")
    parser.add_argument("--no-base-state", action="store_true")
    parser.add_argument("--run-id")
    parser.add_argument("--python-checks", action="store_true")
    parser.add_argument("--summary", type=Path)
    parser.add_argument("--convention-report", type=Path)
    parser.add_argument("--contract-report", type=Path)
    args = parser.parse_args()
    run_id = args.run_id or datetime.now(UTC).strftime("%Y%m%d%H%M%S").lower()
    state_root = args.repo_root / "dbt" / "target" / "phase7-ci"
    summary = args.summary or (
        args.repo_root / "reports" / "data-quality" / "phase7-ci-summary.json"
    )
    convention_report = args.convention_report or (
        args.repo_root / "reports" / "data-quality" / "phase7-conventions.json"
    )
    contract_report = args.contract_report or (
        args.repo_root / "reports" / "data-quality" / "phase7-contract-diff.json"
    )
    try:
        result = run_slim_ci(
            repo_root=args.repo_root,
            base_ref=None if args.no_base_state else args.base_ref,
            run_id=run_id,
            state_root=state_root,
            summary_path=summary,
            convention_report_path=convention_report,
            contract_report_path=contract_report,
            run_python_checks=args.python_checks,
        )
    except SlimCIError as error:
        print(json.dumps({"status": "error", "error": str(error)}, sort_keys=True))
        return 2
    payload = result.to_dict()
    payload["artifacts"] = existing_artifacts(Path(result.state_directory))
    print(json.dumps(payload, sort_keys=True))
    return 0 if result.status == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
