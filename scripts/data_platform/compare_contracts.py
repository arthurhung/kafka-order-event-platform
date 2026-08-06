"""Compare published dbt contracts in previous and current manifests."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from data_platform.contracts import ContractInputError, compare_contracts, write_contract_report


def main() -> int:
    """Write a contract report and block known breaking changes."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--previous-manifest", type=Path)
    parser.add_argument("--current-manifest", type=Path, default=Path("dbt/target/manifest.json"))
    parser.add_argument("--previous-git-sha")
    parser.add_argument("--current-git-sha")
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("reports/data-quality/phase7-contract-diff.json"),
    )
    args = parser.parse_args()
    try:
        report = compare_contracts(
            args.previous_manifest,
            args.current_manifest,
            previous_git_sha=args.previous_git_sha,
            current_git_sha=args.current_git_sha,
        )
        write_contract_report(report, args.report)
    except ContractInputError as error:
        print(json.dumps({"status": "error", "error": str(error)}, sort_keys=True))
        return 2
    print(json.dumps(report.to_dict(), sort_keys=True))
    return 1 if report.blocking_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
