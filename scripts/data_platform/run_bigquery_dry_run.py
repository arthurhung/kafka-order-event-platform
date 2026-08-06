"""Run a Phase 8A dry-run provider without cloud fallback."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from data_platform.bigquery_cost_policy import evaluate_cost_policy, load_cost_policy
from data_platform.bigquery_dry_run import (
    DryRunInputError,
    DryRunProvider,
    run_provider,
    write_dry_run_report,
)


def main() -> int:
    """Run a provider and, for fixtures, evaluate its deterministic policy."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--provider", choices=[item.value for item in DryRunProvider], required=True
    )
    parser.add_argument("--fixture", type=Path)
    parser.add_argument(
        "--policy", type=Path, default=Path("config/data_platform/bigquery_cost_policy.json")
    )
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--git-sha")
    args = parser.parse_args()
    provider = DryRunProvider(args.provider)
    try:
        report = run_provider(provider, args.fixture, git_sha=args.git_sha)
        if provider is DryRunProvider.LOCAL_FIXTURE:
            result = evaluate_cost_policy(load_cost_policy(args.policy), report)
            report["policy_decision"] = result.decision
            report["policy_findings"] = [
                {"severity": item.severity, "rule": item.rule, "message": item.message}
                for item in result.findings
            ]
            report["status"] = result.decision
        write_dry_run_report(report, args.report)
    except (ValueError, DryRunInputError) as error:
        print(json.dumps({"status": "invalid", "error": str(error)}, sort_keys=True))
        return 2
    print(json.dumps(report, sort_keys=True))
    if provider is not DryRunProvider.LOCAL_FIXTURE:
        return 3
    return 1 if report["status"] in {"block", "invalid"} else 0


if __name__ == "__main__":
    raise SystemExit(main())
