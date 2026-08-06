"""Evaluate one deterministic Phase 8A cost fixture."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from data_platform.bigquery_cost_policy import (
    CostPolicyInputError,
    evaluate_cost_policy,
    load_cost_policy,
    write_cost_report,
)
from data_platform.phase8a_common import load_json_object


def main() -> int:
    """Evaluate a fixture and return policy exit codes."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--git-sha")
    args = parser.parse_args()
    try:
        result = evaluate_cost_policy(load_cost_policy(args.policy), load_json_object(args.fixture))
        write_cost_report(result, args.report, git_sha=args.git_sha)
    except (ValueError, CostPolicyInputError) as error:
        print(json.dumps({"status": "invalid", "error": str(error)}, sort_keys=True))
        return 2
    print(json.dumps(result.to_dict(git_sha=args.git_sha), sort_keys=True))
    return 1 if result.blocked else 0


if __name__ == "__main__":
    raise SystemExit(main())
