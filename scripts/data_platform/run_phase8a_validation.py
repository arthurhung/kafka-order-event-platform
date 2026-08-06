"""Run the aggregate local-only Phase 8A validation gate."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from data_platform.phase8a_validation import Phase8AValidationError, run_phase8a_validation


def main() -> int:
    """Run Phase 8A with explicit run-specific artifacts."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--run-id")
    parser.add_argument("--target", default="local")
    parser.add_argument("--previous-manifest", type=Path)
    parser.add_argument("--previous-cost-policy", type=Path)
    args = parser.parse_args()
    run_id = args.run_id or datetime.now(UTC).strftime("local_%Y%m%d%H%M%S")
    try:
        result = run_phase8a_validation(
            repo_root=args.repo_root,
            run_id=run_id,
            target=args.target,
            previous_manifest=args.previous_manifest,
            previous_cost_policy=args.previous_cost_policy,
        )
    except (OSError, ValueError, Phase8AValidationError) as error:
        print(json.dumps({"status": "invalid", "error": str(error)}, sort_keys=True))
        return 2
    print(json.dumps(result.to_dict(), sort_keys=True))
    return 0 if result.status == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
