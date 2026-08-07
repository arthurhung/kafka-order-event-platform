"""Run deterministic Phase 10 review rules on normalized input."""

import argparse
import json
from pathlib import Path

from data_platform.dbt_review import DbtReviewRequest, review_dbt_changes, write_review_report


def main() -> None:
    """Read strict review input and write stable findings."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    request = DbtReviewRequest.model_validate_json(args.request.read_text(encoding="utf-8"))
    report = review_dbt_changes(request)
    write_review_report(report, args.report)
    print(json.dumps(report.model_dump(mode="json"), sort_keys=True))
    raise SystemExit(1 if report.status == "blocked" else 0)


if __name__ == "__main__":
    main()
