"""Validate bounded BigQuery query examples with the deterministic local lexer."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from data_platform.bigquery_compatibility import (
    CompatibilityInputError,
    extract_bigquery_policies,
    extract_published_sql,
)
from data_platform.bigquery_sql_policy import (
    SQLPolicyReport,
    analyze_sql,
    validate_sql_directory,
    write_sql_policy_report,
)


def main() -> int:
    """Validate query fixtures and return non-zero for blocking findings."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--sql-directory", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--git-sha")
    args = parser.parse_args()
    try:
        policies = extract_bigquery_policies(args.manifest)
        query_report = validate_sql_directory(args.sql_directory, policies, git_sha=args.git_sha)
        model_findings = [
            finding
            for model, sql in extract_published_sql(args.manifest).items()
            for finding in analyze_sql(
                model,
                sql,
                partition_field=None,
                require_partition_filter=False,
            )
        ]
        findings = tuple(
            sorted(
                (*query_report.findings, *model_findings),
                key=lambda item: (item.model, item.severity, item.rule),
            )
        )
        report = SQLPolicyReport(
            "blocked" if any(item.severity == "error" for item in findings) else "passed",
            query_report.models_checked,
            findings,
            args.git_sha,
        )
        write_sql_policy_report(report, args.report)
    except (OSError, CompatibilityInputError) as error:
        print(json.dumps({"status": "invalid", "error": str(error)}, sort_keys=True))
        return 2
    print(json.dumps(report.to_dict(), sort_keys=True))
    return 1 if report.error_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
