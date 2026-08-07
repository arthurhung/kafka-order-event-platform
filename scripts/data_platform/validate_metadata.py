"""Validate generated Phase 9 metadata schemas and relationships."""

import argparse
from datetime import UTC, datetime
from pathlib import Path

from data_platform.metadata_index import MetadataBuildError, validate_build
from data_platform.phase9_evidence import write_json


def main() -> None:
    """Validate the fixed generated metadata directory."""
    parser = argparse.ArgumentParser(description="Validate Phase 9 metadata outputs")
    parser.add_argument("--output-dir", type=Path, default=Path("reports/metadata"))
    parser.add_argument("--report", type=Path)
    arguments = parser.parse_args()
    try:
        summary = validate_build(arguments.output_dir)
    except MetadataBuildError as error:
        parser.exit(1, f"metadata-validate failed: {error}\n")
    if arguments.report is not None:
        write_json(
            arguments.report,
            {
                "schema_version": 1,
                "report_type": "phase9_metadata_validation",
                "status": summary.status,
                "evidence_level": "static_validation",
                "generated_at": datetime.now(UTC).isoformat(),
                "asset_count": summary.asset_count,
                "lineage_edge_count": summary.edge_count,
                "missing_artifacts": summary.missing_artifacts,
                "warnings": summary.warnings,
                "schema_validation": "passed",
                "cross_file_consistency": "passed",
            },
        )
    print(summary.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
