"""Command-line entry point for deterministic dbt draft scaffolding."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from data_platform.scaffolding import ScaffoldError, ScaffoldRequest, scaffold_model


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", required=True)
    parser.add_argument("--layer", choices=("staging", "intermediate", "marts"), required=True)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--domain", required=True)
    parser.add_argument("--grain", required=True)
    parser.add_argument("--project-dir", type=Path, default=Path("dbt"))
    return parser


def main() -> int:
    """Generate a scaffold and emit a machine-readable result."""
    args = _parser().parse_args()
    try:
        result = scaffold_model(
            ScaffoldRequest(
                name=args.name,
                layer=args.layer,
                owner=args.owner,
                domain=args.domain,
                grain=args.grain,
            ),
            project_dir=args.project_dir,
        )
    except (OSError, ScaffoldError) as error:
        print(json.dumps({"status": "error", "error": str(error)}, sort_keys=True))
        return 2
    print(
        json.dumps(
            {
                "status": result.status,
                "model_name": result.model_name,
                "sql_path": str(result.sql_path),
                "yaml_path": str(result.yaml_path),
                "blocking_todos": True,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
