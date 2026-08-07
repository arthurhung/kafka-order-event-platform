"""Validate generated scaffold files without executing arbitrary commands."""

import argparse
import json
import re
from pathlib import Path


def main() -> None:
    """Require SQL, model YAML, and unit-test YAML with no unresolved markers."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-dir", type=Path, required=True)
    parser.add_argument("--model-name", required=True)
    parser.add_argument("--layer", choices=("staging", "intermediate", "marts"), required=True)
    args = parser.parse_args()
    if "/" in args.model_name or ".." in args.model_name:
        parser.error("unsafe model name")
    directory = args.project_dir / "models" / args.layer
    files = [
        directory / f"{args.model_name}.sql",
        directory / f"{args.model_name}.yml",
        directory / f"{args.model_name}_unit_test.yml",
    ]
    missing = [path.name for path in files if not path.is_file()]
    unresolved = [
        path.name
        for path in files
        if path.is_file()
        and re.search(r"\{\{ [A-Z][A-Z0-9_]+ \}\}", path.read_text(encoding="utf-8"))
    ]
    status = "passed" if not missing and not unresolved else "failed"
    print(
        json.dumps({"status": status, "missing": missing, "unresolved": unresolved}, sort_keys=True)
    )
    raise SystemExit(0 if status == "passed" else 1)


if __name__ == "__main__":
    main()
