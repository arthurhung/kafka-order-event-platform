"""Detect changed dbt model files from an explicit diff-name list."""

import argparse
import json
from pathlib import Path

from data_platform.dbt_review import detect_changed_model_paths


def main() -> None:
    """Filter stable model paths without invoking arbitrary git commands."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--paths", type=Path, required=True)
    args = parser.parse_args()
    paths = detect_changed_model_paths(args.paths.read_text(encoding="utf-8").splitlines())
    print(json.dumps({"changed_model_files": paths}, sort_keys=True))


if __name__ == "__main__":
    main()
