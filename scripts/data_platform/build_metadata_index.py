"""Build deterministic Phase 9 metadata files."""

import argparse
from pathlib import Path

from data_platform.metadata_index import MetadataBuilder, MetadataBuildError, write_build


def main() -> None:
    """Build from the fixed repository layout and report degraded inputs honestly."""
    parser = argparse.ArgumentParser(description="Build the Phase 9 metadata index")
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    parser.add_argument("--output-dir", type=Path, default=Path("reports/metadata"))
    arguments = parser.parse_args()
    try:
        result = MetadataBuilder(arguments.repository_root).build()
        output = arguments.output_dir
        if not output.is_absolute():
            output = arguments.repository_root / output
        write_build(result, output)
    except MetadataBuildError as error:
        parser.exit(2, f"metadata-index failed: {error}\n")
    print(result.summary.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
