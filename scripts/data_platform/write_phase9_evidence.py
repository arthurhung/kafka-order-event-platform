"""Write Phase 9 CI and security evidence summaries."""

import argparse
from pathlib import Path

from data_platform.phase9_evidence import write_phase9_evidence


def main() -> None:
    """Validate fixed reports and write the aggregate evidence."""
    parser = argparse.ArgumentParser(description="Write Phase 9 acceptance evidence")
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    arguments = parser.parse_args()
    try:
        summary, security = write_phase9_evidence(arguments.repository_root)
    except (OSError, ValueError) as error:
        parser.exit(1, f"phase9 evidence failed: {type(error).__name__}\n")
    print(f"wrote {summary.relative_to(arguments.repository_root.resolve())}")
    print(f"wrote {security.relative_to(arguments.repository_root.resolve())}")


if __name__ == "__main__":
    main()
