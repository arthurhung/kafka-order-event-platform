import os
import subprocess
import sys
from pathlib import Path

from tests.data_platform.unit.test_metadata_index import artifact_fixture


def test_phase9_cli_build_validate_and_smoke(tmp_path: Path) -> None:
    artifact_fixture(tmp_path)
    repository = Path(__file__).resolve().parents[3]
    environment = {**os.environ, "PYTHONPATH": str(repository / "src")}
    output = tmp_path / "reports/metadata"
    build = subprocess.run(  # noqa: S603 - fixed local Python entry point
        [
            sys.executable,
            str(repository / "scripts/data_platform/build_metadata_index.py"),
            "--repository-root",
            str(tmp_path),
            "--output-dir",
            str(output),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    validate = subprocess.run(  # noqa: S603 - fixed local Python entry point
        [
            sys.executable,
            str(repository / "scripts/data_platform/validate_metadata.py"),
            "--output-dir",
            str(output),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    smoke = subprocess.run(  # noqa: S603 - fixed local Python entry point
        [
            sys.executable,
            str(repository / "scripts/data_platform/mcp_smoke.py"),
            "--repository-root",
            str(tmp_path),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
        cwd=repository,
    )
    assert build.returncode == 0, build.stderr
    assert validate.returncode == 0, validate.stderr
    assert smoke.returncode == 0, smoke.stderr
    assert '"status": "passed"' in smoke.stdout


def test_phase9_cli_rejects_missing_and_malformed_input(tmp_path: Path) -> None:
    repository = Path(__file__).resolve().parents[3]
    environment = {**os.environ, "PYTHONPATH": str(repository / "src")}
    command = [
        sys.executable,
        str(repository / "scripts/data_platform/build_metadata_index.py"),
        "--repository-root",
        str(tmp_path),
    ]
    missing = subprocess.run(  # noqa: S603 - fixed local Python entry point
        command, check=False, capture_output=True, text=True, env=environment
    )
    path = tmp_path / "dbt/target/manifest.json"
    path.parent.mkdir(parents=True)
    path.write_text("[]")
    malformed = subprocess.run(  # noqa: S603 - fixed local Python entry point
        command, check=False, capture_output=True, text=True, env=environment
    )
    assert missing.returncode == 2
    assert malformed.returncode == 2
