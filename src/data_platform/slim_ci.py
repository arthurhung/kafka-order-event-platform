"""Local and CI orchestration for dbt state-based validation."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Literal

import psycopg
from psycopg import sql

from data_platform.contracts import compare_contracts, write_contract_report
from data_platform.conventions import validate_manifest, write_convention_report

_SAFE_RUN_ID = re.compile(r"^[a-z0-9_]+$")
_ARTIFACT_NAMES = ("manifest.json", "catalog.json", "run_results.json", "sources.json")


class SlimCIError(RuntimeError):
    """Raised when a Slim CI command or policy gate fails."""


@dataclass(frozen=True, slots=True)
class CommandResult:
    """Sanitized evidence for one subprocess."""

    command: tuple[str, ...]
    exit_code: int
    output_tail: str


@dataclass(frozen=True, slots=True)
class SlimCIResult:
    """Outcome and artifacts from one local Slim CI run."""

    status: Literal["passed", "failed"]
    mode: Literal["state_modified_plus", "full_ci_fallback"]
    run_id: str
    base_ref: str | None
    base_git_sha: str | None
    current_git_sha: str | None
    selected_models: tuple[str, ...]
    state_directory: str
    convention_report: str
    contract_report: str
    commands: tuple[CommandResult, ...]
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable summary."""
        value = asdict(self)
        value.update(
            {
                "report_type": "phase7_slim_ci",
                "schema_version": 1,
                "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "environment": "local_or_github_ci",
            }
        )
        return value


def _redact(text: str, environment: dict[str, str]) -> str:
    redacted = text
    for key in ("POSTGRES_PASSWORD", "DATABASE_URL", "DBT_ENV_SECRET_PASSWORD"):
        secret = environment.get(key)
        if secret:
            redacted = redacted.replace(secret, "[REDACTED]")
    return redacted[-4000:]


def _run(
    command: Sequence[str],
    *,
    cwd: Path,
    environment: dict[str, str],
    evidence: list[CommandResult],
    allow_failure: bool = False,
) -> subprocess.CompletedProcess[str]:
    subprocess_environment = {
        key: value for key, value in environment.items() if not key.startswith("COV_CORE_")
    }
    completed = subprocess.run(  # noqa: S603 - commands are fixed by the CI orchestrator
        list(command),
        cwd=cwd,
        env=subprocess_environment,
        check=False,
        capture_output=True,
        text=True,
    )
    output = _redact(completed.stdout + completed.stderr, environment)
    evidence.append(CommandResult(tuple(command), completed.returncode, output))
    if completed.returncode and not allow_failure:
        raise SlimCIError(f"command failed ({completed.returncode}): {' '.join(command)}")
    return completed


def _git_value(repo_root: Path, arguments: Sequence[str]) -> str | None:
    completed = subprocess.run(  # noqa: S603 - git arguments are internally allowlisted
        ["git", *arguments],  # noqa: S607 - project requires Git from PATH
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip() if completed.returncode == 0 else None


def _extract_revision(repo_root: Path, revision: str, destination: Path) -> str | None:
    sha = _git_value(repo_root, ["rev-parse", "--verify", f"{revision}^{{commit}}"])
    if sha is None:
        return None
    archive = subprocess.run(  # noqa: S603 - fixed read-only git archive command
        ["git", "archive", revision],  # noqa: S607 - project requires Git from PATH
        cwd=repo_root,
        check=False,
        capture_output=True,
    )
    if archive.returncode:
        return None
    destination.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=BytesIO(archive.stdout), mode="r:") as bundle:
        bundle.extractall(destination, filter="data")
    return sha


def _dbt_command(
    action: str,
    *,
    project_dir: Path,
    profiles_dir: Path,
    target_path: Path,
    extra: Sequence[str] = (),
) -> list[str]:
    command = [
        "dbt",
        *action.split(),
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
        "--target",
        "ci",
    ]
    if action != "deps":
        command.extend(("--target-path", str(target_path)))
    command.extend(extra)
    return command


def _selected_models(output: str) -> tuple[str, ...]:
    selected: set[str] = set()
    for line in output.splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict) and item.get("resource_type") == "model" and item.get("name"):
            selected.add(str(item["name"]))
    return tuple(sorted(selected))


def select_modified_models(
    *,
    project_dir: Path,
    profiles_dir: Path,
    target_path: Path,
    state_path: Path,
    repo_root: Path,
    environment: dict[str, str],
    evidence: list[CommandResult],
) -> tuple[str, ...]:
    """Return dbt's native state:modified+ model selection."""
    command = _dbt_command(
        "ls",
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        target_path=target_path,
        extra=(
            "--select",
            "state:modified+",
            "--state",
            str(state_path),
            "--resource-type",
            "model",
            "--output",
            "json",
            "--output-keys",
            "name resource_type",
        ),
    )
    completed = _run(command, cwd=repo_root, environment=environment, evidence=evidence)
    return _selected_models(completed.stdout)


def _copy_profile(source: Path, destination_project: Path) -> Path:
    if not source.is_file():
        raise SlimCIError(f"dbt profile is unavailable: {source}")
    profiles_dir = destination_project
    shutil.copy2(source, profiles_dir / "profiles.yml")
    return profiles_dir


def cleanup_ci_schemas(environment: dict[str, str], prefixes: Sequence[str]) -> None:
    """Drop only schemas created for this run; never match or mutate public."""
    for prefix in prefixes:
        if not prefix.startswith("analytics_ci_") or not _SAFE_RUN_ID.fullmatch(
            prefix.removeprefix("analytics_ci_")
        ):
            raise SlimCIError(f"unsafe CI schema prefix: {prefix}")
    connection = psycopg.connect(
        host=environment.get("POSTGRES_HOST", "localhost"),
        port=int(environment.get("POSTGRES_PORT", "5432")),
        dbname=environment.get("POSTGRES_DB", "streaming"),
        user=environment.get("POSTGRES_USER", "streaming"),
        password=environment.get("POSTGRES_PASSWORD"),
        connect_timeout=10,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "select schema_name from information_schema.schemata "
                "where schema_name = any(%s) or schema_name like any(%s)",
                (list(prefixes), [f"{prefix}\\_%" for prefix in prefixes]),
            )
            schemas = [row[0] for row in cursor.fetchall()]
            for schema_name in schemas:
                if not any(
                    schema_name == prefix or schema_name.startswith(f"{prefix}_")
                    for prefix in prefixes
                ):
                    raise SlimCIError(f"refusing unsafe schema cleanup: {schema_name}")
                cursor.execute(
                    sql.SQL("drop schema {} cascade").format(sql.Identifier(schema_name))
                )
        connection.commit()
    finally:
        connection.close()


def _write_summary(result: SlimCIResult, summary_path: Path) -> None:
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _preserve_run_results(target_path: Path) -> None:
    source = target_path / "run_results.json"
    if source.is_file():
        shutil.copy2(source, target_path / "slim_build_run_results.json")


def run_slim_ci(
    *,
    repo_root: Path,
    base_ref: str | None,
    run_id: str,
    state_root: Path,
    summary_path: Path,
    convention_report_path: Path,
    contract_report_path: Path,
    run_python_checks: bool = False,
) -> SlimCIResult:
    """Build a base state, validate changes, and execute dbt Slim CI or fallback."""
    if not _SAFE_RUN_ID.fullmatch(run_id):
        raise SlimCIError("run_id must contain only lowercase letters, digits, and underscores")
    repo_root = repo_root.resolve()
    current_project = repo_root / "dbt"
    profile_path = current_project / "profiles.yml"
    run_state = (state_root / run_id).resolve()
    base_target = run_state / "base"
    current_target = run_state / "current"
    base_schema = f"analytics_ci_base_{run_id}"
    current_schema = f"analytics_ci_current_{run_id}"
    current_sha = _git_value(repo_root, ["rev-parse", "HEAD"])
    evidence: list[CommandResult] = []
    selected: tuple[str, ...] = ()
    mode: Literal["state_modified_plus", "full_ci_fallback"] = "full_ci_fallback"
    resolved_base_sha: str | None = None
    error_message: str | None = None

    environment = os.environ.copy()
    environment.update({"DBT_TARGET": "ci", "DBT_TARGET_SCHEMA": current_schema})
    result: SlimCIResult
    try:
        run_state.mkdir(parents=True, exist_ok=False)
        with tempfile.TemporaryDirectory(prefix="phase7-base-") as temp_directory:
            base_checkout = Path(temp_directory) / "repository"
            if base_ref:
                resolved_base_sha = _extract_revision(repo_root, base_ref, base_checkout)
            if resolved_base_sha is not None:
                mode = "state_modified_plus"
                base_project = base_checkout / "dbt"
                base_profiles = _copy_profile(profile_path, base_project)
                base_environment = environment | {"DBT_TARGET_SCHEMA": base_schema}
                _run(
                    _dbt_command(
                        "deps",
                        project_dir=base_project,
                        profiles_dir=base_profiles,
                        target_path=base_target,
                    ),
                    cwd=base_checkout,
                    environment=base_environment,
                    evidence=evidence,
                )
                _run(
                    _dbt_command(
                        "build",
                        project_dir=base_project,
                        profiles_dir=base_profiles,
                        target_path=base_target,
                    ),
                    cwd=base_checkout,
                    environment=base_environment,
                    evidence=evidence,
                )
                _run(
                    _dbt_command(
                        "docs generate",
                        project_dir=base_project,
                        profiles_dir=base_profiles,
                        target_path=base_target,
                    ),
                    cwd=base_checkout,
                    environment=base_environment,
                    evidence=evidence,
                )

            _run(
                _dbt_command(
                    "deps",
                    project_dir=current_project,
                    profiles_dir=current_project,
                    target_path=current_target,
                ),
                cwd=repo_root,
                environment=environment,
                evidence=evidence,
            )
            _run(
                _dbt_command(
                    "parse",
                    project_dir=current_project,
                    profiles_dir=current_project,
                    target_path=current_target,
                ),
                cwd=repo_root,
                environment=environment,
                evidence=evidence,
            )
            current_manifest = current_target / "manifest.json"
            convention_report = validate_manifest(current_manifest)
            write_convention_report(convention_report, convention_report_path)
            if convention_report.error_count:
                raise SlimCIError("convention validation contains blocking findings")
            contract_report = compare_contracts(
                base_target / "manifest.json" if mode == "state_modified_plus" else None,
                current_manifest,
                previous_git_sha=resolved_base_sha,
                current_git_sha=current_sha,
            )
            write_contract_report(contract_report, contract_report_path)
            if contract_report.blocking_count:
                raise SlimCIError("contract comparison contains blocking findings")

            if mode == "state_modified_plus":
                selected = select_modified_models(
                    project_dir=current_project,
                    profiles_dir=current_project,
                    target_path=current_target,
                    state_path=base_target,
                    repo_root=repo_root,
                    environment=environment,
                    evidence=evidence,
                )
                _run(
                    _dbt_command(
                        "build",
                        project_dir=current_project,
                        profiles_dir=current_project,
                        target_path=current_target,
                        extra=(
                            "--select",
                            "state:modified+",
                            "--defer",
                            "--state",
                            str(base_target),
                        ),
                    ),
                    cwd=repo_root,
                    environment=environment,
                    evidence=evidence,
                )
            else:
                listing = _run(
                    _dbt_command(
                        "ls",
                        project_dir=current_project,
                        profiles_dir=current_project,
                        target_path=current_target,
                        extra=(
                            "--resource-type",
                            "model",
                            "--output",
                            "json",
                            "--output-keys",
                            "name resource_type",
                        ),
                    ),
                    cwd=repo_root,
                    environment=environment,
                    evidence=evidence,
                )
                selected = _selected_models(listing.stdout)
                _run(
                    _dbt_command(
                        "build",
                        project_dir=current_project,
                        profiles_dir=current_project,
                        target_path=current_target,
                    ),
                    cwd=repo_root,
                    environment=environment,
                    evidence=evidence,
                )
            _preserve_run_results(current_target)
            _run(
                _dbt_command(
                    "docs generate",
                    project_dir=current_project,
                    profiles_dir=current_project,
                    target_path=current_target,
                ),
                cwd=repo_root,
                environment=environment,
                evidence=evidence,
            )
            _run(
                _dbt_command(
                    "source freshness",
                    project_dir=current_project,
                    profiles_dir=current_project,
                    target_path=current_target,
                ),
                cwd=repo_root,
                environment=environment,
                evidence=evidence,
            )
            if run_python_checks:
                python_environment = environment | {
                    "DBT_TARGET": "local",
                    "DBT_TARGET_SCHEMA": "analytics_local",
                }
                for command in (
                    ("make", "lint"),
                    ("make", "typecheck"),
                    ("make", "test"),
                ):
                    _run(
                        command,
                        cwd=repo_root,
                        environment=python_environment,
                        evidence=evidence,
                    )
        cleanup_ci_schemas(environment, (base_schema, current_schema))
        result = SlimCIResult(
            status="passed",
            mode=mode,
            run_id=run_id,
            base_ref=base_ref,
            base_git_sha=resolved_base_sha,
            current_git_sha=current_sha,
            selected_models=selected,
            state_directory=str(run_state),
            convention_report=str(convention_report_path),
            contract_report=str(contract_report_path),
            commands=tuple(evidence),
        )
    except (OSError, SlimCIError) as error:
        error_message = str(error)
        try:
            cleanup_ci_schemas(environment, (base_schema, current_schema))
        except (OSError, psycopg.Error, SlimCIError) as cleanup_error:
            error_message = f"{error_message}; cleanup failed: {cleanup_error}"
        result = SlimCIResult(
            status="failed",
            mode=mode,
            run_id=run_id,
            base_ref=base_ref,
            base_git_sha=resolved_base_sha,
            current_git_sha=current_sha,
            selected_models=selected,
            state_directory=str(run_state),
            convention_report=str(convention_report_path),
            contract_report=str(contract_report_path),
            commands=tuple(evidence),
            error=error_message,
        )
    _write_summary(result, summary_path)
    return result


def existing_artifacts(state_directory: Path) -> tuple[str, ...]:
    """List required or optional state artifacts that were actually produced."""
    return tuple(
        str(path)
        for side in ("base", "current")
        for name in (*_ARTIFACT_NAMES, "slim_build_run_results.json")
        if (path := state_directory / side / name).is_file()
    )
