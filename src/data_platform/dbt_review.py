"""Deterministic-first dbt change review rules for Phase 10."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class ReviewFinding(BaseModel):
    """Stable finding schema required by the review Skill."""

    model_config = ConfigDict(extra="forbid")

    severity: Literal["blocking", "warning", "suggestion"]
    file: str
    model: str
    rule: str
    reason: str
    impact: str
    recommendation: str
    evidence: list[str]


class ReviewColumn(BaseModel):
    """Documented current column contract used by deterministic review."""

    model_config = ConfigDict(extra="forbid")

    data_type: str = Field(min_length=1, max_length=100)
    description: str = Field(max_length=500)


class ReviewModel(BaseModel):
    """Normalized changed-model fixture or adapter input."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=r"^(stg_|int_|fct_|dim_|mart_)[a-z0-9_]+$")
    file: str = Field(
        pattern=r"^dbt/models/(staging|intermediate|marts)/[a-z0-9_./-]+\.(sql|ya?ml)$"
    )
    layer: Literal["staging", "intermediate", "marts"]
    sql: str
    description: str = ""
    columns: dict[str, ReviewColumn] = Field(default_factory=dict)
    owner: str | None = None
    slo: dict[str, object] = Field(default_factory=dict)
    contract_enforced: bool = False
    tests: list[str] = Field(default_factory=list)
    baseline_columns: dict[str, str] | None = None
    baseline_grain: str | None = None
    materialized: str = "view"
    downstream_assets: list[str] | None = None


class DbtReviewRequest(BaseModel):
    """Input containing normalized changed models."""

    model_config = ConfigDict(extra="forbid")

    generated_at: AwareDatetime
    models: list[ReviewModel]


class DbtReviewReport(BaseModel):
    """Machine-readable deterministic PR review."""

    report_type: Literal["dbt_pr_review"] = "dbt_pr_review"
    schema_version: Literal[1] = 1
    status: Literal["passed", "warning", "blocked", "degraded"]
    evidence_level: Literal["static_validation"] = "static_validation"
    findings: list[ReviewFinding]
    finding_counts: dict[str, int]
    deterministic_status: Literal["deterministic"] = "deterministic"
    generated_at: AwareDatetime


def detect_changed_model_paths(paths: list[str]) -> list[str]:
    """Return stable dbt model SQL/YAML paths from an explicit changed-path list."""
    return sorted(
        {
            path
            for path in paths
            if path.startswith("dbt/models/")
            and path.endswith((".sql", ".yml", ".yaml"))
            and ".." not in Path(path).parts
        }
    )


def _finding(
    model: ReviewModel,
    severity: Literal["blocking", "warning", "suggestion"],
    rule: str,
    reason: str,
    impact: str,
    recommendation: str,
    evidence: list[str],
) -> ReviewFinding:
    return ReviewFinding(
        severity=severity,
        file=model.file,
        model=model.name,
        rule=rule,
        reason=reason,
        impact=impact,
        recommendation=recommendation,
        evidence=evidence,
    )


def review_dbt_changes(request: DbtReviewRequest) -> DbtReviewReport:
    """Apply deterministic contract, documentation, SQL, and cost rules."""
    findings: list[ReviewFinding] = []
    degraded = False
    normalized_sql: dict[str, list[ReviewModel]] = {}
    for model in request.models:
        sql = re.sub(r"--.*?$", "", model.sql, flags=re.MULTILINE).casefold()
        normalized_sql.setdefault(re.sub(r"\s+", " ", sql).strip(), []).append(model)
        if model.layer != "staging" and "source(" in sql:
            findings.append(
                _finding(
                    model,
                    "blocking",
                    "direct-source-usage",
                    "Non-staging model reads a source directly.",
                    "Layer boundaries and reusable business logic are bypassed.",
                    "Reference a staging or intermediate model.",
                    [model.file],
                )
            )
        if re.search(r"\bselect\s+(?:distinct\s+)?\*", sql):
            findings.append(
                _finding(
                    model,
                    "blocking",
                    "select-star",
                    "Published projection uses SELECT *.",
                    "Schema can change without contract review.",
                    "List columns explicitly.",
                    [model.file],
                )
            )
        if not model.description.strip():
            findings.append(
                _finding(
                    model,
                    "blocking",
                    "missing-model-description",
                    "Model description is missing.",
                    "Consumers cannot verify semantics or grain.",
                    "Add a model description beginning with its grain where required.",
                    [model.file],
                )
            )
        elif model.layer in {
            "intermediate",
            "marts",
        } and not model.description.casefold().startswith("grain: one row per "):
            findings.append(
                _finding(
                    model,
                    "blocking",
                    "missing-grain",
                    "Published grain is not explicit.",
                    "Uniqueness and join safety cannot be reviewed.",
                    "Start the description with 'Grain: one row per ...'.",
                    [model.file],
                )
            )
        if model.layer == "marts":
            if not model.owner:
                findings.append(
                    _finding(
                        model,
                        "blocking",
                        "missing-owner",
                        "Published mart has no owner.",
                        "Operational accountability is unknown.",
                        "Declare meta.owner.",
                        [model.file],
                    )
                )
            if not model.slo:
                findings.append(
                    _finding(
                        model,
                        "blocking",
                        "missing-slo",
                        "Published mart has no SLO.",
                        "Freshness expectations cannot be enforced.",
                        "Declare freshness and availability metadata.",
                        [model.file],
                    )
                )
            if not model.contract_enforced:
                findings.append(
                    _finding(
                        model,
                        "blocking",
                        "missing-contract",
                        "Published mart contract is not enforced.",
                        "Breaking changes are not bounded.",
                        "Enable the model contract.",
                        [model.file],
                    )
                )
        for column, metadata in sorted(model.columns.items()):
            if not metadata.description.strip():
                findings.append(
                    _finding(
                        model,
                        "blocking",
                        "missing-column-description",
                        f"Column {column} has no description.",
                        "Generated docs are incomplete.",
                        "Document the column semantics.",
                        [f"{model.file}:{column}"],
                    )
                )
        if model.baseline_columns is None:
            degraded = True
            findings.append(
                _finding(
                    model,
                    "warning",
                    "baseline-unavailable",
                    "Previous contract artifact is unavailable.",
                    "Breaking changes cannot be ruled out.",
                    "Provide the previous manifest or contract fixture.",
                    [],
                )
            )
        else:
            for column, old_type in sorted(model.baseline_columns.items()):
                if column not in model.columns:
                    findings.append(
                        _finding(
                            model,
                            "blocking",
                            "published-contract-column-removed",
                            f"Published column {column} was removed.",
                            "Downstream consumers can fail.",
                            "Restore the column or version the data product.",
                            [f"baseline:{column}"],
                        )
                    )
                elif old_type.casefold() != model.columns[column].data_type.casefold():
                    findings.append(
                        _finding(
                            model,
                            "blocking",
                            "published-contract-type-changed",
                            (
                                f"Column {column} changed type from {old_type} "
                                f"to {model.columns[column].data_type}."
                            ),
                            "Consumers may fail or silently coerce values.",
                            "Restore compatibility or version the data product.",
                            [f"baseline:{column}"],
                        )
                    )
        if (
            model.baseline_grain
            and model.description
            and model.baseline_grain.casefold() not in model.description.casefold()
        ):
            findings.append(
                _finding(
                    model,
                    "blocking",
                    "published-grain-changed",
                    "Published grain differs from baseline.",
                    "Row counts and consumer joins can change.",
                    "Version the data product or restore the grain.",
                    ["baseline:grain"],
                )
            )
        if model.materialized == "incremental":
            if "is_incremental()" not in sql or not re.search(r"dateadd|interval|lookback", sql):
                findings.append(
                    _finding(
                        model,
                        "blocking",
                        "unsafe-incremental-lookback",
                        "Incremental model has no bounded late-data lookback.",
                        "Late events may be permanently omitted.",
                        "Add an idempotent watermark with bounded lookback.",
                        [model.file],
                    )
                )
            if "event_date" in sql and not re.search(
                r"where[\s\S]*(event_date|_partitiondate)\s*(>=|=|between)", sql
            ):
                findings.append(
                    _finding(
                        model,
                        "warning",
                        "missing-partition-filter",
                        "Partitioned query has no bounded partition predicate.",
                        "BigQuery execution may scan excessive data.",
                        "Add a required bounded partition filter.",
                        [model.file],
                    )
                )
        if re.search(r"sum\s*\([^)]*(amount|revenue)", sql) and "currency" not in sql:
            findings.append(
                _finding(
                    model,
                    "blocking",
                    "multi-currency-aggregation",
                    "Money is aggregated without a currency grouping or FX policy.",
                    "Formal monetary totals may combine incompatible units.",
                    "Group by currency or apply an approved FX conversion.",
                    [model.file],
                )
            )
        if re.search(r"\s/\s*[a-z_(]", sql) and "nullif(" not in sql and "safe_divide(" not in sql:
            findings.append(
                _finding(
                    model,
                    "warning",
                    "divide-by-zero",
                    "Division has no zero-denominator guard.",
                    "The model may fail or return invalid metrics.",
                    "Use nullif or safe_divide.",
                    [model.file],
                )
            )
        if re.search(r"avg\s*\([^)]*(average|avg)", sql):
            findings.append(
                _finding(
                    model,
                    "warning",
                    "unweighted-average",
                    "An average of pre-aggregated averages may be unweighted.",
                    "Published metrics can be biased.",
                    "Use numerator/denominator weighted aggregation.",
                    [model.file],
                )
            )
        if re.search(r"\bjoin\b", sql) and not re.search(r"\bon\b", sql):
            findings.append(
                _finding(
                    model,
                    "blocking",
                    "join-explosion-risk",
                    "JOIN has no detected predicate.",
                    "Row multiplication can corrupt the grain.",
                    "Add and test a grain-preserving join predicate.",
                    [model.file],
                )
            )
        if model.downstream_assets is None:
            degraded = True
            findings.append(
                _finding(
                    model,
                    "warning",
                    "lineage-unavailable",
                    "Downstream lineage evidence is unavailable.",
                    "Impact scope is unknown.",
                    "Query Phase 9 get_downstream_impact.",
                    [],
                )
            )
        required_tests = {"unique", "not_null"} if model.layer == "marts" else set()
        missing_tests = sorted(required_tests - set(model.tests))
        if missing_tests:
            findings.append(
                _finding(
                    model,
                    "blocking",
                    "missing-required-tests",
                    f"Required tests are missing: {missing_tests}.",
                    "The declared grain or required keys are not enforced.",
                    "Add the missing deterministic tests.",
                    [model.file],
                )
            )
    for models in normalized_sql.values():
        if len(models) < 2:
            continue
        names = sorted(model.name for model in models)
        for model in models:
            findings.append(
                _finding(
                    model,
                    "warning",
                    "duplicated-business-logic",
                    f"Identical normalized SQL appears in models {names}.",
                    "Business logic may diverge when maintained in multiple places.",
                    "Extract the shared transformation into one reviewed intermediate model.",
                    [item.file for item in models],
                )
            )
    ordered = sorted(
        findings, key=lambda item: (item.severity, item.file, item.model, item.rule, item.reason)
    )
    counts = {
        name: sum(item.severity == name for item in ordered)
        for name in ("blocking", "warning", "suggestion")
    }
    status: Literal["passed", "warning", "blocked", "degraded"]
    if counts["blocking"]:
        status = "blocked"
    elif degraded:
        status = "degraded"
    elif counts["warning"]:
        status = "warning"
    else:
        status = "passed"
    return DbtReviewReport(
        status=status,
        findings=ordered,
        finding_counts=counts,
        generated_at=request.generated_at,
    )


def write_review_report(report: DbtReviewReport, path: Path) -> None:
    """Write stable JSON findings."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report.model_dump(mode="json"), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
