"""Deterministic lexical checks for local BigQuery SQL policy evidence."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from data_platform.phase8a_common import report_header, write_json

Severity = Literal["error", "warning", "info"]
_TOKEN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|::|<=|>=|<>|!=|[(),*/=<>.+-]")


class SQLAnalysisError(ValueError):
    """Raised when SQL input cannot be safely tokenized."""


@dataclass(frozen=True, slots=True)
class SQLFinding:
    """One deterministic SQL policy finding."""

    severity: Severity
    rule: str
    model: str
    message: str


@dataclass(frozen=True, slots=True)
class SQLPolicyReport:
    """SQL policy results for bounded local query examples."""

    status: Literal["passed", "blocked"]
    models_checked: tuple[str, ...]
    findings: tuple[SQLFinding, ...]
    git_sha: str | None = None

    @property
    def error_count(self) -> int:
        """Return the number of blocking findings."""
        return sum(item.severity == "error" for item in self.findings)

    def to_dict(self) -> dict[str, Any]:
        """Return a machine-readable report."""
        counts = Counter(item.severity for item in self.findings)
        value = report_header(
            "bigquery_sql_policy", git_sha=self.git_sha, evidence_level="static_validation"
        )
        levels: tuple[Severity, ...] = ("error", "warning", "info")
        value.update(
            {
                "status": self.status,
                "models_checked": list(self.models_checked),
                "finding_counts": {level: counts[level] for level in levels},
                "policy_findings": [asdict(item) for item in self.findings],
                "errors": [item.message for item in self.findings if item.severity == "error"],
                "warnings": [item.message for item in self.findings if item.severity == "warning"],
            }
        )
        return value


def mask_comments_and_literals(sql: str) -> str:
    """Mask comments and quoted content while preserving positions and punctuation."""
    output = list(sql)
    index = 0
    state: str | None = None
    quote = ""
    while index < len(sql):
        pair = sql[index : index + 2]
        char = sql[index]
        if state is None:
            if pair == "--":
                state = "line"
                output[index] = output[index + 1] = " "
                index += 2
                continue
            if pair == "/*":
                state = "block"
                output[index] = output[index + 1] = " "
                index += 2
                continue
            if char in ("'", '"', "`"):
                state = "quote"
                quote = char
                output[index] = " "
                index += 1
                continue
            index += 1
            continue
        output[index] = " "
        if state == "line":
            if char == "\n":
                state = None
            index += 1
            continue
        if state == "block":
            if pair == "*/":
                output[index + 1] = " "
                state = None
                index += 2
            else:
                index += 1
            continue
        if char == quote:
            if index + 1 < len(sql) and sql[index + 1] == quote:
                output[index + 1] = " "
                index += 2
            else:
                state = None
                index += 1
        elif char == "\\" and index + 1 < len(sql):
            output[index + 1] = " "
            index += 2
        else:
            index += 1
    if state not in (None, "line"):
        raise SQLAnalysisError(f"unbalanced SQL {state}")
    return "".join(output)


def tokenize(sql: str) -> tuple[str, ...]:
    """Return normalized SQL tokens after safe masking."""
    return tuple(item.lower() for item in _TOKEN.findall(mask_comments_and_literals(sql)))


def _has_bounded_predicate(tokens: tuple[str, ...], field: str) -> bool:
    lower = False
    upper = False
    for index, token in enumerate(tokens):
        if token != field.lower():
            continue
        nearby = tokens[index + 1 : index + 4]
        lower = lower or any(item in {">", ">=", "between"} for item in nearby)
        upper = upper or any(item in {"<", "<=", "between"} for item in nearby)
        if index and tokens[index - 1] == "between":
            lower = upper = True
    return lower and upper


def _has_comma_join(tokens: tuple[str, ...]) -> bool:
    """Detect a top-level comma only inside the first FROM clause."""
    try:
        index = tokens.index("from") + 1
    except ValueError:
        return False
    depth = 0
    terminators = {"where", "group", "having", "order", "limit", "union", "qualify"}
    for lexeme in tokens[index:]:
        if lexeme == "(":
            depth += 1
        elif lexeme == ")":
            depth = max(0, depth - 1)
        elif depth == 0 and lexeme in terminators:
            return False
        elif depth == 0 and lexeme == ",":
            return True
    return False


def analyze_sql(
    model: str,
    sql: str,
    *,
    partition_field: str | None,
    require_partition_filter: bool,
    monetary: bool = False,
    incremental: bool = False,
) -> tuple[SQLFinding, ...]:
    """Analyze one query without claiming parser or runtime compatibility."""
    findings: list[SQLFinding] = []
    try:
        masked = mask_comments_and_literals(sql)
        tokens = tuple(item.lower() for item in _TOKEN.findall(masked))
    except SQLAnalysisError as error:
        return (SQLFinding("error", "sql-unknown", model, str(error)),)
    token_set = set(tokens)
    if "BLOCKING_TODO" in sql:
        findings.append(SQLFinding("error", "blocking-todo", model, "query contains BLOCKING_TODO"))
    for index, word in enumerate(tokens[:-1]):
        if word == "select" and tokens[index + 1] == "*":
            findings.append(
                SQLFinding("error", "select-star", model, "wildcard projection is prohibited")
            )
    if any(tokens[index : index + 2] == ("cross", "join") for index in range(len(tokens) - 1)):
        findings.append(
            SQLFinding("error", "cross-join", model, "unbounded CROSS JOIN is prohibited")
        )
    if _has_comma_join(tokens):
        findings.append(
            SQLFinding(
                "warning", "comma-join", model, "comma join requires cartesian-product review"
            )
        )
    if require_partition_filter and partition_field:
        if not _has_bounded_predicate(tokens, partition_field):
            findings.append(
                SQLFinding(
                    "error",
                    "partition-predicate",
                    model,
                    f"bounded lower and upper predicates are required for {partition_field}",
                )
            )
        hostile = re.search(
            rf"\b(date|cast|extract)\s*\(\s*{re.escape(partition_field)}\b", masked, re.IGNORECASE
        )
        if hostile:
            findings.append(
                SQLFinding(
                    "warning",
                    "partition-transformation",
                    model,
                    "partition field transformation may prevent pruning",
                )
            )
    if (
        incremental
        and "lookback" not in sql.lower()
        and not (partition_field and _has_bounded_predicate(tokens, partition_field))
    ):
        findings.append(
            SQLFinding(
                "error",
                "incremental-lookback",
                model,
                "incremental query requires bounded lookback evidence",
            )
        )
    if monetary and "sum" in token_set:
        try:
            group_index = tokens.index("group")
            grouped = tokens[group_index + 1 :]
        except ValueError:
            grouped = ()
        if "currency" not in grouped:
            findings.append(
                SQLFinding(
                    "error",
                    "currency-grouping",
                    model,
                    "monetary aggregation must preserve currency grouping",
                )
            )
    if "/" in tokens and not ({"nullif", "safe_divide", "case"} & token_set):
        findings.append(
            SQLFinding("error", "unsafe-division", model, "division requires a zero-safe policy")
        )
    postgres_patterns = (
        (r"::\s*[A-Za-z_]", "postgres-cast"),
        (r"\bfilter\s*\(\s*where\b", "postgres-filter"),
        (r"\bdistinct\s+on\s*\(", "postgres-distinct-on"),
        (r"\bat\s+time\s+zone\b", "postgres-time-zone"),
        (r"\bbool_or\s*\(", "postgres-bool-or"),
        (r"\barray_agg\s*\(", "postgres-array"),
        (r"count\s*\(\s*distinct\s*\(", "postgres-tuple-distinct"),
    )
    for pattern, rule in postgres_patterns:
        if re.search(pattern, masked, re.IGNORECASE):
            findings.append(
                SQLFinding(
                    "warning",
                    rule,
                    model,
                    "PostgreSQL-only syntax remains planned, not BigQuery runtime validated",
                )
            )
    findings.append(
        SQLFinding(
            "info", "query-checked", model, "query was checked by the local deterministic lexer"
        )
    )
    return tuple(findings)


def validate_sql_directory(
    directory: Path, policies: dict[str, dict[str, Any]], *, git_sha: str | None = None
) -> SQLPolicyReport:
    """Validate model-named SQL examples from one directory."""
    findings: list[SQLFinding] = []
    models: list[str] = []
    for path in sorted(directory.glob("*.sql")):
        model = path.stem
        policy = policies.get(model)
        if policy is None:
            findings.append(SQLFinding("error", "query-policy", model, "query has no model policy"))
            continue
        models.append(model)
        partition = policy.get("partition_by")
        field = partition.get("field") if isinstance(partition, dict) else None
        findings.extend(
            analyze_sql(
                model,
                path.read_text(encoding="utf-8"),
                partition_field=field if isinstance(field, str) else None,
                require_partition_filter=policy.get("require_partition_filter") is True,
                monetary=model == "mart_daily_sales",
            )
        )
    ordered = tuple(sorted(findings, key=lambda item: (item.model, item.severity, item.rule)))
    return SQLPolicyReport(
        status="blocked" if any(item.severity == "error" for item in ordered) else "passed",
        models_checked=tuple(sorted(models)),
        findings=ordered,
        git_sha=git_sha,
    )


def write_sql_policy_report(report: SQLPolicyReport, path: Path) -> None:
    """Write a SQL policy report."""
    write_json(path, report.to_dict())
