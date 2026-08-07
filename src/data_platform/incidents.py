"""Deterministic, evidence-based data incident diagnosis."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Protocol, cast

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from data_platform.metadata_security import redact

PROHIBITED_ACTIONS = (
    "No tables were deleted or truncated.",
    "No schema, IAM, cloud resource, or production data was mutated.",
    "No unrestricted SQL or arbitrary shell command was executed.",
    "No pipeline was rerun and no Kafka offset or consumer group was changed.",
    "No PR was approved, merged, committed, or pushed.",
)
SAFE_INCIDENT_ID = re.compile(r"^[A-Z0-9][A-Z0-9_-]{2,63}$")


class IncidentState(StrEnum):
    """Allowed states in the Phase 10 diagnosis workflow."""

    RECEIVED = "RECEIVED"
    VALIDATED = "VALIDATED"
    ASSET_RESOLVED = "ASSET_RESOLVED"
    QUALITY_CHECKED = "QUALITY_CHECKED"
    LINEAGE_TRACED = "LINEAGE_TRACED"
    PIPELINE_CHECKED = "PIPELINE_CHECKED"
    KAFKA_CHECKED = "KAFKA_CHECKED"
    EVIDENCE_CORRELATED = "EVIDENCE_CORRELATED"
    DIAGNOSIS_PRODUCED = "DIAGNOSIS_PRODUCED"
    HUMAN_REVIEW_REQUIRED = "HUMAN_REVIEW_REQUIRED"
    DEGRADED_DIAGNOSIS = "DEGRADED_DIAGNOSIS"


class IncidentAlert(BaseModel):
    """Validated alert input with no arbitrary path, SQL, or shell fields."""

    model_config = ConfigDict(extra="forbid")

    incident_id: str = Field(min_length=3, max_length=64)
    alert_type: Literal["freshness", "quality", "consumer_lag", "pipeline"]
    asset: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_.-]{1,149}$")
    pipeline: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_.-]{1,149}$")
    observed_at: AwareDatetime
    severity: Literal["low", "medium", "high", "critical"]
    message: str = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def validate_identity(self) -> IncidentAlert:
        """Require a safe incident identifier and one resolvable target."""
        if not SAFE_INCIDENT_ID.fullmatch(self.incident_id):
            raise ValueError("incident_id must use uppercase letters, numbers, '_' or '-'")
        if self.asset is None and self.pipeline is None:
            raise ValueError("asset or pipeline is required")
        lowered = self.message.casefold()
        if any(marker in lowered for marker in ("../", "select ", "drop ", "rm -")):
            raise ValueError("message contains a prohibited path, SQL, or shell shape")
        return self


class EvidenceItem(BaseModel):
    """Sanitized evidence identity and bounded summary."""

    model_config = ConfigDict(extra="forbid")

    evidence_id: str
    source: str
    evidence_level: Literal[
        "static_validation",
        "simulated",
        "sandbox_observed",
        "cloud_observed",
        "local_execution",
        "not_available",
    ]
    observed_or_generated_at: AwareDatetime | None
    freshness_status: Literal["current", "stale", "unknown", "not_available"]
    tool_status: str
    sanitized_summary: str


class Fact(BaseModel):
    """A statement directly supported by evidence IDs."""

    statement: str
    evidence: list[str] = Field(min_length=1)


class Hypothesis(BaseModel):
    """An inference that remains distinct from confirmed facts."""

    description: str
    confidence: Literal["high", "medium", "low"]
    evidence: list[str]


class IncidentReport(BaseModel):
    """Stable JSON schema for a Phase 10 incident diagnosis."""

    model_config = ConfigDict(extra="forbid")

    report_type: Literal["incident_diagnosis"] = "incident_diagnosis"
    schema_version: Literal[1] = 1
    incident_id: str
    status: Literal["completed", "degraded"]
    summary: str
    state_history: list[IncidentState]
    confirmed_facts: list[Fact]
    hypotheses: list[Hypothesis]
    rejected_hypotheses: list[Hypothesis]
    most_likely_cause: Hypothesis
    affected_assets: list[str]
    customer_or_business_impact: list[str]
    recommended_actions: list[str]
    backfill_plan: list[str]
    validation_plan: list[str]
    unknowns: list[str]
    evidence_inventory: list[EvidenceItem]
    prohibited_actions_not_executed: list[str]
    human_approval_required: list[str]
    generated_at: AwareDatetime


class ReadOnlyEvidenceClient(Protocol):
    """Only the read-only call surface needed by diagnosis."""

    def call(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        """Return a structured Phase 9 tool response."""


class FixtureEvidenceClient:
    """Typed deterministic fake for scenario and unit tests."""

    def __init__(self, responses: Mapping[str, dict[str, Any]]) -> None:
        """Store fixed per-tool responses."""
        self.responses = dict(responses)

    def call(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        """Return a defensive JSON copy of one fixed response."""
        del arguments
        if tool_name not in self.responses:
            raise KeyError(tool_name)
        return cast(dict[str, Any], json.loads(json.dumps(self.responses[tool_name])))


class IncidentDiagnoser:
    """Correlate read-only tool evidence without exposing mutation methods."""

    def __init__(self, client: ReadOnlyEvidenceClient) -> None:
        """Use a client that can only issue named read-only queries."""
        self.client = client

    def diagnose(self, alert: IncidentAlert) -> IncidentReport:
        """Run the deterministic state machine and return a review-only report."""
        states = [IncidentState.RECEIVED, IncidentState.VALIDATED]
        responses, errors = self._collect(alert, states)
        evidence = self._inventory(alert, responses, errors)
        facts = self._facts(alert, responses, evidence)
        hypotheses, rejected, unknowns = self._correlate(alert, responses, errors, evidence)
        degraded = bool(errors) or any(
            (
                item.tool_status in {"partial", "not_available", "error"}
                or item.freshness_status == "stale"
            )
            and item.source != "get_cost_estimate"
            for item in evidence
        )
        states.append(IncidentState.EVIDENCE_CORRELATED)
        if degraded:
            states.append(IncidentState.DEGRADED_DIAGNOSIS)
        states.extend((IncidentState.DIAGNOSIS_PRODUCED, IncidentState.HUMAN_REVIEW_REQUIRED))
        cause = (
            hypotheses[0]
            if hypotheses
            else Hypothesis(
                description="Available evidence does not support a single root cause.",
                confidence="low",
                evidence=[],
            )
        )
        assets = self._affected_assets(alert, responses)
        return IncidentReport(
            incident_id=alert.incident_id,
            status="degraded" if degraded else "completed",
            summary=self._summary(alert, cause, degraded),
            state_history=states,
            confirmed_facts=facts,
            hypotheses=hypotheses,
            rejected_hypotheses=rejected,
            most_likely_cause=cause,
            affected_assets=assets,
            customer_or_business_impact=[
                "Data consumers may observe stale or policy-blocked analytics outputs; "
                "row-level impact is unknown."
            ],
            recommended_actions=self._recommendations(alert, cause),
            backfill_plan=self._backfill_plan(alert),
            validation_plan=self._validation_plan(alert),
            unknowns=sorted(set(unknowns)),
            evidence_inventory=evidence,
            prohibited_actions_not_executed=list(PROHIBITED_ACTIONS),
            human_approval_required=[
                "Approve any pipeline rerun or backfill after reviewing scope and idempotency.",
                "Approve any production data repair, deployment, or Kafka consumer action.",
            ],
            generated_at=alert.observed_at,
        )

    def _collect(
        self, alert: IncidentAlert, states: list[IncidentState]
    ) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
        asset = alert.asset or ""
        calls: tuple[tuple[str, dict[str, Any], IncidentState], ...] = (
            (
                "search_data_assets",
                {"query": asset or alert.pipeline or "pipeline"},
                IncidentState.ASSET_RESOLVED,
            ),
            ("get_model_owner", {"model_name": asset}, IncidentState.ASSET_RESOLVED),
            ("get_quality_status", {"model_name": asset}, IncidentState.QUALITY_CHECKED),
            (
                "get_upstream_lineage",
                {"model_name": asset, "max_depth": 5},
                IncidentState.LINEAGE_TRACED,
            ),
            (
                "get_downstream_impact",
                {"model_name": asset, "max_depth": 5},
                IncidentState.LINEAGE_TRACED,
            ),
            (
                "get_recent_pipeline_failures",
                {"pipeline_name": alert.pipeline or "retail_data_platform_pipeline", "limit": 10},
                IncidentState.PIPELINE_CHECKED,
            ),
            (
                "get_consumer_lag",
                {"consumer_group": "order-processing-group-v1"},
                IncidentState.KAFKA_CHECKED,
            ),
            (
                "get_cost_estimate",
                {"model_name": asset, "preferred_evidence_level": "best_available"},
                IncidentState.PIPELINE_CHECKED,
            ),
        )
        responses: dict[str, dict[str, Any]] = {}
        errors: dict[str, str] = {}
        for name, arguments, state in calls:
            if not asset and "model_name" in arguments:
                continue
            try:
                responses[name] = self.client.call(name, arguments)
            except (KeyError, RuntimeError, TimeoutError, ValueError) as error:
                errors[name] = type(error).__name__
            if state not in states:
                states.append(state)
        return responses, errors

    @staticmethod
    def _inventory(
        alert: IncidentAlert,
        responses: dict[str, dict[str, Any]],
        errors: dict[str, str],
    ) -> list[EvidenceItem]:
        inventory: list[EvidenceItem] = [
            EvidenceItem(
                evidence_id="alert",
                source="incident_alert",
                evidence_level="local_execution",
                observed_or_generated_at=alert.observed_at,
                freshness_status="current",
                tool_status="ok",
                sanitized_summary=(
                    f"Validated {alert.alert_type} alert for {alert.asset or alert.pipeline}."
                ),
            )
        ]
        for tool, response in sorted(responses.items()):
            status = str(response.get("status", "error"))
            level = str(response.get("evidence_level", "not_available"))
            if level not in {
                "static_validation",
                "simulated",
                "sandbox_observed",
                "cloud_observed",
                "local_execution",
                "not_available",
            }:
                level = "not_available"
            summary = json.dumps(redact(response.get("data", {})), sort_keys=True)[:500]
            observed = response.get("generated_at")
            try:
                timestamp = datetime.fromisoformat(str(observed).replace("Z", "+00:00"))
            except ValueError:
                timestamp = None
            if timestamp is not None and timestamp.tzinfo is None:
                timestamp = None
            stale = (
                bool(response.get("data", {}).get("stale_artifact_warning"))
                if isinstance(response.get("data"), dict)
                else False
            )
            stale = stale or (
                timestamp is not None
                and (alert.observed_at - timestamp).total_seconds() > 24 * 60 * 60
            )
            inventory.append(
                EvidenceItem(
                    evidence_id=f"mcp:{tool}",
                    source=tool,
                    evidence_level=level,
                    observed_or_generated_at=timestamp,
                    freshness_status="stale"
                    if stale
                    else ("not_available" if status == "not_available" else "current"),
                    tool_status=status,
                    sanitized_summary=summary,
                )
            )
        for tool, category in sorted(errors.items()):
            inventory.append(
                EvidenceItem(
                    evidence_id=f"mcp:{tool}",
                    source=tool,
                    evidence_level="not_available",
                    observed_or_generated_at=alert.observed_at,
                    freshness_status="not_available",
                    tool_status="error",
                    sanitized_summary=f"Tool failed with sanitized category {category}.",
                )
            )
        return inventory

    @staticmethod
    def _facts(
        alert: IncidentAlert,
        responses: dict[str, dict[str, Any]],
        evidence: list[EvidenceItem],
    ) -> list[Fact]:
        ids = {item.source: item.evidence_id for item in evidence}
        facts = [
            Fact(
                statement=(
                    f"A {alert.alert_type} alert was received for {alert.asset or alert.pipeline}."
                ),
                evidence=["alert"],
            )
        ]
        quality = responses.get("get_quality_status", {}).get("data", {})
        if isinstance(quality, dict) and quality.get("overall_status") in {"fail", "warn", "pass"}:
            facts.append(
                Fact(
                    statement=f"Latest indexed quality status is {quality['overall_status']}.",
                    evidence=[ids["get_quality_status"]],
                )
            )
        failures = responses.get("get_recent_pipeline_failures", {}).get("data", {})
        if isinstance(failures, dict) and failures.get("failures"):
            facts.append(
                Fact(
                    statement="Indexed pipeline failure evidence is present.",
                    evidence=[ids["get_recent_pipeline_failures"]],
                )
            )
        lag = responses.get("get_consumer_lag", {}).get("data", {})
        if isinstance(lag, dict) and isinstance(lag.get("partitions"), list):
            total = sum(
                int(row.get("lag", 0))
                for row in lag["partitions"]
                if isinstance(row, dict) and isinstance(row.get("lag"), int)
            )
            facts.append(
                Fact(
                    statement=f"Observed total Kafka consumer lag is {total}.",
                    evidence=[ids["get_consumer_lag"]],
                )
            )
        return facts

    @staticmethod
    def _correlate(
        alert: IncidentAlert,
        responses: dict[str, dict[str, Any]],
        errors: dict[str, str],
        evidence: list[EvidenceItem],
    ) -> tuple[list[Hypothesis], list[Hypothesis], list[str]]:
        ids = {item.source: item.evidence_id for item in evidence}
        quality = responses.get("get_quality_status", {}).get("data", {})
        failures = responses.get("get_recent_pipeline_failures", {}).get("data", {})
        lag = responses.get("get_consumer_lag", {}).get("data", {})
        total_lag = 0
        if isinstance(lag, dict) and isinstance(lag.get("partitions"), list):
            total_lag = sum(
                int(row.get("lag", 0))
                for row in lag["partitions"]
                if isinstance(row, dict) and isinstance(row.get("lag"), int)
            )
        hypotheses: list[Hypothesis] = []
        rejected: list[Hypothesis] = []
        if (
            isinstance(failures, dict)
            and failures.get("failures")
            and alert.alert_type == "freshness"
        ):
            hypotheses.append(
                Hypothesis(
                    description=(
                        "A recent pipeline task failure may have caused the freshness breach."
                    ),
                    confidence="high",
                    evidence=[
                        ids["get_recent_pipeline_failures"],
                        ids.get("get_quality_status", "alert"),
                    ],
                )
            )
        elif total_lag > 0 and alert.alert_type in {"freshness", "consumer_lag"}:
            hypotheses.append(
                Hypothesis(
                    description=(
                        "Upstream ingestion delay from rising Kafka lag may explain stale inputs."
                    ),
                    confidence="high",
                    evidence=[ids["get_consumer_lag"], ids.get("get_quality_status", "alert")],
                )
            )
        elif (
            isinstance(quality, dict)
            and quality.get("overall_status") == "fail"
            and alert.alert_type == "quality"
        ):
            hypotheses.append(
                Hypothesis(
                    description=(
                        "A deterministic data quality gate failure may have blocked publication."
                    ),
                    confidence="high",
                    evidence=[ids["get_quality_status"]],
                )
            )
        if total_lag == 0 and "get_consumer_lag" in ids:
            rejected.append(
                Hypothesis(
                    description="Kafka consumer backlog caused the incident.",
                    confidence="medium",
                    evidence=[ids["get_consumer_lag"]],
                )
            )
        unknowns = [f"Tool evidence unavailable: {tool}." for tool in errors]
        for item in evidence:
            if item.tool_status in {"partial", "not_available", "error"}:
                unknowns.append(f"{item.source} did not provide complete current evidence.")
        if not hypotheses:
            unknowns.append("The available evidence does not establish one causal root cause.")
        return hypotheses, rejected, unknowns

    @staticmethod
    def _affected_assets(alert: IncidentAlert, responses: dict[str, dict[str, Any]]) -> list[str]:
        assets = {alert.asset} if alert.asset else set()
        impact = responses.get("get_downstream_impact", {}).get("data", {})
        if isinstance(impact, dict):
            for item in impact.get("impacted_assets", []):
                if isinstance(item, str):
                    assets.add(item)
        return sorted(item for item in assets if item)

    @staticmethod
    def _summary(alert: IncidentAlert, cause: Hypothesis, degraded: bool) -> str:
        prefix = "Degraded diagnosis" if degraded else "Evidence-based diagnosis"
        return f"{prefix} for {alert.incident_id}: {cause.description}"

    @staticmethod
    def _recommendations(alert: IncidentAlert, cause: Hypothesis) -> list[str]:
        actions = ["Have a human inspect the cited artifacts and validate the proposed cause."]
        if "pipeline" in cause.description.casefold():
            actions.append(
                "Review the failed task and approve a bounded idempotent rerun only if safe."
            )
        if "kafka" in cause.description.casefold() or alert.alert_type == "consumer_lag":
            actions.append("Inspect consumer health and partition lag without resetting offsets.")
        if "quality" in cause.description.casefold() or alert.alert_type == "quality":
            actions.append("Identify invalid records and design a reviewed data repair plan.")
        return actions

    @staticmethod
    def _backfill_plan(alert: IncidentAlert) -> list[str]:
        return [
            f"Determine the bounded time window ending at {alert.observed_at.isoformat()}.",
            "Estimate affected models through lineage and confirm idempotent keys.",
            "Obtain human approval before executing any rerun or backfill.",
        ]

    @staticmethod
    def _validation_plan(alert: IncidentAlert) -> list[str]:
        return [
            f"Review command: make dbt-build  # human executes after approving {alert.incident_id}",
            "Review command: make dbt-test  # validation recommendation only",
            "Re-query quality, freshness, lineage impact, and Kafka lag through "
            "read-only MCP tools.",
        ]


def render_markdown(report: IncidentReport) -> str:
    """Render a stable human-readable representation without raw artifact payloads."""
    lines = [
        f"# Incident {report.incident_id}",
        "",
        f"Status: `{report.status}`",
        "",
        report.summary,
        "",
        "## Confirmed facts",
        "",
    ]
    lines.extend(
        f"- {fact.statement} Evidence: {', '.join(fact.evidence)}"
        for fact in report.confirmed_facts
    )
    lines.extend(("", "## Hypotheses", ""))
    lines.extend(
        f"- [{item.confidence}] {item.description} Evidence: {', '.join(item.evidence) or 'none'}"
        for item in report.hypotheses
    )
    lines.extend(("", "## Rejected hypotheses", ""))
    lines.extend(
        f"- [{item.confidence}] {item.description} Evidence: {', '.join(item.evidence) or 'none'}"
        for item in report.rejected_hypotheses
    )
    lines.extend(("", "## Unknowns", ""))
    lines.extend(f"- {item}" for item in report.unknowns)
    lines.extend(("", "## Affected assets", ""))
    lines.extend(f"- {item}" for item in report.affected_assets)
    lines.extend(("", "## Recommended actions", ""))
    lines.extend(f"- {item}" for item in report.recommended_actions)
    lines.extend(("", "## Backfill plan", ""))
    lines.extend(f"- {item}" for item in report.backfill_plan)
    lines.extend(("", "## Validation plan", ""))
    lines.extend(f"- {item}" for item in report.validation_plan)
    lines.extend(("", "## Human approval required", ""))
    lines.extend(f"- {item}" for item in report.human_approval_required)
    return "\n".join(lines) + "\n"


def write_incident_report(report: IncidentReport, output_root: Path) -> tuple[Path, Path]:
    """Write JSON and Markdown only below a caller-selected reports directory."""
    root = output_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    json_path = (root / f"{report.incident_id}.json").resolve()
    markdown_path = (root / f"{report.incident_id}.md").resolve()
    if not json_path.is_relative_to(root) or not markdown_path.is_relative_to(root):
        raise ValueError("incident report path escapes the output root")
    json_path.write_text(
        json.dumps(report.model_dump(mode="json"), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(render_markdown(report), encoding="utf-8")
    return json_path, markdown_path
