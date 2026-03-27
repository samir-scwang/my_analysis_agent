from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from app.agents.deep_analysis.models import (
    ArtifactRef,
    CaveatDraft,
    ClaimDraft,
    DeepAnalysisAgentOutput,
    ExecutedStepTrace,
    FindingDraft,
)
from app.schemas.evidence import (
    ArtifactManifest,
    Caveat,
    ClaimEvidenceMapItem,
    ClaimSupport,
    EvidenceChart,
    EvidencePack,
    EvidenceTable,
    ExecutedStep,
    Finding,
    Provenance,
)


def _to_iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_chart_type(path: str) -> str:
    suffix = Path(path).suffix.lower()
    if suffix == ".png":
        return "image_chart"
    return "chart"


def _infer_table_format(path: str) -> str:
    suffix = Path(path).suffix.lower()
    if suffix == ".csv":
        return "csv"
    if suffix in {".xlsx", ".xlsm", ".xltx", ".xltm"}:
        return "xlsx"
    if suffix == ".json":
        return "json"
    return "markdown"


def _file_exists(path: str) -> bool:
    try:
        return Path(path).exists()
    except Exception:
        return False


def _artifact_index(artifacts: List[ArtifactRef]) -> Dict[str, ArtifactRef]:
    return {a.artifact_id: a for a in artifacts}


def _build_tables(artifacts: List[ArtifactRef]) -> List[EvidenceTable]:
    tables: List[EvidenceTable] = []

    for artifact in artifacts:
        if artifact.artifact_type != "table":
            continue
        if not _file_exists(artifact.path):
            continue

        tables.append(
            EvidenceTable(
                table_id=artifact.artifact_id,
                title=artifact.title,
                table_type="agent_generated_table",
                path=artifact.path,
                format=_infer_table_format(artifact.path),  # type: ignore[arg-type]
                columns=[],
                row_count=0,
                description=artifact.description,
            )
        )

    return tables


def _build_charts(artifacts: List[ArtifactRef]) -> List[EvidenceChart]:
    charts: List[EvidenceChart] = []

    for artifact in artifacts:
        if artifact.artifact_type != "chart":
            continue
        if not _file_exists(artifact.path):
            continue

        charts.append(
            EvidenceChart(
                chart_id=artifact.artifact_id,
                title=artifact.title,
                chart_type=_normalize_chart_type(artifact.path),
                path=artifact.path,
                x="unknown",
                y="unknown",
                purpose=artifact.description or "",
                topic_tags=artifact.topic_tags,
                information_density_score=None,
            )
        )

    return charts


def _build_findings(findings: List[FindingDraft], artifacts_by_id: Dict[str, ArtifactRef]) -> List[Finding]:
    result: List[Finding] = []

    for item in findings:
        result.append(
            Finding(
                finding_id=item.finding_id,
                title=item.title,
                statement=item.statement,
                category=item.category,
                importance=item.importance,
                confidence=item.confidence,
                supporting_claim_ids=[],
                topic_tags=item.topic_tags,
            )
        )

    return result


def _build_caveats(caveats: List[CaveatDraft]) -> List[Caveat]:
    result: List[Caveat] = []

    for item in caveats:
        result.append(
            Caveat(
                caveat_id=item.caveat_id,
                message=item.message,
                severity=item.severity,
                related_claim_ids=item.related_claim_ids,
            )
        )

    return result


def _claim_to_map_item(claim: ClaimDraft) -> ClaimEvidenceMapItem:
    return ClaimEvidenceMapItem(
        claim_id=claim.claim_id,
        claim_text=claim.claim_text,
        claim_type=claim.claim_type,
        confidence=claim.confidence,
        support=ClaimSupport(
            table_ids=claim.table_ids,
            chart_ids=claim.chart_ids,
            finding_ids=claim.finding_ids,
            stat_refs=claim.stat_refs,
        ),
        caveat_ids=claim.caveat_ids,
    )


def _build_claims(claims: List[ClaimDraft]) -> List[ClaimEvidenceMapItem]:
    return [_claim_to_map_item(claim) for claim in claims]


def _build_executed_steps(steps: List[ExecutedStepTrace]) -> List[ExecutedStep]:
    result: List[ExecutedStep] = []

    for step in steps:
        result.append(
            ExecutedStep(
                step_id=step.step_id,
                step_type=step.step_type,
                description=step.description,
                code_ref=step.code_ref,
                output_refs=step.output_refs,
                status=step.status,
            )
        )

    return result


def _link_findings_to_claims(
    findings: List[Finding],
    claims: List[ClaimEvidenceMapItem],
) -> List[Finding]:
    claim_map: Dict[str, List[str]] = {}
    for claim in claims:
        for fid in claim.support.finding_ids:
            claim_map.setdefault(fid, []).append(claim.claim_id)

    linked: List[Finding] = []
    for finding in findings:
        linked.append(
            finding.model_copy(
                update={
                    "supporting_claim_ids": claim_map.get(finding.finding_id, []),
                }
            )
        )
    return linked


def _build_ranked_findings(findings: List[Finding]) -> List[str]:
    priority_order = {"high": 3, "medium": 2, "low": 1}
    confidence_order = {"high": 3, "medium": 2, "low": 1}

    ranked = sorted(
        findings,
        key=lambda x: (
            priority_order.get(x.importance, 0),
            confidence_order.get(x.confidence, 0),
        ),
        reverse=True,
    )
    return [f.finding_id for f in ranked]


def _build_artifact_manifest(
    tables: List[EvidenceTable],
    charts: List[EvidenceChart],
) -> ArtifactManifest:
    return ArtifactManifest(
        chart_paths=[c.path for c in charts],
        table_paths=[t.path for t in tables],
        appendix_paths=[],
    )


def _build_analysis_plan(
    *,
    analysis_brief: Dict[str, Any],
    dataset_context: Dict[str, Any],
    execution_mode: str,
    revision_context: Dict[str, Any],
    agent_output: DeepAnalysisAgentOutput,
) -> Dict[str, Any]:
    return {
        "topics": analysis_brief.get("must_cover_topics", []),
        "metrics": analysis_brief.get("recommended_metrics", []),
        "dimensions": analysis_brief.get("recommended_dimensions", []),
        "planned_chart_families": analysis_brief.get("chart_policy", {}).get("preferred_chart_types", []),
        "planned_tables": analysis_brief.get("table_policy", {}).get("must_have_tables", []),
        "analysis_actions": [a.model_dump() for a in agent_output.planned_actions],
        "planner_notes": agent_output.plan.get("planner_notes", "deepagent_plan"),
        "execution_mode": execution_mode,
        "revision_context": revision_context,
        "dataset_business_hints": dataset_context.get("business_hints", []),
    }


def _build_dataset_summary(
    *,
    state: Dict[str, Any],
    dataset_context: Dict[str, Any],
    dataset_path: str,
) -> Dict[str, Any]:
    table_profile = {}
    tables = dataset_context.get("tables", [])
    if tables:
        table_profile = tables[0] or {}

    return {
        "dataset_id": state.get("dataset_id"),
        "source_path": dataset_path,
        "row_count": table_profile.get("row_count", 0),
        "column_count": table_profile.get("column_count", 0),
        "key_dimensions": dataset_context.get("candidate_dimension_columns", []),
        "key_measures": dataset_context.get("candidate_measure_columns", []),
        "time_coverage": dataset_context.get("time_coverage", {}),
    }


def build_evidence_pack_from_agent_output(
    *,
    state: Dict[str, Any],
    agent_output: DeepAnalysisAgentOutput,
    revision_round: int,
) -> EvidencePack:
    dataset_context = state.get("dataset_context", {}) or {}
    analysis_brief = state.get("analysis_brief", {}) or {}
    dataset_path = state.get("dataset_path", "")
    execution_mode = state.get("execution_mode", "normal")
    revision_context = state.get("revision_context", {}) or {}

    artifacts_by_id = _artifact_index(agent_output.artifacts)
    tables = _build_tables(agent_output.artifacts)
    charts = _build_charts(agent_output.artifacts)
    findings = _build_findings(agent_output.findings, artifacts_by_id)
    claims = _build_claims(agent_output.claims)
    findings = _link_findings_to_claims(findings, claims)
    caveats = _build_caveats(agent_output.caveats)
    executed_steps = _build_executed_steps(agent_output.executed_steps)
    ranked_findings = _build_ranked_findings(findings)

    artifact_manifest = _build_artifact_manifest(tables, charts)

    evidence_pack_id = f"ep_{revision_round + 1:03d}"
    parent_history = state.get("evidence_pack_history", []) or []
    parent_evidence_pack_id = None
    if parent_history:
        parent_evidence_pack_id = parent_history[-1].get("evidence_pack_id")

    revision_applied: List[str] = []
    if execution_mode == "revision":
        for item in revision_context.get("revision_tasks", []) or []:
            task_id = item.get("task_id")
            if task_id:
                revision_applied.append(task_id)

    return EvidencePack(
        evidence_pack_id=evidence_pack_id,
        parent_evidence_pack_id=parent_evidence_pack_id,
        version=1,
        analysis_round=revision_round,
        revision_applied=revision_applied,
        dataset_summary=_build_dataset_summary(
            state=state,
            dataset_context=dataset_context,
            dataset_path=dataset_path,
        ),
        analysis_plan=_build_analysis_plan(
            analysis_brief=analysis_brief,
            dataset_context=dataset_context,
            execution_mode=execution_mode,
            revision_context=revision_context,
            agent_output=agent_output,
        ),
        executed_steps=executed_steps,
        findings=findings,
        ranked_findings=ranked_findings,
        tables=tables,
        charts=charts,
        claim_evidence_map=claims,
        caveats=caveats,
        rejected_charts=agent_output.rejected_charts,
        rejected_hypotheses=agent_output.rejected_hypotheses,
        artifact_manifest=artifact_manifest,
        provenance=Provenance(
            generated_at=_to_iso_utc_now(),
            execution_environment="deepagent+python+pandas+matplotlib",
        ),
        superseded_items={},
    )
