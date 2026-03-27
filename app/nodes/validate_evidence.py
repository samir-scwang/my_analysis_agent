from __future__ import annotations

import os
from typing import Any, Dict, List, Set, cast

from app.schemas.state import AnalysisGraphState
from app.schemas.validation import (
    ArtifactCheck,
    CoverageCheck,
    ValidationIssue,
    ValidationResult,
)
from app.services.dataframe_io import is_tabular_path, load_dataframe


def _collect_chart_ids(evidence_pack: Dict[str, Any]) -> Set[str]:
    return {c.get("chart_id") for c in evidence_pack.get("charts", []) if c.get("chart_id")}


def _collect_table_ids(evidence_pack: Dict[str, Any]) -> Set[str]:
    return {t.get("table_id") for t in evidence_pack.get("tables", []) if t.get("table_id")}


def _collect_chart_topic_tags(evidence_pack: Dict[str, Any]) -> Set[str]:
    tags: Set[str] = set()
    for c in evidence_pack.get("charts", []):
        for tag in c.get("topic_tags", []) or []:
            tags.add(tag)
    return tags


def _collect_finding_topic_tags(evidence_pack: Dict[str, Any]) -> Set[str]:
    tags: Set[str] = set()
    for f in evidence_pack.get("findings", []):
        for tag in f.get("topic_tags", []) or []:
            tags.add(tag)
    return tags


def _check_required_sections(evidence_pack: Dict[str, Any]) -> List[ValidationIssue]:
    errors: List[ValidationIssue] = []

    required_keys = [
        "dataset_summary",
        "analysis_plan",
        "executed_steps",
        "findings",
        "tables",
        "charts",
        "claim_evidence_map",
        "artifact_manifest",
        "provenance",
    ]

    for key in required_keys:
        if key not in evidence_pack:
            errors.append(
                ValidationIssue(
                    type="missing_required_section",
                    message=f"evidence_pack is missing required section: {key}",
                )
            )

    return errors


def _check_artifact_paths(evidence_pack: Dict[str, Any]) -> ArtifactCheck:
    artifact_check = ArtifactCheck()

    for chart in evidence_pack.get("charts", []):
        chart_id = chart.get("chart_id", "unknown_chart")
        path = chart.get("path")
        if not path or not os.path.exists(path):
            artifact_check.missing_chart_files.append(chart_id)

    for table in evidence_pack.get("tables", []):
        table_id = table.get("table_id", "unknown_table")
        path = table.get("path")
        if not path or not os.path.exists(path):
            artifact_check.missing_table_files.append(table_id)
            continue

        # 简单检查表是否为空
        try:
            if is_tabular_path(path):
                df = load_dataframe(path)
                if df.empty:
                    artifact_check.empty_tables.append(table_id)
        except Exception:
            # 读表失败也当成 warning 更合适，这里先算 empty/bad asset
            artifact_check.empty_tables.append(table_id)

    return artifact_check


def _check_claim_support_links(evidence_pack: Dict[str, Any]) -> List[str]:
    broken_links: List[str] = []

    chart_ids = _collect_chart_ids(evidence_pack)
    table_ids = _collect_table_ids(evidence_pack)
    finding_ids = {
        f.get("finding_id")
        for f in evidence_pack.get("findings", [])
        if f.get("finding_id")
    }

    for claim in evidence_pack.get("claim_evidence_map", []):
        claim_id = claim.get("claim_id", "unknown_claim")
        support = claim.get("support", {}) or {}

        support_table_ids = support.get("table_ids", []) or []
        support_chart_ids = support.get("chart_ids", []) or []
        support_finding_ids = support.get("finding_ids", []) or []

        # 至少要有某种 support
        if not (support_table_ids or support_chart_ids or support_finding_ids):
            broken_links.append(f"{claim_id}: no support attached")
            continue

        for tid in support_table_ids:
            if tid not in table_ids:
                broken_links.append(f"{claim_id}: missing table ref {tid}")

        for cid in support_chart_ids:
            if cid not in chart_ids:
                broken_links.append(f"{claim_id}: missing chart ref {cid}")

        for fid in support_finding_ids:
            if fid not in finding_ids:
                broken_links.append(f"{claim_id}: missing finding ref {fid}")

    return broken_links


def _check_topic_coverage(
    analysis_brief: Dict[str, Any],
    evidence_pack: Dict[str, Any],
) -> CoverageCheck:
    must_cover_topics = analysis_brief.get("must_cover_topics", []) or []

    chart_tags = _collect_chart_topic_tags(evidence_pack)
    finding_tags = _collect_finding_topic_tags(evidence_pack)

    covered: List[str] = []
    missing: List[str] = []

    # 有些 topic 可以通过 findings 覆盖，有些最好也至少有 chart/table
    # 这里先做轻量检查：topic 出现在 chart tags 或 finding tags 即视为覆盖
    covered_tags = chart_tags.union(finding_tags)

    for topic in must_cover_topics:
        if topic in covered_tags:
            covered.append(topic)
        else:
            missing.append(topic)

    return CoverageCheck(
        must_cover_topics_total=len(must_cover_topics),
        must_cover_topics_covered=len(covered),
        covered_topics=covered,
        missing_topics=missing,
    )


def _detect_redundancy_signals(evidence_pack: Dict[str, Any]) -> List[dict]:
    signals: List[dict] = []
    charts = evidence_pack.get("charts", []) or []

    # 非常轻量的重复检测：同 chart_type + 同 x + 同 y 视为疑似重复
    seen = {}
    for chart in charts:
        key = (
            chart.get("chart_type"),
            str(chart.get("x")),
            str(chart.get("y")),
        )
        chart_id = chart.get("chart_id")
        if key in seen:
            signals.append(
                {
                    "type": "possible_duplicate_chart",
                    "chart_ids": [seen[key], chart_id],
                    "reason": "same chart_type/x/y signature",
                }
            )
        else:
            seen[key] = chart_id

    return signals


def validate_evidence_node(state: AnalysisGraphState) -> AnalysisGraphState:
    warnings = list(state.get("warnings", []))
    errors = list(state.get("errors", []))

    analysis_brief = state.get("analysis_brief")
    evidence_pack = state.get("evidence_pack")

    if not analysis_brief or analysis_brief.get("status") == "stub":
        errors.append(
            {
                "type": "missing_analysis_brief",
                "message": "analysis_brief is required before validate_evidence.",
            }
        )
        return cast(
            AnalysisGraphState,
            {
                **state,
                "status": "FAILED",
                "errors": errors,
                "warnings": warnings,
            },
        )

    if not evidence_pack or evidence_pack.get("status") == "stub":
        errors.append(
            {
                "type": "missing_evidence_pack",
                "message": "evidence_pack is required before validate_evidence.",
            }
        )
        return cast(
            AnalysisGraphState,
            {
                **state,
                "status": "FAILED",
                "errors": errors,
                "warnings": warnings,
            },
        )

    try:
        hard_errors: List[ValidationIssue] = []
        soft_warnings: List[ValidationIssue] = []

        # 1) 基础结构检查
        hard_errors.extend(_check_required_sections(evidence_pack))

        # 2) 资产路径检查
        artifact_check = _check_artifact_paths(evidence_pack)

        for cid in artifact_check.missing_chart_files:
            hard_errors.append(
                ValidationIssue(
                    type="missing_chart_file",
                    message=f"Chart file missing for chart_id={cid}",
                )
            )

        for tid in artifact_check.missing_table_files:
            hard_errors.append(
                ValidationIssue(
                    type="missing_table_file",
                    message=f"Table file missing for table_id={tid}",
                )
            )

        for tid in artifact_check.empty_tables:
            soft_warnings.append(
                ValidationIssue(
                    type="empty_table",
                    message=f"Table appears empty or unreadable for table_id={tid}",
                )
            )

        # 3) claim support link 检查
        broken_links = _check_claim_support_links(evidence_pack)
        artifact_check.broken_claim_links = broken_links

        for msg in broken_links:
            hard_errors.append(
                ValidationIssue(
                    type="broken_claim_link",
                    message=msg,
                )
            )

        # 4) findings / charts / tables 空检查
        if not evidence_pack.get("findings"):
            hard_errors.append(
                ValidationIssue(
                    type="missing_findings",
                    message="evidence_pack has no findings",
                )
            )

        if not evidence_pack.get("tables"):
            hard_errors.append(
                ValidationIssue(
                    type="missing_tables",
                    message="evidence_pack has no tables",
                )
            )

        if not evidence_pack.get("charts"):
            soft_warnings.append(
                ValidationIssue(
                    type="missing_charts",
                    message="evidence_pack has no charts",
                )
            )

        # 5) coverage 检查
        coverage_check = _check_topic_coverage(analysis_brief, evidence_pack)
        for topic in coverage_check.missing_topics:
            soft_warnings.append(
                ValidationIssue(
                    type="missing_topic_coverage",
                    message=f"must_cover_topic not covered: {topic}",
                )
            )

        # 6) redundancy 检测
        redundancy_signals = _detect_redundancy_signals(evidence_pack)

        validation_result = ValidationResult(
            valid=len(hard_errors) == 0,
            hard_errors=hard_errors,
            warnings=soft_warnings,
            coverage_check=coverage_check,
            artifact_check=artifact_check,
            redundancy_signals=redundancy_signals,
        )

        return cast(
            AnalysisGraphState,
            {
                **state,
                "validation_result": validation_result.model_dump(),
                "status": "EVIDENCE_VALIDATED",
                "warnings": warnings,
                "errors": errors,
            },
        )

    except Exception as e:
        errors.append(
            {
                "type": "validate_evidence_failed",
                "message": str(e),
            }
        )
        return cast(
            AnalysisGraphState,
            {
                **state,
                "status": "FAILED",
                "warnings": warnings,
                "errors": errors,
            },
        )
