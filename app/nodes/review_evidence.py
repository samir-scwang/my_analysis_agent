from __future__ import annotations

from typing import Any, Dict, List, cast

from app.schemas.review import (
    CoverageAssessment,
    EvidenceAssessment,
    OverclaimAssessment,
    RedundancyAssessment,
    RevisionTask,
    RevisionTaskRef,
    ReviewResult,
)
from app.schemas.state import AnalysisGraphState


def _assess_coverage(validation_result: Dict[str, Any]) -> CoverageAssessment:
    coverage = validation_result.get("coverage_check", {}) or {}
    covered_topics = coverage.get("covered_topics", []) or []
    missing_topics = coverage.get("missing_topics", []) or []
    total = coverage.get("must_cover_topics_total", 0)
    covered = coverage.get("must_cover_topics_covered", 0)

    if total == 0:
        overall = "poor"
    else:
        ratio = covered / total
        if ratio >= 1.0:
            overall = "excellent"
        elif ratio >= 0.75:
            overall = "good"
        elif ratio >= 0.5:
            overall = "partial"
        else:
            overall = "poor"

    return CoverageAssessment(
        overall=overall,
        covered_topics=covered_topics,
        missing_topics=missing_topics,
    )


def _assess_evidence_sufficiency(evidence_pack: Dict[str, Any]) -> EvidenceAssessment:
    claims = evidence_pack.get("claim_evidence_map", []) or []
    weak_claim_ids: List[str] = []
    unsupported_claim_ids: List[str] = []

    if not claims:
        return EvidenceAssessment(
            supported_claims_ratio=0.0,
            weak_claim_ids=[],
            unsupported_claim_ids=[],
        )

    supported_count = 0
    for claim in claims:
        claim_id = claim.get("claim_id", "unknown_claim")
        support = claim.get("support", {}) or {}

        table_ids = support.get("table_ids", []) or []
        chart_ids = support.get("chart_ids", []) or []
        finding_ids = support.get("finding_ids", []) or []

        support_count = len(table_ids) + len(chart_ids) + len(finding_ids)

        if support_count == 0:
            unsupported_claim_ids.append(claim_id)
        else:
            supported_count += 1
            if support_count == 1:
                weak_claim_ids.append(claim_id)

    ratio = supported_count / max(len(claims), 1)

    return EvidenceAssessment(
        supported_claims_ratio=round(ratio, 4),
        weak_claim_ids=weak_claim_ids,
        unsupported_claim_ids=unsupported_claim_ids,
    )


def _assess_redundancy(validation_result: Dict[str, Any]) -> RedundancyAssessment:
    signals = validation_result.get("redundancy_signals", []) or []
    chart_groups: List[List[str]] = []

    for item in signals:
        if item.get("type") == "possible_duplicate_chart":
            chart_ids = item.get("chart_ids", []) or []
            if chart_ids:
                chart_groups.append(chart_ids)

    return RedundancyAssessment(
        redundant_chart_groups=chart_groups,
        redundant_table_groups=[],
    )


def _assess_overclaim(
    evidence_pack: Dict[str, Any],
    dataset_context: Dict[str, Any],
) -> OverclaimAssessment:
    flagged_claim_ids: List[str] = []
    causal_language_violations: List[str] = []

    row_count = evidence_pack.get("dataset_summary", {}).get("row_count", 0)
    claims = evidence_pack.get("claim_evidence_map", []) or []

    for claim in claims:
        claim_id = claim.get("claim_id", "unknown_claim")
        claim_text = str(claim.get("claim_text", ""))
        confidence = claim.get("confidence", "medium")

        # 简单因果语言检查
        causal_markers = ["导致", "驱动", "造成", "引发", "because", "caused by", "driven by"]
        if any(marker in claim_text for marker in causal_markers):
            flagged_claim_ids.append(claim_id)
            causal_language_violations.append(claim_id)

        # 小样本时高强度 comparative/high confidence 视为风险
        if row_count < 20 and confidence == "high" and claim.get("claim_type") == "comparative":
            flagged_claim_ids.append(claim_id)

    # 去重
    flagged_claim_ids = list(dict.fromkeys(flagged_claim_ids))
    causal_language_violations = list(dict.fromkeys(causal_language_violations))

    return OverclaimAssessment(
        flagged_claim_ids=flagged_claim_ids,
        causal_language_violations=causal_language_violations,
    )


def _build_revision_tasks(
    coverage_assessment: CoverageAssessment,
    evidence_assessment: EvidenceAssessment,
    redundancy_assessment: RedundancyAssessment,
    overclaim_assessment: OverclaimAssessment,
    validation_result: Dict[str, Any],
) -> tuple[List[RevisionTask], List[RevisionTask], List[RevisionTask]]:
    must_fix: List[RevisionTask] = []
    should_fix: List[RevisionTask] = []
    nice_to_have: List[RevisionTask] = []

    task_idx = 1

    # validator hard errors -> must_fix
    for err in validation_result.get("hard_errors", []) or []:
        must_fix.append(
            RevisionTask(
                task_id=f"fix_{task_idx:03d}",
                task_type="improve_coverage",
                priority="must_fix",
                goal=f"修复验证层硬错误：{err.get('message')}",
                reason="validator_hard_error",
                acceptance_criteria=[
                    "相关硬错误消失",
                    "validation_result.valid = True",
                ],
            )
        )
        task_idx += 1

    # missing topic coverage -> must_fix
    for topic in coverage_assessment.missing_topics:
        must_fix.append(
            RevisionTask(
                task_id=f"fix_{task_idx:03d}",
                task_type="improve_coverage",
                priority="must_fix",
                goal=f"补齐主题覆盖：{topic}",
                reason="must_cover_topic_missing",
                related_topic=topic,
                acceptance_criteria=[
                    f"{topic} 对应的 findings 或 charts 必须出现",
                    "validation coverage 中不再缺失该 topic",
                ],
            )
        )
        task_idx += 1

    # unsupported claims -> must_fix
    for claim_id in evidence_assessment.unsupported_claim_ids:
        must_fix.append(
            RevisionTask(
                task_id=f"fix_{task_idx:03d}",
                task_type="rewrite_claim",
                priority="must_fix",
                goal=f"为 claim 补证据或移除 unsupported claim：{claim_id}",
                reason="unsupported_claim",
                related_claim_ids=[claim_id],
                acceptance_criteria=[
                    "该 claim 至少绑定一个有效 table/chart/finding",
                    "或从最终 claim_evidence_map 中移除",
                ],
            )
        )
        task_idx += 1

    # overclaim -> must_fix
    for claim_id in overclaim_assessment.flagged_claim_ids:
        must_fix.append(
            RevisionTask(
                task_id=f"fix_{task_idx:03d}",
                task_type="downgrade_claim",
                priority="must_fix",
                goal=f"降低 claim 语气或置信度：{claim_id}",
                reason="possible_overclaim",
                related_claim_ids=[claim_id],
                acceptance_criteria=[
                    "claim 文本改为描述性或弱化表达",
                    "confidence 与样本规模一致",
                ],
            )
        )
        task_idx += 1

    # weak claims -> should_fix
    for claim_id in evidence_assessment.weak_claim_ids:
        should_fix.append(
            RevisionTask(
                task_id=f"fix_{task_idx:03d}",
                task_type="add_chart",
                priority="should_fix",
                goal=f"为弱证据 claim 增补图表或表格：{claim_id}",
                reason="weak_claim_support",
                related_claim_ids=[claim_id],
                acceptance_criteria=[
                    "该 claim 的 support_count > 1",
                ],
            )
        )
        task_idx += 1

    # redundancy -> should_fix
    for group in redundancy_assessment.redundant_chart_groups:
        should_fix.append(
            RevisionTask(
                task_id=f"fix_{task_idx:03d}",
                task_type="deduplicate_chart",
                priority="should_fix",
                goal=f"检查并去重可能重复的图表：{group}",
                reason="possible_duplicate_chart",
                related_chart_ids=group,
                acceptance_criteria=[
                    "重复图表减少为 1 张或差异被明确扩大",
                ],
            )
        )
        task_idx += 1

    return must_fix, should_fix, nice_to_have


def _build_revision_refs(
    must_fix: List[RevisionTask],
    should_fix: List[RevisionTask],
    nice_to_have: List[RevisionTask],
) -> List[RevisionTaskRef]:
    refs: List[RevisionTaskRef] = []
    for task in must_fix:
        refs.append(RevisionTaskRef(task_id=task.task_id, priority="must_fix"))
    for task in should_fix:
        refs.append(RevisionTaskRef(task_id=task.task_id, priority="should_fix"))
    for task in nice_to_have:
        refs.append(RevisionTaskRef(task_id=task.task_id, priority="nice_to_have"))
    return refs


def _compute_score(
    coverage_assessment: CoverageAssessment,
    evidence_assessment: EvidenceAssessment,
    overclaim_assessment: OverclaimAssessment,
    validation_result: Dict[str, Any],
) -> float:
    score = 1.0

    total = max(
        validation_result.get("coverage_check", {}).get("must_cover_topics_total", 0),
        1,
    )
    covered = validation_result.get("coverage_check", {}).get("must_cover_topics_covered", 0)
    coverage_ratio = covered / total
    score -= (1.0 - coverage_ratio) * 0.4

    score -= (1.0 - evidence_assessment.supported_claims_ratio) * 0.3
    score -= min(len(overclaim_assessment.flagged_claim_ids) * 0.05, 0.2)
    score -= min(len(validation_result.get("hard_errors", []) or []) * 0.2, 0.4)

    return round(max(score, 0.0), 4)


def _compute_severity(
    must_fix: List[RevisionTask],
    validation_result: Dict[str, Any],
) -> str:
    hard_error_count = len(validation_result.get("hard_errors", []) or [])
    must_fix_count = len(must_fix)

    if hard_error_count > 0:
        return "critical"
    if must_fix_count >= 3:
        return "high"
    if must_fix_count >= 1:
        return "medium"
    return "low"


def review_evidence_node(state: AnalysisGraphState) -> AnalysisGraphState:
    warnings = list(state.get("warnings", []))
    errors = list(state.get("errors", []))

    dataset_context = state.get("dataset_context")
    analysis_brief = state.get("analysis_brief")
    evidence_pack = state.get("evidence_pack")
    validation_result = state.get("validation_result")

    if not dataset_context or dataset_context.get("status") == "stub":
        errors.append(
            {
                "type": "missing_dataset_context",
                "message": "dataset_context is required before review_evidence.",
            }
        )
        return cast(
            AnalysisGraphState,
            {**state, "status": "FAILED", "errors": errors, "warnings": warnings},
        )

    if not analysis_brief or analysis_brief.get("status") == "stub":
        errors.append(
            {
                "type": "missing_analysis_brief",
                "message": "analysis_brief is required before review_evidence.",
            }
        )
        return cast(
            AnalysisGraphState,
            {**state, "status": "FAILED", "errors": errors, "warnings": warnings},
        )

    if not evidence_pack or evidence_pack.get("status") == "stub":
        errors.append(
            {
                "type": "missing_evidence_pack",
                "message": "evidence_pack is required before review_evidence.",
            }
        )
        return cast(
            AnalysisGraphState,
            {**state, "status": "FAILED", "errors": errors, "warnings": warnings},
        )

    if not validation_result:
        errors.append(
            {
                "type": "missing_validation_result",
                "message": "validation_result is required before review_evidence.",
            }
        )
        return cast(
            AnalysisGraphState,
            {**state, "status": "FAILED", "errors": errors, "warnings": warnings},
        )

    try:
        coverage_assessment = _assess_coverage(validation_result)
        evidence_assessment = _assess_evidence_sufficiency(evidence_pack)
        redundancy_assessment = _assess_redundancy(validation_result)
        overclaim_assessment = _assess_overclaim(evidence_pack, dataset_context)

        must_fix, should_fix, nice_to_have = _build_revision_tasks(
            coverage_assessment=coverage_assessment,
            evidence_assessment=evidence_assessment,
            redundancy_assessment=redundancy_assessment,
            overclaim_assessment=overclaim_assessment,
            validation_result=validation_result,
        )

        revision_refs = _build_revision_refs(must_fix, should_fix, nice_to_have)
        score = _compute_score(
            coverage_assessment=coverage_assessment,
            evidence_assessment=evidence_assessment,
            overclaim_assessment=overclaim_assessment,
            validation_result=validation_result,
        )
        severity = _compute_severity(must_fix, validation_result)
        approved = len(must_fix) == 0 and validation_result.get("valid", False)

        if approved:
            review_summary = "证据包覆盖完整、引用有效、无必须修复项，可以进入写作阶段。"
        else:
            review_summary = "证据包存在必须修复项，建议进入 revision loop 进行定向修正。"

        review_result = ReviewResult(
            review_id=f"rev_{state.get('revision_round', 0) + 1:03d}",
            approved=approved,
            score=score,
            severity=severity,
            coverage_assessment=coverage_assessment,
            evidence_assessment=evidence_assessment,
            redundancy_assessment=redundancy_assessment,
            overclaim_assessment=overclaim_assessment,
            must_fix=must_fix,
            should_fix=should_fix,
            nice_to_have=nice_to_have,
            revision_tasks=revision_refs,
            review_summary=review_summary,
        )

        history = list(state.get("review_history", []))
        history.append(review_result.model_dump())

        return cast(
            AnalysisGraphState,
            {
                **state,
                "review_result": review_result.model_dump(),
                "review_history": history,
                "status": "UNDER_REVIEW",
                "warnings": warnings,
                "errors": errors,
            },
        )

    except Exception as e:
        errors.append(
            {
                "type": "review_evidence_failed",
                "message": str(e),
            }
        )
        return cast(
            AnalysisGraphState,
            {**state, "status": "FAILED", "errors": errors, "warnings": warnings},
        )