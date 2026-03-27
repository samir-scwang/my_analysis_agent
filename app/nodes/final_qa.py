from __future__ import annotations

from typing import Any, Dict, List, cast

from app.schemas.final_qa import FinalQAResult
from app.schemas.state import AnalysisGraphState


def _check_report_title(
    report_draft: Dict[str, Any],
    hard_errors: List[str],
    checked: List[str],
) -> None:
    title = str(report_draft.get("title", "")).strip()
    checked.append("title_present")
    if not title:
        hard_errors.append("Report title is empty.")


def _check_report_content(
    report_draft: Dict[str, Any],
    hard_errors: List[str],
    checked: List[str],
) -> None:
    content = str(report_draft.get("content", "")).strip()
    checked.append("content_present")

    if not content:
        hard_errors.append("Report content is empty.")
        return

    if len(content) < 80:
        hard_errors.append("Report content is too short to be publishable.")


def _check_required_sections(
    report_draft: Dict[str, Any],
    warnings: List[str],
    checked: List[str],
) -> None:
    content = str(report_draft.get("content", ""))

    required_sections = [
        ("引言", ["引言"]),
        ("执行摘要", ["执行摘要"]),
        ("结论与建议", ["结论与建议"]),
        ("分析限制与说明", ["分析限制与说明", "风险与限制"]),
    ]

    for section_name, aliases in required_sections:
        checked.append(f"section:{section_name}")
        if not any(alias in content for alias in aliases):
            warnings.append(f"Missing recommended section: {section_name}")


def _check_degraded_notice(
    report_draft: Dict[str, Any],
    degraded_output: bool,
    hard_errors: List[str],
    checked: List[str],
) -> None:
    checked.append("degraded_notice_check")
    content = str(report_draft.get("content", ""))

    if degraded_output and "降级输出说明" not in content:
        hard_errors.append("Degraded report must contain degraded output notice.")


def _check_report_metadata(
    report_draft: Dict[str, Any],
    warnings: List[str],
    checked: List[str],
) -> None:
    checked.append("report_metadata_check")

    metadata = report_draft.get("report_metadata", {}) or {}
    if not metadata:
        warnings.append("report_metadata is missing.")
        return

    review_meta = metadata.get("review", {}) or {}
    if not review_meta:
        warnings.append("report_metadata.review is missing.")


def _check_artifact_consistency(
    report_draft: Dict[str, Any],
    validation_result: Dict[str, Any],
    warnings: List[str],
    checked: List[str],
) -> None:
    checked.append("artifact_consistency_check")

    artifact_check = validation_result.get("artifact_check", {}) or {}
    missing_chart_files = set(artifact_check.get("missing_chart_files", []) or [])
    missing_table_files = set(artifact_check.get("missing_table_files", []) or [])

    used_chart_ids = set(report_draft.get("used_chart_ids", []) or [])
    used_table_ids = set(report_draft.get("used_table_ids", []) or [])
    content = str(report_draft.get("content", ""))

    for chart_id in missing_chart_files:
        if chart_id in used_chart_ids and "图表资源缺失，未能成功附带。" not in content:
            warnings.append(
                f"Missing chart {chart_id} is used but not described as missing in report body."
            )

    for table_id in missing_table_files:
        if table_id in used_table_ids and "表格资源缺失，未能成功附带。" not in content:
            warnings.append(
                f"Missing table {table_id} is used but not described as missing in report body."
            )


def _check_upstream_validation_result(
    validation_result: Dict[str, Any],
    hard_errors: List[str],
    warnings: List[str],
    checked: List[str],
) -> None:
    checked.append("upstream_validation_result_check")

    upstream_valid = validation_result.get("valid", True)
    upstream_hard_errors = validation_result.get("hard_errors", []) or []
    upstream_warnings = validation_result.get("warnings", []) or []

    if upstream_warnings:
        for item in upstream_warnings:
            if isinstance(item, dict):
                msg = item.get("message") or str(item)
            else:
                msg = str(item)
            warnings.append(f"Upstream validation warning: {msg}")

    if upstream_valid is False:
        if upstream_hard_errors:
            for item in upstream_hard_errors:
                if isinstance(item, dict):
                    msg = item.get("message") or str(item)
                else:
                    msg = str(item)
                hard_errors.append(f"Upstream validation hard error: {msg}")
        else:
            hard_errors.append("Upstream validation marked result as invalid.")


def final_qa_node(state: AnalysisGraphState) -> AnalysisGraphState:
    warnings = list(state.get("warnings", []))
    errors = list(state.get("errors", []))

    report_draft = state.get("report_draft")
    validation_result = state.get("validation_result", {}) or {}
    degraded_output = state.get("degraded_output", False)

    if not report_draft:
        errors.append(
            {
                "type": "missing_report_draft",
                "message": "report_draft is required before final_qa.",
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
        qa_hard_errors: List[str] = []
        qa_warnings: List[str] = []
        checked_items: List[str] = []

        _check_report_title(report_draft, qa_hard_errors, checked_items)
        _check_report_content(report_draft, qa_hard_errors, checked_items)
        _check_required_sections(report_draft, qa_warnings, checked_items)
        _check_degraded_notice(report_draft, degraded_output, qa_hard_errors, checked_items)
        _check_report_metadata(report_draft, qa_warnings, checked_items)
        _check_artifact_consistency(report_draft, validation_result, qa_warnings, checked_items)
        _check_upstream_validation_result(validation_result, qa_hard_errors, qa_warnings, checked_items)

        publish_ready = len(qa_hard_errors) == 0
        qa_summary = (
            "Final QA passed. Report is ready to publish."
            if publish_ready
            else "Final QA found blocking issues. Report is not ready to publish."
        )

        final_qa_result = FinalQAResult(
            publish_ready=publish_ready,
            qa_summary=qa_summary,
            hard_errors=qa_hard_errors,
            warnings=qa_warnings,
            checked_items=checked_items,
        )

        return cast(
            AnalysisGraphState,
            {
                **state,
                "final_qa_result": final_qa_result.model_dump(),
                "status": "FINAL_QA_DONE",
                "warnings": warnings,
                "errors": errors,
            },
        )

    except Exception as e:
        errors.append(
            {
                "type": "final_qa_failed",
                "message": str(e),
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
