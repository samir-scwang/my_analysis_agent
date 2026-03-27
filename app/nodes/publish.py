from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, cast

from app.schemas.state import AnalysisGraphState


def _collect_linked_artifacts(evidence_pack: Dict[str, Any]) -> Dict[str, List[str]]:
    charts = evidence_pack.get("artifact_manifest", {}).get("chart_paths", []) or []
    tables = evidence_pack.get("artifact_manifest", {}).get("table_paths", []) or []
    appendix = evidence_pack.get("artifact_manifest", {}).get("appendix_paths", []) or []

    return {
        "chart_paths": charts,
        "table_paths": tables,
        "appendix_paths": appendix,
    }


def publish_node(state: AnalysisGraphState) -> AnalysisGraphState:
    warnings = list(state.get("warnings", []))
    errors = list(state.get("errors", []))

    report_draft = state.get("report_draft")
    final_qa_result = state.get("final_qa_result", {}) or {}
    evidence_pack = state.get("evidence_pack", {}) or {}
    request_id = state.get("request_id", "unknown_request")

    if not report_draft:
        errors.append(
            {
                "type": "missing_report_draft",
                "message": "report_draft is required before publish.",
            }
        )
        return cast(
            AnalysisGraphState,
            {**state, "status": "FAILED", "errors": errors, "warnings": warnings},
        )

    if not final_qa_result:
        errors.append(
            {
                "type": "missing_final_qa_result",
                "message": "final_qa_result is required before publish.",
            }
        )
        return cast(
            AnalysisGraphState,
            {**state, "status": "FAILED", "errors": errors, "warnings": warnings},
        )

    try:
        publish_ready = final_qa_result.get("publish_ready", False)
        output_dir = Path("app/artifacts/reports")
        output_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{request_id}_report.md"
        final_report_path = output_dir / filename
        final_report_path.write_text(str(report_draft.get("content", "")), encoding="utf-8")

        linked_artifacts = _collect_linked_artifacts(evidence_pack)

        publish_result = {
            "status": "published" if publish_ready else "published_with_warnings",
            "final_report_path": str(final_report_path.resolve()),
            "linked_artifacts": linked_artifacts,
            "publish_ready": publish_ready,
        }

        return cast(
            AnalysisGraphState,
            {
                **state,
                "publish_result": publish_result,
                "status": "PUBLISHED",
                "warnings": warnings,
                "errors": errors,
            },
        )

    except Exception as e:
        errors.append(
            {
                "type": "publish_failed",
                "message": str(e),
            }
        )
        return cast(
            AnalysisGraphState,
            {**state, "status": "FAILED", "errors": errors, "warnings": warnings},
        )