from __future__ import annotations

from typing import cast

from app.agents.deep_analysis.evidence_builder import build_evidence_pack_from_agent_output
from app.agents.deep_analysis.models import (
    DeepAnalysisAgentInput,
    build_default_output_contract,
)
from app.schemas.state import AnalysisGraphState
from app.services.analysis_workspace import ensure_workspace_from_state
from app.services.deepagent_service import DeepAgentService


def _validate_required_inputs(
    state: AnalysisGraphState,
    *,
    warnings: list[dict],
    errors: list[dict],
) -> tuple[bool, AnalysisGraphState | None]:
    dataset_path = state.get("dataset_path")
    dataset_context = state.get("dataset_context")
    analysis_brief = state.get("analysis_brief")

    if not dataset_path:
        errors.append(
            {
                "type": "missing_dataset_path",
                "message": "dataset_path is required for deep_analysis.",
            }
        )
        return (
            False,
            cast(
                AnalysisGraphState,
                {
                    **state,
                    "status": "FAILED",
                    "warnings": warnings,
                    "errors": errors,
                },
            ),
        )

    if not dataset_context or dataset_context.get("status") == "stub":
        errors.append(
            {
                "type": "missing_dataset_context",
                "message": "dataset_context is required for deep_analysis.",
            }
        )
        return (
            False,
            cast(
                AnalysisGraphState,
                {
                    **state,
                    "status": "FAILED",
                    "warnings": warnings,
                    "errors": errors,
                },
            ),
        )

    if not analysis_brief or analysis_brief.get("status") == "stub":
        errors.append(
            {
                "type": "missing_analysis_brief",
                "message": "analysis_brief is required for deep_analysis.",
            }
        )
        return (
            False,
            cast(
                AnalysisGraphState,
                {
                    **state,
                    "status": "FAILED",
                    "warnings": warnings,
                    "errors": errors,
                },
            ),
        )

    return True, None


def _build_agent_input(
    *,
    state: AnalysisGraphState,
    workspace: dict,
) -> DeepAnalysisAgentInput:
    analysis_brief = state.get("analysis_brief", {}) or {}
    outputs_dir = workspace["outputs_dir"]
    structured_output_path = f"{outputs_dir}/structured_result.json"

    output_contract = build_default_output_contract(
        structured_output_path=structured_output_path,
        must_cover_topics=analysis_brief.get("must_cover_topics", []) or [],
    )

    return DeepAnalysisAgentInput(
        request_id=state.get("request_id", "unknown_request"),
        dataset_id=state.get("dataset_id", "unknown_dataset"),
        dataset_path=state["dataset_path"],
        dataset_context=state["dataset_context"],
        analysis_brief=analysis_brief,
        normalized_task=state.get("normalized_task", {}) or {},
        execution_mode=state.get("execution_mode", "normal"),
        revision_round=state.get("revision_round", 0),
        revision_context=state.get("revision_context", {}) or {},
        workspace_root=workspace["root_dir"],
        dataset_local_path=workspace["dataset_local_path"],
        output_contract=output_contract,
    )


def deep_analysis_node(state: AnalysisGraphState) -> AnalysisGraphState:
    warnings = list(state.get("warnings", []))
    errors = list(state.get("errors", []))

    ok, failed_state = _validate_required_inputs(
        state,
        warnings=warnings,
        errors=errors,
    )
    if not ok:
        return cast(AnalysisGraphState, failed_state)

    revision_round = state.get("revision_round", 0)

    try:
        workspace = ensure_workspace_from_state(state=state)
    except Exception as e:
        errors.append(
            {
                "type": "analysis_workspace_failed",
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

    try:
        agent_input = _build_agent_input(
            state=state,
            workspace=workspace,
        )

        service = DeepAgentService()
        agent_output = service.run_analysis(agent_input=agent_input)

        evidence_pack = build_evidence_pack_from_agent_output(
            state={
                **state,
                "analysis_workspace": workspace,
            },
            agent_output=agent_output,
            revision_round=revision_round,
        )

        history = list(state.get("evidence_pack_history", []))
        history.append(evidence_pack.model_dump())

        return cast(
            AnalysisGraphState,
            {
                **state,
                "analysis_workspace": workspace,
                "agent_plan": agent_output.plan,
                "agent_trace": agent_output.trace,
                "agent_run_metadata": agent_output.run_metadata,
                "evidence_pack": evidence_pack.model_dump(),
                "evidence_pack_history": history,
                "status": "ANALYSIS_DONE",
                "warnings": warnings,
                "errors": errors,
            },
        )

    except Exception as e:
        errors.append(
            {
                "type": "deep_analysis_failed",
                "message": str(e),
            }
        )
        return cast(
            AnalysisGraphState,
            {
                **state,
                "analysis_workspace": workspace,
                "status": "FAILED",
                "warnings": warnings,
                "errors": errors,
            },
        )