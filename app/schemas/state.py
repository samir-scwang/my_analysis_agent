from __future__ import annotations

from typing import Any, Dict, List, Literal, TypedDict


GraphStatus = Literal[
    "INIT",
    "TASK_NORMALIZED",
    "DATASET_PROFILED",
    "BRIEF_READY",
    "ANALYSIS_DONE",
    "EVIDENCE_VALIDATED",
    "UNDER_REVIEW",
    "REVISION_REQUIRED",
    "REPORT_WRITTEN",
    "FINAL_QA_DONE",
    "PUBLISHED",
    "DEGRADED_OUTPUT",
    "FAILED",
]


class AnalysisGraphState(TypedDict, total=False):
    request_id: str
    session_id: str
    user_id: str
    dataset_id: str
    dataset_path: str
    user_prompt: str

    input_config: Dict[str, Any]
    memory_context: Dict[str, Any]

    normalized_task: Dict[str, Any]
    dataset_context: Dict[str, Any]
    analysis_brief: Dict[str, Any]

    evidence_pack: Dict[str, Any]
    evidence_pack_history: List[Dict[str, Any]]

    validation_result: Dict[str, Any]
    review_result: Dict[str, Any]
    review_history: List[Dict[str, Any]]

    revision_round: int
    max_review_rounds: int
    revision_tasks: List[Dict[str, Any]]
    revision_context: Dict[str, Any]

    execution_mode: str  # normal / revision / degraded

    report_draft: Dict[str, Any]
    final_qa_result: Dict[str, Any]
    publish_result: Dict[str, Any]
    status: GraphStatus
    errors: List[Dict[str, Any]]
    warnings: List[Dict[str, Any]]
    degraded_output: bool

    agent_plan: Dict[str, Any]
    agent_trace: List[Dict[str, Any]]
    agent_run_metadata: Dict[str, Any]
    analysis_workspace: Dict[str, Any]