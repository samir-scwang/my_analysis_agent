from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


ExecutionMode = Literal["normal", "revision", "degraded"]
ArtifactType = Literal["table", "chart", "script", "log", "json"]
FindingCategory = Literal[
    "trend",
    "comparison",
    "distribution",
    "composition",
    "anomaly",
    "association",
    "summary",
]
PriorityLevel = Literal["high", "medium", "low"]
ClaimType = Literal["descriptive", "comparative", "associational", "diagnostic"]


class DeepAnalysisAgentInput(BaseModel):
    """
    Node -> deepagent 的输入契约。
    这里不要直接塞整个 state，只放 deepagent 真正需要消费的字段。
    """

    request_id: str = "unknown_request"
    dataset_id: str = "unknown_dataset"
    dataset_path: str
    dataset_context: Dict[str, Any]
    analysis_brief: Dict[str, Any]
    normalized_task: Dict[str, Any] = Field(default_factory=dict)

    execution_mode: ExecutionMode = "normal"
    revision_round: int = 0
    revision_context: Dict[str, Any] = Field(default_factory=dict)

    workspace_root: str
    dataset_local_path: str

    output_contract: Dict[str, Any] = Field(default_factory=dict)


class PlannedAction(BaseModel):
    """
    agent 的高层计划动作。
    后面可映射到 state.agent_plan 与 evidence_pack.analysis_plan。
    """

    action: str
    metrics: List[str] = Field(default_factory=list)
    group_col: Optional[str] = None
    time_col: Optional[str] = None
    priority: PriorityLevel = "medium"
    reason: Optional[str] = None


class ArtifactRef(BaseModel):
    """
    agent 生成的文件引用。
    先用统一 artifact 模型承接，后面再由 evidence_builder 映射成
    EvidenceTable / EvidenceChart 等业务模型。
    """

    artifact_id: str
    artifact_type: ArtifactType
    title: str
    path: str
    format: Optional[str] = None
    topic_tags: List[str] = Field(default_factory=list)
    description: Optional[str] = None


class FindingDraft(BaseModel):
    """
    agent 产出的 finding 草稿。
    supporting_artifact_ids 用于 builder 进一步解析成 claim-support 关系。
    """

    finding_id: str
    title: str
    statement: str
    category: FindingCategory
    importance: PriorityLevel = "medium"
    confidence: PriorityLevel = "medium"
    topic_tags: List[str] = Field(default_factory=list)
    supporting_artifact_ids: List[str] = Field(default_factory=list)


class ClaimDraft(BaseModel):
    """
    agent 产出的 claim 草稿。
    这里保持和 EvidencePack 中 claim_evidence_map 尽量接近，但先不直接耦合。
    """

    claim_id: str
    claim_text: str
    claim_type: ClaimType = "descriptive"
    confidence: PriorityLevel = "medium"

    table_ids: List[str] = Field(default_factory=list)
    chart_ids: List[str] = Field(default_factory=list)
    finding_ids: List[str] = Field(default_factory=list)
    stat_refs: List[str] = Field(default_factory=list)
    caveat_ids: List[str] = Field(default_factory=list)


class CaveatDraft(BaseModel):
    """
    agent 产出的 caveat 草稿。
    """

    caveat_id: str
    message: str
    severity: PriorityLevel = "medium"
    related_claim_ids: List[str] = Field(default_factory=list)


class ExecutedStepTrace(BaseModel):
    """
    轻量执行轨迹，避免把 agent 的完整内部思维链写入 state。
    """

    step_id: str
    step_type: str
    description: str
    status: Literal["success", "failed", "skipped"] = "success"
    output_refs: List[str] = Field(default_factory=list)
    code_ref: Optional[str] = None
    extra: Dict[str, Any] = Field(default_factory=dict)


class DeepAnalysisAgentOutput(BaseModel):
    """
    deepagent -> node 的输出契约。
    node/evidence_builder 只依赖这个结构，而不依赖 deepagent 内部细节。
    """

    plan: Dict[str, Any] = Field(default_factory=dict)
    planned_actions: List[PlannedAction] = Field(default_factory=list)

    executed_steps: List[ExecutedStepTrace] = Field(default_factory=list)
    artifacts: List[ArtifactRef] = Field(default_factory=list)

    findings: List[FindingDraft] = Field(default_factory=list)
    claims: List[ClaimDraft] = Field(default_factory=list)
    caveats: List[CaveatDraft] = Field(default_factory=list)

    rejected_charts: List[Dict[str, Any]] = Field(default_factory=list)
    rejected_hypotheses: List[Dict[str, Any]] = Field(default_factory=list)

    trace: List[Dict[str, Any]] = Field(default_factory=list)
    run_metadata: Dict[str, Any] = Field(default_factory=dict)


def build_default_output_contract(
    *,
    structured_output_path: str,
    must_cover_topics: List[str],
) -> Dict[str, Any]:
    """
    供 node 层快速构造 output_contract。
    """
    return {
        "must_cover_topics": must_cover_topics,
        "must_produce_artifacts": True,
        "required_artifact_types": ["table"],
        "preferred_artifact_types": ["chart"],
        "must_write_structured_output_json": True,
        "structured_output_path": structured_output_path,
        "must_keep_claims_evidence_linked": True,
    }