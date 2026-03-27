from __future__ import annotations

from typing import List, Literal, Optional
from pydantic import BaseModel, Field


class ExecutedStep(BaseModel):
    step_id: str
    step_type: str
    description: str
    code_ref: Optional[str] = None
    output_refs: List[str] = Field(default_factory=list)
    status: Literal["success", "failed", "skipped"] = "success"


class Finding(BaseModel):
    finding_id: str
    title: str
    statement: str
    category: Literal[
        "trend",
        "comparison",
        "distribution",
        "composition",
        "anomaly",
        "association",
        "summary",
    ]
    importance: Literal["high", "medium", "low"] = "medium"
    confidence: Literal["high", "medium", "low"] = "medium"
    supporting_claim_ids: List[str] = Field(default_factory=list)
    topic_tags: List[str] = Field(default_factory=list)


class EvidenceTable(BaseModel):
    table_id: str
    title: str
    table_type: str
    path: str
    format: Literal["csv", "xlsx", "json", "markdown"] = "csv"
    columns: List[str] = Field(default_factory=list)
    row_count: int = 0
    description: Optional[str] = None


class EvidenceChart(BaseModel):
    chart_id: str
    title: str
    chart_type: str
    path: str
    spec_ref: Optional[str] = None
    x: str | List[str]
    y: str | List[str]
    group_by: Optional[List[str]] = None
    purpose: str = ""
    topic_tags: List[str] = Field(default_factory=list)
    information_density_score: Optional[float] = None


class ClaimSupport(BaseModel):
    table_ids: List[str] = Field(default_factory=list)
    chart_ids: List[str] = Field(default_factory=list)
    finding_ids: List[str] = Field(default_factory=list)
    stat_refs: List[str] = Field(default_factory=list)


class ClaimEvidenceMapItem(BaseModel):
    claim_id: str
    claim_text: str
    claim_type: Literal["descriptive", "comparative", "associational", "diagnostic"] = "descriptive"
    confidence: Literal["high", "medium", "low"] = "medium"
    support: ClaimSupport = Field(default_factory=ClaimSupport)
    caveat_ids: List[str] = Field(default_factory=list)


class Caveat(BaseModel):
    caveat_id: str
    message: str
    severity: Literal["high", "medium", "low"] = "medium"
    related_claim_ids: List[str] = Field(default_factory=list)


class ArtifactManifest(BaseModel):
    chart_paths: List[str] = Field(default_factory=list)
    table_paths: List[str] = Field(default_factory=list)
    appendix_paths: List[str] = Field(default_factory=list)


class Provenance(BaseModel):
    code_bundle_path: Optional[str] = None
    execution_environment: Optional[str] = "python+pandas+matplotlib"
    generated_at: Optional[str] = None
    source_dataset_version: Optional[str] = None


class EvidencePack(BaseModel):
    evidence_pack_id: str
    parent_evidence_pack_id: Optional[str] = None
    version: int = 1
    analysis_round: int = 0
    revision_applied: List[str] = Field(default_factory=list)

    dataset_summary: dict = Field(default_factory=dict)
    analysis_plan: dict = Field(default_factory=dict)
    executed_steps: List[ExecutedStep] = Field(default_factory=list)

    findings: List[Finding] = Field(default_factory=list)
    ranked_findings: List[str] = Field(default_factory=list)

    tables: List[EvidenceTable] = Field(default_factory=list)
    charts: List[EvidenceChart] = Field(default_factory=list)

    claim_evidence_map: List[ClaimEvidenceMapItem] = Field(default_factory=list)
    caveats: List[Caveat] = Field(default_factory=list)

    rejected_charts: List[dict] = Field(default_factory=list)
    rejected_hypotheses: List[dict] = Field(default_factory=list)

    artifact_manifest: ArtifactManifest = Field(default_factory=ArtifactManifest)
    provenance: Provenance = Field(default_factory=Provenance)

    superseded_items: dict = Field(default_factory=dict)
