from __future__ import annotations

from typing import List, Literal, Optional
from pydantic import BaseModel, Field


class CoverageAssessment(BaseModel):
    overall: Literal["excellent", "good", "partial", "poor"] = "good"
    covered_topics: List[str] = Field(default_factory=list)
    missing_topics: List[str] = Field(default_factory=list)


class EvidenceAssessment(BaseModel):
    supported_claims_ratio: float = 1.0
    weak_claim_ids: List[str] = Field(default_factory=list)
    unsupported_claim_ids: List[str] = Field(default_factory=list)


class RedundancyAssessment(BaseModel):
    redundant_chart_groups: List[List[str]] = Field(default_factory=list)
    redundant_table_groups: List[List[str]] = Field(default_factory=list)


class OverclaimAssessment(BaseModel):
    flagged_claim_ids: List[str] = Field(default_factory=list)
    causal_language_violations: List[str] = Field(default_factory=list)


class RevisionTask(BaseModel):
    task_id: str
    task_type: Literal[
        "add_analysis",
        "add_chart",
        "add_table",
        "revise_finding",
        "remove_chart",
        "remove_table",
        "downgrade_claim",
        "rewrite_claim",
        "improve_coverage",
        "deduplicate_chart",
        "deduplicate_table",
    ]
    priority: Literal["must_fix", "should_fix", "nice_to_have"]
    goal: str
    reason: Optional[str] = None
    related_topic: Optional[str] = None
    related_claim_ids: List[str] = Field(default_factory=list)
    related_chart_ids: List[str] = Field(default_factory=list)
    related_table_ids: List[str] = Field(default_factory=list)
    acceptance_criteria: List[str] = Field(default_factory=list)


class RevisionTaskRef(BaseModel):
    task_id: str
    priority: Literal["must_fix", "should_fix", "nice_to_have"]


class ReviewResult(BaseModel):
    review_id: str
    approved: bool
    score: float
    severity: Literal["low", "medium", "high", "critical"]

    coverage_assessment: CoverageAssessment = Field(default_factory=CoverageAssessment)
    evidence_assessment: EvidenceAssessment = Field(default_factory=EvidenceAssessment)
    redundancy_assessment: RedundancyAssessment = Field(default_factory=RedundancyAssessment)
    overclaim_assessment: OverclaimAssessment = Field(default_factory=OverclaimAssessment)

    must_fix: List[RevisionTask] = Field(default_factory=list)
    should_fix: List[RevisionTask] = Field(default_factory=list)
    nice_to_have: List[RevisionTask] = Field(default_factory=list)
    revision_tasks: List[RevisionTaskRef] = Field(default_factory=list)

    review_summary: str = ""