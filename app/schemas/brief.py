from __future__ import annotations

from typing import List, Literal
from pydantic import BaseModel, Field


class ReportStyle(BaseModel):
    language: str = "zh-CN"
    tone: Literal["professional", "concise", "executive"] = "professional"
    detail_level: Literal["low", "medium", "high"] = "high"


class ChartPolicy(BaseModel):
    target_chart_range: List[int] = Field(default_factory=lambda: [4, 8])
    max_total_charts: int = 8
    max_similar_chart_per_metric: int = 2
    preferred_chart_types: List[str] = Field(default_factory=list)
    avoid_chart_types: List[str] = Field(default_factory=list)


class TablePolicy(BaseModel):
    max_total_tables: int = 4
    must_have_tables: List[str] = Field(default_factory=list)


class ConfidencePolicy(BaseModel):
    default_claim_level: Literal[
        "descriptive_or_associational",
        "descriptive_only"
    ] = "descriptive_or_associational"
    forbid_causal_language_without_evidence: bool = True


class RevisionPolicy(BaseModel):
    max_review_rounds: int = 2
    must_fix_first: bool = True


class AnalysisBrief(BaseModel):
    brief_id: str
    version: int = 1
    task_type: str
    business_goal: str
    target_audience: str

    report_style: ReportStyle = Field(default_factory=ReportStyle)

    must_cover_topics: List[str] = Field(default_factory=list)
    optional_topics: List[str] = Field(default_factory=list)
    must_not_do: List[str] = Field(default_factory=list)

    recommended_metrics: List[str] = Field(default_factory=list)
    recommended_dimensions: List[str] = Field(default_factory=list)

    chart_policy: ChartPolicy = Field(default_factory=ChartPolicy)
    table_policy: TablePolicy = Field(default_factory=TablePolicy)

    completion_criteria: List[str] = Field(default_factory=list)
    confidence_policy: ConfidencePolicy = Field(default_factory=ConfidencePolicy)
    revision_policy: RevisionPolicy = Field(default_factory=RevisionPolicy)

    brief_notes: str | None = None