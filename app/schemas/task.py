from __future__ import annotations

from typing import List, Literal, Optional
from pydantic import BaseModel, Field


TaskType = Literal[
    "descriptive_analysis",
    "diagnostic_analysis",
    "exploratory_analysis",
    "comparative_analysis",
    "reporting",
    "unknown",
]

AudienceType = Literal[
    "business_stakeholders",
    "executives",
    "analysts",
    "technical_team",
    "general",
    "unknown",
]


class AmbiguityItem(BaseModel):
    field: str
    status: Literal["unspecified", "conflicting", "low_confidence"]
    fallback_policy: str


class TaskConstraints(BaseModel):
    language: str = "zh-CN"
    prefer_visualization: bool = True
    detail_level: Literal["low", "medium", "high"] = "high"
    desired_output_formats: List[str] = Field(default_factory=lambda: ["markdown"])


class NormalizedTask(BaseModel):
    task_type: TaskType = "unknown"
    analysis_mode: str = "reporting"
    business_goal: str
    target_audience: AudienceType = "unknown"
    primary_questions: List[str] = Field(default_factory=list)
    constraints: TaskConstraints = Field(default_factory=TaskConstraints)
    ambiguities: List[AmbiguityItem] = Field(default_factory=list)
    success_intent: str = "produce_publishable_analysis_report"
    normalization_notes: Optional[str] = None