from __future__ import annotations

from typing import List, Literal
from pydantic import BaseModel, Field


class ValidationIssue(BaseModel):
    type: str
    message: str


class CoverageCheck(BaseModel):
    must_cover_topics_total: int = 0
    must_cover_topics_covered: int = 0
    covered_topics: List[str] = Field(default_factory=list)
    missing_topics: List[str] = Field(default_factory=list)


class ArtifactCheck(BaseModel):
    missing_chart_files: List[str] = Field(default_factory=list)
    missing_table_files: List[str] = Field(default_factory=list)
    empty_tables: List[str] = Field(default_factory=list)
    broken_claim_links: List[str] = Field(default_factory=list)


class ValidationResult(BaseModel):
    valid: bool = True
    hard_errors: List[ValidationIssue] = Field(default_factory=list)
    warnings: List[ValidationIssue] = Field(default_factory=list)

    coverage_check: CoverageCheck = Field(default_factory=CoverageCheck)
    artifact_check: ArtifactCheck = Field(default_factory=ArtifactCheck)

    redundancy_signals: List[dict] = Field(default_factory=list)