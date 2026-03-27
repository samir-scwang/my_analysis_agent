from __future__ import annotations

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class ReportSection(BaseModel):
    section_id: str
    title: str
    level: int = 2
    content: str


class ReportDraft(BaseModel):
    title: str
    subtitle: Optional[str] = None
    content: str
    degraded_output: bool = False
    used_chart_ids: List[str] = Field(default_factory=list)
    used_table_ids: List[str] = Field(default_factory=list)
    used_finding_ids: List[str] = Field(default_factory=list)
    report_metadata: Dict[str, Any] = Field(default_factory=dict)