from __future__ import annotations

from typing import List
from pydantic import BaseModel, Field


class FinalQAResult(BaseModel):
    publish_ready: bool
    qa_summary: str = ""
    hard_errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    checked_items: List[str] = Field(default_factory=list)