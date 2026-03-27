from __future__ import annotations

from typing import List, Literal, Optional
from pydantic import BaseModel, Field


PhysicalType = Literal[
    "int",
    "float",
    "bool",
    "string",
    "datetime",
    "unknown",
]

SemanticType = Literal[
    "metric",
    "category",
    "date",
    "id",
    "text",
    "unknown",
]


class ColumnProfile(BaseModel):
    name: str
    physical_type: PhysicalType = "unknown"
    semantic_type: SemanticType = "unknown"
    null_ratio: float = 0.0
    unique_ratio: float = 0.0
    non_null_count: int = 0
    unique_count: int = 0
    sample_values: List[str] = Field(default_factory=list)
    role_candidates: List[str] = Field(default_factory=list)
    semantic_confidence: float = 0.5


class MissingnessItem(BaseModel):
    column: str
    null_ratio: float


class OutlierHint(BaseModel):
    column: str
    method: str
    outlier_ratio: float


class TableProfile(BaseModel):
    table_name: str
    row_count: int
    column_count: int
    columns: List[ColumnProfile] = Field(default_factory=list)


class TimeCoverage(BaseModel):
    min: Optional[str] = None
    max: Optional[str] = None
    granularity_candidates: List[str] = Field(default_factory=list)


class DataQualitySummary(BaseModel):
    missingness: List[MissingnessItem] = Field(default_factory=list)
    high_cardinality_columns: List[str] = Field(default_factory=list)
    potential_outliers: List[OutlierHint] = Field(default_factory=list)
    duplicate_rows_ratio: float = 0.0


class DatasetContext(BaseModel):
    dataset_id: str
    profile_version: str = "1.0"
    source_path: str
    tables: List[TableProfile] = Field(default_factory=list)

    candidate_time_columns: List[str] = Field(default_factory=list)
    candidate_measure_columns: List[str] = Field(default_factory=list)
    candidate_dimension_columns: List[str] = Field(default_factory=list)
    candidate_id_columns: List[str] = Field(default_factory=list)

    data_quality_summary: DataQualitySummary = Field(default_factory=DataQualitySummary)
    time_coverage: TimeCoverage = Field(default_factory=TimeCoverage)

    business_hints: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)