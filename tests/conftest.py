from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    path = tmp_path / "sales_sample.csv"
    df = pd.DataFrame(
        {
            "date": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04"],
            "region": ["East", "West", "East", "South"],
            "product": ["A", "B", "A", "C"],
            "sales": [100, 150, 120, 180],
            "profit": [20, 35, 25, 50],
        }
    )
    df.to_csv(path, index=False)
    return path


@pytest.fixture
def sample_xlsx(tmp_path: Path) -> Path:
    path = tmp_path / "sales_sample.xlsx"
    df = pd.DataFrame(
        {
            "date": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04"],
            "region": ["East", "West", "East", "South"],
            "product": ["A", "B", "A", "C"],
            "sales": [100, 150, 120, 180],
            "profit": [20, 35, 25, 50],
        }
    )
    df.to_excel(path, index=False)
    return path


@pytest.fixture
def base_state(sample_csv: Path) -> dict:
    return {
        "request_id": "req_test_001",
        "dataset_id": "ds_test_001",
        "dataset_path": str(sample_csv),
        "user_prompt": "请做一份销售数据分析报告",
        "normalized_task": {
            "task_type": "reporting",
            "analysis_mode": "reporting",
            "business_goal": "生成可发布的数据分析报告",
            "target_audience": "business_stakeholders",
            "primary_questions": ["销售表现", "时间趋势", "区域差异", "产品结构"],
            "constraints": {
                "language": "zh-CN",
                "prefer_visualization": True,
                "detail_level": "high",
                "desired_output_formats": ["markdown"],
            },
            "ambiguities": [],
            "success_intent": "produce_publishable_analysis_report",
            "normalization_notes": "test",
        },
        "dataset_context": {
            "dataset_id": "ds_test_001",
            "source_path": str(sample_csv),
            "tables": [
                {
                    "table_name": sample_csv.name,
                    "row_count": 4,
                    "column_count": 5,
                    "columns": [],
                }
            ],
            "candidate_time_columns": ["date"],
            "candidate_measure_columns": ["sales", "profit"],
            "candidate_dimension_columns": ["region", "product"],
            "candidate_id_columns": [],
            "data_quality_summary": {},
            "time_coverage": {"min": "2025-01-01", "max": "2025-01-04"},
            "business_hints": ["该数据集适合做时间趋势分析。"],
            "warnings": [],
        },
        "analysis_brief": {
            "brief_id": "brief_001",
            "version": 1,
            "task_type": "reporting",
            "business_goal": "生成分析报告",
            "target_audience": "business_stakeholders",
            "report_style": {
                "language": "zh-CN",
                "tone": "professional",
                "detail_level": "high",
            },
            "must_cover_topics": [
                "overall_performance",
                "time_trend",
                "regional_comparison",
                "product_mix",
            ],
            "optional_topics": [],
            "must_not_do": ["未经证据支持的因果推断"],
            "recommended_metrics": ["sales", "profit"],
            "recommended_dimensions": ["region", "product", "date"],
            "chart_policy": {
                "target_chart_range": [2, 4],
                "max_total_charts": 4,
                "max_similar_chart_per_metric": 2,
                "preferred_chart_types": ["line", "bar"],
                "avoid_chart_types": ["low_information_pie"],
            },
            "table_policy": {
                "max_total_tables": 6,
                "must_have_tables": ["summary_kpi_table", "regional_comparison_table"],
            },
            "completion_criteria": ["所有 must_cover_topics 均被覆盖"],
            "confidence_policy": {
                "default_claim_level": "descriptive_or_associational",
                "forbid_causal_language_without_evidence": True,
            },
            "revision_policy": {
                "max_review_rounds": 2,
                "must_fix_first": True,
            },
            "brief_notes": "test",
        },
        "execution_mode": "normal",
        "revision_round": 0,
        "revision_context": {},
        "warnings": [],
        "errors": [],
    }
