from __future__ import annotations

import json
from pathlib import Path

from app.agents.deep_analysis.tools import (
    finalize_structured_output_tool,
    group_compare_chart_tool,
    group_compare_tool,
    inspect_dataset_tool,
    profile_columns_tool,
    register_artifact_tool,
    summarize_metrics_tool,
    time_trend_tool,
)


def test_inspect_dataset_tool(sample_csv: Path):
    result = inspect_dataset_tool.invoke(
        {
            "dataset_path": str(sample_csv),
            "max_rows": 3,
        }
    )
    payload = json.loads(result)

    assert payload["row_count"] == 4
    assert payload["column_count"] == 5
    assert "date" in payload["columns"]
    assert "sales" in payload["columns"]
    assert len(payload["preview"]) == 3


def test_profile_columns_tool(sample_csv: Path):
    result = profile_columns_tool.invoke(
        {
            "dataset_path": str(sample_csv),
            "max_sample_values": 3,
        }
    )
    payload = json.loads(result)

    assert payload["row_count"] == 4
    assert payload["column_count"] == 5
    assert len(payload["columns_profile"]) == 5

    col_map = {item["name"]: item for item in payload["columns_profile"]}
    assert "sales" in col_map
    assert "region" in col_map
    assert "dtype" in col_map["sales"]
    assert "sample_values" in col_map["region"]


def test_summarize_metrics_tool(sample_csv: Path, tmp_path: Path):
    output_csv = tmp_path / "tables" / "summary.csv"

    result = summarize_metrics_tool.invoke(
        {
            "dataset_path": str(sample_csv),
            "metrics": ["sales", "profit"],
            "output_csv_path": str(output_csv),
        }
    )
    payload = json.loads(result)

    assert output_csv.exists()
    assert payload["table_path"] == str(output_csv)
    assert payload["row_count"] == 2
    assert "metric" in payload["columns"]
    assert "sum" in payload["columns"]

    content = output_csv.read_text(encoding="utf-8")
    assert "sales" in content
    assert "profit" in content


def test_time_trend_tool(sample_csv: Path, tmp_path: Path):
    output_csv = tmp_path / "tables" / "time_trend.csv"
    output_chart = tmp_path / "charts" / "time_trend.png"
    print(tmp_path)
    result = time_trend_tool.invoke(
        {
            "dataset_path": str(sample_csv),
            "time_col": "date",
            "metrics": ["sales", "profit"],
            "output_csv_path": str(output_csv),
            "output_chart_path": str(output_chart),
            "grain": "day",
        }
    )
    payload = json.loads(result)

    assert output_csv.exists()
    assert output_chart.exists()
    assert payload["table_path"] == str(output_csv)
    assert payload["chart_path"] == str(output_chart)
    assert payload["time_col"] == "date"
    assert payload["grain"] == "day"
    assert payload["row_count"] == 4
    assert "time_trend" in payload["topic_tags"]


def test_group_compare_tool(sample_csv: Path, tmp_path: Path):
    output_csv = tmp_path / "tables" / "group_compare.csv"

    result = group_compare_tool.invoke(
        {
            "dataset_path": str(sample_csv),
            "group_col": "region",
            "metrics": ["sales", "profit"],
            "output_csv_path": str(output_csv),
        }
    )
    payload = json.loads(result)

    assert output_csv.exists()
    assert payload["table_path"] == str(output_csv)
    assert payload["group_col"] == "region"
    assert payload["row_count"] >= 1
    assert "sales" in payload["columns"]
    assert "profit" in payload["columns"]


def test_group_compare_chart_tool(sample_csv: Path, tmp_path: Path):
    output_csv = tmp_path / "tables" / "region_compare.csv"
    output_chart = tmp_path / "charts" / "region_compare.png"

    result = group_compare_chart_tool.invoke(
        {
            "dataset_path": str(sample_csv),
            "group_col": "region",
            "metrics": ["sales", "profit"],
            "output_csv_path": str(output_csv),
            "output_chart_path": str(output_chart),
            "top_n": 10,
        }
    )
    payload = json.loads(result)

    assert output_csv.exists()
    assert output_chart.exists()
    assert payload["table_path"] == str(output_csv)
    assert payload["chart_path"] == str(output_chart)
    assert payload["group_col"] == "region"
    assert "regional_comparison" in payload["topic_tags"]


def test_register_artifact_tool(tmp_path: Path):
    file_path = tmp_path / "tables" / "artifact.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text("metric,sum\nsales,100\n", encoding="utf-8")

    result = register_artifact_tool.invoke(
        {
            "artifact_id": "table_001",
            "artifact_type": "table",
            "title": "Summary Table",
            "path": str(file_path),
            "topic_tags": ["overall_performance"],
            "description": "summary table",
            "format": "csv",
        }
    )
    payload = json.loads(result)

    assert payload["artifact_id"] == "table_001"
    assert payload["artifact_type"] == "table"
    assert payload["path"] == str(file_path)
    assert payload["format"] == "csv"
    assert payload["topic_tags"] == ["overall_performance"]


def test_finalize_structured_output_tool(tmp_path: Path):
    output_path = tmp_path / "outputs" / "structured_result.json"

    payload = {
        "plan": {"mode": "normal"},
        "planned_actions": [],
        "executed_steps": [],
        "artifacts": [],
        "findings": [],
        "claims": [],
        "caveats": [],
        "rejected_charts": [],
        "rejected_hypotheses": [],
        "trace": [],
        "run_metadata": {"agent_type": "deepagent"},
    }

    result = finalize_structured_output_tool.invoke(
        {
            "output_path": str(output_path),
            "payload_json": json.dumps(payload, ensure_ascii=False),
        }
    )
    response = json.loads(result)

    assert output_path.exists()
    assert response["structured_output_path"] == str(output_path)
    assert response["written"] is True
    assert response["missing_keys"] == []

    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["plan"]["mode"] == "normal"
    assert "run_metadata" in written