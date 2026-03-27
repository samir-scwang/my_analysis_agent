from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.agents.deep_analysis.tools import (
    finalize_structured_output_tool,
    group_compare_chart_tool,
    group_compare_tool,
    register_artifact_tool,
    summarize_metrics_tool,
    time_trend_tool,
)

def test_summarize_metrics_tool_raises_when_no_valid_metrics(sample_csv: Path, tmp_path: Path):
    output_csv = tmp_path / "tables" / "summary.csv"

    with pytest.raises(ValueError, match="No valid metrics|valid metrics"):
        summarize_metrics_tool.invoke(
            {
                "dataset_path": str(sample_csv),
                "metrics": ["not_exists_1", "not_exists_2"],
                "output_csv_path": str(output_csv),
            }
        )


def test_time_trend_tool_raises_when_time_col_missing(sample_csv: Path, tmp_path: Path):
    output_csv = tmp_path / "tables" / "time_trend.csv"
    output_chart = tmp_path / "charts" / "time_trend.png"

    with pytest.raises(ValueError, match="time_col not found"):
        time_trend_tool.invoke(
            {
                "dataset_path": str(sample_csv),
                "time_col": "not_exists_time",
                "metrics": ["sales"],
                "output_csv_path": str(output_csv),
                "output_chart_path": str(output_chart),
            }
        )


def test_time_trend_tool_raises_when_no_valid_metrics(sample_csv: Path, tmp_path: Path):
    output_csv = tmp_path / "tables" / "time_trend.csv"
    output_chart = tmp_path / "charts" / "time_trend.png"

    with pytest.raises(ValueError, match="No valid metrics"):
        time_trend_tool.invoke(
            {
                "dataset_path": str(sample_csv),
                "time_col": "date",
                "metrics": ["foo_metric"],
                "output_csv_path": str(output_csv),
                "output_chart_path": str(output_chart),
            }
        )


def test_group_compare_tool_raises_when_group_col_missing(sample_csv: Path, tmp_path: Path):
    output_csv = tmp_path / "tables" / "group_compare.csv"

    with pytest.raises(ValueError, match="group_col not found"):
        group_compare_tool.invoke(
            {
                "dataset_path": str(sample_csv),
                "group_col": "not_exists_group",
                "metrics": ["sales", "profit"],
                "output_csv_path": str(output_csv),
            }
        )


def test_group_compare_tool_raises_when_no_valid_metrics(sample_csv: Path, tmp_path: Path):
    output_csv = tmp_path / "tables" / "group_compare.csv"

    with pytest.raises(ValueError, match="No valid metrics"):
        group_compare_tool.invoke(
            {
                "dataset_path": str(sample_csv),
                "group_col": "region",
                "metrics": ["foo_metric"],
                "output_csv_path": str(output_csv),
            }
        )


def test_group_compare_chart_tool_raises_when_group_col_missing(sample_csv: Path, tmp_path: Path):
    output_csv = tmp_path / "tables" / "group_compare.csv"
    output_chart = tmp_path / "charts" / "group_compare.png"

    with pytest.raises(ValueError, match="group_col not found"):
        group_compare_chart_tool.invoke(
            {
                "dataset_path": str(sample_csv),
                "group_col": "not_exists_group",
                "metrics": ["sales"],
                "output_csv_path": str(output_csv),
                "output_chart_path": str(output_chart),
            }
        )


def test_group_compare_chart_tool_raises_when_no_valid_metrics(sample_csv: Path, tmp_path: Path):
    output_csv = tmp_path / "tables" / "group_compare.csv"
    output_chart = tmp_path / "charts" / "group_compare.png"

    with pytest.raises(ValueError, match="No valid metrics"):
        group_compare_chart_tool.invoke(
            {
                "dataset_path": str(sample_csv),
                "group_col": "region",
                "metrics": ["foo_metric"],
                "output_csv_path": str(output_csv),
                "output_chart_path": str(output_chart),
            }
        )


def test_register_artifact_tool_raises_when_file_missing(tmp_path: Path):
    missing_file = tmp_path / "tables" / "missing.csv"

    with pytest.raises(FileNotFoundError, match="Artifact file does not exist"):
        register_artifact_tool.invoke(
            {
                "artifact_id": "table_001",
                "artifact_type": "table",
                "title": "Missing Table",
                "path": str(missing_file),
                "topic_tags": ["overall_performance"],
            }
        )


def test_finalize_structured_output_tool_fills_missing_required_keys(tmp_path: Path):
    output_path = tmp_path / "outputs" / "structured_result.json"

    bad_payload = {
        "plan": {"mode": "normal"},
        "planned_actions": [],
        # 故意缺少 executed_steps / artifacts / findings 等
    }

    result = finalize_structured_output_tool.invoke(
        {
            "output_path": str(output_path),
            "payload_json": json.dumps(bad_payload, ensure_ascii=False),
        }
    )
    response = json.loads(result)
    written = json.loads(output_path.read_text(encoding="utf-8"))

    assert output_path.exists()
    assert response["written"] is True
    assert response["missing_keys"] == [
        "artifacts",
        "caveats",
        "claims",
        "executed_steps",
        "findings",
        "rejected_charts",
        "rejected_hypotheses",
        "run_metadata",
        "trace",
    ]
    assert written["executed_steps"] == []
    assert written["artifacts"] == []
    assert written["findings"] == []


def test_finalize_structured_output_tool_raises_when_payload_is_not_object(tmp_path: Path):
    output_path = tmp_path / "outputs" / "structured_result.json"

    with pytest.raises(ValueError, match="JSON object"):
        finalize_structured_output_tool.invoke(
            {
                "output_path": str(output_path),
                "payload_json": json.dumps(["not", "an", "object"], ensure_ascii=False),
            }
        )


def test_finalize_structured_output_tool_overwrites_existing_file(tmp_path: Path):
    output_path = tmp_path / "outputs" / "structured_result.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text('{"old": true}', encoding="utf-8")

    payload = {
        "plan": {"mode": "revision"},
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
    written = json.loads(output_path.read_text(encoding="utf-8"))

    assert response["written"] is True
    assert written["plan"]["mode"] == "revision"
    assert "old" not in written
