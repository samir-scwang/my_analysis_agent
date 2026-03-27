from __future__ import annotations

import json
from pathlib import Path

from app.agents.deep_analysis.tools import inspect_dataset_tool, summarize_metrics_tool
from app.nodes.build_dataset_context import build_dataset_context_node
from app.nodes.validate_evidence import _check_artifact_paths
from app.nodes.write_report import read_table_artifact


def test_build_dataset_context_node_supports_xlsx(sample_xlsx: Path, base_state: dict):
    state = {
        **base_state,
        "dataset_id": "ds_test_xlsx",
        "dataset_path": str(sample_xlsx),
    }
    result = build_dataset_context_node(state)

    assert result["status"] == "DATASET_PROFILED"
    assert result["dataset_context"]["source_path"] == str(sample_xlsx)
    assert result["dataset_context"]["tables"][0]["table_name"] == sample_xlsx.name
    assert "sales" in result["dataset_context"]["candidate_measure_columns"]


def test_deep_analysis_tools_support_xlsx(sample_xlsx: Path, tmp_path: Path):
    inspect_payload = json.loads(
        inspect_dataset_tool.invoke(
            {
                "dataset_path": str(sample_xlsx),
                "max_rows": 2,
            }
        )
    )
    assert inspect_payload["row_count"] == 4
    assert "profit" in inspect_payload["columns"]

    output_csv = tmp_path / "tables" / "summary_from_xlsx.csv"
    summarize_payload = json.loads(
        summarize_metrics_tool.invoke(
            {
                "dataset_path": str(sample_xlsx),
                "metrics": ["sales", "profit"],
                "output_csv_path": str(output_csv),
            }
        )
    )
    assert output_csv.exists()
    assert summarize_payload["table_path"] == str(output_csv)


def test_validate_and_write_report_helpers_support_xlsx_table(sample_xlsx: Path):
    artifact_check = _check_artifact_paths(
        {
            "tables": [
                {
                    "table_id": "table_xlsx",
                    "path": str(sample_xlsx),
                }
            ],
            "charts": [],
        }
    )

    assert artifact_check.missing_table_files == []
    assert artifact_check.empty_tables == []

    df = read_table_artifact(str(sample_xlsx))
    assert list(df.columns) == ["date", "region", "product", "sales", "profit"]
