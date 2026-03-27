from __future__ import annotations

from pathlib import Path

from app.nodes.write_report import write_report_node


def _fake_generate_report_stream_to_file(
    data,
    report_path,
    *,
    json_path=None,
    echo=True,
):
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "# 销售分析主题报告\n\n"
        "**撰写时间：2026-03-26 10:00:00**\n\n"
        "---\n\n"
        "## 1. 引言\n\n"
        "　　这是引言。\n\n"
        "---\n\n"
        "## 2. 执行摘要\n\n"
        "　　这是摘要。\n\n"
        "---\n\n"
        "## 3. 结论与建议\n\n"
        "　　这是结论。\n",
        encoding="utf-8",
    )
    return path


def test_write_report_node_uses_orchestrator_worker(monkeypatch, base_state: dict, tmp_path: Path):
    import app.nodes.write_report as target_module

    outputs_dir = tmp_path / "deepagent_runs" / "req_test_001" / "round_0" / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)

    state = {
        **base_state,
        "analysis_workspace": {
            "root_dir": str(outputs_dir.parent),
            "outputs_dir": str(outputs_dir),
        },
        "validation_result": {"valid": True, "hard_errors": [], "warnings": []},
        "review_result": {"approved": True, "score": 0.9, "severity": "low"},
        "evidence_pack": {
            "charts": [
                {
                    "chart_id": "chart_001",
                    "title": "Sales Trend",
                    "path": str(outputs_dir.parent / "charts" / "sales_trend.png"),
                    "topic_tags": ["time_trend"],
                }
            ],
            "tables": [
                {
                    "table_id": "table_001",
                    "title": "Summary Table",
                    "table_type": "summary_kpi",
                    "path": str(outputs_dir.parent / "tables" / "summary.csv"),
                    "format": "csv",
                }
            ],
            "findings": [
                {
                    "finding_id": "finding_001",
                    "title": "整体表现",
                    "statement": "sales 稳定增长。",
                    "category": "summary",
                    "importance": "high",
                    "confidence": "medium",
                    "topic_tags": ["overall_performance"],
                }
            ],
            "claim_evidence_map": [
                {
                    "claim_id": "claim_001",
                    "claim_text": "sales 稳定增长。",
                    "confidence": "medium",
                    "support": {"finding_ids": ["finding_001"]},
                }
            ],
            "caveats": [],
        },
    }

    monkeypatch.setattr(target_module, "generate_report_stream_to_file", _fake_generate_report_stream_to_file)

    result = write_report_node(state)

    assert result["status"] == "REPORT_WRITTEN"
    assert result["report_draft"]["title"] == "销售分析主题报告"
    assert "## 1. 引言" in result["report_draft"]["content"]
    assert result["report_draft"]["report_metadata"]["generator"] == "write_report_node"
    assert Path(result["report_draft"]["report_metadata"]["report_path"]).exists()
