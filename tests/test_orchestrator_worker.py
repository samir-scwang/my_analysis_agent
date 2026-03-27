from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import app.services.llm_service as llm_module
import app.nodes.write_report as write_report_module


class _FakeWorkerLLMService:
    def invoke(self, messages, temperature: float = 0.2):
        return SimpleNamespace(content="流式测试标题")

    def stream_invoke(self, messages, temperature: float = 0.2):
        yield "第一段。"
        yield "第二段。"


def _load_worker_module(module_name: str, worker_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, worker_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_generate_report_stream_to_file_inserts_workspace_tables_and_charts(monkeypatch, tmp_path: Path):
    worker_path = Path("E:/myagent/analysis_agent/app/nodes/Orchestrator-worker.py")
    monkeypatch.setattr(llm_module, "LLMService", _FakeWorkerLLMService)
    monkeypatch.setattr(write_report_module, "llm", _FakeWorkerLLMService())

    worker_module = _load_worker_module("test_orchestrator_worker_stream", worker_path)

    workspace = tmp_path / "round_0"
    outputs_dir = workspace / "outputs"
    tables_dir = workspace / "tables"
    charts_dir = workspace / "charts"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)
    charts_dir.mkdir(parents=True, exist_ok=True)

    table_path = tables_dir / "summary_kpi_table.csv"
    table_path.write_text("metric,sum\ngmv,1000\ncost,600\n", encoding="utf-8")

    chart_path = charts_dir / "time_trend_chart.png"
    chart_path.write_bytes(b"fake-png")

    json_path = outputs_dir / "structured_result.json"
    json_path.write_text(
        json.dumps(
            {
                "user_prompt": "请生成销售分析报告",
                "analysis_brief": {
                    "must_cover_topics": ["overall_performance", "time_trend"],
                },
                "dataset_context": {
                    "source_path": "E:/datasets/demo_sales.csv",
                    "tables": [{"row_count": 10, "column_count": 7}],
                },
                "findings": [],
                "claims": [],
                "caveats": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    report_path = outputs_dir / "report.md"
    data = worker_module.load_structured_result(json_path)
    written_path = worker_module.generate_report_stream_to_file(data, report_path, json_path=json_path)
    content = report_path.read_text(encoding="utf-8")

    assert written_path == report_path
    assert report_path.exists()
    assert "流式测试标题" in content
    assert "## 1. 引言" in content
    assert "## 2. 执行摘要" in content
    assert "## 3. 整体表现分析" in content
    assert "　　第一段。第二段。" in content
    assert "表 1" in content
    assert "Summary Kpi Table" in content
    assert "<table>" in content
    expected_chart_ref = os.path.relpath(chart_path.resolve(), report_path.parent.resolve()).replace("\\", "/")
    assert "图 1" in content
    assert f'src="{expected_chart_ref}"' in content
