from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from app.agents.deep_analysis.models import (
    DeepAnalysisAgentInput,
    DeepAnalysisAgentOutput,
)
import app.services.deepagent_service as target_module
from app.services.deepagent_service import (
    DeepAgentService,
    _decode_subprocess_output,
    _normalize_backend_file_info_paths,
)


class FakeModel:
    def invoke(self, prompt: str):
        return SimpleNamespace(content="print('hello from fake model')")


@pytest.fixture
def fake_settings():
    return SimpleNamespace(
        deepagent_enable_fallback=True,
        deepagent_backend="local_shell",
        deepagent_skills_dir=None,
        deepagent_max_steps=25,
        deepagent_verbose=False,
    )


@pytest.fixture
def agent_input(tmp_path: Path) -> DeepAnalysisAgentInput:
    workspace_root = tmp_path / "workspace"
    for name in ["input", "scripts", "tables", "charts", "logs", "outputs"]:
        (workspace_root / name).mkdir(parents=True, exist_ok=True)

    dataset_local_path = workspace_root / "input" / "sales_sample.csv"
    dataset_local_path.write_text(
        "\n".join(
            [
                "date,region,product,sales,profit",
                "2025-01-01,East,A,100,20",
                "2025-01-02,West,B,150,35",
                "2025-01-03,East,A,120,25",
            ]
        ),
        encoding="utf-8",
    )

    return DeepAnalysisAgentInput(
        request_id="req_test_001",
        dataset_id="ds_test_001",
        dataset_path=str(dataset_local_path),
        dataset_context={
            "candidate_time_columns": ["date"],
            "candidate_measure_columns": ["sales", "profit"],
            "candidate_dimension_columns": ["region", "product"],
            "candidate_id_columns": [],
            "business_hints": ["适合时间趋势分析"],
        },
        analysis_brief={
            "must_cover_topics": ["overall_performance", "time_trend"],
            "recommended_metrics": ["sales", "profit"],
            "recommended_dimensions": ["region", "product"],
            "chart_policy": {"preferred_chart_types": ["line", "bar"]},
            "table_policy": {"must_have_tables": ["summary_kpi_table"]},
        },
        normalized_task={
            "task_type": "reporting",
            "target_audience": "business_stakeholders",
        },
        execution_mode="normal",
        revision_round=0,
        revision_context={},
        workspace_root=str(workspace_root),
        dataset_local_path=str(dataset_local_path),
        output_contract={
            "structured_output_path": str(workspace_root / "outputs" / "structured_result.json"),
            "must_cover_topics": ["overall_performance", "time_trend"],
        },
    )


def test_run_analysis_falls_back_when_deepagent_fails(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings,
    agent_input: DeepAnalysisAgentInput,
):
    monkeypatch.setattr(target_module, "settings", fake_settings)
    monkeypatch.setattr(target_module, "build_langchain_chat_model", lambda **kwargs: FakeModel())

    service = DeepAgentService()

    def fake_run_with_deepagent(*, agent_input, started_at):
        raise RuntimeError("deepagent failed")

    captured = {}

    def fake_run_with_script_fallback(*, agent_input, started_at, fallback_reason):
        captured["fallback_reason"] = fallback_reason
        return DeepAnalysisAgentOutput(
            plan={"planner_notes": "fallback_used"},
            planned_actions=[],
            executed_steps=[],
            artifacts=[],
            findings=[],
            claims=[],
            caveats=[],
            rejected_charts=[],
            rejected_hypotheses=[],
            trace=[{"type": "fallback_triggered"}],
            run_metadata={"agent_type": "deepagent"},
        )

    monkeypatch.setattr(service, "_run_with_deepagent", fake_run_with_deepagent)
    monkeypatch.setattr(service, "_run_with_script_fallback", fake_run_with_script_fallback)

    result = service.run_analysis(agent_input=agent_input)

    assert result.plan["planner_notes"] == "fallback_used"
    assert result.trace[0]["type"] == "fallback_triggered"
    assert "deepagent failed" in captured["fallback_reason"]


def test_run_analysis_raises_when_fallback_disabled(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings,
    agent_input: DeepAnalysisAgentInput,
):
    fake_settings.deepagent_enable_fallback = False

    monkeypatch.setattr(target_module, "settings", fake_settings)
    monkeypatch.setattr(target_module, "build_langchain_chat_model", lambda **kwargs: FakeModel())

    service = DeepAgentService()

    def fake_run_with_deepagent(*, agent_input, started_at):
        raise RuntimeError("deepagent hard failure")

    monkeypatch.setattr(service, "_run_with_deepagent", fake_run_with_deepagent)

    with pytest.raises(RuntimeError, match="deepagent hard failure"):
        service.run_analysis(agent_input=agent_input)


def test_load_structured_output_merges_trace_and_metadata(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings,
    tmp_path: Path,
):
    monkeypatch.setattr(target_module, "settings", fake_settings)
    monkeypatch.setattr(target_module, "build_langchain_chat_model", lambda **kwargs: FakeModel())

    service = DeepAgentService()

    output_path = tmp_path / "structured_result.json"
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
        "trace": [{"type": "original_trace"}],
        "run_metadata": {"agent_type": "deepagent"},
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    result = service._load_structured_output(
        structured_output_path=str(output_path),
        execution_mode="normal",
        started_at=0.0,
        extra_trace=[{"type": "extra_trace"}],
    )

    assert result.plan["mode"] == "normal"
    assert len(result.trace) == 2
    assert result.trace[0]["type"] == "original_trace"
    assert result.trace[1]["type"] == "extra_trace"

    assert result.run_metadata["agent_type"] == "deepagent"
    assert result.run_metadata["execution_mode"] == "normal"
    assert result.run_metadata["structured_output_path"] == str(output_path)
    assert result.run_metadata["loaded_from_json"] is True
    assert result.run_metadata["fallback_enabled"] is True
    assert result.run_metadata["configured_backend"] == "local_shell"


def test_load_structured_output_coerces_executed_step_fields_to_strings(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings,
    tmp_path: Path,
):
    monkeypatch.setattr(target_module, "settings", fake_settings)
    monkeypatch.setattr(target_module, "build_langchain_chat_model", lambda **kwargs: FakeModel())

    service = DeepAgentService()

    output_path = tmp_path / "structured_result.json"
    payload = {
        "plan": {"mode": "normal"},
        "planned_actions": [],
        "executed_steps": [
            {
                "step_id": 1,
                "step_type": 2,
                "description": 3,
                "status": "completed",
                "output_refs": [4, 5],
                "code_ref": 6,
            }
        ],
        "artifacts": [],
        "findings": [],
        "claims": [],
        "caveats": [],
        "rejected_charts": [],
        "rejected_hypotheses": [],
        "trace": [],
        "run_metadata": {"agent_type": "deepagent"},
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    result = service._load_structured_output(
        structured_output_path=str(output_path),
        execution_mode="normal",
        started_at=0.0,
        extra_trace=[],
    )

    assert result.executed_steps[0].step_id == "1"
    assert result.executed_steps[0].step_type == "2"
    assert result.executed_steps[0].description == "3"
    assert result.executed_steps[0].output_refs == ["4", "5"]
    assert result.executed_steps[0].code_ref == "6"
    assert result.executed_steps[0].status == "success"


def test_load_structured_output_coerces_rejected_items_to_dicts(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings,
    tmp_path: Path,
):
    monkeypatch.setattr(target_module, "settings", fake_settings)
    monkeypatch.setattr(target_module, "build_langchain_chat_model", lambda **kwargs: FakeModel())

    service = DeepAgentService()

    output_path = tmp_path / "structured_result.json"
    payload = {
        "plan": {"mode": "normal"},
        "planned_actions": [],
        "executed_steps": [],
        "artifacts": [],
        "findings": [],
        "claims": [],
        "caveats": [],
        "rejected_charts": [
            "低信息密度，未采用",
        ],
        "rejected_hypotheses": [
            "未进行因果推断分析，因为样本量较小且缺乏控制变量",
            {"hypothesis_id": "custom_001", "reason": "已有结构化对象"},
        ],
        "trace": [],
        "run_metadata": {"agent_type": "deepagent"},
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    result = service._load_structured_output(
        structured_output_path=str(output_path),
        execution_mode="normal",
        started_at=0.0,
        extra_trace=[],
    )

    assert result.rejected_charts == [
        {"chart_id": "rejected_chart_001", "reason": "低信息密度，未采用"}
    ]
    assert result.rejected_hypotheses == [
        {
            "hypothesis_id": "rejected_hypothesis_001",
            "reason": "未进行因果推断分析，因为样本量较小且缺乏控制变量",
        },
        {"hypothesis_id": "custom_001", "reason": "已有结构化对象"},
    ]


def test_load_structured_output_raises_when_file_missing(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings,
    tmp_path: Path,
):
    monkeypatch.setattr(target_module, "settings", fake_settings)
    monkeypatch.setattr(target_module, "build_langchain_chat_model", lambda **kwargs: FakeModel())

    service = DeepAgentService()

    missing_path = tmp_path / "missing.json"

    with pytest.raises(RuntimeError, match="structured_result.json not found"):
        service._load_structured_output(
            structured_output_path=str(missing_path),
            execution_mode="normal",
            started_at=0.0,
            extra_trace=[],
        )


def test_run_script_step_with_repair_synthesizes_missing_result_json(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings,
    agent_input: DeepAnalysisAgentInput,
):
    monkeypatch.setattr(target_module, "settings", fake_settings)
    monkeypatch.setattr(target_module, "build_langchain_chat_model", lambda **kwargs: FakeModel())

    service = DeepAgentService()
    output_csv_path = str(Path(agent_input.workspace_root) / "tables" / "table_r0_region_product_cross.csv")

    script_code = f"""
from pathlib import Path
import pandas as pd

df = pd.read_csv({agent_input.dataset_local_path!r})
result = df.groupby(["region", "product"], as_index=False).agg({{"sales": "sum", "profit": "sum"}})
out_path = Path({output_csv_path!r})
out_path.parent.mkdir(parents=True, exist_ok=True)
result.to_csv(out_path, index=False)
print("ok")
""".strip()

    monkeypatch.setattr(
        service,
        "_generate_step_script",
        lambda *, agent_input, step: script_code,
    )

    step = {
        "step_id": "step_050",
        "kind": "script",
        "name": "region_product_cross",
        "goal": "生成区域-产品交叉分析表，补充更细粒度的比较视角",
        "group_cols": ["region", "product"],
        "metrics": ["sales", "profit"],
        "output_csv_path": output_csv_path,
        "expected_outputs": ["table"],
    }

    result = service._run_script_step_with_repair(
        agent_input=agent_input,
        step=step,
        max_attempts=1,
    )

    result_json_path = Path(result["result_json_path"])
    payload = json.loads(result_json_path.read_text(encoding="utf-8"))

    assert result["status"] == "success"
    assert result["result_synthesized"] is True
    assert result_json_path.exists()
    assert payload["findings"] == []
    assert payload["claims"] == []
    assert payload["caveats"] == []
    assert len(payload["artifacts"]) == 1
    assert payload["artifacts"][0]["artifact_type"] == "table"
    assert payload["artifacts"][0]["path"] == output_csv_path


def test_sync_skills_to_workspace_uses_custom_dir_and_ignores_pycache(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings,
    agent_input: DeepAnalysisAgentInput,
    tmp_path: Path,
):
    skills_source = tmp_path / "custom_skills"
    skill_dir = skills_source / "data-analysis"
    pycache_dir = skills_source / "__pycache__"
    nested_pycache_dir = skill_dir / "__pycache__"

    skill_dir.mkdir(parents=True, exist_ok=True)
    pycache_dir.mkdir(parents=True, exist_ok=True)
    nested_pycache_dir.mkdir(parents=True, exist_ok=True)

    (skill_dir / "SKILL.md").write_text("# test skill\n", encoding="utf-8")
    (pycache_dir / "ignored.pyc").write_bytes(b"pyc")
    (nested_pycache_dir / "ignored.pyc").write_bytes(b"pyc")

    fake_settings.deepagent_skills_dir = str(skills_source)

    monkeypatch.setattr(target_module, "settings", fake_settings)
    monkeypatch.setattr(target_module, "build_langchain_chat_model", lambda **kwargs: FakeModel())

    service = DeepAgentService()
    workspace_skills_dir = service._sync_skills_to_workspace(agent_input=agent_input)

    assert workspace_skills_dir == Path(agent_input.workspace_root) / "skills"
    assert (workspace_skills_dir / "data-analysis" / "SKILL.md").exists()
    assert not (workspace_skills_dir / "__pycache__").exists()
    assert not (workspace_skills_dir / "data-analysis" / "__pycache__").exists()


def test_run_with_deepagent_passes_workspace_scoped_skills(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings,
    agent_input: DeepAnalysisAgentInput,
    tmp_path: Path,
):
    skills_source = tmp_path / "custom_skills"
    (skills_source / "data-analysis").mkdir(parents=True, exist_ok=True)
    (skills_source / "data-analysis" / "SKILL.md").write_text("# test skill\n", encoding="utf-8")
    fake_settings.deepagent_skills_dir = str(skills_source)

    monkeypatch.setattr(target_module, "settings", fake_settings)
    monkeypatch.setattr(target_module, "build_langchain_chat_model", lambda **kwargs: FakeModel())
    monkeypatch.setattr(target_module, "get_deep_analysis_tools", lambda: [])

    captured: dict = {}
    prompt_inputs: dict = {}

    fake_deepagents = ModuleType("deepagents")

    def fake_create_deep_agent(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(invoke=lambda payload: {"ok": True, "payload": payload})

    fake_deepagents.create_deep_agent = fake_create_deep_agent
    monkeypatch.setitem(sys.modules, "deepagents", fake_deepagents)
    monkeypatch.setattr(target_module, "build_system_prompt", lambda: "system")
    monkeypatch.setattr(
        target_module,
        "build_full_agent_prompt",
        lambda **kwargs: prompt_inputs.update(kwargs) or "user",
    )

    service = DeepAgentService()
    monkeypatch.setattr(service, "_build_backend", lambda *, workspace_root: ("fake_backend", "fake_backend"))
    monkeypatch.setattr(
        service,
        "_load_structured_output",
        lambda **kwargs: DeepAnalysisAgentOutput(
            plan={},
            planned_actions=[],
            executed_steps=[],
            artifacts=[],
            findings=[],
            claims=[],
            caveats=[],
            rejected_charts=[],
            rejected_hypotheses=[],
            trace=[],
            run_metadata={},
        ),
    )

    service._run_with_deepagent(agent_input=agent_input, started_at=0.0)

    expected_skills_dir = Path(agent_input.workspace_root) / "skills"
    assert captured["skills"] == [str(expected_skills_dir)]
    assert (expected_skills_dir / "data-analysis" / "SKILL.md").exists()
    assert prompt_inputs["workspace"]["root_dir"] == agent_input.workspace_root
    assert prompt_inputs["workspace"]["dataset_local_path"] == agent_input.dataset_local_path
    assert prompt_inputs["workspace"]["input_dir"] == str(Path(agent_input.workspace_root) / "input")
    assert prompt_inputs["output_contract"]["structured_output_path"] == agent_input.output_contract["structured_output_path"]


def test_load_structured_output_normalizes_artifact_type_path_description(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings,
    tmp_path: Path,
):
    monkeypatch.setattr(target_module, "settings", fake_settings)
    monkeypatch.setattr(target_module, "build_langchain_chat_model", lambda **kwargs: FakeModel())

    service = DeepAgentService()
    structured_path = tmp_path / "structured_result.json"
    table_path = tmp_path / "table_r0_region_product_cross.csv"
    table_path.write_text("region,product,gmv\nEast,A,100\n", encoding="utf-8")

    structured_path.write_text(
        json.dumps(
            {
                "plan": {},
                "planned_actions": [],
                "executed_steps": [],
                "artifacts": [
                    {
                        "type": "table",
                        "path": str(table_path),
                        "description": "区域-产品交叉分析表，包含GMV、成本、利润和利润率",
                    }
                ],
                "findings": [],
                "claims": [],
                "caveats": [],
                "trace": [],
                "run_metadata": {},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    output = service._load_structured_output(
        structured_output_path=str(structured_path),
        execution_mode="normal",
        started_at=0.0,
        extra_trace=[],
    )

    assert len(output.artifacts) == 1
    assert output.artifacts[0].artifact_type == "table"
    assert output.artifacts[0].artifact_id == "table_r0_region_product_cross"
    assert output.artifacts[0].title == "Table R0 Region Product Cross"


def test_build_backend_local_shell_disables_virtual_mode(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings,
    tmp_path: Path,
):
    monkeypatch.setattr(target_module, "settings", fake_settings)
    monkeypatch.setattr(target_module, "build_langchain_chat_model", lambda **kwargs: FakeModel())

    captured: dict = {}
    fake_backends = ModuleType("deepagents.backends")

    class FakeLocalShellBackend:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    fake_backends.LocalShellBackend = FakeLocalShellBackend
    monkeypatch.setitem(sys.modules, "deepagents.backends", fake_backends)

    service = DeepAgentService()
    backend, backend_name = service._build_backend(workspace_root=str(tmp_path / "workspace"))

    assert backend_name == "local_shell"
    assert captured["virtual_mode"] is False
    assert captured["root_dir"] == str((tmp_path / "workspace").resolve())


def test_normalize_backend_file_info_paths_uses_posix_directory_names():
    result = _normalize_backend_file_info_paths(
        [
            {
                "path": r"E:\myagent\analysis_agent\app\artifacts\deepagent_runs\req_00\round_0\skills\data-analysis\\",
                "is_dir": True,
            },
            {
                "path": r"E:\myagent\analysis_agent\app\artifacts\deepagent_runs\req_00\round_0\skills\data-analysis\SKILL.md",
                "is_dir": False,
            },
        ]
    )

    assert result[0]["path"].endswith("/skills/data-analysis/")
    assert result[1]["path"].endswith("/skills/data-analysis/SKILL.md")


def test_decode_subprocess_output_supports_gbk_bytes():
    assert _decode_subprocess_output("这是输出".encode("gbk")) == "这是输出"


def test_build_backend_local_shell_execute_tolerates_non_utf8_output(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings,
    tmp_path: Path,
):
    monkeypatch.setattr(target_module, "settings", fake_settings)
    monkeypatch.setattr(target_module, "build_langchain_chat_model", lambda **kwargs: FakeModel())

    fake_backends = ModuleType("deepagents.backends")
    fake_protocol = ModuleType("deepagents.backends.protocol")

    class FakeExecuteResponse:
        def __init__(self, *, output, exit_code, truncated):
            self.output = output
            self.exit_code = exit_code
            self.truncated = truncated

    class FakeLocalShellBackend:
        def __init__(self, **kwargs):
            self._default_timeout = kwargs.get("timeout", 120)
            self._max_output_bytes = kwargs.get("max_output_bytes", 100_000)
            self._env = kwargs.get("env", {})
            self.cwd = Path(kwargs["root_dir"])

        def ls_info(self, path: str):
            return []

        async def als_info(self, path: str):
            return []

    fake_backends.LocalShellBackend = FakeLocalShellBackend
    fake_protocol.ExecuteResponse = FakeExecuteResponse
    monkeypatch.setitem(sys.modules, "deepagents.backends", fake_backends)
    monkeypatch.setitem(sys.modules, "deepagents.backends.protocol", fake_protocol)

    completed = subprocess.CompletedProcess(
        args="echo test",
        returncode=0,
        stdout="这是输出".encode("gbk"),
        stderr="错误".encode("gbk"),
    )
    monkeypatch.setattr(target_module.subprocess, "run", lambda *args, **kwargs: completed)

    service = DeepAgentService()
    backend, backend_name = service._build_backend(workspace_root=str(tmp_path / "workspace"))
    result = backend.execute("echo test")

    assert backend_name == "local_shell"
    assert "这是输出" in result.output
    assert "[stderr] 错误" in result.output
