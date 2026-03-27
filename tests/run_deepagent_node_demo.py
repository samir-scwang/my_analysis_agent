from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from app.agents.deep_analysis.evidence_builder import build_evidence_pack_from_agent_output
from app.agents.deep_analysis.models import (
    DeepAnalysisAgentInput,
    DeepAnalysisAgentOutput,
    build_default_output_contract,
)
from app.agents.deep_analysis.prompts import build_full_agent_prompt, build_system_prompt
from app.agents.deep_analysis.tools import get_deep_analysis_tools
from app.config import settings
from app.services.analysis_workspace import ensure_workspace_from_state
from app.services.langchain_llm_factory import build_langchain_chat_model


def make_demo_dataset(dataset_path: Path) -> None:
    dataset_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
        [
            {"date": "2025-01-01", "region": "East", "product": "A", "sales": 100, "profit": 20},
            {"date": "2025-01-02", "region": "West", "product": "B", "sales": 150, "profit": 35},
            {"date": "2025-01-03", "region": "East", "product": "A", "sales": 120, "profit": 25},
            {"date": "2025-01-04", "region": "South", "product": "C", "sales": 180, "profit": 50},
            {"date": "2025-01-05", "region": "North", "product": "B", "sales": 160, "profit": 42},
            {"date": "2025-01-06", "region": "West", "product": "A", "sales": 140, "profit": 30},
            {"date": "2025-01-07", "region": "East", "product": "C", "sales": 210, "profit": 60},
            {"date": "2025-01-08", "region": "South", "product": "B", "sales": 170, "profit": 48},
            {"date": "2025-01-09", "region": "North", "product": "A", "sales": 190, "profit": 55},
            {"date": "2025-01-10", "region": "West", "product": "C", "sales": 130, "profit": 28},
        ]
    )
    df.to_csv(dataset_path, index=False, encoding="utf-8")


def build_demo_state(dataset_path: Path) -> dict:
    return {
        "request_id": "req_demo_001",
        "session_id": "sess_demo_001",
        "user_id": "user_demo_001",
        "dataset_id": "ds_demo_001",
        "dataset_path": str(dataset_path),
        "user_prompt": "请基于这份销售数据，生成一份包含整体表现、时间趋势、区域对比和产品结构的深度分析报告。",
        "input_config": {"language": "zh-CN", "output_format": ["markdown"]},
        "memory_context": {},
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
            "normalization_notes": "demo_manual_state",
        },
        "dataset_context": {
            "dataset_id": "ds_demo_001",
            "source_path": str(dataset_path),
            "tables": [
                {
                    "table_name": dataset_path.name,
                    "row_count": 10,
                    "column_count": 5,
                    "columns": [],
                }
            ],
            "candidate_time_columns": ["date"],
            "candidate_measure_columns": ["sales", "profit"],
            "candidate_dimension_columns": ["region", "product"],
            "candidate_id_columns": [],
            "data_quality_summary": {},
            "time_coverage": {"min": "2025-01-01", "max": "2025-01-10"},
            "business_hints": [
                "该数据集适合做时间趋势分析。",
                "该数据集适合做区域对比分析。",
                "该数据集适合做产品结构与品类分析。",
            ],
            "warnings": [],
        },
        "analysis_brief": {
            "brief_id": "brief_demo_001",
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
            "must_not_do": [
                "未经证据支持的因果推断",
                "生成重复信息量图表",
            ],
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
            "completion_criteria": [
                "所有 must_cover_topics 均被覆盖",
                "每个核心结论可追溯到图表或表格证据",
            ],
            "confidence_policy": {
                "default_claim_level": "descriptive_or_associational",
                "forbid_causal_language_without_evidence": True,
            },
            "revision_policy": {
                "max_review_rounds": 2,
                "must_fix_first": True,
            },
            "brief_notes": "demo_manual_state",
        },
        "evidence_pack_history": [],
        "review_history": [],
        "revision_round": 0,
        "max_review_rounds": 2,
        "revision_tasks": [],
        "revision_context": {},
        "execution_mode": "normal",
        "status": "BRIEF_READY",
        "warnings": [],
        "errors": [],
        "degraded_output": False,
    }


def build_backend(workspace_root: str):
    backend_name = settings.deepagent_backend.strip().lower()

    if backend_name == "daytona":
        from daytona import Daytona
        from langchain_daytona import DaytonaSandbox

        sandbox = Daytona().create()
        return DaytonaSandbox(sandbox=sandbox), "daytona_sandbox"

    if backend_name == "local_shell":
        from deepagents.backends import LocalShellBackend

        return (
            LocalShellBackend(
                root_dir=str(Path(workspace_root).resolve()),
                env={
                    "PATH": __import__("os").environ.get("PATH", ""),
                    "PYTHONUNBUFFERED": "1",
                },
            ),
            "local_shell",
        )

    raise RuntimeError(f"Unsupported backend: {settings.deepagent_backend}")


def print_stream_event(event: Any) -> None:
    """
    尽量兼容 deepagents / langgraph 的 updates 事件结构。
    """
    print("\n" + "-" * 100)
    print("[STREAM EVENT]")
    if isinstance(event, tuple):
        print("tuple:", event)
        return

    if isinstance(event, dict):
        for k, v in event.items():
            if isinstance(v, (dict, list)):
                try:
                    print(f"{k}: {json.dumps(v, ensure_ascii=False, indent=2, default=str)[:4000]}")
                except Exception:
                    print(f"{k}: {str(v)[:4000]}")
            else:
                print(f"{k}: {v}")
        return

    print(str(event))


def print_summary(evidence_pack: dict, workspace: dict) -> None:
    print("\n" + "=" * 100)
    print("FINAL SUMMARY")
    print("=" * 100)

    print("\nworkspace:")
    for k, v in workspace.items():
        print(f"  {k}: {v}")

    print("\nfindings:")
    for item in evidence_pack.get("findings", []):
        print("-", item.get("title"), "=>", item.get("statement"))

    print("\nclaims:")
    for item in evidence_pack.get("claim_evidence_map", []):
        print("-", item.get("claim_id"), "=>", item.get("claim_text"))

    print("\ntables:")
    for item in evidence_pack.get("tables", []):
        print("-", item.get("table_id"), "=>", item.get("path"))

    print("\ncharts:")
    for item in evidence_pack.get("charts", []):
        print("-", item.get("chart_id"), "=>", item.get("path"))


def main() -> None:
    project_root = Path(__file__).resolve().parent.parent
    demo_dir = project_root / "tmp" / "deepagent_stream_demo"
    dataset_path = demo_dir / "sales_demo.csv"

    make_demo_dataset(dataset_path)
    state = build_demo_state(dataset_path)

    workspace = ensure_workspace_from_state(state=state)

    output_contract = build_default_output_contract(
        structured_output_path=str(Path(workspace["outputs_dir"]) / "structured_result.json"),
        must_cover_topics=state["analysis_brief"].get("must_cover_topics", []),
    )

    agent_input = DeepAnalysisAgentInput(
        request_id=state.get("request_id", "unknown_request"),
        dataset_id=state.get("dataset_id", "unknown_dataset"),
        dataset_path=state["dataset_path"],
        dataset_context=state["dataset_context"],
        analysis_brief=state["analysis_brief"],
        normalized_task=state.get("normalized_task", {}) or {},
        execution_mode=state.get("execution_mode", "normal"),
        revision_round=state.get("revision_round", 0),
        revision_context=state.get("revision_context", {}) or {},
        workspace_root=workspace["root_dir"],
        dataset_local_path=workspace["dataset_local_path"],
        output_contract=output_contract,
    )

    model = build_langchain_chat_model(temperature=0.1, streaming=True)
    backend, backend_name = build_backend(agent_input.workspace_root)

    from deepagents import create_deep_agent

    tools = get_deep_analysis_tools()
    system_prompt = build_system_prompt()
    user_prompt = build_full_agent_prompt(
        workspace=workspace,
        output_contract=output_contract,
        normalized_task=agent_input.normalized_task,
        dataset_context=agent_input.dataset_context,
        analysis_brief=agent_input.analysis_brief,
        execution_mode=agent_input.execution_mode,
        revision_context=agent_input.revision_context,
    )

    if settings.deepagent_skills_dir:
        skills_root = Path(settings.deepagent_skills_dir)
    else:
        skills_root = (
            project_root / "app" / "agents" / "deep_analysis" / "skills"
        )

    # 对本地 backend，skills 路径最好确认可访问
    if not skills_root.exists():
        raise RuntimeError(f"Skills directory not found: {skills_root}")

    print(f"Using backend: {backend_name}")
    print(f"Workspace root: {workspace['root_dir']}")
    print(f"Dataset local path: {workspace['dataset_local_path']}")
    print(f"Structured output: {output_contract['structured_output_path']}")

    create_kwargs = {
        "model": model,
        "system_prompt": system_prompt,
        "backend": backend,
        "tools": tools,
        "skills": [str(skills_root)],
    }

    agent = create_deep_agent(**create_kwargs)

    print("\n" + "=" * 100)
    print("START STREAM")
    print("=" * 100)

    for event in agent.stream(
        {"messages": [{"role": "user", "content": user_prompt}]},
        stream_mode="updates",
        stream_subgraphs=True,
    ):
        print_stream_event(event)

    print("\n" + "=" * 100)
    print("STREAM FINISHED")
    print("=" * 100)

    structured_output_path = Path(output_contract["structured_output_path"])
    if not structured_output_path.exists():
        raise RuntimeError(f"structured_result.json not found: {structured_output_path}")

    raw = json.loads(structured_output_path.read_text(encoding="utf-8"))
    # normalized = self._normalize_structured_output_payload(raw)
    agent_output = DeepAnalysisAgentOutput.model_validate(raw)

    evidence_pack = build_evidence_pack_from_agent_output(
        state={
            **state,
            "analysis_workspace": workspace,
        },
        agent_output=agent_output,
        revision_round=state.get("revision_round", 0),
    )

    result_json = demo_dir / "deep_analysis_stream_result.json"
    result_json.write_text(
        json.dumps(
            {
                "workspace": workspace,
                "agent_output": agent_output.model_dump(),
                "evidence_pack": evidence_pack.model_dump(),
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    print_summary(evidence_pack.model_dump(), workspace)
    print(f"\nSaved full result to: {result_json}")


if __name__ == "__main__":
    main()