from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from app.agents.deep_analysis.models import (
    DeepAnalysisAgentInput,
    DeepAnalysisAgentOutput,
    ExecutedStepTrace,
)
from app.agents.deep_analysis.prompts import (
    build_full_agent_prompt,
    build_step_repair_prompt,
    build_step_script_prompt,
    build_system_prompt,
)
from app.agents.deep_analysis.tools import get_deep_analysis_tools
from app.config import settings
from app.services.langchain_llm_factory import build_langchain_chat_model


def _normalize_backend_file_info_paths(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            normalized.append(item)
            continue

        path = item.get("path")
        is_dir = bool(item.get("is_dir"))
        if isinstance(path, str) and path:
            suffix = "/" if is_dir else ""
            normalized_path = Path(path.rstrip("/\\")).as_posix() + suffix
            normalized.append({**item, "path": normalized_path})
        else:
            normalized.append(item)
    return normalized


def _decode_subprocess_output(data: bytes | str | None) -> str:
    if data is None:
        return ""
    if isinstance(data, str):
        return data

    for encoding in ("utf-8", "utf-8-sig", "gb18030", "cp936"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue

    return data.decode("utf-8", errors="replace")


def _slug_to_title(text: str) -> str:
    cleaned = str(text or "").replace("_", " ").replace("-", " ").strip()
    return " ".join(part.capitalize() for part in cleaned.split()) or "Untitled"


class DeepAgentService:
    """
    Deep analysis execution service.

    执行策略：
    1. 优先尝试 deepagent 主路径（tool + backend + skills）。
    2. 若主路径失败，进入 fallback。
    3. fallback 采用：
       - 先 plan
       - tool-first
       - tools 不足时生成小脚本
       - 小脚本失败时在当前轮次内修补
       - 最后统一收敛 structured_result.json
    """

    def __init__(self) -> None:
        self.model = build_langchain_chat_model(temperature=0.1, streaming=False)

    # -------------------------------------------------------------------------
    # public entry
    # -------------------------------------------------------------------------
    def run_analysis(
        self,
        *,
        agent_input: DeepAnalysisAgentInput,
    ) -> DeepAnalysisAgentOutput:
        started_at = time.time()

        try:
            return self._run_with_deepagent(
                agent_input=agent_input,
                started_at=started_at,
            )
        except Exception as deepagent_error:

            if not settings.deepagent_enable_fallback:
                raise

            return self._run_with_script_fallback(
                agent_input=agent_input,
                started_at=started_at,
                fallback_reason=str(deepagent_error),
            )

    # -------------------------------------------------------------------------
    # deepagent main path
    # -------------------------------------------------------------------------
    def _build_backend(self, *, workspace_root: str):
        backend_name = settings.deepagent_backend.strip().lower()

        if backend_name == "daytona":
            try:
                from daytona import Daytona
                from langchain_daytona import DaytonaSandbox

                sandbox = Daytona().create()
                return DaytonaSandbox(sandbox=sandbox), "daytona_sandbox"
            except Exception as e:
                raise RuntimeError(f"Failed to initialize daytona backend: {e}") from e

        if backend_name == "local_shell":
            try:
                from deepagents.backends import LocalShellBackend

                if os.name == "nt":
                    class WindowsFriendlyLocalShellBackend(LocalShellBackend):
                        def ls_info(self, path: str) -> List[Dict[str, Any]]:
                            return _normalize_backend_file_info_paths(super().ls_info(path))

                        async def als_info(self, path: str) -> List[Dict[str, Any]]:
                            return _normalize_backend_file_info_paths(await super().als_info(path))

                        def execute(self, command: str, timeout: int | None = None):
                            from deepagents.backends.protocol import ExecuteResponse

                            if not command or not isinstance(command, str):
                                return ExecuteResponse(
                                    output="Error: Command must be a non-empty string.",
                                    exit_code=1,
                                    truncated=False,
                                )

                            effective_timeout = timeout if timeout is not None else self._default_timeout
                            if effective_timeout <= 0:
                                raise ValueError(f"timeout must be positive, got {effective_timeout}")

                            try:
                                result = subprocess.run(
                                    command,
                                    check=False,
                                    shell=True,
                                    capture_output=True,
                                    text=False,
                                    timeout=effective_timeout,
                                    env=self._env,
                                    cwd=str(self.cwd),
                                )

                                stdout_text = _decode_subprocess_output(result.stdout)
                                stderr_text = _decode_subprocess_output(result.stderr)

                                output_parts: List[str] = []
                                if stdout_text:
                                    output_parts.append(stdout_text)
                                if stderr_text:
                                    stderr_lines = stderr_text.strip().split("\n")
                                    output_parts.extend(f"[stderr] {line}" for line in stderr_lines if line)

                                output = "\n".join(output_parts) if output_parts else "<no output>"

                                truncated = False
                                if len(output) > self._max_output_bytes:
                                    output = output[: self._max_output_bytes]
                                    output += f"\n\n... Output truncated at {self._max_output_bytes} bytes."
                                    truncated = True

                                if result.returncode != 0:
                                    output = f"{output.rstrip()}\n\nExit code: {result.returncode}"

                                return ExecuteResponse(
                                    output=output,
                                    exit_code=result.returncode,
                                    truncated=truncated,
                                )

                            except subprocess.TimeoutExpired:
                                if timeout is not None:
                                    msg = (
                                        f"Error: Command timed out after {effective_timeout} seconds "
                                        "(custom timeout). The command may be stuck or require more time."
                                    )
                                else:
                                    msg = (
                                        f"Error: Command timed out after {effective_timeout} seconds. "
                                        "For long-running commands, re-run using the timeout parameter."
                                    )
                                return ExecuteResponse(output=msg, exit_code=124, truncated=False)
                            except Exception as e:
                                return ExecuteResponse(
                                    output=f"Error executing command: {e}",
                                    exit_code=1,
                                    truncated=False,
                                )
                else:
                    WindowsFriendlyLocalShellBackend = LocalShellBackend

                env = {
                    "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                    "PYTHONUNBUFFERED": "1",
                    "PYTHONIOENCODING": "utf-8",
                    "PYTHONUTF8": "1",
                }
                return (
                    WindowsFriendlyLocalShellBackend(
                        root_dir=str(Path(workspace_root).resolve()),
                        env=env,
                        virtual_mode=False,
                    ),
                    "local_shell",
                )
            except Exception as e:
                raise RuntimeError(f"Failed to initialize local_shell backend: {e}") from e

        raise RuntimeError(
            f"Unsupported DEEPAGENT_BACKEND={settings.deepagent_backend!r}. "
            "Expected one of: local_shell, daytona"
        )

    def _resolve_skills_root(self) -> Path:
        if settings.deepagent_skills_dir:
            return Path(settings.deepagent_skills_dir)
        return (
            Path(__file__).resolve().parent.parent
            / "agents"
            / "deep_analysis"
            / "skills"
        )

    def _sync_skills_to_workspace(self, *, agent_input: DeepAnalysisAgentInput) -> Path:
        skills_root = self._resolve_skills_root()
        if not skills_root.exists():
            raise RuntimeError(f"Skills directory not found: {skills_root}")
        if not skills_root.is_dir():
            raise RuntimeError(f"Skills path is not a directory: {skills_root}")

        workspace_skills_dir = Path(agent_input.workspace_root) / "skills"
        shutil.copytree(
            skills_root,
            workspace_skills_dir,
            dirs_exist_ok=True,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
        )
        return workspace_skills_dir

    def _run_with_deepagent(
        self,
        *,
        agent_input: DeepAnalysisAgentInput,
        started_at: float,
    ) -> DeepAnalysisAgentOutput:
        try:
            from deepagents import create_deep_agent
        except ImportError as e:
            raise RuntimeError(
                "deepagents is not installed. Please install and configure deepagents first."
            ) from e

        system_prompt = build_system_prompt()
        user_prompt = build_full_agent_prompt(
            workspace={
                "root_dir": agent_input.workspace_root,
                "dataset_local_path": agent_input.dataset_local_path,
                "input_dir": str(Path(agent_input.workspace_root) / "input"),
                "scripts_dir": str(Path(agent_input.workspace_root) / "scripts"),
                "tables_dir": str(Path(agent_input.workspace_root) / "tables"),
                "charts_dir": str(Path(agent_input.workspace_root) / "charts"),
                "logs_dir": str(Path(agent_input.workspace_root) / "logs"),
                "outputs_dir": str(Path(agent_input.workspace_root) / "outputs"),
            },
            output_contract=dict(agent_input.output_contract or {}),
            normalized_task=agent_input.normalized_task,
            dataset_context=agent_input.dataset_context,
            analysis_brief=agent_input.analysis_brief,
            execution_mode=agent_input.execution_mode,
            revision_context=agent_input.revision_context,
        )

        workspace_skills_dir = self._sync_skills_to_workspace(agent_input=agent_input)

        backend, backend_name = self._build_backend(workspace_root=agent_input.workspace_root)
        tools = get_deep_analysis_tools()

        create_kwargs = {
            "model": self.model,
            "system_prompt": system_prompt,
            "skills": [str(workspace_skills_dir)],
            "backend": backend,
            "tools": tools,
        }

        agent = create_deep_agent(**create_kwargs)

        result = agent.invoke(
            {
                "messages": [
                    {"role": "user", "content": user_prompt},
                ]
            }
        )

        structured_output_path = agent_input.output_contract.get("structured_output_path")
        if not structured_output_path:
            raise RuntimeError("structured_output_path is missing from output_contract.")

        output = self._load_structured_output(
            structured_output_path=structured_output_path,
            execution_mode=agent_input.execution_mode,
            started_at=started_at,
            extra_trace=[
                {
                    "type": "deepagent_invoke",
                    "status": "success",
                    "backend": backend_name,
                    "result_preview": str(result)[:500],
                    "skills_root": str(workspace_skills_dir),
                }
            ],
        )
        return output

    # -------------------------------------------------------------------------
    # fallback path: plan -> tool-first -> small scripts -> repair -> finalize
    # -------------------------------------------------------------------------
    def _run_with_script_fallback(
        self,
        *,
        agent_input: DeepAnalysisAgentInput,
        started_at: float,
        fallback_reason: str,
    ) -> DeepAnalysisAgentOutput:
        workspace_root = Path(agent_input.workspace_root)
        outputs_dir = workspace_root / "outputs"
        structured_output_path = Path(
            agent_input.output_contract.get(
                "structured_output_path",
                str(outputs_dir / "structured_result.json"),
            )
        )

        trace: List[Dict[str, Any]] = [
            {"type": "fallback_triggered", "reason": fallback_reason}
        ]

        plan = self._build_execution_plan(agent_input=agent_input)
        tool_map = self._get_tool_map()

        executed_steps: List[ExecutedStepTrace] = []
        artifacts: List[Dict[str, Any]] = []
        findings: List[Dict[str, Any]] = []
        claims: List[Dict[str, Any]] = []
        caveats: List[Dict[str, Any]] = []

        for step in plan.get("steps", []):
            step_id = step["step_id"]
            step_kind = step["kind"]

            if step_kind == "tool":
                tool_result = self._execute_tool_step(step=step, tool_map=tool_map)
                trace.append(
                    {
                        "type": "tool_step",
                        "step_id": step_id,
                        "tool_name": step.get("tool_name"),
                        "result": tool_result,
                    }
                )
                executed_steps.append(
                    ExecutedStepTrace(
                        step_id=step_id,
                        step_type=f"tool:{step.get('tool_name')}",
                        description=step.get("goal", step.get("name")),
                        status="success",
                        output_refs=[],
                        code_ref=None,
                        extra={"tool_result": tool_result},
                    )
                )
                step_artifacts, step_findings, step_claims, step_caveats = self._harvest_tool_step_outputs(
                    step=step,
                    tool_result=tool_result,
                    tool_map=tool_map,
                )
                artifacts.extend(step_artifacts)
                findings.extend(step_findings)
                claims.extend(step_claims)
                caveats.extend(step_caveats)

            elif step_kind == "script":
                script_result = self._run_script_step_with_repair(
                    agent_input=agent_input,
                    step=step,
                    max_attempts=3,
                )
                trace.append(
                    {
                        "type": "script_step",
                        "step_id": step_id,
                        "result": script_result,
                    }
                )
                executed_steps.append(
                    ExecutedStepTrace(
                        step_id=step_id,
                        step_type="script",
                        description=step.get("goal", step.get("name")),
                        status="success",
                        output_refs=[
                            script_result["script_path"],
                            script_result["stdout_log"],
                            script_result["stderr_log"],
                            script_result["result_json_path"],
                        ],
                        code_ref=script_result["script_path"],
                        extra={"attempt": script_result["attempt"]},
                    )
                )
                step_payload = json.loads(
                    Path(script_result["result_json_path"]).read_text(encoding="utf-8")
                )
                artifacts.extend(step_payload.get("artifacts", []))
                findings.extend(step_payload.get("findings", []))
                claims.extend(step_payload.get("claims", []))
                caveats.extend(step_payload.get("caveats", []))

            else:
                raise RuntimeError(f"Unsupported step kind: {step_kind}")

        caveats.extend(self._build_dataset_caveats(agent_input=agent_input))

        raw_payload = self._build_structured_output_payload(
            agent_input=agent_input,
            plan=plan,
            executed_steps=executed_steps,
            artifacts=artifacts,
            findings=findings,
            claims=claims,
            caveats=caveats,
            trace=trace,
        )

        finalize_tool = tool_map["finalize_structured_output_tool"]
        finalize_tool.invoke(
            {
                "output_path": str(structured_output_path),
                "payload_json": json.dumps(raw_payload, ensure_ascii=False),
            }
        )

        output = self._load_structured_output(
            structured_output_path=str(structured_output_path),
            execution_mode=agent_input.execution_mode,
            started_at=started_at,
            extra_trace=[
                {"type": "fallback_planned", "plan_step_count": len(plan.get("steps", []))},
                {"type": "structured_output_finalized", "path": str(structured_output_path)},
            ],
        )

        output.executed_steps = executed_steps + list(output.executed_steps or [])
        output.plan = {
            **(output.plan or {}),
            "planner_notes": (output.plan or {}).get("planner_notes", "tool_first_fallback_plan"),
        }
        return output

    # -------------------------------------------------------------------------
    # execution plan
    # -------------------------------------------------------------------------
    def _build_execution_plan(self, *, agent_input: DeepAnalysisAgentInput) -> Dict[str, Any]:
        """
        采用 rule-based 的小而稳计划，减少额外模型请求和 timeout。
        """
        brief = agent_input.analysis_brief or {}
        dataset_context = agent_input.dataset_context or {}

        metrics = brief.get(
            "recommended_metrics",
            dataset_context.get("candidate_measure_columns", []),
        )[:2]

        time_col = self._pick_primary_time_col(dataset_context)
        region_col = self._pick_region_col(dataset_context, brief)
        product_col = self._pick_product_col(dataset_context, brief)

        workspace_root = Path(agent_input.workspace_root)
        tables_dir = workspace_root / "tables"
        charts_dir = workspace_root / "charts"

        steps: List[Dict[str, Any]] = []

        steps.append(
            {
                "step_id": "step_001",
                "kind": "tool",
                "name": "inspect_dataset",
                "goal": "检查数据集基础信息与预览内容",
                "tool_name": "inspect_dataset_tool",
                "tool_args": {
                    "dataset_path": agent_input.dataset_local_path,
                    "max_rows": 5,
                },
                "expected_outputs": [],
            }
        )

        steps.append(
            {
                "step_id": "step_002",
                "kind": "tool",
                "name": "profile_columns",
                "goal": "构建列级画像，辅助后续分析与 caveat 识别",
                "tool_name": "profile_columns_tool",
                "tool_args": {
                    "dataset_path": agent_input.dataset_local_path,
                    "max_sample_values": 5,
                },
                "expected_outputs": [],
            }
        )

        if metrics:
            steps.append(
                {
                    "step_id": "step_010",
                    "kind": "tool",
                    "name": "summary_kpi",
                    "goal": "生成整体指标汇总表",
                    "tool_name": "summarize_metrics_tool",
                    "tool_args": {
                        "dataset_path": agent_input.dataset_local_path,
                        "metrics": metrics,
                        "output_csv_path": str(tables_dir / "table_r0_summary_kpi.csv"),
                    },
                    "expected_outputs": ["table"],
                }
            )

        if time_col and metrics:
            steps.append(
                {
                    "step_id": "step_020",
                    "kind": "tool",
                    "name": "time_trend",
                    "goal": "生成时间趋势表和折线图",
                    "tool_name": "time_trend_tool",
                    "tool_args": {
                        "dataset_path": agent_input.dataset_local_path,
                        "time_col": time_col,
                        "metrics": metrics,
                        "output_csv_path": str(tables_dir / "table_r0_time_trend.csv"),
                        "output_chart_path": str(charts_dir / "chart_r0_time_trend.png"),
                        "grain": None,
                    },
                    "expected_outputs": ["table", "chart"],
                }
            )

        if region_col and metrics:
            steps.append(
                {
                    "step_id": "step_030",
                    "kind": "tool",
                    "name": "regional_comparison",
                    "goal": "生成区域对比表和条形图",
                    "tool_name": "group_compare_chart_tool",
                    "tool_args": {
                        "dataset_path": agent_input.dataset_local_path,
                        "group_col": region_col,
                        "metrics": metrics,
                        "output_csv_path": str(tables_dir / "table_r0_region_compare.csv"),
                        "output_chart_path": str(charts_dir / "chart_r0_region_compare.png"),
                        "top_n": 20,
                    },
                    "expected_outputs": ["table", "chart"],
                }
            )

        if product_col and metrics:
            steps.append(
                {
                    "step_id": "step_040",
                    "kind": "tool",
                    "name": "product_mix",
                    "goal": "生成产品结构表和条形图",
                    "tool_name": "group_compare_chart_tool",
                    "tool_args": {
                        "dataset_path": agent_input.dataset_local_path,
                        "group_col": product_col,
                        "metrics": metrics,
                        "output_csv_path": str(tables_dir / "table_r0_product_mix.csv"),
                        "output_chart_path": str(charts_dir / "chart_r0_product_mix.png"),
                        "top_n": 20,
                    },
                    "expected_outputs": ["table", "chart"],
                }
            )

        if region_col and product_col and metrics:
            steps.append(
                {
                    "step_id": "step_050",
                    "kind": "script",
                    "name": "region_product_cross",
                    "goal": "生成区域-产品交叉分析表，补充更细粒度的比较视角",
                    "group_cols": [region_col, product_col],
                    "metrics": metrics,
                    "output_csv_path": str(tables_dir / "table_r0_region_product_cross.csv"),
                    "expected_outputs": ["table"],
                }
            )

        return {
            "mode": agent_input.execution_mode,
            "must_cover_topics": brief.get("must_cover_topics", []),
            "steps": steps,
            "planner_notes": "rule_based_tool_first_plan",
        }

    # -------------------------------------------------------------------------
    # tool-first execution
    # -------------------------------------------------------------------------
    def _get_tool_map(self) -> Dict[str, Any]:
        tools = get_deep_analysis_tools()
        return {tool.name: tool for tool in tools}

    def _execute_tool_step(self, *, step: Dict[str, Any], tool_map: Dict[str, Any]) -> Dict[str, Any]:
        tool_name = step.get("tool_name")
        if tool_name not in tool_map:
            raise RuntimeError(f"Tool not found: {tool_name}")

        tool = tool_map[tool_name]
        raw = tool.invoke(step.get("tool_args", {}))
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except Exception:
                return {"raw_result": raw}
        if isinstance(raw, dict):
            return raw
        return {"raw_result": raw}

    def _register_artifact(
        self,
        *,
        tool_map: Dict[str, Any],
        artifact_id: str,
        artifact_type: str,
        title: str,
        path: str,
        topic_tags: List[str] | None = None,
        description: str | None = None,
        format: str | None = None,
    ) -> Dict[str, Any]:
        raw = tool_map["register_artifact_tool"].invoke(
            {
                "artifact_id": artifact_id,
                "artifact_type": artifact_type,
                "title": title,
                "path": path,
                "topic_tags": topic_tags or [],
                "description": description,
                "format": format,
            }
        )
        if isinstance(raw, str):
            return json.loads(raw)
        return raw

    def _harvest_tool_step_outputs(
        self,
        *,
        step: Dict[str, Any],
        tool_result: Dict[str, Any],
        tool_map: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        artifacts: List[Dict[str, Any]] = []
        findings: List[Dict[str, Any]] = []
        claims: List[Dict[str, Any]] = []
        caveats: List[Dict[str, Any]] = []

        step_id = step["step_id"]
        step_name = step["name"]

        if step_name == "summary_kpi":
            table_path = tool_result.get("table_path")
            if table_path:
                table_artifact = self._register_artifact(
                    tool_map=tool_map,
                    artifact_id="table_summary_kpi",
                    artifact_type="table",
                    title="整体KPI汇总表",
                    path=table_path,
                    topic_tags=["overall_performance"],
                    description="核心指标整体汇总表",
                    format="csv",
                )
                artifacts.append(table_artifact)

                try:
                    df = pd.read_csv(table_path)
                    if not df.empty:
                        row = df.iloc[0]
                        metric = str(row.get("metric", "metric"))
                        total = row.get("sum")
                        statement = f"核心指标 {metric} 的总体规模为 {total}。"

                        findings.append(
                            {
                                "finding_id": "finding_001",
                                "title": "整体指标概览",
                                "statement": statement,
                                "category": "summary",
                                "importance": "high",
                                "confidence": "high",
                                "topic_tags": ["overall_performance"],
                                "supporting_artifact_ids": ["table_summary_kpi"],
                            }
                        )
                        claims.append(
                            {
                                "claim_id": "claim_001",
                                "claim_text": statement,
                                "claim_type": "descriptive",
                                "confidence": "high",
                                "table_ids": ["table_summary_kpi"],
                                "chart_ids": [],
                                "finding_ids": ["finding_001"],
                                "stat_refs": [f"{metric}_sum"],
                                "caveat_ids": [],
                            }
                        )
                except Exception:
                    pass

        elif step_name == "time_trend":
            table_path = tool_result.get("table_path")
            chart_path = tool_result.get("chart_path")

            if table_path:
                artifacts.append(
                    self._register_artifact(
                        tool_map=tool_map,
                        artifact_id="table_time_trend",
                        artifact_type="table",
                        title="时间趋势表",
                        path=table_path,
                        topic_tags=["time_trend"],
                        description="按时间聚合的趋势表",
                        format="csv",
                    )
                )
            if chart_path:
                artifacts.append(
                    self._register_artifact(
                        tool_map=tool_map,
                        artifact_id="chart_time_trend",
                        artifact_type="chart",
                        title="时间趋势图",
                        path=chart_path,
                        topic_tags=["time_trend"],
                        description="时间趋势折线图",
                        format="png",
                    )
                )

            try:
                df = pd.read_csv(table_path)
                metrics = step.get("tool_args", {}).get("metrics", [])
                primary = metrics[0] if metrics else None
                if primary and not df.empty and primary in df.columns:
                    first_val = float(df[primary].iloc[0])
                    last_val = float(df[primary].iloc[-1])
                    trend_desc = "上升" if last_val > first_val else "下降" if last_val < first_val else "基本持平"
                    statement = f"{primary} 在观察期内整体呈{trend_desc}趋势。"

                    findings.append(
                        {
                            "finding_id": "finding_002",
                            "title": "时间趋势表现",
                            "statement": statement,
                            "category": "trend",
                            "importance": "high",
                            "confidence": "medium",
                            "topic_tags": ["time_trend"],
                            "supporting_artifact_ids": ["table_time_trend", "chart_time_trend"],
                        }
                    )
                    claims.append(
                        {
                            "claim_id": "claim_002",
                            "claim_text": statement,
                            "claim_type": "descriptive",
                            "confidence": "medium",
                            "table_ids": ["table_time_trend"],
                            "chart_ids": ["chart_time_trend"],
                            "finding_ids": ["finding_002"],
                            "stat_refs": [],
                            "caveat_ids": [],
                        }
                    )
            except Exception:
                pass

        elif step_name == "regional_comparison":
            table_path = tool_result.get("table_path")
            chart_path = tool_result.get("chart_path")

            if table_path:
                artifacts.append(
                    self._register_artifact(
                        tool_map=tool_map,
                        artifact_id="table_regional_comparison",
                        artifact_type="table",
                        title="区域对比表",
                        path=table_path,
                        topic_tags=["regional_comparison"],
                        description="按区域聚合的对比表",
                        format="csv",
                    )
                )
            if chart_path:
                artifacts.append(
                    self._register_artifact(
                        tool_map=tool_map,
                        artifact_id="chart_regional_comparison",
                        artifact_type="chart",
                        title="区域对比图",
                        path=chart_path,
                        topic_tags=["regional_comparison"],
                        description="区域对比条形图",
                        format="png",
                    )
                )

            try:
                df = pd.read_csv(table_path)
                metrics = step.get("tool_args", {}).get("metrics", [])
                group_col = step.get("tool_args", {}).get("group_col")
                primary = metrics[0] if metrics else None
                if primary and group_col and not df.empty:
                    top_row = df.iloc[0]
                    top_name = top_row[group_col]
                    statement = f"{group_col} 维度下，{top_name} 在 {primary} 上表现最高。"

                    findings.append(
                        {
                            "finding_id": "finding_003",
                            "title": "区域表现对比",
                            "statement": statement,
                            "category": "comparison",
                            "importance": "high",
                            "confidence": "medium",
                            "topic_tags": ["regional_comparison"],
                            "supporting_artifact_ids": ["table_regional_comparison", "chart_regional_comparison"],
                        }
                    )
                    claims.append(
                        {
                            "claim_id": "claim_003",
                            "claim_text": statement,
                            "claim_type": "comparative",
                            "confidence": "medium",
                            "table_ids": ["table_regional_comparison"],
                            "chart_ids": ["chart_regional_comparison"],
                            "finding_ids": ["finding_003"],
                            "stat_refs": [],
                            "caveat_ids": [],
                        }
                    )
            except Exception:
                pass

        elif step_name == "product_mix":
            table_path = tool_result.get("table_path")
            chart_path = tool_result.get("chart_path")

            if table_path:
                artifacts.append(
                    self._register_artifact(
                        tool_map=tool_map,
                        artifact_id="table_product_mix",
                        artifact_type="table",
                        title="产品结构表",
                        path=table_path,
                        topic_tags=["product_mix"],
                        description="按产品维度聚合的结构表",
                        format="csv",
                    )
                )
            if chart_path:
                artifacts.append(
                    self._register_artifact(
                        tool_map=tool_map,
                        artifact_id="chart_product_mix",
                        artifact_type="chart",
                        title="产品结构图",
                        path=chart_path,
                        topic_tags=["product_mix"],
                        description="产品结构条形图",
                        format="png",
                    )
                )

            try:
                df = pd.read_csv(table_path)
                metrics = step.get("tool_args", {}).get("metrics", [])
                group_col = step.get("tool_args", {}).get("group_col")
                primary = metrics[0] if metrics else None
                if primary and group_col and not df.empty:
                    top_row = df.iloc[0]
                    top_name = top_row[group_col]
                    statement = f"{group_col} 维度下，{top_name} 在 {primary} 上贡献最高。"

                    findings.append(
                        {
                            "finding_id": "finding_004",
                            "title": "产品结构表现",
                            "statement": statement,
                            "category": "composition",
                            "importance": "medium",
                            "confidence": "medium",
                            "topic_tags": ["product_mix"],
                            "supporting_artifact_ids": ["table_product_mix", "chart_product_mix"],
                        }
                    )
                    claims.append(
                        {
                            "claim_id": "claim_004",
                            "claim_text": statement,
                            "claim_type": "comparative",
                            "confidence": "medium",
                            "table_ids": ["table_product_mix"],
                            "chart_ids": ["chart_product_mix"],
                            "finding_ids": ["finding_004"],
                            "stat_refs": [],
                            "caveat_ids": [],
                        }
                    )
            except Exception:
                pass

        return artifacts, findings, claims, caveats

    # -------------------------------------------------------------------------
    # small script generation + in-round repair
    # -------------------------------------------------------------------------
    def _run_script_step_with_repair(
        self,
        *,
        agent_input: DeepAnalysisAgentInput,
        step: Dict[str, Any],
        max_attempts: int = 3,
    ) -> Dict[str, Any]:
        workspace_root = Path(agent_input.workspace_root)
        scripts_dir = workspace_root / "scripts"
        logs_dir = workspace_root / "logs"
        outputs_dir = workspace_root / "outputs"

        previous_code = ""
        previous_error = ""

        result_json_path = outputs_dir / f"{step['step_id']}_result.json"

        for attempt in range(1, max_attempts + 1):
            suffix = f"{step['step_id']}_attempt_{attempt}"
            script_path = scripts_dir / f"{suffix}.py"
            stdout_log = logs_dir / f"{suffix}_stdout.log"
            stderr_log = logs_dir / f"{suffix}_stderr.log"

            if attempt == 1:
                code = self._generate_step_script(
                    agent_input=agent_input,
                    step={**step, "result_json_path": str(result_json_path)},
                )
            else:
                code = self._repair_step_script(
                    agent_input=agent_input,
                    step={**step, "result_json_path": str(result_json_path)},
                    previous_code=previous_code,
                    previous_error=previous_error,
                )

            script_path.write_text(code, encoding="utf-8")

            run_result = self._execute_python_script(
                script_path=script_path,
                stdout_log=stdout_log,
                stderr_log=stderr_log,
            )

            if run_result["returncode"] == 0:
                result_synthesized = False
                if not result_json_path.exists():
                    result_synthesized = self._synthesize_script_step_result(
                        step=step,
                        result_json_path=result_json_path,
                    )
                if not result_json_path.exists():
                    raise RuntimeError(
                        f"Script step succeeded but result_json not found: {result_json_path}"
                    )
                return {
                    "status": "success",
                    "attempt": attempt,
                    "script_path": str(script_path),
                    "stdout_log": str(stdout_log),
                    "stderr_log": str(stderr_log),
                    "result_json_path": str(result_json_path),
                    "result_synthesized": result_synthesized,
                }

            previous_code = code
            previous_error = stderr_log.read_text(encoding="utf-8", errors="ignore")

        raise RuntimeError(
            f"Script step failed after {max_attempts} attempts. "
            f"Last stderr: {stderr_log}"
        )

    def _infer_topic_tags_for_step(self, *, step: Dict[str, Any]) -> List[str]:
        text_parts = [
            str(step.get("name", "")),
            str(step.get("goal", "")),
            str(step.get("tool_name", "")),
        ]
        text_parts.extend(str(value) for value in step.get("group_cols", []) or [])
        text = " ".join(text_parts).lower()

        tags: List[str] = []
        if any(token in text for token in ["time", "date", "trend", "日期", "时间", "趋势"]):
            tags.append("time_trend")
        if any(token in text for token in ["region", "area", "区域", "地区"]):
            tags.append("regional_comparison")
        if any(token in text for token in ["product", "category", "sku", "产品", "品类"]):
            tags.append("product_mix")
        if not tags and any(
            token in text for token in ["summary", "kpi", "overall", "汇总", "概览", "整体"]
        ):
            tags.append("overall_performance")

        return list(dict.fromkeys(tags))

    def _build_synthesized_script_step_payload(self, *, step: Dict[str, Any]) -> Dict[str, Any] | None:
        artifacts: List[Dict[str, Any]] = []
        topic_tags = self._infer_topic_tags_for_step(step=step)
        description = step.get("goal") or step.get("name") or step.get("step_id") or "script_output"

        output_csv_path = step.get("output_csv_path")
        if output_csv_path and Path(output_csv_path).exists():
            table_path = Path(output_csv_path)
            artifacts.append(
                {
                    "artifact_id": table_path.stem,
                    "artifact_type": "table",
                    "title": f"{description}结果表",
                    "path": str(table_path),
                    "format": table_path.suffix.lstrip(".").lower() or "csv",
                    "topic_tags": topic_tags,
                    "description": description,
                }
            )

        output_chart_path = step.get("output_chart_path")
        if output_chart_path and Path(output_chart_path).exists():
            chart_path = Path(output_chart_path)
            artifacts.append(
                {
                    "artifact_id": chart_path.stem,
                    "artifact_type": "chart",
                    "title": f"{description}图",
                    "path": str(chart_path),
                    "format": chart_path.suffix.lstrip(".").lower() or "png",
                    "topic_tags": topic_tags,
                    "description": description,
                }
            )

        if not artifacts:
            return None

        return {
            "artifacts": artifacts,
            "findings": [],
            "claims": [],
            "caveats": [],
        }

    def _synthesize_script_step_result(
        self,
        *,
        step: Dict[str, Any],
        result_json_path: Path,
    ) -> bool:
        payload = self._build_synthesized_script_step_payload(step=step)
        if payload is None:
            return False

        result_json_path.parent.mkdir(parents=True, exist_ok=True)
        result_json_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return True

    def _generate_step_script(
        self,
        *,
        agent_input: DeepAnalysisAgentInput,
        step: Dict[str, Any],
    ) -> str:
        workspace = {
            "root_dir": agent_input.workspace_root,
            "input_dir": str(Path(agent_input.workspace_root) / "input"),
            "scripts_dir": str(Path(agent_input.workspace_root) / "scripts"),
            "tables_dir": str(Path(agent_input.workspace_root) / "tables"),
            "charts_dir": str(Path(agent_input.workspace_root) / "charts"),
            "logs_dir": str(Path(agent_input.workspace_root) / "logs"),
            "outputs_dir": str(Path(agent_input.workspace_root) / "outputs"),
        }

        prompt = build_step_script_prompt(
            step=step,
            dataset_local_path=agent_input.dataset_local_path,
            workspace=workspace,
        )
        return self._generate_python_script(prompt=prompt)

    def _repair_step_script(
        self,
        *,
        agent_input: DeepAnalysisAgentInput,
        step: Dict[str, Any],
        previous_code: str,
        previous_error: str,
    ) -> str:
        workspace = {
            "root_dir": agent_input.workspace_root,
            "input_dir": str(Path(agent_input.workspace_root) / "input"),
            "scripts_dir": str(Path(agent_input.workspace_root) / "scripts"),
            "tables_dir": str(Path(agent_input.workspace_root) / "tables"),
            "charts_dir": str(Path(agent_input.workspace_root) / "charts"),
            "logs_dir": str(Path(agent_input.workspace_root) / "logs"),
            "outputs_dir": str(Path(agent_input.workspace_root) / "outputs"),
        }

        prompt = build_step_repair_prompt(
            step=step,
            previous_code=previous_code,
            previous_error=previous_error,
            dataset_local_path=agent_input.dataset_local_path,
            workspace=workspace,
        )
        return self._generate_python_script(prompt=prompt)

    # -------------------------------------------------------------------------
    # model output text helpers
    # -------------------------------------------------------------------------
    def _generate_python_script(self, *, prompt: str) -> str:
        msg = self.model.invoke(prompt)
        text = self._extract_text(msg)

        text = text.strip()
        if text.startswith("```"):
            lines = text.splitlines()
            if lines and lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            text = "\n".join(lines).strip()

        return text

    def _extract_text(self, msg: Any) -> str:
        content = getattr(msg, "content", msg)
        if isinstance(content, list):
            parts: List[str] = []
            for block in content:
                if isinstance(block, dict):
                    parts.append(block.get("text", ""))
                else:
                    parts.append(str(block))
            return "\n".join(parts).strip()
        return str(content).strip()

    # -------------------------------------------------------------------------
    # process execution
    # -------------------------------------------------------------------------
    def _execute_python_script(
        self,
        *,
        script_path: Path,
        stdout_log: Path,
        stderr_log: Path,
    ) -> Dict[str, Any]:
        stdout_log.parent.mkdir(parents=True, exist_ok=True)
        stderr_log.parent.mkdir(parents=True, exist_ok=True)

        with stdout_log.open("w", encoding="utf-8") as out_f, stderr_log.open(
            "w", encoding="utf-8"
        ) as err_f:
            result = subprocess.run(
                ["python", str(script_path)],
                stdout=out_f,
                stderr=err_f,
                text=True,
                cwd=str(script_path.parent.parent),
                check=False,
            )

        return {"returncode": result.returncode}

    # -------------------------------------------------------------------------
    # payload assembly
    # -------------------------------------------------------------------------
    def _build_dataset_caveats(self, *, agent_input: DeepAnalysisAgentInput) -> List[Dict[str, Any]]:
        caveats: List[Dict[str, Any]] = []
        dataset_context = agent_input.dataset_context or {}
        tables = dataset_context.get("tables", []) or []

        row_count = 0
        if tables and isinstance(tables[0], dict):
            row_count = int(tables[0].get("row_count", 0) or 0)

        if 0 < row_count < 20:
            caveats.append(
                {
                    "caveat_id": "caveat_small_sample",
                    "message": "当前样本量较小，分析结论应以描述性解读为主。",
                    "severity": "medium",
                    "related_claim_ids": [],
                }
            )

        missingness = (
            dataset_context.get("data_quality_summary", {}).get("missingness", []) or []
        )
        if missingness:
            caveats.append(
                {
                    "caveat_id": "caveat_missingness",
                    "message": "数据集中存在缺失值，部分汇总结果可能受影响。",
                    "severity": "medium",
                    "related_claim_ids": [],
                }
            )

        return caveats

    def _build_structured_output_payload(
        self,
        *,
        agent_input: DeepAnalysisAgentInput,
        plan: Dict[str, Any],
        executed_steps: List[ExecutedStepTrace],
        artifacts: List[Dict[str, Any]],
        findings: List[Dict[str, Any]],
        claims: List[Dict[str, Any]],
        caveats: List[Dict[str, Any]],
        trace: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        return {
            "plan": {
                "mode": agent_input.execution_mode,
                "must_cover_topics": plan.get("must_cover_topics", []),
                "planner_notes": plan.get("planner_notes", "tool_first_fallback_plan"),
            },
            "planned_actions": [
                {
                    "action": step.get("name"),
                    "metrics": step.get("metrics", []) or step.get("tool_args", {}).get("metrics", []) or [],
                    "group_col": step.get("tool_args", {}).get("group_col"),
                    "time_col": step.get("tool_args", {}).get("time_col"),
                }
                for step in plan.get("steps", [])
            ],
            "executed_steps": [s.model_dump() for s in executed_steps],
            "artifacts": artifacts,
            "findings": findings,
            "claims": claims,
            "caveats": caveats,
            "rejected_charts": [],
            "rejected_hypotheses": [],
            "trace": trace,
            "run_metadata": {
                "agent_type": "fallback_tool_first",
                "execution_mode": agent_input.execution_mode,
                "dataset_path": agent_input.dataset_local_path,
                "workspace_root": agent_input.workspace_root,
                "completion_status": "completed",
            },
        }

    # -------------------------------------------------------------------------
    # structured output loading + normalization
    # -------------------------------------------------------------------------
    def _load_structured_output(
        self,
        *,
        structured_output_path: str,
        execution_mode: str,
        started_at: float,
        extra_trace: List[Dict[str, Any]] | None = None,
    ) -> DeepAnalysisAgentOutput:
        path = Path(structured_output_path)
        if not path.exists():
            raise RuntimeError(f"structured_result.json not found: {structured_output_path}")

        raw = json.loads(path.read_text(encoding="utf-8"))
        normalized = self._normalize_structured_output_payload(raw)
        output = DeepAnalysisAgentOutput.model_validate(normalized)

        finished_at = time.time()
        elapsed = round(finished_at - started_at, 4)

        trace = list(output.trace or [])
        if extra_trace:
            trace.extend(extra_trace)

        run_metadata = dict(output.run_metadata or {})
        run_metadata.setdefault("agent_type", "deepagent")
        run_metadata.setdefault("execution_mode", execution_mode)
        run_metadata["structured_output_path"] = structured_output_path
        run_metadata["elapsed_seconds"] = elapsed
        run_metadata["loaded_from_json"] = True
        run_metadata["fallback_enabled"] = settings.deepagent_enable_fallback
        run_metadata["configured_backend"] = settings.deepagent_backend

        if settings.deepagent_verbose:
            run_metadata["verbose"] = True

        return output.model_copy(
            update={
                "trace": trace,
                "run_metadata": run_metadata,
            }
        )

    def _normalize_structured_output_payload(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(raw or {})

        # 1) planned_actions: list[str] -> list[dict]
        planned_actions = normalized.get("planned_actions", [])
        if isinstance(planned_actions, list):
            converted_actions = []
            for idx, item in enumerate(planned_actions, start=1):
                if isinstance(item, str):
                    converted_actions.append(
                        {
                            "action": item,
                            "metrics": [],
                            "group_col": None,
                            "time_col": None,
                        }
                    )
                elif isinstance(item, dict):
                    converted_actions.append(
                        {
                            "action": item.get("action") or item.get("name") or f"action_{idx:03d}",
                            "metrics": item.get("metrics", []) or [],
                            "group_col": item.get("group_col"),
                            "time_col": item.get("time_col"),
                        }
                    )
            normalized["planned_actions"] = converted_actions
        else:
            normalized["planned_actions"] = []

        # 2) executed_steps: 宽松字段映射
        executed_steps = normalized.get("executed_steps", [])
        if isinstance(executed_steps, list):
            converted_steps = []
            for idx, item in enumerate(executed_steps, start=1):
                if not isinstance(item, dict):
                    continue

                raw_status = str(item.get("status", "success")).lower()
                if raw_status == "completed":
                    raw_status = "success"
                if raw_status not in {"success", "failed", "skipped"}:
                    raw_status = "success"

                step_name = item.get("step") or item.get("step_type") or item.get("name") or f"step_{idx:03d}"
                desc = (
                    item.get("description")
                    or item.get("details")
                    or item.get("result")
                    or item.get("summary")
                    or step_name
                )

                converted_steps.append(
                    {
                        "step_id": str(item.get("step_id") or f"step_{idx:03d}"),
                        "step_type": str(item.get("step_type") or step_name),
                        "description": str(desc),
                        "status": raw_status,
                        "output_refs": [str(ref) for ref in (item.get("output_refs", []) or [])],
                        "code_ref": None if item.get("code_ref") is None else str(item.get("code_ref")),
                    }
                )
            normalized["executed_steps"] = converted_steps
        else:
            normalized["executed_steps"] = []

        # 3) findings
        findings = normalized.get("findings", [])
        if isinstance(findings, list):
            converted_findings = []
            for idx, item in enumerate(findings, start=1):
                if isinstance(item, str):
                    converted_findings.append(
                        {
                            "finding_id": f"finding_{idx:03d}",
                            "title": f"Finding {idx}",
                            "statement": item,
                            "category": "summary",
                            "importance": "medium",
                            "confidence": "medium",
                            "topic_tags": [],
                            "supporting_artifact_ids": [],
                        }
                    )
                    continue

                if not isinstance(item, dict):
                    continue

                topic = item.get("topic")
                statement = (
                    item.get("statement")
                    or item.get("description")
                    or item.get("finding")
                    or item.get("text")
                    or ""
                )
                category = item.get("category")
                if not category:
                    if topic == "time_trend":
                        category = "trend"
                    elif topic == "regional_comparison":
                        category = "comparison"
                    elif topic == "product_mix":
                        category = "composition"
                    else:
                        category = "summary"

                converted_findings.append(
                    {
                        "finding_id": item.get("finding_id") or f"finding_{idx:03d}",
                        "title": item.get("title") or topic or f"Finding {idx}",
                        "statement": statement,
                        "category": category,
                        "importance": item.get("importance", "medium"),
                        "confidence": item.get("confidence", "medium"),
                        "topic_tags": item.get("topic_tags") or ([topic] if topic else []),
                        "supporting_artifact_ids": item.get("supporting_artifact_ids")
                        or item.get("evidence")
                        or [],
                    }
                )
            normalized["findings"] = converted_findings
        else:
            normalized["findings"] = []

        # 4) claims
        claims = normalized.get("claims", [])
        if isinstance(claims, list):
            converted_claims = []
            for idx, item in enumerate(claims, start=1):
                if isinstance(item, str):
                    converted_claims.append(
                        {
                            "claim_id": f"claim_{idx:03d}",
                            "claim_text": item,
                            "claim_type": "descriptive",
                            "confidence": "medium",
                            "table_ids": [],
                            "chart_ids": [],
                            "finding_ids": [],
                            "stat_refs": [],
                            "caveat_ids": [],
                        }
                    )
                    continue

                if not isinstance(item, dict):
                    continue

                supporting_artifacts = item.get("supporting_artifacts", []) or []
                table_ids = item.get("table_ids", []) or [a for a in supporting_artifacts if str(a).startswith("table_")]
                chart_ids = item.get("chart_ids", []) or [a for a in supporting_artifacts if str(a).startswith("chart_")]

                converted_claims.append(
                    {
                        "claim_id": item.get("claim_id") or f"claim_{idx:03d}",
                        "claim_text": item.get("claim_text") or item.get("claim") or item.get("text") or "",
                        "claim_type": item.get("claim_type", "descriptive"),
                        "confidence": item.get("confidence", "medium"),
                        "table_ids": table_ids,
                        "chart_ids": chart_ids,
                        "finding_ids": item.get("finding_ids")
                        or item.get("supporting_findings")
                        or [],
                        "stat_refs": item.get("stat_refs", []) or [],
                        "caveat_ids": item.get("caveat_ids", []) or [],
                    }
                )
            normalized["claims"] = converted_claims
        else:
            normalized["claims"] = []

        # 5) caveats
        caveats = normalized.get("caveats", [])
        if isinstance(caveats, list):
            converted_caveats = []
            for idx, item in enumerate(caveats, start=1):
                if isinstance(item, str):
                    converted_caveats.append(
                        {
                            "caveat_id": f"caveat_{idx:03d}",
                            "message": item,
                            "severity": "medium",
                            "related_claim_ids": [],
                        }
                    )
                elif isinstance(item, dict):
                    converted_caveats.append(
                        {
                            "caveat_id": item.get("caveat_id") or f"caveat_{idx:03d}",
                            "message": item.get("message") or item.get("description") or item.get("text") or "",
                            "severity": item.get("severity", "medium"),
                            "related_claim_ids": item.get("related_claim_ids", []) or [],
                        }
                    )
            normalized["caveats"] = converted_caveats
        else:
            normalized["caveats"] = []

        # 6) trace: dict -> [dict]
        trace = normalized.get("trace", [])
        if isinstance(trace, dict):
            normalized["trace"] = [trace]
        elif not isinstance(trace, list):
            normalized["trace"] = []

        # 7) artifacts: 宽松字段映射
        artifacts = normalized.get("artifacts", [])
        if isinstance(artifacts, list):
            converted_artifacts = []
            for idx, item in enumerate(artifacts, start=1):
                if not isinstance(item, dict):
                    continue

                path = str(item.get("path") or item.get("file_path") or "").strip()
                artifact_type = str(
                    item.get("artifact_type")
                    or item.get("type")
                    or ""
                ).strip().lower()

                path_obj = Path(path) if path else None
                if not artifact_type and path_obj is not None:
                    suffix = path_obj.suffix.lower()
                    if suffix in {".csv", ".xlsx", ".xlsm", ".xltx", ".xltm", ".json", ".md"}:
                        artifact_type = "table" if suffix in {".csv", ".xlsx", ".xlsm", ".xltx", ".xltm"} else "json"
                    elif suffix in {".png", ".jpg", ".jpeg", ".svg", ".webp"}:
                        artifact_type = "chart"

                if artifact_type not in {"table", "chart", "script", "log", "json"}:
                    artifact_type = "json" if path_obj and path_obj.suffix.lower() == ".json" else "table"

                stem = path_obj.stem if path_obj and path_obj.stem else f"artifact_{idx:03d}"
                artifact_id = str(
                    item.get("artifact_id")
                    or item.get("id")
                    or stem
                ).strip()
                if not artifact_id:
                    artifact_id = f"artifact_{idx:03d}"

                title = str(
                    item.get("title")
                    or item.get("name")
                    or _slug_to_title(stem)
                ).strip()
                if not title:
                    title = _slug_to_title(artifact_id)

                topic = item.get("topic")
                topic_tags = item.get("topic_tags")
                if isinstance(topic_tags, list):
                    normalized_topic_tags = [str(tag).strip() for tag in topic_tags if str(tag).strip()]
                elif topic:
                    normalized_topic_tags = [str(topic).strip()]
                else:
                    normalized_topic_tags = []

                converted_artifacts.append(
                    {
                        "artifact_id": artifact_id,
                        "artifact_type": artifact_type,
                        "title": title,
                        "path": path,
                        "format": item.get("format") or (path_obj.suffix.lstrip(".").lower() if path_obj else None),
                        "topic_tags": normalized_topic_tags,
                        "description": item.get("description"),
                    }
                )
            normalized["artifacts"] = converted_artifacts
        else:
            normalized["artifacts"] = []

        # 8) rejected_charts / rejected_hypotheses: list[str] -> list[dict]
        rejected_charts = normalized.get("rejected_charts", [])
        if isinstance(rejected_charts, list):
            converted_rejected_charts = []
            for idx, item in enumerate(rejected_charts, start=1):
                if isinstance(item, str):
                    converted_rejected_charts.append(
                        {
                            "chart_id": f"rejected_chart_{idx:03d}",
                            "reason": item,
                        }
                    )
                elif isinstance(item, dict):
                    converted_rejected_charts.append(item)
            normalized["rejected_charts"] = converted_rejected_charts
        else:
            normalized["rejected_charts"] = []

        rejected_hypotheses = normalized.get("rejected_hypotheses", [])
        if isinstance(rejected_hypotheses, list):
            converted_rejected_hypotheses = []
            for idx, item in enumerate(rejected_hypotheses, start=1):
                if isinstance(item, str):
                    converted_rejected_hypotheses.append(
                        {
                            "hypothesis_id": f"rejected_hypothesis_{idx:03d}",
                            "reason": item,
                        }
                    )
                elif isinstance(item, dict):
                    converted_rejected_hypotheses.append(item)
            normalized["rejected_hypotheses"] = converted_rejected_hypotheses
        else:
            normalized["rejected_hypotheses"] = []

        normalized.setdefault("plan", {})
        normalized.setdefault("artifacts", [])
        normalized.setdefault("run_metadata", {})

        return normalized

    # -------------------------------------------------------------------------
    # small field selectors
    # -------------------------------------------------------------------------
    def _pick_primary_time_col(self, dataset_context: Dict[str, Any]) -> str | None:
        cols = dataset_context.get("candidate_time_columns", []) or []
        return cols[0] if cols else None

    def _pick_region_col(self, dataset_context: Dict[str, Any], brief: Dict[str, Any]) -> str | None:
        dims = brief.get("recommended_dimensions", []) or dataset_context.get("candidate_dimension_columns", []) or []
        ids = set(dataset_context.get("candidate_id_columns", []) or [])
        for d in dims:
            if d in ids:
                continue
            low = str(d).lower()
            if "region" in low or "地区" in str(d) or "区域" in str(d):
                return d
        return None

    def _pick_product_col(self, dataset_context: Dict[str, Any], brief: Dict[str, Any]) -> str | None:
        dims = brief.get("recommended_dimensions", []) or dataset_context.get("candidate_dimension_columns", []) or []
        ids = set(dataset_context.get("candidate_id_columns", []) or [])
        for d in dims:
            if d in ids:
                continue
            low = str(d).lower()
            if "product" in low or "category" in low or "品类" in str(d) or "产品" in str(d):
                return d
        return None
