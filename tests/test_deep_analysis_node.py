from __future__ import annotations

from pathlib import Path

from app.agents.deep_analysis.models import (
    ArtifactRef,
    ClaimDraft,
    DeepAnalysisAgentOutput,
    ExecutedStepTrace,
    FindingDraft,
    PlannedAction,
)
from app.nodes.deep_analysis import deep_analysis_node


class FakeDeepAgentService:
    def run_analysis(self, *, agent_input):
        workspace_root = Path(agent_input.workspace_root)
        tables_dir = workspace_root / "tables"
        charts_dir = workspace_root / "charts"

        table_path = tables_dir / "table_r0_summary.csv"
        chart_path = charts_dir / "chart_r0_sales.png"

        table_path.write_text("metric,sum\nsales,550\nprofit,130\n", encoding="utf-8")
        chart_path.write_bytes(b"fake-png")

        return DeepAnalysisAgentOutput(
            plan={
                "mode": "normal",
                "must_cover_topics": agent_input.analysis_brief.get("must_cover_topics", []),
                "planner_notes": "fake_agent_plan",
            },
            planned_actions=[
                PlannedAction(action="summary_kpi", metrics=["sales", "profit"]),
                PlannedAction(action="time_trend", metrics=["sales"], time_col="date"),
            ],
            executed_steps=[
                ExecutedStepTrace(
                    step_id="step_001",
                    step_type="write_table",
                    description="Wrote summary table",
                    status="success",
                    output_refs=["table_r0_summary"],
                )
            ],
            artifacts=[
                ArtifactRef(
                    artifact_id="table_r0_summary",
                    artifact_type="table",
                    title="Summary Table",
                    path=str(table_path),
                    format="csv",
                    topic_tags=["overall_performance"],
                ),
                ArtifactRef(
                    artifact_id="chart_r0_sales",
                    artifact_type="chart",
                    title="Sales Trend",
                    path=str(chart_path),
                    format="png",
                    topic_tags=["time_trend"],
                ),
            ],
            findings=[
                FindingDraft(
                    finding_id="finding_001",
                    title="整体表现",
                    statement="核心指标 sales 表现稳定。",
                    category="summary",
                    importance="high",
                    confidence="medium",
                    topic_tags=["overall_performance"],
                    supporting_artifact_ids=["table_r0_summary"],
                )
            ],
            claims=[
                ClaimDraft(
                    claim_id="claim_001",
                    claim_text="sales 总体表现稳定。",
                    claim_type="descriptive",
                    confidence="medium",
                    table_ids=["table_r0_summary"],
                    chart_ids=["chart_r0_sales"],
                    finding_ids=["finding_001"],
                )
            ],
            caveats=[],
            trace=[{"type": "fake_run"}],
            run_metadata={"agent_type": "fake_deepagent"},
        )


def test_deep_analysis_node_success(monkeypatch, base_state: dict, tmp_path: Path):
    import app.nodes.deep_analysis as target_module
    import app.services.analysis_workspace as workspace_module

    def fake_ensure_workspace_from_state(*, state, base_dir=None):
        root = tmp_path / "deepagent_runs" / "req_test_001" / "round_0"
        for name in ["input", "scripts", "tables", "charts", "logs", "outputs"]:
            (root / name).mkdir(parents=True, exist_ok=True)

        dataset_local = root / "input" / Path(state["dataset_path"]).name
        dataset_local.write_text(Path(state["dataset_path"]).read_text(encoding="utf-8"), encoding="utf-8")

        return {
            "root_dir": str(root),
            "input_dir": str(root / "input"),
            "scripts_dir": str(root / "scripts"),
            "tables_dir": str(root / "tables"),
            "charts_dir": str(root / "charts"),
            "logs_dir": str(root / "logs"),
            "outputs_dir": str(root / "outputs"),
            "dataset_local_path": str(dataset_local),
        }

    monkeypatch.setattr(target_module, "DeepAgentService", FakeDeepAgentService)
    monkeypatch.setattr(target_module, "ensure_workspace_from_state", fake_ensure_workspace_from_state)

    result = deep_analysis_node(base_state)

    assert result["status"] == "ANALYSIS_DONE"
    assert "evidence_pack" in result
    assert "evidence_pack_history" in result
    assert "analysis_workspace" in result
    assert "agent_plan" in result
    assert "agent_trace" in result
    assert "agent_run_metadata" in result

    ep = result["evidence_pack"]
    assert ep["evidence_pack_id"] == "ep_001"
    assert len(ep["tables"]) == 1
    assert len(ep["charts"]) == 1
    assert len(ep["findings"]) == 1
    assert len(ep["claim_evidence_map"]) == 1
