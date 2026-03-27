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
from app.nodes.validate_evidence import validate_evidence_node


class FakeDeepAgentService:
    def run_analysis(self, *, agent_input):
        workspace_root = Path(agent_input.workspace_root)
        table_path = workspace_root / "tables" / "table_r0_summary.csv"
        chart_path = workspace_root / "charts" / "chart_r0_sales.png"

        table_path.write_text("metric,sum\nsales,550\n", encoding="utf-8")
        chart_path.write_bytes(b"fake-png")

        return DeepAnalysisAgentOutput(
            plan={"planner_notes": "fake_plan"},
            planned_actions=[PlannedAction(action="summary_kpi", metrics=["sales"])],
            executed_steps=[
                ExecutedStepTrace(
                    step_id="step_001",
                    step_type="summary_kpi",
                    description="Built summary table",
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
                    statement="sales 表现稳定。",
                    category="summary",
                    importance="high",
                    confidence="medium",
                    topic_tags=["overall_performance", "time_trend"],
                    supporting_artifact_ids=["table_r0_summary", "chart_r0_sales"],
                )
            ],
            claims=[
                ClaimDraft(
                    claim_id="claim_001",
                    claim_text="sales 表现稳定。",
                    claim_type="descriptive",
                    confidence="medium",
                    table_ids=["table_r0_summary"],
                    chart_ids=["chart_r0_sales"],
                    finding_ids=["finding_001"],
                )
            ],
            caveats=[],
            trace=[],
            run_metadata={},
        )


def test_validate_after_deep_analysis(monkeypatch, base_state: dict, tmp_path: Path):
    import app.nodes.deep_analysis as target_module

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

    state = deep_analysis_node(base_state)
    state = validate_evidence_node(state)

    assert state["status"] == "EVIDENCE_VALIDATED"
    assert state["validation_result"]["valid"] is True
