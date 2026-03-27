from __future__ import annotations

import json
from pathlib import Path

from app.agents.deep_analysis.evidence_builder import build_evidence_pack_from_agent_output
from app.agents.deep_analysis.models import (
    ArtifactRef,
    CaveatDraft,
    ClaimDraft,
    DeepAnalysisAgentOutput,
    ExecutedStepTrace,
    FindingDraft,
    PlannedAction,
)


def test_build_evidence_pack_from_agent_output(base_state: dict, tmp_path: Path):
    tables_dir = tmp_path / "tables"
    charts_dir = tmp_path / "charts"
    tables_dir.mkdir(parents=True, exist_ok=True)
    charts_dir.mkdir(parents=True, exist_ok=True)

    table_path = tables_dir / "table_r0_summary.csv"
    chart_path = charts_dir / "chart_r0_sales.png"

    table_path.write_text("metric,sum\nsales,550\n", encoding="utf-8")
    chart_path.write_bytes(b"fake-png")

    agent_output = DeepAnalysisAgentOutput(
        plan={"planner_notes": "test_plan"},
        planned_actions=[
            PlannedAction(action="summary_kpi", metrics=["sales", "profit"]),
        ],
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
                statement="sales 总体表现稳定。",
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
                claim_text="sales 总体规模较高。",
                claim_type="descriptive",
                confidence="medium",
                table_ids=["table_r0_summary"],
                chart_ids=["chart_r0_sales"],
                finding_ids=["finding_001"],
            )
        ],
        caveats=[
            CaveatDraft(
                caveat_id="caveat_001",
                message="样本量较小，结论偏描述性。",
                severity="medium",
                related_claim_ids=["claim_001"],
            )
        ],
        trace=[{"type": "unit_test"}],
        run_metadata={"agent_type": "deepagent"},
    )

    state = {
        **base_state,
        "dataset_path": base_state["dataset_path"],
        "execution_mode": "normal",
        "revision_context": {},
        "evidence_pack_history": [],
    }

    evidence_pack = build_evidence_pack_from_agent_output(
        state=state,
        agent_output=agent_output,
        revision_round=0,
    )

    assert evidence_pack.evidence_pack_id == "ep_001"
    assert len(evidence_pack.tables) == 1
    assert len(evidence_pack.charts) == 1
    assert len(evidence_pack.findings) == 1
    assert len(evidence_pack.claim_evidence_map) == 1
    assert len(evidence_pack.caveats) == 1
    assert evidence_pack.artifact_manifest.table_paths == [str(table_path)]
    assert evidence_pack.artifact_manifest.chart_paths == [str(chart_path)]