from __future__ import annotations

from langgraph.graph import END, START, StateGraph

from app.nodes.build_analysis_brief import build_analysis_brief_node
from app.nodes.build_dataset_context import build_dataset_context_node
from app.nodes.deep_analysis import deep_analysis_node
from app.nodes.final_qa import final_qa_node
from app.nodes.normalize_task import normalize_task_node
from app.nodes.prepare_degraded_output import prepare_degraded_output_node
from app.nodes.prepare_revision import prepare_revision_node
from app.nodes.publish import publish_node
from app.nodes.review_evidence import review_evidence_node
from app.nodes.validate_evidence import validate_evidence_node
from app.nodes.write_report import write_report_node
from app.schemas.state import AnalysisGraphState


def route_after_review(state: AnalysisGraphState) -> str:
    review = state.get("review_result", {}) or {}
    approved = review.get("approved", False)

    if approved:
        return "approved"

    current_round = state.get("revision_round", 0)
    max_rounds = state.get("max_review_rounds", 2)

    if current_round < max_rounds:
        return "revise"

    return "degrade"


def build_graph():
    graph = StateGraph(AnalysisGraphState)

    graph.add_node("normalize_task", normalize_task_node)
    graph.add_node("build_dataset_context", build_dataset_context_node)
    graph.add_node("build_analysis_brief", build_analysis_brief_node)
    graph.add_node("deep_analysis", deep_analysis_node)
    graph.add_node("write_report", write_report_node)
    # graph.add_node("validate_evidence", validate_evidence_node)
    # graph.add_node("review_evidence", review_evidence_node)
    # graph.add_node("prepare_revision", prepare_revision_node)
    # graph.add_node("prepare_degraded_output", prepare_degraded_output_node)
    # graph.add_node("final_qa", final_qa_node)
    # graph.add_node("publish", publish_node)

    graph.add_edge(START, "normalize_task")
    graph.add_edge("normalize_task", "build_dataset_context")
    graph.add_edge("build_dataset_context", "build_analysis_brief")
    graph.add_edge("build_analysis_brief", "deep_analysis")
    graph.add_edge("deep_analysis", "write_report")
    graph.add_edge("write_report", END)

    # graph.add_edge("deep_analysis", "validate_evidence")
    # graph.add_edge("validate_evidence", "review_evidence")
    #
    # graph.add_conditional_edges(
    #     "review_evidence",
    #     route_after_review,
    #     {
    #         "approved": "write_report",
    #         "revise": "prepare_revision",
    #         "degrade": "prepare_degraded_output",
    #     },
    # )
    #
    # graph.add_edge("prepare_revision", "deep_analysis")
    # graph.add_edge("prepare_degraded_output", "write_report")
    # graph.add_edge("write_report", "final_qa")
    # graph.add_edge("final_qa", "publish")
    # graph.add_edge("publish", END)

    return graph.compile()
