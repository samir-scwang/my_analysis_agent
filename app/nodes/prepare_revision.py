from __future__ import annotations

from typing import cast

from app.schemas.state import AnalysisGraphState


def prepare_revision_node(state: AnalysisGraphState) -> AnalysisGraphState:
    review = state.get("review_result", {})
    current_round = state.get("revision_round", 0)

    must_fix = review.get("must_fix", []) or []
    should_fix = review.get("should_fix", []) or []
    nice_to_have = review.get("nice_to_have", []) or []
    revision_tasks = review.get("revision_tasks", []) or []

    next_round = current_round + 1

    revision_context = {
        "round": next_round,
        "mode": "targeted_patch",
        "must_fix": must_fix,
        "should_fix": should_fix,
        "nice_to_have": nice_to_have,
        "revision_tasks": revision_tasks,
        "source_review_id": review.get("review_id"),
    }

    return cast(
        AnalysisGraphState,
        {
            **state,
            "revision_round": next_round,
            "revision_tasks": revision_tasks,
            "revision_context": revision_context,
            "execution_mode": "revision",
            "status": "REVISION_REQUIRED",
        },
    )