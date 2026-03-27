from __future__ import annotations

from typing import cast

from app.schemas.state import AnalysisGraphState


def prepare_degraded_output_node(state: AnalysisGraphState) -> AnalysisGraphState:
    return cast(
        AnalysisGraphState,
        {
            **state,
            "execution_mode": "degraded",
            "degraded_output": True,
            "status": "DEGRADED_OUTPUT",
        },
    )