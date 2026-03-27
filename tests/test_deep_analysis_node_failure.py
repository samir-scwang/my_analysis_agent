from __future__ import annotations

from app.nodes.deep_analysis import deep_analysis_node


def test_deep_analysis_node_missing_dataset_path(base_state: dict):
    state = {**base_state}
    state.pop("dataset_path", None)

    result = deep_analysis_node(state)

    assert result["status"] == "FAILED"
    assert result["errors"]
    assert result["errors"][0]["type"] == "missing_dataset_path"
