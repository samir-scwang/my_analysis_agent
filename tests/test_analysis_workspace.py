from __future__ import annotations

from pathlib import Path

from app.services.analysis_workspace import create_analysis_workspace


def test_create_analysis_workspace_creates_dirs(sample_csv: Path, tmp_path: Path):
    ws = create_analysis_workspace(
        dataset_path=str(sample_csv),
        request_id="req_test",
        revision_round=1,
        base_dir=tmp_path,
    )

    assert Path(ws.root_dir).exists()
    assert Path(ws.input_dir).exists()
    assert Path(ws.scripts_dir).exists()
    assert Path(ws.tables_dir).exists()
    assert Path(ws.charts_dir).exists()
    assert Path(ws.logs_dir).exists()
    assert Path(ws.outputs_dir).exists()

    dataset_local = Path(ws.dataset_local_path)
    assert dataset_local.exists()
    assert dataset_local.name == sample_csv.name