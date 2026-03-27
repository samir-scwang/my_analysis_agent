from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient


def _configure_frontend_state(monkeypatch, tmp_path: Path):
    import app.api_frontend as target

    artifacts_dir = tmp_path / "artifacts"
    frontend_state_dir = artifacts_dir / "frontend_state"
    sessions_dir = frontend_state_dir / "sessions"
    store_file = frontend_state_dir / "workspace_store.json"
    workflow_runs_dir = artifacts_dir / "deepagent_runs"

    monkeypatch.setattr(target, "ARTIFACTS_DIR", artifacts_dir)
    monkeypatch.setattr(target, "FRONTEND_STATE_DIR", frontend_state_dir)
    monkeypatch.setattr(target, "SESSIONS_DIR", sessions_dir)
    monkeypatch.setattr(target, "STORE_FILE", store_file)
    monkeypatch.setattr(target, "WORKFLOW_RUNS_DIR", workflow_runs_dir)
    target.ensure_state_dirs()
    return target


def test_create_session_starts_empty_queue(monkeypatch, tmp_path: Path):
    target = _configure_frontend_state(monkeypatch, tmp_path)
    client = TestClient(target.app)

    response = client.post(
        "/api/frontend/sessions",
        json={"title": "测试会话", "prompt": "请分析上传数据"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["title"] == "测试会话"
    assert payload["status"] == "queued"
    assert payload["progressPercent"] == 0
    assert payload["currentStep"] == "等待上传文件夹。"
    assert payload["reports"] == []


def test_upload_folder_triggers_real_workflow_without_mock_assets(monkeypatch, tmp_path: Path, sample_csv: Path):
    target = _configure_frontend_state(monkeypatch, tmp_path)
    launched: list[tuple[str, str, str]] = []
    monkeypatch.setattr(target, "start_session_workflow", lambda session_id, dataset_path, dataset_label: launched.append((session_id, dataset_path, dataset_label)))

    client = TestClient(target.app)
    session = client.post("/api/frontend/sessions", json={"title": "上传测试", "prompt": ""}).json()

    with sample_csv.open("rb") as handle:
        response = client.post(
            f"/api/frontend/sessions/{session['id']}/folder",
            files=[("files", (sample_csv.name, handle, "text/csv"))],
            data={"relative_paths": f"dataset_bundle/{sample_csv.name}"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "running"
    assert payload["progressPercent"] == 8
    assert payload["datasetLabel"] == sample_csv.name
    assert len(payload["uploads"]) == 1
    assert payload["charts"] == []
    assert payload["tables"] == []
    assert payload["reports"] == []
    assert launched and launched[0][0] == session["id"]
    assert launched[0][2] == sample_csv.name
