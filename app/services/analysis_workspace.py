from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from app.config import settings


@dataclass(frozen=True)
class AnalysisWorkspace:
    root_dir: str
    input_dir: str
    scripts_dir: str
    tables_dir: str
    charts_dir: str
    logs_dir: str
    outputs_dir: str
    dataset_local_path: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "root_dir": self.root_dir,
            "input_dir": self.input_dir,
            "scripts_dir": self.scripts_dir,
            "tables_dir": self.tables_dir,
            "charts_dir": self.charts_dir,
            "logs_dir": self.logs_dir,
            "outputs_dir": self.outputs_dir,
            "dataset_local_path": self.dataset_local_path,
        }


def _safe_name(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value.strip())
    cleaned = cleaned.strip("_")
    return cleaned or "unknown"


def _default_request_id(request_id: str | None) -> str:
    return _safe_name(request_id or "unknown_request")


def _build_workspace_root(
    *,
    request_id: str,
    revision_round: int,
    base_dir: str | Path | None = None,
) -> Path:
    if base_dir is None:
        if settings.deepagent_workspace_base_dir:
            base = Path(settings.deepagent_workspace_base_dir)
        else:
            base = Path(__file__).resolve().parent.parent / "artifacts" / "deepagent_runs"
    else:
        base = Path(base_dir)

    return base / request_id / f"round_{revision_round}"


def _ensure_dirs(root: Path) -> Dict[str, Path]:
    input_dir = root / "input"
    scripts_dir = root / "scripts"
    tables_dir = root / "tables"
    charts_dir = root / "charts"
    logs_dir = root / "logs"
    outputs_dir = root / "outputs"

    for path in [input_dir, scripts_dir, tables_dir, charts_dir, logs_dir, outputs_dir]:
        path.mkdir(parents=True, exist_ok=True)

    return {
        "input_dir": input_dir,
        "scripts_dir": scripts_dir,
        "tables_dir": tables_dir,
        "charts_dir": charts_dir,
        "logs_dir": logs_dir,
        "outputs_dir": outputs_dir,
    }


def _copy_dataset_to_workspace(dataset_path: str, input_dir: Path) -> Path:
    src = Path(dataset_path)
    if not src.exists():
        raise FileNotFoundError(f"Dataset path does not exist: {dataset_path}")

    dst = input_dir / src.name
    if src.resolve() != dst.resolve():
        shutil.copy2(src, dst)

    return dst


def create_analysis_workspace(
    *,
    dataset_path: str,
    request_id: str | None,
    revision_round: int,
    base_dir: str | Path | None = None,
) -> AnalysisWorkspace:
    safe_request_id = _default_request_id(request_id)
    root = _build_workspace_root(
        request_id=safe_request_id,
        revision_round=revision_round,
        base_dir=base_dir,
    )
    dirs = _ensure_dirs(root)
    dataset_local_path = _copy_dataset_to_workspace(dataset_path, dirs["input_dir"])

    return AnalysisWorkspace(
        root_dir=str(root),
        input_dir=str(dirs["input_dir"]),
        scripts_dir=str(dirs["scripts_dir"]),
        tables_dir=str(dirs["tables_dir"]),
        charts_dir=str(dirs["charts_dir"]),
        logs_dir=str(dirs["logs_dir"]),
        outputs_dir=str(dirs["outputs_dir"]),
        dataset_local_path=str(dataset_local_path),
    )


def ensure_workspace_from_state(
    *,
    state: Dict[str, Any],
    base_dir: str | Path | None = None,
) -> Dict[str, Any]:
    dataset_path = state.get("dataset_path")
    if not dataset_path:
        raise ValueError("dataset_path is required to create analysis workspace.")

    workspace = create_analysis_workspace(
        dataset_path=dataset_path,
        request_id=state.get("request_id"),
        revision_round=state.get("revision_round", 0),
        base_dir=base_dir,
    )
    return workspace.to_dict()