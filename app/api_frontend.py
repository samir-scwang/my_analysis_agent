from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path, PurePosixPath
from threading import RLock, Thread
from typing import Literal
from uuid import uuid4

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.nodes.build_analysis_brief import build_analysis_brief_node
from app.nodes.build_dataset_context import build_dataset_context_node
from app.nodes.deep_analysis import deep_analysis_node
from app.nodes.normalize_task import normalize_task_node
from app.nodes.write_report import write_report_node
from app.schemas.state import AnalysisGraphState
from app.services.dataframe_io import CSV_SUFFIXES, EXCEL_SUFFIXES, PARQUET_SUFFIXES

APP_DIR = Path(__file__).resolve().parent
ARTIFACTS_DIR = APP_DIR / "artifacts"
FRONTEND_STATE_DIR = ARTIFACTS_DIR / "frontend_state"
SESSIONS_DIR = FRONTEND_STATE_DIR / "sessions"
STORE_FILE = FRONTEND_STATE_DIR / "workspace_store.json"
WORKFLOW_RUNS_DIR = ARTIFACTS_DIR / "deepagent_runs"
STORE_LOCK = RLock()

CHART_SUFFIXES = {".png", ".jpg", ".jpeg", ".svg", ".webp"}
TABLE_SUFFIXES = {".csv", ".xlsx", ".xls", ".parquet"}
REPORT_SUFFIXES = {".md", ".markdown", ".html", ".pdf"}
DATASET_SUFFIXES = set(CSV_SUFFIXES) | set(EXCEL_SUFFIXES) | set(PARQUET_SUFFIXES)


def now_label() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")


def ensure_state_dirs() -> None:
    FRONTEND_STATE_DIR.mkdir(parents=True, exist_ok=True)
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)


def to_file_url(path: Path) -> str | None:
    try:
        relative = path.resolve().relative_to(ARTIFACTS_DIR.resolve())
    except ValueError:
        return None
    return f"/api/frontend/files/{relative.as_posix()}"


def human_size(num_bytes: int) -> str:
    value = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} {unit}"
        value /= 1024
    return f"{int(num_bytes)} B"


def read_text_excerpt(path: Path, limit: int = 720) -> str:
    if not path.exists() or path.suffix.lower() not in {".md", ".markdown", ".txt", ".csv", ".html"}:
        return ""
    content = path.read_text(encoding="utf-8", errors="ignore").strip()
    if len(content) <= limit:
        return content
    return f"{content[:limit].rstrip()}..."


def normalize_relative_path(raw_relative_path: str | None, fallback_name: str) -> Path:
    candidate = (raw_relative_path or fallback_name).replace("\\", "/").strip()
    parts = [part for part in PurePosixPath(candidate).parts if part not in {"", ".", ".."}]
    if not parts:
        return Path(fallback_name).name
    return Path(*parts)


def _safe_name(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value.strip())
    cleaned = cleaned.strip("_")
    return cleaned or "unknown"


def humanize_stem(stem: str) -> str:
    text = stem.replace("_", " ").replace("-", " ").strip()
    if not text:
        return "未命名文件"
    tokens = []
    for token in text.split():
        upper = token.upper()
        if upper in {"KPI", "GMV", "ROI", "CSV", "XLSX", "PDF"}:
            tokens.append(upper)
        else:
            tokens.append(token.capitalize())
    return " ".join(tokens)


class WorkspaceSummary(BaseModel):
    userName: str
    activeSession: str
    sessionCount: int
    uploadedFileCount: int
    generatedCount: int
    latestActivityAt: str


class SessionUploadRecord(BaseModel):
    id: str
    filename: str
    relativePath: str
    sizeLabel: str
    uploadedAt: str
    fileUrl: str | None = None


class SessionStage(BaseModel):
    id: str
    label: str
    detail: str
    status: Literal["completed", "running", "pending", "failed"]
    updatedAt: str


class SessionAsset(BaseModel):
    id: str
    title: str
    kind: Literal["report", "chart", "table"]
    filename: str
    status: Literal["draft", "ready"]
    createdAt: str
    summary: str
    sizeLabel: str | None = None
    previewUrl: str | None = None
    downloadUrl: str | None = None
    excerpt: str | None = None


class SessionEvent(BaseModel):
    id: str
    title: str
    detail: str
    timestamp: str


class SessionRecord(BaseModel):
    id: str
    title: str
    prompt: str
    datasetLabel: str
    status: Literal["queued", "running", "completed", "failed"]
    createdAt: str
    updatedAt: str
    progressPercent: int
    currentStep: str
    summary: str
    uploads: list[SessionUploadRecord]
    stages: list[SessionStage]
    charts: list[SessionAsset]
    tables: list[SessionAsset]
    reports: list[SessionAsset]
    events: list[SessionEvent]


class WorkspacePayload(BaseModel):
    summary: WorkspaceSummary
    sessions: list[SessionRecord]


class CreateSessionRequest(BaseModel):
    title: str = "新建分析会话"
    prompt: str = ""


class UpdateSessionRequest(BaseModel):
    title: str | None = None
    prompt: str | None = None


def build_stage_records(
    timestamp: str,
    upload_count: int,
    chart_count: int,
    table_count: int,
    report_count: int,
    completed: bool,
) -> list[SessionStage]:
    if completed:
        return [
            SessionStage(
                id="stage_upload",
                label="文件夹接收",
                detail=f"已归档 {upload_count} 个源文件。",
                status="completed",
                updatedAt=timestamp,
            ),
            SessionStage(
                id="stage_profile",
                label="数据解析",
                detail="系统已整理上传目录结构，并识别核心数据资源。",
                status="completed",
                updatedAt=timestamp,
            ),
            SessionStage(
                id="stage_assets",
                label="图表与表格生成",
                detail=f"已准备 {chart_count} 张图表和 {table_count} 张表格。",
                status="completed",
                updatedAt=timestamp,
            ),
            SessionStage(
                id="stage_report",
                label="报告编排",
                detail=f"已生成 {report_count} 份最终报告，可直接下载。",
                status="completed",
                updatedAt=timestamp,
            ),
        ]
    return [
        SessionStage(
            id="stage_upload",
            label="文件夹接收",
            detail=f"已接收 {upload_count} 个文件。",
            status="completed",
            updatedAt=timestamp,
        ),
        SessionStage(
            id="stage_profile",
            label="数据解析",
            detail="正在整理目录并抽取可分析文件。",
            status="running",
            updatedAt=timestamp,
        ),
        SessionStage(
            id="stage_assets",
            label="图表与表格生成",
            detail="等待数据解析完成后继续执行。",
            status="pending",
            updatedAt=timestamp,
        ),
        SessionStage(
            id="stage_report",
            label="报告编排",
            detail="等待图表和表格产出后启动。",
            status="pending",
            updatedAt=timestamp,
        ),
    ]


def build_asset(kind: Literal["report", "chart", "table"], path: Path, created_at: str) -> SessionAsset:
    download_url = to_file_url(path)
    preview_url = download_url if kind == "chart" else None
    excerpt = read_text_excerpt(path, limit=960 if kind == "report" else 360)
    if kind == "report":
        summary = "最终报告已整理完成，可在当前界面预览并直接下载。"
    elif kind == "chart":
        summary = "已生成可视化图表，可在会话内查看并下载原始图片。"
    else:
        summary = "已生成结构化表格，可用于引用、复核与下载。"

    return SessionAsset(
        id=f"{kind}_{uuid4().hex[:10]}",
        title=humanize_stem(path.stem),
        kind=kind,
        filename=path.name,
        status="ready",
        createdAt=created_at,
        summary=summary,
        sizeLabel=human_size(path.stat().st_size) if path.exists() else None,
        previewUrl=preview_url,
        downloadUrl=download_url,
        excerpt=excerpt or None,
    )


def collect_assets(kind: Literal["report", "chart", "table"], directory: Path, created_at: str) -> list[SessionAsset]:
    if not directory.exists():
        return []
    if kind == "chart":
        allowed = CHART_SUFFIXES
    elif kind == "table":
        allowed = TABLE_SUFFIXES
    else:
        allowed = REPORT_SUFFIXES
    assets = [
        build_asset(kind, path, created_at)
        for path in sorted(directory.iterdir())
        if path.is_file() and path.suffix.lower() in allowed
    ]
    return assets


def build_session_record(
    *,
    session_id: str,
    title: str,
    prompt: str,
    dataset_label: str,
    created_at: str,
    uploads: list[SessionUploadRecord],
    charts: list[SessionAsset],
    tables: list[SessionAsset],
    reports: list[SessionAsset],
    events: list[SessionEvent],
    updated_at: str | None = None,
    session_status: Literal["queued", "running", "completed", "failed"] | None = None,
    progress_percent: int | None = None,
    current_step: str | None = None,
    summary: str | None = None,
    stages: list[SessionStage] | None = None,
) -> SessionRecord:
    completed = bool(reports or charts or tables)
    derived_stages = build_stage_records(
        created_at,
        upload_count=len(uploads),
        chart_count=len(charts),
        table_count=len(tables),
        report_count=len(reports),
        completed=completed,
    )
    derived_progress = 100 if completed else 35
    derived_current_step = "报告与可下载资产已准备完毕。" if completed else "正在整理上传文件。"
    derived_summary = (
        f"当前会话已归档 {len(uploads)} 个源文件，生成 {len(charts)} 张图表、"
        f"{len(tables)} 张表格和 {len(reports)} 份报告。"
        if completed
        else f"当前会话已接收 {len(uploads)} 个文件，正在生成分析产物。"
    )
    return SessionRecord(
        id=session_id,
        title=title,
        prompt=prompt,
        datasetLabel=dataset_label,
        status=session_status or ("completed" if completed else "running"),
        createdAt=created_at,
        updatedAt=updated_at or created_at,
        progressPercent=derived_progress if progress_percent is None else progress_percent,
        currentStep=derived_current_step if current_step is None else current_step,
        summary=derived_summary if summary is None else summary,
        uploads=uploads,
        stages=stages or derived_stages,
        charts=charts,
        tables=tables,
        reports=reports,
        events=events,
    )


def _runtime_stage_records(
    *,
    phase: Literal["waiting_upload", "queued", "profile", "analysis", "report", "completed", "failed"],
    timestamp: str,
    upload_count: int,
    chart_count: int,
    table_count: int,
    report_count: int,
    failure_message: str = "",
) -> list[SessionStage]:
    stage2_status: Literal["completed", "running", "pending", "failed"] = "pending"
    stage3_status: Literal["completed", "running", "pending", "failed"] = "pending"
    stage4_status: Literal["completed", "running", "pending", "failed"] = "pending"

    stage2_detail = "等待上传文件夹后开始任务理解与数据解析。"
    stage3_detail = "等待数据解析完成后执行深度分析。"
    stage4_detail = "等待图表、表格生成后开始报告编排。"

    if phase in {"queued", "profile", "analysis", "report", "completed"}:
        stage2_status = "running"
        stage2_detail = "正在标准化任务、解析数据结构并生成分析 brief。"

    if phase in {"analysis", "report", "completed"}:
        stage2_status = "completed"
        stage2_detail = "已完成任务理解、数据画像与分析 brief。"
        stage3_status = "running"
        stage3_detail = "正在执行深度分析并生成图表、表格等证据产物。"

    if phase in {"report", "completed"}:
        stage3_status = "completed"
        stage3_detail = f"已准备 {chart_count} 张图表和 {table_count} 张表格。"
        stage4_status = "running"
        stage4_detail = "正在编排最终报告并整理下载内容。"

    if phase == "completed":
        stage4_status = "completed"
        stage4_detail = f"已生成 {report_count} 份最终报告，可直接下载。"

    if phase == "failed":
        if chart_count or table_count or report_count:
            stage2_status = "completed"
            stage2_detail = "任务理解与数据解析已完成。"
            if report_count:
                stage3_status = "completed"
                stage3_detail = f"已准备 {chart_count} 张图表和 {table_count} 张表格。"
                stage4_status = "failed"
                stage4_detail = failure_message or "报告编排失败。"
            else:
                stage3_status = "failed"
                stage3_detail = failure_message or "深度分析阶段失败。"
        elif upload_count:
            stage2_status = "failed"
            stage2_detail = failure_message or "任务理解与数据解析失败。"

    return [
        SessionStage(
            id="stage_upload",
            label="文件夹接收",
            detail=f"已归档 {upload_count} 个源文件。" if upload_count else "等待上传文件夹。",
            status="completed" if upload_count else ("failed" if phase == "failed" else "pending"),
            updatedAt=timestamp,
        ),
        SessionStage(
            id="stage_profile",
            label="任务理解与数据解析",
            detail=stage2_detail,
            status=stage2_status,
            updatedAt=timestamp,
        ),
        SessionStage(
            id="stage_assets",
            label="深度分析与产物生成",
            detail=stage3_detail,
            status=stage3_status,
            updatedAt=timestamp,
        ),
        SessionStage(
            id="stage_report",
            label="报告编排",
            detail=stage4_detail,
            status=stage4_status,
            updatedAt=timestamp,
        ),
    ]


def _runtime_session_values(
    *,
    phase: Literal["waiting_upload", "queued", "profile", "analysis", "report", "completed", "failed"],
    upload_count: int,
    chart_count: int,
    table_count: int,
    report_count: int,
    failure_message: str = "",
) -> tuple[Literal["queued", "running", "completed", "failed"], int, str, str]:
    if phase == "waiting_upload":
        return (
            "queued",
            0,
            "等待上传文件夹。",
            "当前会话尚未上传文件夹，上传后会启动真实分析流程。",
        )
    if phase == "queued":
        return (
            "running",
            8,
            "已接收文件夹，正在准备分析任务。",
            f"当前会话已接收 {upload_count} 个文件，正在启动真实分析流程。",
        )
    if phase == "profile":
        return (
            "running",
            35,
            "正在理解任务并解析数据结构。",
            f"当前会话已接收 {upload_count} 个文件，正在完成任务标准化、数据画像和分析 brief。",
        )
    if phase == "analysis":
        return (
            "running",
            72,
            "正在执行深度分析并生成图表表格。",
            f"当前会话已接收 {upload_count} 个文件，正在生成图表、表格等分析产物。",
        )
    if phase == "report":
        return (
            "running",
            90,
            "正在编排最终报告。",
            f"已生成 {chart_count} 张图表和 {table_count} 张表格，正在整理最终报告。",
        )
    if phase == "completed":
        return (
            "completed",
            100,
            "报告与可下载资产已准备完毕。",
            f"当前会话已归档 {upload_count} 个源文件，生成 {chart_count} 张图表、{table_count} 张表格和 {report_count} 份报告。",
        )
    return (
        "failed",
        0,
        "分析流程执行失败。",
        failure_message or "真实分析流程执行失败，请检查错误详情后重试。",
    )


def build_empty_session(title: str, prompt: str) -> SessionRecord:
    created_at = now_label()
    session_id = f"sess_{uuid4().hex[:8]}"
    session_root = get_session_root(session_id)
    (session_root / "uploads").mkdir(parents=True, exist_ok=True)
    status, progress_percent, current_step, summary = _runtime_session_values(
        phase="waiting_upload",
        upload_count=0,
        chart_count=0,
        table_count=0,
        report_count=0,
    )
    return build_session_record(
        session_id=session_id,
        title=title.strip() or "新建分析会话",
        prompt=prompt.strip(),
        dataset_label="尚未上传文件夹",
        created_at=created_at,
        uploads=[],
        charts=[],
        tables=[],
        reports=[],
        events=[
            SessionEvent(
                id=f"evt_{uuid4().hex[:8]}",
                title="会话已创建",
                detail="当前会话尚未上传文件夹。上传后会启动真实分析流程。",
                timestamp=created_at,
            )
        ],
        session_status=status,
        progress_percent=progress_percent,
        current_step=current_step,
        summary=summary,
        stages=_runtime_stage_records(
            phase="waiting_upload",
            timestamp=created_at,
            upload_count=0,
            chart_count=0,
            table_count=0,
            report_count=0,
        ),
    )


def build_session_summary(payload: WorkspacePayload) -> WorkspaceSummary:
    active_session = payload.sessions[0].title if payload.sessions else "尚未创建会话"
    latest_activity = payload.sessions[0].updatedAt if payload.sessions else now_label()
    uploaded_file_count = sum(len(session.uploads) for session in payload.sessions)
    generated_count = sum(
        len(session.charts) + len(session.tables) + len(session.reports)
        for session in payload.sessions
    )
    return WorkspaceSummary(
        userName=payload.summary.userName if payload.summary and payload.summary.userName else "Analysis User",
        activeSession=active_session,
        sessionCount=len(payload.sessions),
        uploadedFileCount=uploaded_file_count,
        generatedCount=generated_count,
        latestActivityAt=latest_activity,
    )


def get_session_root(session_id: str) -> Path:
    return SESSIONS_DIR / session_id

def replace_session(payload: WorkspacePayload, session: SessionRecord) -> None:
    for index, item in enumerate(payload.sessions):
        if item.id == session.id:
            payload.sessions[index] = session
            return
    payload.sessions.insert(0, session)


def get_session_or_404(payload: WorkspacePayload, session_id: str) -> SessionRecord:
    session = next((item for item in payload.sessions if item.id == session_id), None)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found.")
    return session


def clear_directory(directory: Path) -> None:
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir(parents=True, exist_ok=True)


def sort_session_events(events: list[SessionEvent]) -> list[SessionEvent]:
    return sorted(events, key=lambda item: item.timestamp, reverse=True)

def _save_store_unlocked(payload: WorkspacePayload) -> None:
    ensure_state_dirs()
    payload.summary = build_session_summary(payload)
    STORE_FILE.write_text(
        json.dumps(payload.model_dump(), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def save_store(payload: WorkspacePayload) -> None:
    with STORE_LOCK:
        _save_store_unlocked(payload)


def maybe_build_upload_record_from_path(
    *,
    upload_id: str,
    stored_path: str,
    filename: str,
    uploaded_at: str,
    relative_path: str | None = None,
) -> SessionUploadRecord:
    path = Path(stored_path)
    return SessionUploadRecord(
        id=upload_id,
        filename=filename,
        relativePath=relative_path or filename,
        sizeLabel=human_size(path.stat().st_size) if path.exists() else "未知大小",
        uploadedAt=uploaded_at,
        fileUrl=to_file_url(path),
    )


def legacy_content_to_asset(content: dict, fallback_created_at: str) -> SessionAsset:
    kind = content.get("kind", "report")
    source_url = content.get("sourceUrl")
    preview_url = content.get("previewUrl") or source_url if kind == "chart" else None
    filename = content.get("title", "artifact")
    if source_url:
        filename = Path(source_url).name or filename
    return SessionAsset(
        id=content.get("id", f"{kind}_{uuid4().hex[:10]}"),
        title=content.get("title", "未命名产物"),
        kind=kind,
        filename=filename,
        status=content.get("status", "ready"),
        createdAt=content.get("createdAt", fallback_created_at),
        summary=content.get("summary", ""),
        previewUrl=preview_url,
        downloadUrl=source_url,
        excerpt=content.get("excerpt"),
    )


def migrate_legacy_store(raw: dict) -> WorkspacePayload:
    uploads_raw = [item for item in raw.get("uploads", []) if isinstance(item, dict)]
    uploads_by_id = {item.get("id"): item for item in uploads_raw}
    tasks_raw = [item for item in raw.get("tasks", []) if isinstance(item, dict)]
    jobs_raw = [item for item in raw.get("jobs", []) if isinstance(item, dict)]
    sessions: list[SessionRecord] = []

    source_records = tasks_raw or jobs_raw
    for index, record in enumerate(source_records):
        created_at = record.get("updatedAt") or raw.get("summary", {}).get("latestActivityAt") or now_label()
        upload_record = uploads_by_id.get(record.get("uploadId"))
        uploads = []
        if upload_record:
            uploads.append(
                maybe_build_upload_record_from_path(
                    upload_id=upload_record.get("id", f"upl_{index}"),
                    stored_path=upload_record.get("storedPath", ""),
                    filename=upload_record.get("filename", "dataset.csv"),
                    uploaded_at=upload_record.get("uploadedAt", created_at),
                )
            )

        raw_contents = record.get("contents")
        if not isinstance(raw_contents, list):
            raw_contents = [
                item
                for item in raw.get("contents", [])
                if isinstance(item, dict) and item.get("sourceJobId") == record.get("id")
            ]
        assets = [legacy_content_to_asset(item, created_at) for item in raw_contents if isinstance(item, dict)]
        charts = [asset for asset in assets if asset.kind == "chart"]
        tables = [asset for asset in assets if asset.kind == "table"]
        reports = [asset for asset in assets if asset.kind == "report"]

        raw_history = record.get("history") if isinstance(record.get("history"), list) else []
        events = [
            SessionEvent(
                id=item.get("id", f"evt_{uuid4().hex[:8]}"),
                title=item.get("title", "历史事件"),
                detail=item.get("detail", ""),
                timestamp=item.get("timestamp", created_at),
            )
            for item in raw_history
            if isinstance(item, dict)
        ]
        if not events:
            events.append(
                SessionEvent(
                    id=f"evt_{uuid4().hex[:8]}",
                    title="已迁移旧版会话",
                    detail="旧版任务线程已迁移为会话视图，可继续查看产物和下载文件。",
                    timestamp=created_at,
                )
            )

        session = build_session_record(
            session_id=record.get("id", f"sess_{uuid4().hex[:8]}"),
            title=record.get("title", f"迁移会话 {index + 1}"),
            prompt=record.get("prompt", ""),
            dataset_label=record.get("datasetName", uploads[0].filename if uploads else "未命名数据集"),
            created_at=created_at,
            uploads=uploads,
            charts=charts,
            tables=tables,
            reports=reports,
            events=events,
        )
        if record.get("status") == "failed":
            session.status = "failed"
            session.progressPercent = 0
            session.currentStep = "旧版任务状态为失败。"
        sessions.append(session)

    if not sessions:
        return sample_seed()

    payload = WorkspacePayload(
        summary=WorkspaceSummary(
            userName=raw.get("summary", {}).get("userName", "Analysis User"),
            activeSession="",
            sessionCount=0,
            uploadedFileCount=0,
            generatedCount=0,
            latestActivityAt=now_label(),
        ),
        sessions=sessions,
    )
    payload.summary = build_session_summary(payload)
    return payload


def sample_seed() -> WorkspacePayload:
    payload = WorkspacePayload(
        summary=WorkspaceSummary(
            userName="Analysis User",
            activeSession="",
            sessionCount=0,
            uploadedFileCount=0,
            generatedCount=0,
            latestActivityAt=now_label(),
        ),
        sessions=[],
    )
    payload.summary = build_session_summary(payload)
    return payload


def _load_store_unlocked() -> WorkspacePayload:
    ensure_state_dirs()
    if not STORE_FILE.exists():
        payload = sample_seed()
        _save_store_unlocked(payload)
        return payload
    raw = json.loads(STORE_FILE.read_text(encoding="utf-8"))
    if "sessions" in raw:
        if not isinstance(raw.get("summary"), dict):
            raw["summary"] = {
                "userName": "Analysis User",
                "activeSession": "",
                "sessionCount": 0,
                "uploadedFileCount": 0,
                "generatedCount": 0,
                "latestActivityAt": now_label(),
            }
        payload = WorkspacePayload.model_validate(raw)
    else:
        payload = migrate_legacy_store(raw)
        _save_store_unlocked(payload)
    payload.summary = build_session_summary(payload)
    return payload


def load_store() -> WorkspacePayload:
    with STORE_LOCK:
        return _load_store_unlocked()


def update_session_in_store(session_id: str, updater) -> SessionRecord:
    with STORE_LOCK:
        payload = _load_store_unlocked()
        session = get_session_or_404(payload, session_id)
        updated = updater(session)
        replace_session(payload, updated)
        payload.sessions = [updated] + [item for item in payload.sessions if item.id != updated.id]
        _save_store_unlocked(payload)
        return updated


def select_primary_dataset(upload_root: Path) -> Path | None:
    suffix_priority = {".csv": 0, ".xlsx": 1, ".xls": 2, ".parquet": 3}
    candidates = [
        path
        for path in upload_root.rglob("*")
        if path.is_file() and path.suffix.lower() in DATASET_SUFFIXES
    ]
    if not candidates:
        return None
    candidates.sort(
        key=lambda path: (
            suffix_priority.get(path.suffix.lower(), 99),
            len(path.parts),
            path.name.lower(),
        )
    )
    return candidates[0]


def collect_report_assets(report_path: Path | None, created_at: str) -> list[SessionAsset]:
    if report_path and report_path.exists():
        return [build_asset("report", report_path, created_at)]
    return []


def collect_generated_assets_from_state(
    state: AnalysisGraphState,
    created_at: str,
) -> tuple[list[SessionAsset], list[SessionAsset], list[SessionAsset]]:
    workspace = state.get("analysis_workspace", {}) or {}
    charts_dir = Path(str(workspace.get("charts_dir", "")).strip()) if workspace.get("charts_dir") else None
    tables_dir = Path(str(workspace.get("tables_dir", "")).strip()) if workspace.get("tables_dir") else None

    charts = collect_assets("chart", charts_dir, created_at) if charts_dir else []
    tables = collect_assets("table", tables_dir, created_at) if tables_dir else []

    report_path: Path | None = None
    report_draft = state.get("report_draft", {}) or {}
    if isinstance(report_draft, dict):
        metadata = report_draft.get("report_metadata", {}) or {}
        candidate = str(metadata.get("report_path", "")).strip() if isinstance(metadata, dict) else ""
        if candidate:
            report_path = Path(candidate)
    reports = collect_report_assets(report_path, created_at)
    return charts, tables, reports


def build_runtime_session(
    base_session: SessionRecord,
    *,
    timestamp: str,
    phase: Literal["waiting_upload", "queued", "profile", "analysis", "report", "completed", "failed"],
    dataset_label: str | None = None,
    uploads: list[SessionUploadRecord] | None = None,
    charts: list[SessionAsset] | None = None,
    tables: list[SessionAsset] | None = None,
    reports: list[SessionAsset] | None = None,
    new_events: list[SessionEvent] | None = None,
    failure_message: str = "",
    current_step: str | None = None,
    summary: str | None = None,
) -> SessionRecord:
    actual_uploads = uploads if uploads is not None else list(base_session.uploads)
    actual_charts = charts if charts is not None else list(base_session.charts)
    actual_tables = tables if tables is not None else list(base_session.tables)
    actual_reports = reports if reports is not None else list(base_session.reports)

    status, progress_percent, default_step, default_summary = _runtime_session_values(
        phase=phase,
        upload_count=len(actual_uploads),
        chart_count=len(actual_charts),
        table_count=len(actual_tables),
        report_count=len(actual_reports),
        failure_message=failure_message,
    )

    events = sort_session_events(list(new_events or []) + list(base_session.events))

    return build_session_record(
        session_id=base_session.id,
        title=base_session.title,
        prompt=base_session.prompt,
        dataset_label=dataset_label or base_session.datasetLabel,
        created_at=base_session.createdAt,
        updated_at=timestamp,
        uploads=actual_uploads,
        charts=actual_charts,
        tables=actual_tables,
        reports=actual_reports,
        events=events,
        session_status=status,
        progress_percent=progress_percent,
        current_step=current_step or default_step,
        summary=summary or default_summary,
        stages=_runtime_stage_records(
            phase=phase,
            timestamp=timestamp,
            upload_count=len(actual_uploads),
            chart_count=len(actual_charts),
            table_count=len(actual_tables),
            report_count=len(actual_reports),
            failure_message=failure_message,
        ),
    )


def extract_failure_message(state: AnalysisGraphState, fallback: str) -> str:
    errors = state.get("errors", []) or []
    if errors and isinstance(errors[-1], dict):
        message = str(errors[-1].get("message", "")).strip()
        if message:
            return message
    return fallback


def clear_session_workflow_outputs(session_id: str) -> None:
    request_root = WORKFLOW_RUNS_DIR / _safe_name(session_id)
    if request_root.exists():
        shutil.rmtree(request_root)


def build_workflow_initial_state(session: SessionRecord, dataset_path: str) -> AnalysisGraphState:
    user_prompt = session.prompt.strip() or "请基于上传的数据生成一份详细、图表丰富、包含表格和结论的中文数据分析报告。"
    return {
        "request_id": session.id,
        "session_id": session.id,
        "user_id": "frontend_user",
        "dataset_id": f"ds_{session.id}",
        "dataset_path": dataset_path,
        "user_prompt": user_prompt,
        "input_config": {
            "language": "zh-CN",
            "output_format": ["markdown"],
        },
        "memory_context": {},
        "revision_round": 0,
        "max_review_rounds": 2,
        "revision_tasks": [],
        "revision_context": {},
        "execution_mode": "normal",
        "warnings": [],
        "errors": [],
        "status": "INIT",
        "degraded_output": False,
    }


def _run_session_workflow(session_id: str, dataset_path: str, dataset_label: str) -> None:
    try:
        clear_session_workflow_outputs(session_id)

        payload = load_store()
        session = get_session_or_404(payload, session_id)
        state = build_workflow_initial_state(session, dataset_path)

        timestamp = now_label()
        update_session_in_store(
            session_id,
            lambda current: build_runtime_session(
                current,
                timestamp=timestamp,
                phase="profile",
                current_step="正在标准化任务并解析数据结构。",
                summary="真实分析流程已启动，正在完成任务理解、数据画像和分析 brief。",
                new_events=[
                    SessionEvent(
                        id=f"evt_{uuid4().hex[:8]}",
                        title="真实分析已启动",
                        detail=f"已识别数据文件 {dataset_label}，开始执行工作流。",
                        timestamp=timestamp,
                    )
                ],
            ),
        )

        for node_name, node_func, phase, step_message, summary_message in [
            ("normalize_task", normalize_task_node, "profile", "已完成任务标准化，正在解析数据结构。", "已完成任务标准化，正在生成数据画像。"),
            ("build_dataset_context", build_dataset_context_node, "profile", "已完成数据画像，正在生成分析 brief。", "已完成数据画像，正在整理分析目标与报告结构。"),
            ("build_analysis_brief", build_analysis_brief_node, "analysis", "分析 brief 已完成，正在执行深度分析。", "已完成任务理解与分析 brief，正在生成图表与表格。"),
            ("deep_analysis", deep_analysis_node, "report", "深度分析已完成，正在编排最终报告。", "图表、表格等产物已生成，正在编排最终报告。"),
            ("write_report", write_report_node, "completed", "报告与可下载资产已准备完毕。", "真实分析流程已完成，可直接查看并下载报告与产物。"),
        ]:
            state = node_func(state)
            timestamp = now_label()

            if state.get("status") == "FAILED":
                message = extract_failure_message(state, f"{node_name} 执行失败。")
                charts, tables, reports = collect_generated_assets_from_state(state, timestamp)
                update_session_in_store(
                    session_id,
                    lambda current, failed_charts=charts, failed_tables=tables, failed_reports=reports: build_runtime_session(
                        current,
                        timestamp=timestamp,
                        phase="failed",
                        dataset_label=dataset_label,
                        charts=failed_charts,
                        tables=failed_tables,
                        reports=failed_reports,
                        failure_message=message,
                        current_step="分析流程执行失败。",
                        summary=message,
                        new_events=[
                            SessionEvent(
                                id=f"evt_{uuid4().hex[:8]}",
                                title="分析流程失败",
                                detail=message,
                                timestamp=timestamp,
                            )
                        ],
                    ),
                )
                return

            charts, tables, reports = collect_generated_assets_from_state(state, timestamp)
            event_detail = summary_message
            event_title = {
                "normalize_task": "任务标准化完成",
                "build_dataset_context": "数据画像完成",
                "build_analysis_brief": "分析 brief 完成",
                "deep_analysis": "深度分析完成",
                "write_report": "最终报告已生成",
            }[node_name]

            update_session_in_store(
                session_id,
                lambda current, node_phase=phase, node_step=step_message, node_summary=summary_message, node_title=event_title, node_detail=event_detail, node_charts=charts, node_tables=tables, node_reports=reports: build_runtime_session(
                    current,
                    timestamp=timestamp,
                    phase=node_phase,
                    dataset_label=dataset_label,
                    charts=node_charts,
                    tables=node_tables,
                    reports=node_reports,
                    current_step=node_step,
                    summary=node_summary,
                    new_events=[
                        SessionEvent(
                            id=f"evt_{uuid4().hex[:8]}",
                            title=node_title,
                            detail=node_detail,
                            timestamp=timestamp,
                        )
                    ],
                ),
            )

    except Exception as exc:
        timestamp = now_label()
        try:
            update_session_in_store(
                session_id,
                lambda current: build_runtime_session(
                    current,
                    timestamp=timestamp,
                    phase="failed",
                    dataset_label=dataset_label,
                    failure_message=str(exc),
                    current_step="分析流程执行失败。",
                    summary=str(exc),
                    new_events=[
                        SessionEvent(
                            id=f"evt_{uuid4().hex[:8]}",
                            title="分析流程失败",
                            detail=str(exc),
                            timestamp=timestamp,
                        )
                    ],
                ),
            )
        except Exception:
            return


def start_session_workflow(session_id: str, dataset_path: str, dataset_label: str) -> None:
    Thread(
        target=_run_session_workflow,
        args=(session_id, dataset_path, dataset_label),
        daemon=True,
        name=f"session-workflow-{session_id}",
    ).start()


app = FastAPI(title="Analysis Agent Frontend API", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount(
    "/api/frontend/files",
    StaticFiles(directory=str(ARTIFACTS_DIR), check_dir=False),
    name="frontend-files",
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/frontend/workspace", response_model=WorkspacePayload)
def get_workspace() -> WorkspacePayload:
    payload = load_store()
    payload.summary = build_session_summary(payload)
    save_store(payload)
    return payload


@app.get("/api/frontend/sessions", response_model=list[SessionRecord])
def get_sessions() -> list[SessionRecord]:
    return load_store().sessions


@app.get("/api/frontend/sessions/{session_id}", response_model=SessionRecord)
def get_session(session_id: str) -> SessionRecord:
    payload = load_store()
    return get_session_or_404(payload, session_id)


@app.post("/api/frontend/sessions", response_model=SessionRecord)
def create_session(request: CreateSessionRequest) -> SessionRecord:
    payload = load_store()
    session = build_empty_session(request.title, request.prompt)
    payload.sessions.insert(0, session)
    payload.summary = build_session_summary(payload)
    save_store(payload)
    return session


@app.patch("/api/frontend/sessions/{session_id}", response_model=SessionRecord)
def update_session(session_id: str, request: UpdateSessionRequest) -> SessionRecord:
    payload = load_store()
    session = get_session_or_404(payload, session_id)
    updated = session.model_copy(
        update={
            "title": request.title.strip() if request.title is not None and request.title.strip() else session.title,
            "prompt": request.prompt.strip() if request.prompt is not None else session.prompt,
            "updatedAt": now_label(),
        }
    )
    replace_session(payload, updated)
    payload.summary = build_session_summary(payload)
    save_store(payload)
    return updated


@app.delete("/api/frontend/sessions/{session_id}")
def delete_session(session_id: str) -> dict[str, str]:
    payload = load_store()
    session = get_session_or_404(payload, session_id)
    payload.sessions = [item for item in payload.sessions if item.id != session.id]
    session_root = get_session_root(session.id)
    if session_root.exists():
        shutil.rmtree(session_root)
    payload.summary = build_session_summary(payload)
    save_store(payload)
    return {"status": "deleted", "sessionId": session.id}


@app.post("/api/frontend/sessions/{session_id}/folder", response_model=SessionRecord)
async def upload_session_folder(
    session_id: str,
    files: list[UploadFile] = File(...),
    relative_paths: list[str] = Form([]),
) -> SessionRecord:
    payload = load_store()
    session = get_session_or_404(payload, session_id)
    created_at = now_label()
    session_root = get_session_root(session_id)
    upload_root = session_root / "uploads"
    generated_root = session_root / "generated"

    if not files:
        raise HTTPException(status_code=400, detail="At least one file is required.")

    clear_directory(upload_root)
    clear_directory(generated_root)

    uploaded_files: list[SessionUploadRecord] = []
    for index, upload in enumerate(files):
        raw_relative_path = relative_paths[index] if index < len(relative_paths) else upload.filename
        relative_path = normalize_relative_path(raw_relative_path, upload.filename or f"file_{index}")
        target_path = upload_root / relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with target_path.open("wb") as output:
            shutil.copyfileobj(upload.file, output)
        uploaded_files.append(
            SessionUploadRecord(
                id=f"upl_{uuid4().hex[:8]}",
                filename=target_path.name,
                relativePath=relative_path.as_posix(),
                sizeLabel=human_size(target_path.stat().st_size),
                uploadedAt=created_at,
                fileUrl=to_file_url(target_path),
            )
        )

    dataset_path = select_primary_dataset(upload_root)
    dataset_label = dataset_path.name if dataset_path else "未识别数据文件"

    if dataset_path is None:
        failed_session = build_runtime_session(
            session,
            timestamp=created_at,
            phase="failed",
            dataset_label=dataset_label,
            uploads=uploaded_files,
            charts=[],
            tables=[],
            reports=[],
            failure_message="上传目录中未找到可分析的数据文件。当前仅支持 csv、xlsx、xls、parquet。",
            current_step="未找到可分析的数据文件。",
            summary="上传目录中未找到可分析的数据文件。当前仅支持 csv、xlsx、xls、parquet。",
            new_events=[
                SessionEvent(
                    id=f"evt_{uuid4().hex[:8]}",
                    title="上传失败",
                    detail="上传目录中未找到可分析的数据文件。当前仅支持 csv、xlsx、xls、parquet。",
                    timestamp=created_at,
                )
            ],
        )
        replace_session(payload, failed_session)
        payload.sessions = [failed_session] + [item for item in payload.sessions if item.id != failed_session.id]
        payload.summary = build_session_summary(payload)
        save_store(payload)
        return failed_session

    uploaded_session = build_runtime_session(
        session,
        timestamp=created_at,
        phase="queued",
        dataset_label=dataset_label,
        uploads=uploaded_files,
        charts=[],
        tables=[],
        reports=[],
        current_step="已接收文件夹，正在准备分析任务。",
        summary=f"当前会话已接收 {len(uploaded_files)} 个文件，正在启动真实分析流程。",
        new_events=[
            SessionEvent(
                id=f"evt_{uuid4().hex[:8]}",
                title="已接收文件夹上传",
                detail=f"会话已归档 {len(uploaded_files)} 个文件，数据文件为 {dataset_label}。",
                timestamp=created_at,
            )
        ],
    )

    replace_session(payload, uploaded_session)
    payload.sessions = [uploaded_session] + [item for item in payload.sessions if item.id != uploaded_session.id]
    payload.summary = build_session_summary(payload)
    save_store(payload)
    start_session_workflow(session_id, str(dataset_path), dataset_label)
    return uploaded_session
