import { useEffect, useMemo, useRef, useState, type ChangeEvent } from "react";
import {
  createSession,
  deleteSession,
  getWorkspace,
  resolveAssetUrl,
  uploadSessionFolder,
} from "./api";
import type { SessionAsset, SessionRecord, WorkspacePayload } from "./types";

const defaultWorkspace: WorkspacePayload = {
  summary: {
    userName: "分析用户",
    activeSession: "尚未创建会话",
    sessionCount: 0,
    uploadedFileCount: 0,
    generatedCount: 0,
    latestActivityAt: "--",
  },
  sessions: [],
};

function formatSessionStatus(status: SessionRecord["status"]): string {
  switch (status) {
    case "queued":
      return "排队中";
    case "running":
      return "运行中";
    case "completed":
      return "已完成";
    case "failed":
      return "失败";
    default:
      return status;
  }
}

function formatStageStatus(status: SessionRecord["stages"][number]["status"]): string {
  switch (status) {
    case "completed":
      return "已完成";
    case "running":
      return "进行中";
    case "pending":
      return "待执行";
    case "failed":
      return "失败";
    default:
      return status;
  }
}

function compactText(text: string, maxLength = 68): string {
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, maxLength).trimEnd()}...`;
}

function getSessionParam(): string {
  return new URLSearchParams(window.location.search).get("session") ?? "";
}

function setSessionParam(sessionId: string) {
  const url = new URL(window.location.href);
  if (sessionId) {
    url.searchParams.set("session", sessionId);
  } else {
    url.searchParams.delete("session");
  }
  window.history.replaceState({}, "", url);
}

function deriveFolderName(files: File[]): string {
  if (files.length === 0) {
    return "";
  }
  const first = files[0].webkitRelativePath || files[0].name;
  return first.split("/")[0] || files[0].name;
}

function latestReport(session?: SessionRecord): SessionAsset | undefined {
  return session?.reports[0];
}

function renderDownloadButton(asset?: SessionAsset, label = "下载文件") {
  const url = resolveAssetUrl(asset?.downloadUrl);
  if (!asset || !url) {
    return (
      <span className="inline-action disabled" role="status">
        暂无文件
      </span>
    );
  }
  return (
    <a className="inline-action" download={asset.filename} href={url} rel="noreferrer" target="_blank">
      {label}
    </a>
  );
}

export default function App() {
  const [workspace, setWorkspace] = useState<WorkspacePayload>(defaultWorkspace);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedSessionId, setSelectedSessionId] = useState("");
  const [selectedFiles, setSelectedFiles] = useState<File[]>([]);
  const folderInputRef = useRef<HTMLInputElement | null>(null);

  async function refreshWorkspace(): Promise<WorkspacePayload> {
    setError(null);
    const payload = await getWorkspace();
    setWorkspace(payload);
    return payload;
  }

  useEffect(() => {
    let mounted = true;
    async function bootstrap() {
      try {
        const payload = await getWorkspace();
        if (!mounted) {
          return;
        }
        setWorkspace(payload);
        const fromUrl = getSessionParam();
        const matched = payload.sessions.find((item) => item.id === fromUrl);
        setSelectedSessionId(matched?.id ?? payload.sessions[0]?.id ?? "");
      } catch (requestError) {
        if (mounted) {
          setError(requestError instanceof Error ? requestError.message : "加载会话失败。");
        }
      } finally {
        if (mounted) {
          setLoading(false);
        }
      }
    }
    void bootstrap();
    return () => {
      mounted = false;
    };
  }, []);

  const selectedSession =
    workspace.sessions.find((item) => item.id === selectedSessionId) ?? workspace.sessions[0];

  useEffect(() => {
    if (selectedSession) {
      setSessionParam(selectedSession.id);
      setSelectedFiles([]);
      if (folderInputRef.current) {
        folderInputRef.current.value = "";
      }
    }
  }, [selectedSession?.id]);

  const folderName = useMemo(() => deriveFolderName(selectedFiles), [selectedFiles]);
  const hasActiveRun = useMemo(
    () => workspace.sessions.some((item) => item.status === "queued" || item.status === "running"),
    [workspace.sessions],
  );

  useEffect(() => {
    if (loading || !hasActiveRun) {
      return;
    }

    const timer = window.setInterval(() => {
      void refreshWorkspace().catch((requestError) => {
        setError(requestError instanceof Error ? requestError.message : "刷新会话状态失败。");
      });
    }, 3000);

    return () => window.clearInterval(timer);
  }, [hasActiveRun, loading]);

  async function handleCreateSession() {
    setBusy("create");
    setError(null);
    try {
      const created = await createSession({
        title: "新建分析会话",
        prompt: "",
      });
      const payload = await refreshWorkspace();
      setSelectedSessionId(created.id || payload.sessions[0]?.id || "");
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "创建会话失败。");
    } finally {
      setBusy(null);
    }
  }

  async function handleDeleteSession(sessionId: string) {
    if (!window.confirm("删除后会移除该会话的文件夹和所有示例产物，是否继续？")) {
      return;
    }
    setBusy(`delete:${sessionId}`);
    setError(null);
    try {
      await deleteSession(sessionId);
      const payload = await refreshWorkspace();
      const fallbackId = payload.sessions[0]?.id ?? "";
      setSelectedSessionId(sessionId === selectedSessionId ? fallbackId : selectedSessionId);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "删除会话失败。");
    } finally {
      setBusy(null);
    }
  }

  async function handleUploadFolder() {
    if (!selectedSession) {
      setError("请先创建一个会话。");
      return;
    }
    if (selectedFiles.length === 0) {
      setError("请先选择文件夹。");
      return;
    }
    setBusy("upload");
    setError(null);
    try {
      await uploadSessionFolder(selectedSession.id, selectedFiles);
      await refreshWorkspace();
      setSelectedFiles([]);
      if (folderInputRef.current) {
        folderInputRef.current.value = "";
      }
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "上传文件夹失败。");
    } finally {
      setBusy(null);
    }
  }

  function handleSelectFolder() {
    folderInputRef.current?.click();
  }

  function handleFolderChange(event: ChangeEvent<HTMLInputElement>) {
    setSelectedFiles(Array.from(event.target.files ?? []));
    setError(null);
  }

  return (
    <div className="chat-shell">
      <aside className="thread-sidebar">
        <div className="sidebar-top">
          <div className="brand-lockup">
            <div className="brand-mark">AA</div>
            <div>
              <strong>Analysis Agent</strong>
              <p>会话线程</p>
            </div>
          </div>

          <div className="sidebar-toolbar">
            <button className="new-session-button" onClick={() => void handleCreateSession()} type="button">
              {busy === "create" ? "正在创建..." : "新建会话"}
            </button>
            <button
              className="sidebar-delete-button"
              disabled={!selectedSession}
              onClick={() => selectedSession && void handleDeleteSession(selectedSession.id)}
              type="button"
            >
              删除会话
            </button>
          </div>
        </div>

        <div className="thread-list">
          {workspace.sessions.map((session) => (
            <button
              className={`thread-row ${session.id === selectedSession?.id ? "active" : ""}`}
              key={session.id}
              onClick={() => setSelectedSessionId(session.id)}
              type="button"
            >
              <div className="thread-title-row">
                <strong>{session.title}</strong>
                <span>{formatSessionStatus(session.status)}</span>
              </div>
              <small>{compactText(session.summary, 42)}</small>
            </button>
          ))}

          {!loading && workspace.sessions.length === 0 ? (
            <div className="empty-state compact">
              <strong>还没有会话</strong>
              <span>点击上方“新建会话”开始创建第一条线程。</span>
            </div>
          ) : null}
        </div>
      </aside>

      <main className="conversation-stage">
        <header className="conversation-header">
          <div className="conversation-copy">
            <p className="eyebrow">Session Workspace</p>
            <h1>{selectedSession?.title ?? "选择一个会话"}</h1>
            <p>
              {selectedSession
                ? "会话设置、文件上传、执行进度和报告产物都收在这一屏内。"
                : "左侧新建或选择一个会话线程后，这里会显示该会话的完整工作区。"}
            </p>
          </div>
        </header>

        {error ? <div className="error-banner">{error}</div> : null}
        {loading ? <div className="loading-panel">正在加载会话线程...</div> : null}

        {!loading && selectedSession ? (
          <section className="conversation-grid">
            <section className="main-column">
              <article className="panel control-panel">
                <div className="panel-header">
                  <div>
                    <p className="eyebrow">Folder Upload</p>
                    <h2>上传分析文件夹</h2>
                  </div>
                  <div className="panel-actions">
                    <span className="panel-badge">{formatSessionStatus(selectedSession.status)}</span>
                  </div>
                </div>

                <div className="control-panel-body">
                  <div className="session-overview">
                    <div className="session-summary-card">
                      <span className="summary-label">当前会话</span>
                      <strong>{selectedSession.title}</strong>
                      <p>{selectedSession.prompt || "上传一个目录后，系统会直接进入真实分析流程。"}</p>
                    </div>

                    <div className="session-summary-card">
                      <span className="summary-label">当前目录</span>
                      <strong>{selectedSession.datasetLabel}</strong>
                      <p>已接收 {selectedSession.uploads.length} 个文件。</p>
                    </div>
                  </div>

                  <div className="control-panel-shell">
                    <div className="upload-block">
                      <div className="subsection-head">
                        <div>
                          <h3>选择目录</h3>
                          <p>上传一个包含数据文件的目录，系统会自动识别并开始分析。</p>
                        </div>
                        <span className="subsection-chip">
                          {selectedFiles.length > 0 ? `${selectedFiles.length} 个文件待上传` : "等待目录"}
                        </span>
                      </div>

                      <input
                        {...({ webkitdirectory: "", directory: "", multiple: true } as any)}
                        className="hidden-input"
                        onChange={handleFolderChange}
                        ref={folderInputRef}
                        type="file"
                      />

                      <button className="folder-dropzone slim" onClick={handleSelectFolder} type="button">
                        <strong>{folderName || "选择一个文件夹绑定到当前会话"}</strong>
                        <p>再次上传会覆盖当前目录。</p>
                      </button>

                      <div className="inline-toolbar">
                        <span className="helper-text">支持 `csv / xlsx / xls / parquet`，重新上传会覆盖当前目录。</span>
                        <button className="primary-button compact" onClick={() => void handleUploadFolder()} type="button">
                          {busy === "upload" ? "上传中..." : "上传文件夹"}
                        </button>
                      </div>
                    </div>
                  </div>
                </div>
              </article>

              <article className="panel activity-panel">
                <div className="panel-header">
                  <div>
                    <p className="eyebrow">Session Activity</p>
                    <h2>执行与动态</h2>
                  </div>
                </div>

                <div className="activity-split">
                  <section className="activity-section progress-section">
                    <div className="section-block-head">
                      <div>
                        <p className="eyebrow">Execution</p>
                        <h3>执行进度</h3>
                      </div>
                      <span className="panel-badge">{selectedSession.progressPercent}%</span>
                    </div>

                    <div className="progress-track">
                      <div className="progress-fill" style={{ width: `${selectedSession.progressPercent}%` }} />
                    </div>

                    <div className="section-summary">
                      <strong>{selectedSession.currentStep}</strong>
                      <p>{selectedSession.summary}</p>
                    </div>

                    <div className="stage-list">
                      {selectedSession.stages.map((stage) => (
                        <article className="stage-row" key={stage.id}>
                          <div className={`stage-dot ${stage.status}`} />
                          <div className="stage-body">
                            <div className="stage-head">
                              <strong>{stage.label}</strong>
                              <span>{formatStageStatus(stage.status)}</span>
                            </div>
                            <p>{stage.detail}</p>
                            <small>{stage.updatedAt}</small>
                          </div>
                        </article>
                      ))}
                    </div>
                  </section>

                  <section className="activity-section timeline-section">
                    <div className="section-block-head">
                      <div>
                        <p className="eyebrow">Timeline</p>
                        <h3>会话动态</h3>
                      </div>
                    </div>

                    <div className="event-list">
                      {selectedSession.events.map((event) => (
                        <article className="event-card" key={event.id}>
                          <strong>{event.title}</strong>
                          <p>{event.detail}</p>
                          <small>{event.timestamp}</small>
                        </article>
                      ))}
                    </div>
                  </section>
                </div>
              </article>
            </section>

            <section className="asset-column">
              <article className="panel report-panel">
                <div className="panel-header">
                  <div>
                    <p className="eyebrow">Final Report</p>
                    <h2>最终报告</h2>
                  </div>
                  {renderDownloadButton(latestReport(selectedSession), "下载报告")}
                </div>

                {latestReport(selectedSession) ? (
                  <div className="report-preview">
                    <h3>{latestReport(selectedSession)?.title}</h3>
                    <p>{latestReport(selectedSession)?.summary}</p>
                    <pre>{latestReport(selectedSession)?.excerpt ?? "当前报告暂无文本预览。"}</pre>
                  </div>
                ) : (
                  <div className="empty-state compact">
                    <strong>还没有报告</strong>
                    <span>上传文件夹后，最终报告会出现在这里。</span>
                  </div>
                )}
              </article>

              <article className="panel assets-panel">
                <div className="assets-split">
                  <section className="asset-section">
                    <div className="panel-header">
                      <div>
                        <p className="eyebrow">Charts</p>
                        <h2>图表</h2>
                      </div>
                      <span className="panel-badge">{selectedSession.charts.length}</span>
                    </div>

                    <div className="asset-list charts">
                      {selectedSession.charts.map((chart) => (
                        <article className="asset-card" key={chart.id}>
                          <div className="asset-head">
                            <strong>{chart.title}</strong>
                            <span>{chart.sizeLabel}</span>
                          </div>
                          <div className="image-frame">
                            {resolveAssetUrl(chart.previewUrl) ? (
                              <img alt={chart.title} src={resolveAssetUrl(chart.previewUrl)} />
                            ) : (
                              <div className="empty-preview">暂无预览</div>
                            )}
                          </div>
                          <p>{chart.summary}</p>
                          {renderDownloadButton(chart, "下载图片")}
                        </article>
                      ))}
                      {selectedSession.charts.length === 0 ? (
                        <div className="empty-state compact">
                          <strong>还没有图表</strong>
                          <span>上传文件夹后，这里会显示生成的图表资产。</span>
                        </div>
                      ) : null}
                    </div>
                  </section>

                  <section className="asset-section">
                    <div className="panel-header">
                      <div>
                        <p className="eyebrow">Tables</p>
                        <h2>表格</h2>
                      </div>
                      <span className="panel-badge">{selectedSession.tables.length}</span>
                    </div>

                    <div className="asset-list tables">
                      {selectedSession.tables.map((table) => (
                        <article className="asset-card" key={table.id}>
                          <div className="asset-head">
                            <strong>{table.title}</strong>
                            <span>{table.sizeLabel}</span>
                          </div>
                          <p>{table.summary}</p>
                          <pre>{table.excerpt ?? "当前表格暂无文本预览。"}</pre>
                          {renderDownloadButton(table, "下载表格")}
                        </article>
                      ))}
                      {selectedSession.tables.length === 0 ? (
                        <div className="empty-state compact">
                          <strong>还没有表格</strong>
                          <span>上传文件夹后，这里会显示生成的表格资产。</span>
                        </div>
                      ) : null}
                    </div>
                  </section>
                </div>
              </article>
            </section>
          </section>
        ) : null}

        {!loading && !selectedSession ? (
          <div className="empty-state large">
            <strong>请选择或新建一个会话</strong>
            <span>左侧新建一条会话线程后，这里会显示当前会话的完整工作区。</span>
          </div>
        ) : null}
      </main>
    </div>
  );
}
