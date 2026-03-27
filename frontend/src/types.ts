export type SessionStatus = "queued" | "running" | "completed" | "failed";

export type StageStatus = "completed" | "running" | "pending" | "failed";

export type AssetKind = "report" | "chart" | "table";

export interface WorkspaceSummary {
  userName: string;
  activeSession: string;
  sessionCount: number;
  uploadedFileCount: number;
  generatedCount: number;
  latestActivityAt: string;
}

export interface SessionUploadRecord {
  id: string;
  filename: string;
  relativePath: string;
  sizeLabel: string;
  uploadedAt: string;
  fileUrl?: string | null;
}

export interface SessionStage {
  id: string;
  label: string;
  detail: string;
  status: StageStatus;
  updatedAt: string;
}

export interface SessionAsset {
  id: string;
  title: string;
  kind: AssetKind;
  filename: string;
  status: "draft" | "ready";
  createdAt: string;
  summary: string;
  sizeLabel?: string | null;
  previewUrl?: string | null;
  downloadUrl?: string | null;
  excerpt?: string | null;
}

export interface SessionEvent {
  id: string;
  title: string;
  detail: string;
  timestamp: string;
}

export interface SessionRecord {
  id: string;
  title: string;
  prompt: string;
  datasetLabel: string;
  status: SessionStatus;
  createdAt: string;
  updatedAt: string;
  progressPercent: number;
  currentStep: string;
  summary: string;
  uploads: SessionUploadRecord[];
  stages: SessionStage[];
  charts: SessionAsset[];
  tables: SessionAsset[];
  reports: SessionAsset[];
  events: SessionEvent[];
}

export interface WorkspacePayload {
  summary: WorkspaceSummary;
  sessions: SessionRecord[];
}

export interface CreateSessionPayload {
  title: string;
  prompt: string;
}

export interface UpdateSessionPayload {
  title?: string;
  prompt?: string;
}
