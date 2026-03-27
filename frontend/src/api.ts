import type { CreateSessionPayload, SessionRecord, UpdateSessionPayload, WorkspacePayload } from "./types";

const API_BASE = (import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000").replace(/\/$/, "");

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, init);
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || `Request failed with status ${response.status}`);
  }
  if (response.status === 204) {
    return undefined as T;
  }
  return (await response.json()) as T;
}

export function getWorkspace(): Promise<WorkspacePayload> {
  return request<WorkspacePayload>("/api/frontend/workspace");
}

export function listSessions(): Promise<SessionRecord[]> {
  return request<SessionRecord[]>("/api/frontend/sessions");
}

export function createSession(payload: CreateSessionPayload): Promise<SessionRecord> {
  return request<SessionRecord>("/api/frontend/sessions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}

export function updateSession(sessionId: string, payload: UpdateSessionPayload): Promise<SessionRecord> {
  return request<SessionRecord>(`/api/frontend/sessions/${sessionId}`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}

export function deleteSession(sessionId: string): Promise<{ status: string; sessionId: string }> {
  return request<{ status: string; sessionId: string }>(`/api/frontend/sessions/${sessionId}`, {
    method: "DELETE",
  });
}

export async function uploadSessionFolder(sessionId: string, files: File[]): Promise<SessionRecord> {
  const formData = new FormData();
  files.forEach((file) => {
    formData.append("files", file, file.name);
    formData.append("relative_paths", file.webkitRelativePath || file.name);
  });
  return request<SessionRecord>(`/api/frontend/sessions/${sessionId}/folder`, {
    method: "POST",
    body: formData,
  });
}

export function resolveAssetUrl(path?: string | null): string | undefined {
  if (!path) {
    return undefined;
  }
  if (/^https?:\/\//.test(path)) {
    return path;
  }
  return `${API_BASE}${path}`;
}
