/**
 * Tiny async client for the ForgeWire hub HTTP API.
 *
 * The Python CLI is the canonical client; the extension only reproduces the
 * read-side surface (list runners / tasks, dispatch, stream tail, cancel).
 * We deliberately use Node's built-in fetch (Node 18+) instead of pulling in
 * a runtime dependency so the published .vsix stays small.
 */

import * as vscode from "vscode";

export interface RunnerInfo {
  runner_id: string;
  hostname: string;
  os: string;
  arch: string;
  state: string;
  tags: string[];
  scope_prefixes: string[];
  current_load: number;
  max_concurrent: number;
  last_heartbeat?: string;
  drain_requested?: boolean;
  [key: string]: unknown;
}

export interface TaskInfo {
  id: number;
  title: string;
  status: string;
  branch: string;
  base_commit: string;
  prompt: string;
  scope_globs: string[];
  worker_id?: string | null;
  todo_id?: string | null;
  created_at?: string;
  claimed_at?: string | null;
  started_at?: string | null;
  completed_at?: string | null;
  required_tags?: string[];
  required_tools?: string[];
  result?: { status?: string; log_tail?: string; error?: string | null };
  [key: string]: unknown;
}

export interface DispatchPayload {
  title: string;
  prompt: string;
  scope_globs: string[];
  branch: string;
  base_commit: string;
  todo_id?: string;
  timeout_minutes?: number;
  priority?: number;
  required_tags?: string[];
  required_tools?: string[];
  tenant?: string;
}

export class HubClient {
  constructor(private readonly baseUrl: string, private readonly token: string) {}

  static fromConfig(): HubClient | undefined {
    const cfg = vscode.workspace.getConfiguration("forgewire");
    const baseUrl = (cfg.get<string>("hubUrl") ?? "").trim();
    const token = (cfg.get<string>("hubToken") ?? "").trim();
    if (!baseUrl || !token) {
      return undefined;
    }
    return new HubClient(baseUrl.replace(/\/+$/, ""), token);
  }

  get url(): string {
    return this.baseUrl;
  }

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const init: RequestInit = {
      method,
      headers: {
        Authorization: `Bearer ${this.token}`,
        "Content-Type": "application/json",
      },
    };
    if (body !== undefined) {
      init.body = JSON.stringify(body);
    }
    const res = await fetch(`${this.baseUrl}${path}`, init);
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`hub HTTP ${res.status}: ${text || res.statusText}`);
    }
    if (res.status === 204) {
      return undefined as T;
    }
    return (await res.json()) as T;
  }

  async healthz(): Promise<{ status: string; protocol_version: number; version: string }> {
    return this.request("GET", "/healthz");
  }

  async listRunners(): Promise<RunnerInfo[]> {
    const j = await this.request<{ runners: RunnerInfo[] }>("GET", "/runners");
    return j.runners ?? [];
  }

  async listTasks(limit = 50, status?: string): Promise<TaskInfo[]> {
    const params = new URLSearchParams({ limit: String(limit) });
    if (status) {
      params.set("status", status);
    }
    const j = await this.request<{ tasks: TaskInfo[] }>("GET", `/tasks?${params.toString()}`);
    return j.tasks ?? [];
  }

  async getTask(id: number): Promise<TaskInfo> {
    return this.request<TaskInfo>("GET", `/tasks/${id}`);
  }

  async dispatch(payload: DispatchPayload): Promise<TaskInfo> {
    return this.request<TaskInfo>("POST", "/tasks", payload);
  }

  async cancel(id: number): Promise<void> {
    await this.request("POST", `/tasks/${id}/cancel`, {});
  }

  /**
   * Stream Server-Sent Events from /tasks/{id}/events. Yields {event, data}
   * tuples until the underlying response ends.
   */
  async *streamEvents(
    id: number,
    signal: AbortSignal
  ): AsyncGenerator<{ event: string; data: string }> {
    const res = await fetch(`${this.baseUrl}/tasks/${id}/events`, {
      headers: { Authorization: `Bearer ${this.token}`, Accept: "text/event-stream" },
      signal,
    });
    if (!res.ok || !res.body) {
      throw new Error(`stream HTTP ${res.status}`);
    }
    const reader = res.body.getReader();
    const decoder = new TextDecoder("utf-8");
    let buffer = "";
    let event = "message";
    let data: string[] = [];
    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        return;
      }
      buffer += decoder.decode(value, { stream: true });
      let idx: number;
      while ((idx = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, idx).replace(/\r$/, "");
        buffer = buffer.slice(idx + 1);
        if (line === "") {
          if (data.length > 0) {
            yield { event, data: data.join("\n") };
          }
          event = "message";
          data = [];
        } else if (line.startsWith("event:")) {
          event = line.slice(6).trim();
        } else if (line.startsWith("data:")) {
          data.push(line.slice(5).replace(/^\s/, ""));
        }
      }
    }
  }
}
