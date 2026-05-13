/**
 * Tiny async client for the ForgeWire hub HTTP API.
 *
 * The Python CLI is the canonical client; the extension only reproduces the
 * read-side surface (list runners / tasks, dispatch, stream tail, cancel).
 * We deliberately use Node's built-in fetch (Node 18+) instead of pulling in
 * a runtime dependency so the published .vsix stays small.
 */

import * as fs from "fs";
import * as os from "os";
import * as path from "path";
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
  workspace_root?: string;
  tenant?: string;
  poll_interval?: number;
  alias?: string;
  host_alias?: string;
  [key: string]: unknown;
}

export interface DispatcherInfo {
  dispatcher_id: string;
  label: string;
  hostname?: string | null;
  metadata: Record<string, unknown>;
  first_seen?: string;
  last_seen?: string;
  [key: string]: unknown;
}

export interface ApprovalInfo {
  approval_id: string;
  status: string;
  envelope_hash?: string;
  task_label?: string;
  branch?: string;
  scope_globs?: string[];
  created_at?: string;
  resolved_at?: string | null;
  approver?: string | null;
  reason?: string | null;
  [key: string]: unknown;
}

export interface SecretInfo {
  name: string;
  version?: number;
  created_at?: string;
  last_rotated_at?: string | null;
  [key: string]: unknown;
}

export interface AuditEvent {
  id?: number;
  task_id?: number | null;
  event_type?: string;
  hash?: string;
  prev_hash?: string;
  created_at?: string;
  payload?: unknown;
  [key: string]: unknown;
}

export interface ClusterHealth {
  backend: string;
  rqlite: { host: string; port: number; consistency: string } | null;
  labels_snapshot: {
    status: string | null;
    applied: number;
    path: string | null;
    exists: boolean;
    size_bytes: number | null;
    mtime: number | null;
  };
}

export type HostRoleName = "hub_head" | "control" | "dispatch" | "command_runner" | "agent_runner";

export interface HostRoleSummary {
  enabled: boolean;
  status: string;
  source: string;
  updated_at?: string | null;
  address?: string;
  runner_ids: string[];
  dispatcher_ids: string[];
  metadata: Record<string, unknown>;
}

export interface HostSummary {
  hostname: string;
  label?: string;
  display_name?: string;
  is_active_hub: boolean;
  roles: Record<HostRoleName, HostRoleSummary>;
  runners: RunnerInfo[];
  dispatchers: DispatcherInfo[];
}

export interface LabelsInfo {
  hub_name: string;
  runner_aliases: Record<string, string>;
  host_aliases: Record<string, string>;
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
  kind?: "agent" | "command";
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
  kind?: "agent" | "command";
}

export class HubClient {
  constructor(private readonly baseUrl: string, private readonly token: string) {}

  static fromConfig(): HubClient | undefined {
    const cfg = vscode.workspace.getConfiguration("forgewireFabric");
    const baseUrl = (cfg.get<string>("hubUrl") ?? "").trim();
    const token = readToken(cfg);
    if (!baseUrl || !token) {
      return undefined;
    }
    return new HubClient(baseUrl.replace(/\/+$/, ""), token);
  }

  /**
   * Probe each candidate URL in priority order (lowest priority first; ties
   * broken by uptime: highest uptime wins). Returns the first reachable
   * candidate plus all probe results for UI display.
   *
   * If `forgewireFabric.hubPin` is set, that URL is returned directly without
   * probing -- this is the manual-override path.
   */
  static async probe(): Promise<{
    active: HubClient | undefined;
    activeUrl: string | undefined;
    pinned: boolean;
    probes: Array<{ url: string; label?: string; priority?: number; ok: boolean; uptime?: number; error?: string }>;
  }> {
    const cfg = vscode.workspace.getConfiguration("forgewireFabric");
    const token = readToken(cfg);
    const pin = (cfg.get<string>("hubPin") ?? "").trim();
    if (pin) {
      const c = token ? new HubClient(pin.replace(/\/+$/, ""), token) : undefined;
      let probe = { url: pin, ok: false } as any;
      if (c) {
        try {
          const h = await c.healthz();
          probe = { url: pin, ok: true, uptime: h.uptime_seconds };
        } catch (err) {
          probe = { url: pin, ok: false, error: String(err) };
        }
      }
      return { active: probe.ok ? c : undefined, activeUrl: probe.ok ? pin : undefined, pinned: true, probes: [probe] };
    }
    const candidates = (cfg.get<Array<{ url: string; label?: string; priority?: number }>>("hubCandidates") ?? []).slice();
    // Back-compat: include hubUrl as an implicit highest-priority candidate.
    const legacy = (cfg.get<string>("hubUrl") ?? "").trim();
    if (legacy && !candidates.find((c) => (c.url ?? "").trim() === legacy)) {
      candidates.push({ url: legacy, label: "default", priority: 100 });
    }
    candidates.sort((a, b) => (a.priority ?? 100) - (b.priority ?? 100));
    const probes: Array<any> = [];
    let active: HubClient | undefined;
    let activeUrl: string | undefined;
    for (const cand of candidates) {
      const url = (cand.url ?? "").trim().replace(/\/+$/, "");
      if (!url || !token) continue;
      const c = new HubClient(url, token);
      try {
        const h = await c.healthz();
        probes.push({ url, label: cand.label, priority: cand.priority, ok: true, uptime: h.uptime_seconds });
        if (!active) {
          active = c;
          activeUrl = url;
        }
      } catch (err) {
        probes.push({ url, label: cand.label, priority: cand.priority, ok: false, error: String(err) });
      }
    }
    return { active, activeUrl, pinned: false, probes };
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

  async healthz(): Promise<{ status: string; protocol_version: number; version: string; uptime_seconds?: number; started_at?: number; host?: string; port?: number }> {
    return this.request("GET", "/healthz");
  }

  async getLabels(): Promise<LabelsInfo> {
    try {
      return await this.request<LabelsInfo>("GET", "/labels");
    } catch {
      return { hub_name: "", runner_aliases: {}, host_aliases: {} };
    }
  }

  async setHubName(name: string, updatedBy?: string): Promise<void> {
    await this.request("PUT", "/labels/hub", { name, updated_by: updatedBy ?? "" });
  }

  async setRunnerAlias(runnerId: string, alias: string, updatedBy?: string): Promise<void> {
    await this.request("PUT", `/labels/runners/${encodeURIComponent(runnerId)}`, {
      alias,
      updated_by: updatedBy ?? "",
    });
  }

  async setHostAlias(hostname: string, alias: string, updatedBy?: string): Promise<void> {
    await this.request("PUT", `/labels/hosts/${encodeURIComponent(hostname)}`, {
      alias,
      updated_by: updatedBy ?? "",
    });
  }

  async drainRunner(runnerId: string): Promise<RunnerInfo> {
    return this.request<RunnerInfo>("POST", `/runners/${encodeURIComponent(runnerId)}/drain-by-dispatcher`, {});
  }

  async undrainRunner(runnerId: string): Promise<RunnerInfo> {
    return this.request<RunnerInfo>("POST", `/runners/${encodeURIComponent(runnerId)}/undrain-by-dispatcher`, {});
  }

  async listRunners(): Promise<RunnerInfo[]> {
    const j = await this.request<{ runners: RunnerInfo[] }>("GET", "/runners");
    return j.runners ?? [];
  }

  async listDispatchers(): Promise<DispatcherInfo[]> {
    const j = await this.request<{ dispatchers: DispatcherInfo[] }>("GET", "/dispatchers");
    return j.dispatchers ?? [];
  }

  async listApprovals(status?: string, limit = 200): Promise<ApprovalInfo[]> {
    const params = new URLSearchParams({ limit: String(limit) });
    if (status) {
      params.set("status", status);
    }
    const j = await this.request<{ approvals: ApprovalInfo[] }>(
      "GET",
      `/approvals?${params.toString()}`
    );
    return j.approvals ?? [];
  }

  async approveApproval(id: string, approver: string, reason?: string): Promise<void> {
    await this.request("POST", `/approvals/${encodeURIComponent(id)}/approve`, {
      approver,
      reason: reason ?? "",
    });
  }

  async denyApproval(id: string, approver: string, reason?: string): Promise<void> {
    await this.request("POST", `/approvals/${encodeURIComponent(id)}/deny`, {
      approver,
      reason: reason ?? "",
    });
  }

  async listSecrets(): Promise<SecretInfo[]> {
    const j = await this.request<{ secrets: SecretInfo[] }>("GET", "/secrets");
    return j.secrets ?? [];
  }

  async auditTail(): Promise<{ chain_tail: unknown }> {
    return this.request("GET", "/audit/tail");
  }

  async auditDay(day: string): Promise<{ day: string; events: AuditEvent[]; verified: boolean; error: string | null }> {
    return this.request("GET", `/audit/day/${encodeURIComponent(day)}`);
  }

  async clusterHealth(): Promise<ClusterHealth> {
    return this.request<ClusterHealth>("GET", "/cluster/health");
  }

  async listHosts(): Promise<HostSummary[]> {
    const j = await this.request<{ hosts: HostSummary[] }>("GET", "/hosts");
    return j.hosts ?? [];
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

function readToken(cfg: vscode.WorkspaceConfiguration): string {
  const configured = (cfg.get<string>("hubToken") ?? "").trim();
  if (configured) {
    return configured;
  }

  const tokenFile = resolveTokenFile((cfg.get<string>("hubTokenFile") ?? "").trim());
  if (!tokenFile) {
    return "";
  }
  try {
    return fs.readFileSync(tokenFile, "utf8").trim();
  } catch {
    return "";
  }
}

function resolveTokenFile(configured: string): string | undefined {
  const candidates = [
    configured,
    process.env.FORGEWIRE_HUB_TOKEN_FILE ?? "",
    path.join(os.homedir(), ".forgewire", "hub.token"),
  ];
  for (const candidate of candidates) {
    const resolved = expandHome(candidate.trim());
    if (resolved && fs.existsSync(resolved)) {
      return resolved;
    }
  }
  return undefined;
}

function expandHome(value: string): string {
  if (!value) {
    return "";
  }
  if (value === "~") {
    return os.homedir();
  }
  if (value.startsWith(`~${path.sep}`) || value.startsWith("~/") || value.startsWith("~\\")) {
    return path.join(os.homedir(), value.slice(2));
  }
  return value;
}
