import * as vscode from "vscode";
import { HubClient, RunnerInfo, TaskInfo } from "./hubClient";

abstract class BaseProvider<T> implements vscode.TreeDataProvider<T> {
  private readonly _onDidChange = new vscode.EventEmitter<T | undefined | void>();
  readonly onDidChangeTreeData = this._onDidChange.event;

  protected items: T[] = [];
  protected error: string | undefined;

  refresh(): void {
    this._onDidChange.fire();
  }

  abstract getTreeItem(element: T): vscode.TreeItem;
  abstract getChildren(element?: T): Promise<T[]>;
}

// ---------------------------------------------------------------------------
// Runners
// ---------------------------------------------------------------------------

export class RunnersProvider extends BaseProvider<RunnerInfo> {
  constructor(private readonly client: () => HubClient | undefined) {
    super();
  }

  async getChildren(): Promise<RunnerInfo[]> {
    const c = this.client();
    if (!c) {
      return [];
    }
    try {
      this.items = await c.listRunners();
      this.error = undefined;
    } catch (err) {
      this.error = err instanceof Error ? err.message : String(err);
      this.items = [];
    }
    return this.items;
  }

  getTreeItem(r: RunnerInfo): vscode.TreeItem {
    const label = `${r.hostname || r.runner_id.slice(0, 8)}  \u00b7  ${r.state}`;
    const item = new vscode.TreeItem(label, vscode.TreeItemCollapsibleState.None);
    item.id = `runner:${r.runner_id}`;
    item.contextValue = "runner";
    item.description = `${r.current_load}/${r.max_concurrent}  ${r.os}/${r.arch}`;
    item.iconPath = new vscode.ThemeIcon(
      r.state === "online" ? "circle-filled" : r.state === "draining" ? "circle-slash" : "circle-outline"
    );
    const tags = (r.tags ?? []).join(", ") || "<no tags>";
    const scopes = (r.scope_prefixes ?? []).join(", ") || "<unscoped>";
    item.tooltip = new vscode.MarkdownString(
      `**${r.hostname}** \`${r.runner_id}\`\n\n` +
        `- state: ${r.state}\n- os: ${r.os} (${r.arch})\n- tags: ${tags}\n- scope: ${scopes}\n` +
        `- last heartbeat: ${r.last_heartbeat ?? "?"}\n- load: ${r.current_load}/${r.max_concurrent}`
    );
    return item;
  }
}

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

export class TasksProvider extends BaseProvider<TaskInfo> {
  constructor(private readonly client: () => HubClient | undefined) {
    super();
  }

  async getChildren(): Promise<TaskInfo[]> {
    const c = this.client();
    if (!c) {
      return [];
    }
    try {
      this.items = await c.listTasks(50);
      this.error = undefined;
    } catch (err) {
      this.error = err instanceof Error ? err.message : String(err);
      this.items = [];
    }
    return this.items;
  }

  getTreeItem(t: TaskInfo): vscode.TreeItem {
    const item = new vscode.TreeItem(
      `#${t.id}  ${t.title}`,
      vscode.TreeItemCollapsibleState.None
    );
    item.id = `task:${t.id}`;
    item.contextValue = "task";
    item.description = `${t.status} \u00b7 ${t.branch}`;
    item.iconPath = new vscode.ThemeIcon(statusIcon(t.status));
    item.tooltip = new vscode.MarkdownString(
      `**#${t.id} ${t.title}** \`${t.status}\`\n\n` +
        `- branch: \`${t.branch}\`\n- base: \`${t.base_commit?.slice(0, 12)}\`\n` +
        `- scope: \`${(t.scope_globs ?? []).join(", ")}\`\n` +
        `- worker: ${t.worker_id ?? "_unassigned_"}\n- created: ${t.created_at ?? "?"}\n` +
        (t.result?.error ? `\n**error:** ${t.result.error}\n` : "")
    );
    item.command = {
      command: "forgewireFabric.showTask",
      title: "Show Task",
      arguments: [t.id],
    };
    return item;
  }
}

function statusIcon(s: string): string {
  switch (s) {
    case "queued":
      return "clock";
    case "running":
      return "loading~spin";
    case "done":
      return "check";
    case "failed":
      return "error";
    case "cancelled":
      return "circle-slash";
    case "timed_out":
      return "warning";
    default:
      return "circle-outline";
  }
}
