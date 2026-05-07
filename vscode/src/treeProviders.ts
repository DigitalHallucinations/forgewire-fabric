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
// Hub
// ---------------------------------------------------------------------------

export interface HubNode {
  key: string;
  label: string;
  description?: string;
  icon?: string;
  tooltip?: string;
  command?: vscode.Command;
}

export class HubProvider implements vscode.TreeDataProvider<HubNode> {
  private readonly _onDidChange = new vscode.EventEmitter<HubNode | undefined | void>();
  readonly onDidChangeTreeData = this._onDidChange.event;

  private cached: HubNode[] = [];

  constructor(private readonly client: () => HubClient | undefined) {}

  refresh(): void {
    this._onDidChange.fire();
  }

  async getChildren(element?: HubNode): Promise<HubNode[]> {
    if (element) {
      return [];
    }
    const c = this.client();
    if (!c) {
      this.cached = [
        {
          key: "state",
          label: "Not connected",
          icon: "debug-disconnect",
          description: "click to connect",
          command: {
            command: "forgewireFabric.connectHub",
            title: "Connect to Hub",
          },
        },
        {
          key: "settings",
          label: "Open Settings\u2026",
          icon: "gear",
          command: {
            command: "forgewireFabric.openSettings",
            title: "Open Settings",
          },
        },
      ];
      return this.cached;
    }

    const nodes: HubNode[] = [
      {
        key: "url",
        label: "URL",
        description: c.url,
        icon: "link",
        tooltip: c.url,
      },
    ];

    try {
      const h = await c.healthz();
      const runners = await c.listRunners().catch(() => [] as RunnerInfo[]);
      const online = runners.filter((r) => r.state === "online").length;
      nodes.push(
        {
          key: "status",
          label: "Status",
          description: h.status,
          icon: h.status === "ok" ? "pass-filled" : "warning",
        },
        {
          key: "version",
          label: "Hub version",
          description: h.version,
          icon: "tag",
        },
        {
          key: "protocol",
          label: "Protocol",
          description: `v${h.protocol_version}`,
          icon: "versions",
        },
        {
          key: "runners",
          label: "Runners",
          description: `${online} online / ${runners.length} total`,
          icon: "server-environment",
          command: {
            command: "forgewireFabric.refresh",
            title: "Refresh",
          },
        }
      );
    } catch (err) {
      nodes.push({
        key: "status",
        label: "Status",
        description: "unreachable",
        icon: "error",
        tooltip: err instanceof Error ? err.message : String(err),
      });
    }

    nodes.push({
      key: "settings",
      label: "Settings\u2026",
      icon: "gear",
      command: {
        command: "forgewireFabric.openSettings",
        title: "Open Settings",
      },
    });

    this.cached = nodes;
    return nodes;
  }

  getTreeItem(n: HubNode): vscode.TreeItem {
    const item = new vscode.TreeItem(n.label, vscode.TreeItemCollapsibleState.None);
    item.id = `hub:${n.key}`;
    item.description = n.description;
    if (n.icon) {
      item.iconPath = new vscode.ThemeIcon(n.icon);
    }
    if (n.tooltip) {
      item.tooltip = n.tooltip;
    }
    if (n.command) {
      item.command = n.command;
    }
    item.contextValue = `hub.${n.key}`;
    return item;
  }
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
