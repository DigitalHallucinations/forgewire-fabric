import * as vscode from "vscode";
import { HubClient, RunnerInfo, TaskInfo } from "./hubClient";

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
  contextValue?: string;
}

export class HubProvider implements vscode.TreeDataProvider<HubNode> {
  private readonly _onDidChange = new vscode.EventEmitter<HubNode | undefined | void>();
  readonly onDidChangeTreeData = this._onDidChange.event;

  constructor(private readonly client: () => HubClient | undefined) {}

  refresh(): void {
    this._onDidChange.fire();
  }

  async getChildren(element?: HubNode): Promise<HubNode[]> {
    if (element) {
      return [];
    }
    const c = this.client();
    const cfg = vscode.workspace.getConfiguration("forgewireFabric");
    let hubName = (cfg.get<string>("hubName") ?? "").trim();

    const renameCmd: vscode.Command = {
      command: "forgewireFabric.renameHub",
      title: "Rename Hub",
    };

    if (!c) {
      return [
        {
          key: "name",
          label: "Name",
          description: hubName || "(unset)",
          icon: "tag",
          tooltip: "Click to set a friendly hub name.",
          command: renameCmd,
          contextValue: "hub.name",
        },
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
          command: { command: "forgewireFabric.openSettings", title: "Open Settings" },
        },
      ];
    }

    const nodes: HubNode[] = [];

    try {
      const labels = await c.getLabels();
      if (labels.hub_name) {
        hubName = labels.hub_name;
      }
    } catch {
      /* ignore */
    }

    nodes.push(
      {
        key: "name",
        label: "Name",
        description: hubName || "(unset)",
        icon: "tag",
        tooltip: "Click to rename this hub fabric-wide.",
        command: renameCmd,
        contextValue: "hub.name",
      },
      {
        key: "url",
        label: "URL",
        description: c.url,
        icon: "link",
        tooltip: c.url,
      }
    );

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
          icon: "versions",
        },
        {
          key: "protocol",
          label: "Protocol",
          description: `v${h.protocol_version}`,
          icon: "symbol-numeric",
        },
        {
          key: "runners",
          label: "Runners",
          description: `${online} online / ${runners.length} total`,
          icon: "server-environment",
          command: { command: "forgewireFabric.refresh", title: "Refresh" },
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
      command: { command: "forgewireFabric.openSettings", title: "Open Settings" },
    });

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
    item.contextValue = n.contextValue ?? `hub.${n.key}`;
    return item;
  }
}

// ---------------------------------------------------------------------------
// Runners (hierarchical: runner -> properties)
// ---------------------------------------------------------------------------

export type RunnerNode =
  | { kind: "runner"; runner: RunnerInfo }
  | { kind: "prop"; runner: RunnerInfo; key: string; label: string; description: string; icon: string };

export class RunnersProvider implements vscode.TreeDataProvider<RunnerNode> {
  private readonly _onDidChange = new vscode.EventEmitter<RunnerNode | undefined | void>();
  readonly onDidChangeTreeData = this._onDidChange.event;

  private aliases: Record<string, string> = {};

  constructor(private readonly client: () => HubClient | undefined) {}

  refresh(): void {
    this._onDidChange.fire();
  }

  async getChildren(element?: RunnerNode): Promise<RunnerNode[]> {
    if (element?.kind === "runner") {
      return runnerProps(element.runner, this.aliases);
    }
    if (element?.kind === "prop") {
      return [];
    }
    const c = this.client();
    if (!c) {
      return [];
    }
    try {
      const [runners, labels] = await Promise.all([
        c.listRunners(),
        c.getLabels().catch(() => ({ hub_name: "", runner_aliases: {} })),
      ]);
      this.aliases = labels.runner_aliases ?? {};
      return runners.map((r) => ({ kind: "runner" as const, runner: r }));
    } catch {
      return [];
    }
  }

  getTreeItem(n: RunnerNode): vscode.TreeItem {
    if (n.kind === "runner") {
      const r = n.runner;
      const alias = this.aliases[r.runner_id];
      const label = alias || r.hostname || r.runner_id.slice(0, 8);
      const item = new vscode.TreeItem(label, vscode.TreeItemCollapsibleState.Collapsed);
      item.id = `runner:${r.runner_id}`;
      item.contextValue = "runner";
      item.description = r.state;
      item.iconPath = new vscode.ThemeIcon(
        r.state === "online" ? "circle-filled" : r.state === "draining" ? "circle-slash" : "circle-outline"
      );
      const tags = (r.tags ?? []).join(", ") || "<no tags>";
      const scopes = (r.scope_prefixes ?? []).join(", ") || "<unscoped>";
      item.tooltip = new vscode.MarkdownString(
        (alias ? `**${alias}**  \u00b7  hostname: ${r.hostname}\n\n` : `**${r.hostname}**\n\n`) +
          `- runner_id: \`${r.runner_id}\`\n- state: ${r.state}\n- os: ${r.os} (${r.arch})\n- tags: ${tags}\n- scope: ${scopes}\n` +
          `- last heartbeat: ${r.last_heartbeat ?? "?"}\n- load: ${r.current_load}/${r.max_concurrent}`
      );
      return item;
    }

    const item = new vscode.TreeItem(n.label, vscode.TreeItemCollapsibleState.None);
    item.id = `runner:${n.runner.runner_id}:${n.key}`;
    item.description = n.description;
    item.iconPath = new vscode.ThemeIcon(n.icon);
    item.contextValue = `runnerProp.${n.key}`;
    return item;
  }
}

function runnerProps(r: RunnerInfo, aliases: Record<string, string>): RunnerNode[] {
  const alias = aliases[r.runner_id];
  const tags = (r.tags ?? []).join(", ") || "<none>";
  const scopes = (r.scope_prefixes ?? []).join(", ") || "<unscoped>";
  const props: RunnerNode[] = [];
  if (alias) {
    props.push({
      kind: "prop",
      runner: r,
      key: "hostname",
      label: "Hostname",
      description: r.hostname,
      icon: "device-desktop",
    });
  }
  props.push(
    {
      kind: "prop",
      runner: r,
      key: "id",
      label: "Runner ID",
      description: r.runner_id,
      icon: "key",
    },
    {
      kind: "prop",
      runner: r,
      key: "load",
      label: "Load",
      description: `${r.current_load}/${r.max_concurrent}`,
      icon: "pulse",
    },
    {
      kind: "prop",
      runner: r,
      key: "os",
      label: "OS / arch",
      description: `${r.os} / ${r.arch}`,
      icon: "device-desktop",
    },
    {
      kind: "prop",
      runner: r,
      key: "tags",
      label: "Tags",
      description: tags,
      icon: "tag",
    },
    {
      kind: "prop",
      runner: r,
      key: "scope",
      label: "Scope",
      description: scopes,
      icon: "folder",
    },
    {
      kind: "prop",
      runner: r,
      key: "heartbeat",
      label: "Last heartbeat",
      description: r.last_heartbeat ?? "?",
      icon: "history",
    }
  );
  return props;
}

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

export type TaskNode =
  | { kind: "task"; task: TaskInfo }
  | { kind: "placeholder"; label: string; icon: string; description?: string };

export class TasksProvider implements vscode.TreeDataProvider<TaskNode> {
  private readonly _onDidChange = new vscode.EventEmitter<TaskNode | undefined | void>();
  readonly onDidChangeTreeData = this._onDidChange.event;

  constructor(private readonly client: () => HubClient | undefined) {}

  refresh(): void {
    this._onDidChange.fire();
  }

  async getChildren(element?: TaskNode): Promise<TaskNode[]> {
    if (element) {
      return [];
    }
    const c = this.client();
    if (!c) {
      return [];
    }
    try {
      const tasks = await c.listTasks(50);
      if (tasks.length === 0) {
        return [
          {
            kind: "placeholder",
            label: "No tasks yet",
            description: "dispatch one to see it here",
            icon: "inbox",
          },
        ];
      }
      return tasks.map((t) => ({ kind: "task" as const, task: t }));
    } catch (err) {
      return [
        {
          kind: "placeholder",
          label: "Hub unreachable",
          description: err instanceof Error ? err.message : String(err),
          icon: "warning",
        },
      ];
    }
  }

  getTreeItem(n: TaskNode): vscode.TreeItem {
    if (n.kind === "placeholder") {
      const item = new vscode.TreeItem(n.label, vscode.TreeItemCollapsibleState.None);
      item.description = n.description;
      item.iconPath = new vscode.ThemeIcon(n.icon);
      item.contextValue = "task.placeholder";
      return item;
    }
    const t = n.task;
    const item = new vscode.TreeItem(`#${t.id}  ${t.title}`, vscode.TreeItemCollapsibleState.None);
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
