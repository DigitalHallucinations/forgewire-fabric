import * as os from "os";
import * as vscode from "vscode";
import {
  ApprovalInfo,
  AuditEvent,
  ClusterHealth,
  DispatcherInfo,
  HubClient,
  RunnerInfo,
  SecretInfo,
  TaskInfo,
} from "./hubClient";

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

export interface ProbeInfo {
  active: HubClient | undefined;
  activeUrl: string | undefined;
  pinned: boolean;
  probes: Array<{ url: string; label?: string; priority?: number; ok: boolean; uptime?: number; error?: string }>;
}

export class HubProvider implements vscode.TreeDataProvider<HubNode> {
  private readonly _onDidChange = new vscode.EventEmitter<HubNode | undefined | void>();
  readonly onDidChangeTreeData = this._onDidChange.event;

  constructor(
    private readonly client: () => HubClient | undefined,
    private readonly probe: () => ProbeInfo | undefined = () => undefined
  ) {}

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
        label: "Active hub",
        description: c.url,
        icon: "link",
        tooltip: new vscode.MarkdownString(
          `Currently dispatching to **${c.url}**.\n\n` +
            (this.probe()?.pinned
              ? "_Pinned manually -- failover is disabled until you unpin._"
              : "_Auto-selected by probing the candidate list in priority order._")
        ).value,
        contextValue: this.probe()?.pinned ? "hub.url.pinned" : "hub.url.auto",
      }
    );

    // Failover candidate list (if configured) so the user can see at a glance
    // which peers are reachable and which one was elected.
    const probe = this.probe();
    if (probe && probe.probes.length > 1) {
      nodes.push({
        key: "candidates",
        label: probe.pinned ? "Pinned" : "Failover candidates",
        description: `${probe.probes.filter((p) => p.ok).length} / ${probe.probes.length} reachable`,
        icon: probe.pinned ? "pin" : "list-tree",
        tooltip: new vscode.MarkdownString(
          probe.probes
            .map((p) => {
              const tag = p.ok ? `up ${formatUptime(p.uptime)}` : `down: ${(p.error ?? "").slice(0, 80)}`;
              const star = p.url === probe.activeUrl ? " **(active)**" : "";
              const lab = p.label ? ` _${p.label}_` : "";
              return `- \`${p.url}\` (prio ${p.priority ?? 100})${lab} \u2014 ${tag}${star}`;
            })
            .join("\n")
        ).value,
        contextValue: "hub.candidates",
      });
    }

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
          key: "uptime",
          label: "Uptime",
          description: formatUptime(h.uptime_seconds),
          icon: "watch",
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
      const color = hubIconColor(n.key, n.description, n.icon);
      item.iconPath = color ? new vscode.ThemeIcon(n.icon, color) : new vscode.ThemeIcon(n.icon);
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

function hubIconColor(key: string, description: string | undefined, icon: string): vscode.ThemeColor | undefined {
  if (key === "status") {
    if (description === "ok") {
      return new vscode.ThemeColor("charts.green");
    }
    return new vscode.ThemeColor("charts.red");
  }
  if (key === "state" && icon === "debug-disconnect") {
    return new vscode.ThemeColor("charts.red");
  }
  if (key === "runners" && description) {
    // "<online> online / <total> total"
    const m = /^(\d+)\s+online\s+\/\s+(\d+)/.exec(description);
    if (m) {
      const online = Number(m[1]);
      const total = Number(m[2]);
      if (online === 0) return new vscode.ThemeColor("charts.red");
      if (online < total) return new vscode.ThemeColor("charts.yellow");
      return new vscode.ThemeColor("charts.green");
    }
  }
  return undefined;
}

// ---------------------------------------------------------------------------
// Runners (hierarchical: kind group -> runner -> properties)
//
// Mirrors the Tasks pane taxonomy: every fabric host is expected to expose
// BOTH a command runner (always-on NSSM service, kind:command) and an
// agent runner (interactive Copilot-Chat MCP, kind:agent). The two groups
// are always shown so the architectural split is visible even when the
// agent bucket is empty (e.g. on a headless host with no logged-in VS Code).
// A runner is bucketed by its self-declared `kind:*` tag; runners that
// predate the taxonomy (no kind tag) default to 'command'.
// ---------------------------------------------------------------------------

function bucketRunner(r: RunnerInfo): "agent" | "command" {
  const tags = r.tags ?? [];
  if (tags.includes("kind:agent")) return "agent";
  // Default bucket: missing/unknown kind is treated as 'command' because
  // every pre-taxonomy NSSM runner is a shell-exec command runner.
  return "command";
}

export type RunnerNode =
  | { kind: "group"; group: "agent" | "command"; count: number }
  | { kind: "runner"; runner: RunnerInfo; parent: "agent" | "command" }
  | { kind: "placeholder"; group: "agent" | "command"; label: string; icon: string; description?: string }
  | { kind: "prop"; runner: RunnerInfo; key: string; label: string; description: string; icon: string };

export class RunnersProvider implements vscode.TreeDataProvider<RunnerNode> {
  private readonly _onDidChange = new vscode.EventEmitter<RunnerNode | undefined | void>();
  readonly onDidChangeTreeData = this._onDidChange.event;

  private aliases: Record<string, string> = {};
  private buckets: { agent: RunnerInfo[]; command: RunnerInfo[] } = { agent: [], command: [] };

  constructor(private readonly client: () => HubClient | undefined) {}

  refresh(): void {
    this._onDidChange.fire();
  }

  async getChildren(element?: RunnerNode): Promise<RunnerNode[]> {
    if (element?.kind === "runner") {
      return runnerProps(element.runner, this.aliases);
    }
    if (element?.kind === "prop" || element?.kind === "placeholder") {
      return [];
    }
    if (element?.kind === "group") {
      const bucket = this.buckets[element.group];
      if (bucket.length === 0) {
        const label = element.group === "agent"
          ? "No agent runners online"
          : "No command runners online";
        const description = element.group === "agent"
          ? "open the 'forgewire-runner' chat mode in VS Code"
          : "start the 'ForgeWireRunner' Windows service";
        return [
          {
            kind: "placeholder",
            group: element.group,
            label,
            icon: element.group === "agent" ? "hubot" : "terminal",
            description,
          },
        ];
      }
      return bucket.map((r) => ({
        kind: "runner" as const,
        runner: r,
        parent: element.group,
      }));
    }

    // Top level: load runners + aliases, populate the two buckets.
    const c = this.client();
    if (!c) {
      this.buckets = { agent: [], command: [] };
      return [
        { kind: "group", group: "agent", count: 0 },
        { kind: "group", group: "command", count: 0 },
      ];
    }
    try {
      const [runners, labels] = await Promise.all([
        c.listRunners(),
        c.getLabels().catch(() => ({ hub_name: "", runner_aliases: {} })),
      ]);
      this.aliases = labels.runner_aliases ?? {};
      this.buckets = { agent: [], command: [] };
      for (const r of runners) {
        this.buckets[bucketRunner(r)].push(r);
      }
    } catch {
      this.buckets = { agent: [], command: [] };
    }
    return [
      { kind: "group", group: "agent", count: this.buckets.agent.length },
      { kind: "group", group: "command", count: this.buckets.command.length },
    ];
  }

  getTreeItem(n: RunnerNode): vscode.TreeItem {
    if (n.kind === "group") {
      const label = n.group === "agent" ? "Agent runners" : "Command runners";
      const item = new vscode.TreeItem(
        label,
        vscode.TreeItemCollapsibleState.Expanded
      );
      item.id = `runners.group.${n.group}`;
      item.description = `${n.count}`;
      item.contextValue = `runners.group.${n.group}`;
      if (n.group === "agent") {
        item.iconPath = new vscode.ThemeIcon("hubot", new vscode.ThemeColor("charts.blue"));
        item.tooltip = new vscode.MarkdownString(
          "**Agent runners** — interactive Copilot-Chat MCP sessions. " +
          "Claim `kind:agent` tasks. Not a daemon; opened on demand in VS Code."
        );
      } else {
        item.iconPath = new vscode.ThemeIcon("terminal", new vscode.ThemeColor("charts.purple"));
        item.tooltip = new vscode.MarkdownString(
          "**Command runners** — always-on shell-exec services (NSSM `ForgeWireRunner`). " +
          "Claim `kind:command` tasks."
        );
      }
      return item;
    }

    if (n.kind === "placeholder") {
      const item = new vscode.TreeItem(n.label, vscode.TreeItemCollapsibleState.None);
      item.id = `runners.${n.group}.placeholder`;
      item.iconPath = new vscode.ThemeIcon(n.icon);
      if (n.description) item.description = n.description;
      item.contextValue = `runners.placeholder.${n.group}`;
      return item;
    }

    if (n.kind === "runner") {
      const r = n.runner;
      const alias = this.aliases[r.runner_id];
      const label = alias || r.hostname || r.runner_id.slice(0, 8);
      const item = new vscode.TreeItem(label, vscode.TreeItemCollapsibleState.Collapsed);
      item.id = `runner:${r.runner_id}`;
      const isLocal = !!r.hostname && r.hostname.toLowerCase() === os.hostname().toLowerCase();
      item.contextValue = runnerContext(r, isLocal);
      item.description = isLocal ? `${r.state} \u00b7 this host` : r.state;
      item.iconPath = runnerIcon(r.state, isLocal);
      const tags = (r.tags ?? []).join(", ") || "<no tags>";
      const scopes = (r.scope_prefixes ?? []).join(", ") || "<unscoped>";
      item.tooltip = new vscode.MarkdownString(
        (alias ? `**${alias}**  \u00b7  hostname: ${r.hostname}\n\n` : `**${r.hostname}**\n\n`) +
          (isLocal ? "_(this host)_\n\n" : "") +
          `- runner_id: \`${r.runner_id}\`\n- kind: \`${n.parent}\`\n- state: ${r.state}\n- os: ${r.os} (${r.arch})\n- tags: ${tags}\n- scope: ${scopes}\n` +
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
  if (r.workspace_root) {
    props.push({
      kind: "prop",
      runner: r,
      key: "workspace_root",
      label: "Workspace root",
      description: String(r.workspace_root),
      icon: "root-folder",
    });
  }
  if (r.tenant) {
    props.push({
      kind: "prop",
      runner: r,
      key: "tenant",
      label: "Tenant",
      description: String(r.tenant),
      icon: "organization",
    });
  }
  if (typeof r.poll_interval === "number") {
    props.push({
      kind: "prop",
      runner: r,
      key: "poll_interval",
      label: "Poll interval",
      description: `${r.poll_interval}s`,
      icon: "watch",
    });
  }
  props.push({
    kind: "prop",
    runner: r,
    key: "capacity",
    label: "Max concurrent",
    description: String(r.max_concurrent),
    icon: "dashboard",
  });
  return props;
}

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

export type TaskNode =
  | { kind: "group"; group: "agent" | "command"; count: number }
  | { kind: "task"; task: TaskInfo; parent: "agent" | "command" }
  | { kind: "placeholder"; label: string; icon: string; description?: string };

export class TasksProvider implements vscode.TreeDataProvider<TaskNode> {
  private readonly _onDidChange = new vscode.EventEmitter<TaskNode | undefined | void>();
  readonly onDidChangeTreeData = this._onDidChange.event;

  private cache: { agent: TaskInfo[]; command: TaskInfo[] } = { agent: [], command: [] };

  constructor(private readonly client: () => HubClient | undefined) {}

  refresh(): void {
    this._onDidChange.fire();
  }

  async getChildren(element?: TaskNode): Promise<TaskNode[]> {
    if (element?.kind === "task" || element?.kind === "placeholder") {
      return [];
    }
    const c = this.client();
    if (!c) {
      return [];
    }
    if (!element) {
      try {
        const tasks = await c.listTasks(50);
        this.cache = bucketTasks(tasks);
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
        return [
          { kind: "group", group: "agent", count: this.cache.agent.length },
          { kind: "group", group: "command", count: this.cache.command.length },
        ];
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
    // element is a group node — return its tasks.
    const bucket = this.cache[element.group];
    if (bucket.length === 0) {
      return [
        {
          kind: "placeholder",
          label: element.group === "agent" ? "No agent tasks" : "No command tasks",
          description: undefined,
          icon: "inbox",
        },
      ];
    }
    return bucket.map((t) => ({ kind: "task" as const, task: t, parent: element.group }));
  }

  getTreeItem(n: TaskNode): vscode.TreeItem {
    if (n.kind === "group") {
      const label = n.group === "agent" ? "Agent tasks" : "Command tasks";
      const item = new vscode.TreeItem(label, vscode.TreeItemCollapsibleState.Expanded);
      item.id = `taskgroup:${n.group}`;
      item.description = `${n.count}`;
      item.iconPath = new vscode.ThemeIcon(
        n.group === "agent" ? "hubot" : "terminal",
        new vscode.ThemeColor(n.group === "agent" ? "charts.blue" : "charts.purple")
      );
      item.contextValue = `taskgroup.${n.group}`;
      item.tooltip = new vscode.MarkdownString(
        n.group === "agent"
          ? "Sealed briefs for Copilot-Chat agent runners (chatmode + MCP)."
          : "Shell/script payloads for non-agent (cmd) runners."
      );
      return item;
    }
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
    item.contextValue = `task.${n.parent}`;
    item.description = `${t.status} \u00b7 ${t.branch}`;
    item.iconPath = new vscode.ThemeIcon(statusIcon(t.status));
    item.tooltip = new vscode.MarkdownString(
      `**#${t.id} ${t.title}** \`${t.status}\`\n\n` +
        `- kind: \`${t.kind ?? "agent"}\`\n` +
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

function bucketTasks(tasks: TaskInfo[]): { agent: TaskInfo[]; command: TaskInfo[] } {
  const agent: TaskInfo[] = [];
  const command: TaskInfo[] = [];
  for (const t of tasks) {
    if (t.kind === "command") {
      command.push(t);
    } else {
      // Default bucket: missing/unknown kind is treated as 'agent' so legacy
      // tasks predating the taxonomy still appear under the agent group.
      agent.push(t);
    }
  }
  return { agent, command };
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

function runnerIcon(state: string, isLocal: boolean): vscode.ThemeIcon {
  // Blue dot for "this host" trumps state-color so the user can spot
  // their own machine at a glance. The state still shows in the
  // description text + tooltip.
  if (isLocal) {
    return new vscode.ThemeIcon("circle-filled", new vscode.ThemeColor("charts.blue"));
  }
  switch (state) {
    case "online":
      return new vscode.ThemeIcon("circle-filled", new vscode.ThemeColor("charts.green"));
    case "draining":
      return new vscode.ThemeIcon("debug-pause", new vscode.ThemeColor("charts.yellow"));
    case "degraded":
      return new vscode.ThemeIcon("warning", new vscode.ThemeColor("charts.orange"));
    case "offline":
      return new vscode.ThemeIcon("circle-filled", new vscode.ThemeColor("charts.red"));
    default:
      return new vscode.ThemeIcon("circle-outline", new vscode.ThemeColor("charts.foreground"));
  }
}

function runnerContext(r: RunnerInfo, isLocal: boolean): string {
  // Drives `view/item/context` `when` clauses: e.g. viewItem == runner.online.local
  const state = r.state || "unknown";
  const where = isLocal ? "local" : "remote";
  return `runner.${state}.${where}`;
}

function formatUptime(seconds: number | undefined): string {
  if (seconds === undefined || seconds === null || !isFinite(seconds) || seconds < 0) return "?";
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

// ---------------------------------------------------------------------------
// Hosts (cluster-typed taxonomy: Loom + Fabric, with cluster health)
//
// A "host" is a physical/virtual machine identified by hostname that may
// be running one or more roles (hub, runner, dispatcher). The Hosts view
// groups by cluster type ("Fabric" = rqlite-backed forgewire-fabric;
// "Loom" = reserved for the second backend that's not yet wired) and lets
// the operator see at a glance which boxes are participating and which
// roles each is running.
// ---------------------------------------------------------------------------

export type HostsNode =
  | { kind: "cluster"; cluster: "fabric" | "loom"; label: string; backend: string | null }
  | { kind: "host"; cluster: "fabric" | "loom"; hostname: string; roles: string[]; runners: RunnerInfo[]; dispatchers: DispatcherInfo[] }
  | { kind: "role"; hostname: string; role: string; description: string; tooltip?: string; icon: string }
  | { kind: "health"; key: string; label: string; description: string; icon: string; tooltip?: string; color?: string }
  | { kind: "placeholder"; label: string; description?: string; icon: string };

export class HostsProvider implements vscode.TreeDataProvider<HostsNode> {
  private readonly _onDidChange = new vscode.EventEmitter<HostsNode | undefined | void>();
  readonly onDidChangeTreeData = this._onDidChange.event;

  private fabricHosts: Map<string, { runners: RunnerInfo[]; dispatchers: DispatcherInfo[] }> = new Map();
  private health: ClusterHealth | undefined;

  constructor(private readonly client: () => HubClient | undefined) {}

  refresh(): void {
    this._onDidChange.fire();
  }

  async getChildren(element?: HostsNode): Promise<HostsNode[]> {
    const c = this.client();
    if (!element) {
      // Top level: cluster groups
      return [
        { kind: "cluster", cluster: "fabric", label: "Fabric", backend: this.health?.backend ?? null },
        { kind: "cluster", cluster: "loom", label: "Loom", backend: null },
      ];
    }
    if (element.kind === "cluster") {
      if (element.cluster === "loom") {
        return [
          {
            kind: "placeholder",
            label: "No Loom cluster configured",
            description: "reserved for substrate backend",
            icon: "circle-slash",
          },
        ];
      }
      // Fabric: load runners + dispatchers + cluster health
      if (!c) {
        return [
          { kind: "placeholder", label: "Not connected", icon: "debug-disconnect" },
        ];
      }
      const nodes: HostsNode[] = [];
      try {
        const [runners, dispatchers, health] = await Promise.all([
          c.listRunners().catch(() => [] as RunnerInfo[]),
          c.listDispatchers().catch(() => [] as DispatcherInfo[]),
          c.clusterHealth().catch(() => undefined as ClusterHealth | undefined),
        ]);
        this.health = health;
        this.fabricHosts = aggregateHosts(runners, dispatchers);
        // Cluster Health sub-section first (always visible).
        nodes.push(...healthNodes(health));
        // Then one node per discovered host.
        for (const [hostname, agg] of this.fabricHosts) {
          const roles: string[] = [];
          if (agg.runners.length) roles.push(`runner${agg.runners.length > 1 ? `\u00d7${agg.runners.length}` : ""}`);
          if (agg.dispatchers.length) roles.push(`dispatcher${agg.dispatchers.length > 1 ? `\u00d7${agg.dispatchers.length}` : ""}`);
          nodes.push({
            kind: "host",
            cluster: "fabric",
            hostname,
            roles,
            runners: agg.runners,
            dispatchers: agg.dispatchers,
          });
        }
        if (this.fabricHosts.size === 0) {
          nodes.push({
            kind: "placeholder",
            label: "No hosts registered",
            description: "no runners or dispatchers have reported in",
            icon: "inbox",
          });
        }
      } catch (err) {
        nodes.push({
          kind: "placeholder",
          label: "Hub unreachable",
          description: err instanceof Error ? err.message : String(err),
          icon: "warning",
        });
      }
      return nodes;
    }
    if (element.kind === "host") {
      const out: HostsNode[] = [];
      for (const r of element.runners) {
        out.push({
          kind: "role",
          hostname: element.hostname,
          role: "runner",
          description: `${r.state} \u00b7 ${r.runner_id.slice(0, 8)}`,
          icon: "server",
          tooltip: `Runner ${r.runner_id}`,
        });
      }
      for (const d of element.dispatchers) {
        out.push({
          kind: "role",
          hostname: element.hostname,
          role: "dispatcher",
          description: `${d.label} \u00b7 ${d.dispatcher_id.slice(0, 8)}`,
          icon: "rocket",
          tooltip: `Dispatcher ${d.dispatcher_id} (last seen ${d.last_seen ?? "?"})`,
        });
      }
      return out;
    }
    return [];
  }

  getTreeItem(n: HostsNode): vscode.TreeItem {
    if (n.kind === "cluster") {
      const item = new vscode.TreeItem(n.label, vscode.TreeItemCollapsibleState.Expanded);
      item.id = `hosts:cluster:${n.cluster}`;
      item.iconPath = new vscode.ThemeIcon(n.cluster === "fabric" ? "circuit-board" : "globe");
      const badge = n.backend ? n.backend : n.cluster === "loom" ? "n/a" : "?";
      item.description = badge;
      item.contextValue = `hosts.cluster.${n.cluster}`;
      item.tooltip =
        n.cluster === "fabric"
          ? `ForgeWire Fabric (rqlite/sqlite backend). Active: ${n.backend ?? "unknown"}.`
          : "Loom: substrate cluster (forgewire_core). Not yet wired into the hub.";
      return item;
    }
    if (n.kind === "host") {
      const item = new vscode.TreeItem(n.hostname, vscode.TreeItemCollapsibleState.Collapsed);
      item.id = `hosts:host:${n.cluster}:${n.hostname}`;
      const isLocal = n.hostname.toLowerCase() === os.hostname().toLowerCase();
      item.description = n.roles.join(" + ") + (isLocal ? " \u00b7 this host" : "");
      item.iconPath = new vscode.ThemeIcon(
        "device-desktop",
        isLocal ? new vscode.ThemeColor("charts.blue") : undefined
      );
      item.contextValue = `hosts.host.${n.cluster}`;
      item.tooltip = `${n.hostname} \u2014 ${n.runners.length} runner(s), ${n.dispatchers.length} dispatcher(s)`;
      return item;
    }
    if (n.kind === "role") {
      const item = new vscode.TreeItem(n.role, vscode.TreeItemCollapsibleState.None);
      item.id = `hosts:role:${n.hostname}:${n.role}:${n.description}`;
      item.description = n.description;
      item.iconPath = new vscode.ThemeIcon(n.icon);
      item.contextValue = `hosts.role.${n.role}`;
      if (n.tooltip) item.tooltip = n.tooltip;
      return item;
    }
    if (n.kind === "health") {
      const item = new vscode.TreeItem(n.label, vscode.TreeItemCollapsibleState.None);
      item.id = `hosts:health:${n.key}`;
      item.description = n.description;
      item.iconPath = n.color
        ? new vscode.ThemeIcon(n.icon, new vscode.ThemeColor(n.color))
        : new vscode.ThemeIcon(n.icon);
      if (n.tooltip) item.tooltip = n.tooltip;
      item.contextValue = `hosts.health.${n.key}`;
      return item;
    }
    const item = new vscode.TreeItem(n.label, vscode.TreeItemCollapsibleState.None);
    item.description = n.description;
    item.iconPath = new vscode.ThemeIcon(n.icon);
    item.contextValue = "hosts.placeholder";
    return item;
  }
}

function aggregateHosts(
  runners: RunnerInfo[],
  dispatchers: DispatcherInfo[]
): Map<string, { runners: RunnerInfo[]; dispatchers: DispatcherInfo[] }> {
  const map = new Map<string, { runners: RunnerInfo[]; dispatchers: DispatcherInfo[] }>();
  const get = (h: string) => {
    let agg = map.get(h);
    if (!agg) {
      agg = { runners: [], dispatchers: [] };
      map.set(h, agg);
    }
    return agg;
  };
  for (const r of runners) {
    const h = (r.hostname || "(unknown)").trim() || "(unknown)";
    get(h).runners.push(r);
  }
  for (const d of dispatchers) {
    const h = (d.hostname || "(unknown)").toString().trim() || "(unknown)";
    get(h).dispatchers.push(d);
  }
  return map;
}

function healthNodes(health: ClusterHealth | undefined): HostsNode[] {
  if (!health) {
    return [
      { kind: "health", key: "status", label: "Cluster health", description: "unknown", icon: "question" },
    ];
  }
  const nodes: HostsNode[] = [];
  nodes.push({
    kind: "health",
    key: "backend",
    label: "Backend",
    description: health.backend,
    icon: "database",
    color: health.backend === "rqlite" ? "charts.green" : "charts.yellow",
    tooltip:
      health.backend === "rqlite"
        ? `rqlite cluster ${health.rqlite?.host}:${health.rqlite?.port} (consistency=${health.rqlite?.consistency})`
        : "Legacy single-node sqlite backend.",
  });
  const s = health.labels_snapshot;
  const sidecarColor =
    s.status === "applied" || s.status === "seeded_from_db"
      ? "charts.green"
      : s.status === "absent" || s.status === "disabled"
        ? "charts.yellow"
        : "charts.red";
  const ageStr = s.mtime
    ? formatUptime(Math.max(0, Math.floor(Date.now() / 1000 - s.mtime)))
    : "n/a";
  nodes.push({
    kind: "health",
    key: "sidecar",
    label: "Labels sidecar",
    description: `${s.status ?? "?"} \u00b7 age ${ageStr}`,
    icon: s.exists ? "save" : "warning",
    color: sidecarColor,
    tooltip: new vscode.MarkdownString(
      `**Labels snapshot sidecar**\n\n` +
        `- path: \`${s.path ?? "(disabled)"}\`\n` +
        `- exists: ${s.exists}\n` +
        `- bytes: ${s.size_bytes ?? "n/a"}\n` +
        `- last applied: ${s.applied} row(s)\n` +
        `- status: \`${s.status ?? "?"}\``
    ).value,
  });
  if (health.rqlite) {
    nodes.push({
      kind: "health",
      key: "rqlite",
      label: "rqlite",
      description: `${health.rqlite.host}:${health.rqlite.port} \u00b7 ${health.rqlite.consistency}`,
      icon: "broadcast",
    });
  }
  return nodes;
}

// ---------------------------------------------------------------------------
// Dispatchers
// ---------------------------------------------------------------------------

export type DispatcherNode =
  | { kind: "dispatcher"; dispatcher: DispatcherInfo }
  | { kind: "prop"; dispatcher: DispatcherInfo; key: string; label: string; description: string; icon: string }
  | { kind: "placeholder"; label: string; description?: string; icon: string };

export class DispatchersProvider implements vscode.TreeDataProvider<DispatcherNode> {
  private readonly _onDidChange = new vscode.EventEmitter<DispatcherNode | undefined | void>();
  readonly onDidChangeTreeData = this._onDidChange.event;

  constructor(private readonly client: () => HubClient | undefined) {}

  refresh(): void {
    this._onDidChange.fire();
  }

  async getChildren(element?: DispatcherNode): Promise<DispatcherNode[]> {
    if (element?.kind === "dispatcher") {
      const d = element.dispatcher;
      const props: DispatcherNode[] = [
        { kind: "prop", dispatcher: d, key: "id", label: "Dispatcher ID", description: d.dispatcher_id, icon: "key" },
        { kind: "prop", dispatcher: d, key: "hostname", label: "Hostname", description: d.hostname ?? "?", icon: "device-desktop" },
        { kind: "prop", dispatcher: d, key: "last_seen", label: "Last seen", description: d.last_seen ?? "?", icon: "history" },
        { kind: "prop", dispatcher: d, key: "first_seen", label: "First seen", description: d.first_seen ?? "?", icon: "calendar" },
      ];
      for (const [k, v] of Object.entries(d.metadata ?? {})) {
        props.push({
          kind: "prop",
          dispatcher: d,
          key: `meta.${k}`,
          label: k,
          description: typeof v === "string" ? v : JSON.stringify(v),
          icon: "info",
        });
      }
      return props;
    }
    if (element?.kind === "prop" || element?.kind === "placeholder") {
      return [];
    }
    const c = this.client();
    if (!c) {
      return [{ kind: "placeholder", label: "Not connected", icon: "debug-disconnect" }];
    }
    try {
      const dispatchers = await c.listDispatchers();
      if (dispatchers.length === 0) {
        return [
          {
            kind: "placeholder",
            label: "No dispatchers registered",
            description: "dispatchers register on first dispatch",
            icon: "inbox",
          },
        ];
      }
      return dispatchers.map((d) => ({ kind: "dispatcher" as const, dispatcher: d }));
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

  getTreeItem(n: DispatcherNode): vscode.TreeItem {
    if (n.kind === "dispatcher") {
      const d = n.dispatcher;
      const item = new vscode.TreeItem(d.label || d.dispatcher_id.slice(0, 8), vscode.TreeItemCollapsibleState.Collapsed);
      item.id = `dispatcher:${d.dispatcher_id}`;
      item.description = d.hostname ?? "";
      item.iconPath = new vscode.ThemeIcon("rocket");
      item.contextValue = "dispatcher";
      item.tooltip = `dispatcher_id: ${d.dispatcher_id}\nhost: ${d.hostname ?? "?"}\nlast_seen: ${d.last_seen ?? "?"}`;
      return item;
    }
    if (n.kind === "prop") {
      const item = new vscode.TreeItem(n.label, vscode.TreeItemCollapsibleState.None);
      item.id = `dispatcher:${n.dispatcher.dispatcher_id}:${n.key}`;
      item.description = n.description;
      item.iconPath = new vscode.ThemeIcon(n.icon);
      item.contextValue = `dispatcherProp.${n.key}`;
      return item;
    }
    const item = new vscode.TreeItem(n.label, vscode.TreeItemCollapsibleState.None);
    item.description = n.description;
    item.iconPath = new vscode.ThemeIcon(n.icon);
    item.contextValue = "dispatcher.placeholder";
    return item;
  }
}

// ---------------------------------------------------------------------------
// Approvals (M2.5.1 human-in-the-loop)
// ---------------------------------------------------------------------------

export type ApprovalNode =
  | { kind: "approval"; approval: ApprovalInfo }
  | { kind: "placeholder"; label: string; description?: string; icon: string };

export class ApprovalsProvider implements vscode.TreeDataProvider<ApprovalNode> {
  private readonly _onDidChange = new vscode.EventEmitter<ApprovalNode | undefined | void>();
  readonly onDidChangeTreeData = this._onDidChange.event;

  constructor(private readonly client: () => HubClient | undefined) {}

  refresh(): void {
    this._onDidChange.fire();
  }

  async getChildren(element?: ApprovalNode): Promise<ApprovalNode[]> {
    if (element) return [];
    const c = this.client();
    if (!c) return [{ kind: "placeholder", label: "Not connected", icon: "debug-disconnect" }];
    try {
      const approvals = await c.listApprovals("pending", 100);
      if (approvals.length === 0) {
        return [{ kind: "placeholder", label: "No pending approvals", icon: "check", description: "queue is clear" }];
      }
      return approvals.map((a) => ({ kind: "approval" as const, approval: a }));
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

  getTreeItem(n: ApprovalNode): vscode.TreeItem {
    if (n.kind === "placeholder") {
      const item = new vscode.TreeItem(n.label, vscode.TreeItemCollapsibleState.None);
      item.description = n.description;
      item.iconPath = new vscode.ThemeIcon(n.icon);
      item.contextValue = "approval.placeholder";
      return item;
    }
    const a = n.approval;
    const item = new vscode.TreeItem(a.task_label || a.approval_id.slice(0, 12), vscode.TreeItemCollapsibleState.None);
    item.id = `approval:${a.approval_id}`;
    item.description = `${a.status} \u00b7 ${a.branch ?? ""}`;
    item.iconPath = new vscode.ThemeIcon(
      a.status === "pending" ? "circle-large-outline" : a.status === "approved" ? "check" : "circle-slash",
      a.status === "pending" ? new vscode.ThemeColor("charts.yellow") : undefined
    );
    item.contextValue = `approval.${a.status}`;
    item.tooltip = new vscode.MarkdownString(
      `**${a.task_label ?? a.approval_id}**\n\n` +
        `- approval_id: \`${a.approval_id}\`\n` +
        `- status: \`${a.status}\`\n` +
        `- branch: \`${a.branch ?? "?"}\`\n` +
        `- scope: \`${(a.scope_globs ?? []).join(", ")}\`\n` +
        `- created: ${a.created_at ?? "?"}\n` +
        (a.resolved_at ? `- resolved: ${a.resolved_at} by ${a.approver ?? "?"}\n` : "") +
        (a.reason ? `- reason: ${a.reason}\n` : "")
    ).value;
    return item;
  }
}

// ---------------------------------------------------------------------------
// Audit log (M2.5.3 hash-chained audit)
// ---------------------------------------------------------------------------

export type AuditNode =
  | { kind: "header"; label: string; description: string; icon: string; tooltip?: string }
  | { kind: "event"; event: AuditEvent }
  | { kind: "placeholder"; label: string; description?: string; icon: string };

export class AuditProvider implements vscode.TreeDataProvider<AuditNode> {
  private readonly _onDidChange = new vscode.EventEmitter<AuditNode | undefined | void>();
  readonly onDidChangeTreeData = this._onDidChange.event;

  constructor(private readonly client: () => HubClient | undefined) {}

  refresh(): void {
    this._onDidChange.fire();
  }

  async getChildren(element?: AuditNode): Promise<AuditNode[]> {
    if (element) return [];
    const c = this.client();
    if (!c) return [{ kind: "placeholder", label: "Not connected", icon: "debug-disconnect" }];
    try {
      const tail = await c.auditTail().catch(() => ({ chain_tail: null }));
      const today = new Date().toISOString().slice(0, 10);
      const day = await c.auditDay(today).catch(() => ({ day: today, events: [] as AuditEvent[], verified: false, error: "unavailable" }));
      const nodes: AuditNode[] = [
        {
          kind: "header",
          label: "Chain tail",
          description: typeof (tail as any).chain_tail === "string"
            ? ((tail as any).chain_tail as string).slice(0, 16) + "\u2026"
            : "n/a",
          icon: "key",
          tooltip: typeof (tail as any).chain_tail === "string" ? (tail as any).chain_tail : "no audit events yet",
        },
        {
          kind: "header",
          label: `Today (${today})`,
          description: `${day.events.length} event(s) \u00b7 verified=${day.verified}`,
          icon: day.verified ? "verified" : "warning",
          tooltip: day.error ? `verification error: ${day.error}` : `${day.events.length} events on ${today}`,
        },
      ];
      const recent = (day.events ?? []).slice(-25).reverse();
      for (const e of recent) {
        nodes.push({ kind: "event", event: e });
      }
      if (recent.length === 0) {
        nodes.push({ kind: "placeholder", label: "No events today", icon: "inbox" });
      }
      return nodes;
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

  getTreeItem(n: AuditNode): vscode.TreeItem {
    if (n.kind === "header") {
      const item = new vscode.TreeItem(n.label, vscode.TreeItemCollapsibleState.None);
      item.description = n.description;
      item.iconPath = new vscode.ThemeIcon(n.icon);
      if (n.tooltip) item.tooltip = n.tooltip;
      item.contextValue = "audit.header";
      return item;
    }
    if (n.kind === "placeholder") {
      const item = new vscode.TreeItem(n.label, vscode.TreeItemCollapsibleState.None);
      item.description = n.description;
      item.iconPath = new vscode.ThemeIcon(n.icon);
      item.contextValue = "audit.placeholder";
      return item;
    }
    const e = n.event;
    const label = e.event_type ?? "event";
    const item = new vscode.TreeItem(label, vscode.TreeItemCollapsibleState.None);
    item.id = `audit:${e.id ?? Math.random()}`;
    item.description = `task=${e.task_id ?? "-"} \u00b7 ${e.created_at ?? "?"}`;
    item.iconPath = new vscode.ThemeIcon("note");
    item.contextValue = "audit.event";
    item.tooltip = new vscode.MarkdownString(
      `**${label}**\n\n` +
        `- task_id: ${e.task_id ?? "?"}\n` +
        `- hash: \`${(e.hash ?? "").slice(0, 24)}\u2026\`\n` +
        `- prev: \`${(e.prev_hash ?? "").slice(0, 24)}\u2026\`\n` +
        `- created: ${e.created_at ?? "?"}\n` +
        (e.payload ? "\n```json\n" + JSON.stringify(e.payload, null, 2).slice(0, 800) + "\n```" : "")
    ).value;
    return item;
  }
}

// ---------------------------------------------------------------------------
// Secrets (M2.5.5a sealed broker -- metadata only, never values)
// ---------------------------------------------------------------------------

export type SecretNode =
  | { kind: "secret"; secret: SecretInfo }
  | { kind: "placeholder"; label: string; description?: string; icon: string };

export class SecretsProvider implements vscode.TreeDataProvider<SecretNode> {
  private readonly _onDidChange = new vscode.EventEmitter<SecretNode | undefined | void>();
  readonly onDidChangeTreeData = this._onDidChange.event;

  constructor(private readonly client: () => HubClient | undefined) {}

  refresh(): void {
    this._onDidChange.fire();
  }

  async getChildren(element?: SecretNode): Promise<SecretNode[]> {
    if (element) return [];
    const c = this.client();
    if (!c) return [{ kind: "placeholder", label: "Not connected", icon: "debug-disconnect" }];
    try {
      const secrets = await c.listSecrets();
      if (secrets.length === 0) {
        return [{ kind: "placeholder", label: "No secrets stored", description: "use the CLI to seal one", icon: "lock" }];
      }
      return secrets.map((s) => ({ kind: "secret" as const, secret: s }));
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

  getTreeItem(n: SecretNode): vscode.TreeItem {
    if (n.kind === "placeholder") {
      const item = new vscode.TreeItem(n.label, vscode.TreeItemCollapsibleState.None);
      item.description = n.description;
      item.iconPath = new vscode.ThemeIcon(n.icon);
      item.contextValue = "secret.placeholder";
      return item;
    }
    const s = n.secret;
    const item = new vscode.TreeItem(s.name, vscode.TreeItemCollapsibleState.None);
    item.id = `secret:${s.name}`;
    item.description = `v${s.version ?? 1}`;
    item.iconPath = new vscode.ThemeIcon("lock", new vscode.ThemeColor("charts.green"));
    item.contextValue = "secret";
    item.tooltip = new vscode.MarkdownString(
      `**${s.name}** (sealed)\n\n` +
        `- version: ${s.version ?? 1}\n` +
        `- created: ${s.created_at ?? "?"}\n` +
        `- last_rotated: ${s.last_rotated_at ?? "never"}\n\n` +
        `_Values are never exposed via the API._`
    ).value;
    return item;
  }
}

// ---------------------------------------------------------------------------
// Labels (cosmetic hub name + per-runner aliases; inline rename)
// ---------------------------------------------------------------------------

export type LabelNode =
  | { kind: "hub"; name: string }
  | { kind: "alias"; runnerId: string; alias: string }
  | { kind: "placeholder"; label: string; description?: string; icon: string };

export class LabelsProvider implements vscode.TreeDataProvider<LabelNode> {
  private readonly _onDidChange = new vscode.EventEmitter<LabelNode | undefined | void>();
  readonly onDidChangeTreeData = this._onDidChange.event;

  constructor(private readonly client: () => HubClient | undefined) {}

  refresh(): void {
    this._onDidChange.fire();
  }

  async getChildren(element?: LabelNode): Promise<LabelNode[]> {
    if (element) return [];
    const c = this.client();
    if (!c) return [{ kind: "placeholder", label: "Not connected", icon: "debug-disconnect" }];
    try {
      const labels = await c.getLabels();
      const nodes: LabelNode[] = [{ kind: "hub", name: labels.hub_name || "" }];
      for (const [rid, alias] of Object.entries(labels.runner_aliases ?? {})) {
        nodes.push({ kind: "alias", runnerId: rid, alias });
      }
      if (Object.keys(labels.runner_aliases ?? {}).length === 0) {
        nodes.push({
          kind: "placeholder",
          label: "No runner aliases",
          description: "right-click a runner to set one",
          icon: "info",
        });
      }
      return nodes;
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

  getTreeItem(n: LabelNode): vscode.TreeItem {
    if (n.kind === "hub") {
      const item = new vscode.TreeItem("Hub name", vscode.TreeItemCollapsibleState.None);
      item.id = "label:hub";
      item.description = n.name || "(unset)";
      item.iconPath = new vscode.ThemeIcon("tag");
      item.contextValue = "label.hub";
      item.command = { command: "forgewireFabric.renameHub", title: "Rename Hub" };
      item.tooltip = "Click to rename the hub fabric-wide.";
      return item;
    }
    if (n.kind === "alias") {
      const item = new vscode.TreeItem(n.alias || "(unset)", vscode.TreeItemCollapsibleState.None);
      item.id = `label:alias:${n.runnerId}`;
      item.description = n.runnerId.slice(0, 8);
      item.iconPath = new vscode.ThemeIcon("symbol-string");
      item.contextValue = "label.alias";
      item.tooltip = `Runner ${n.runnerId}\nClick to rename via the Runners view.`;
      return item;
    }
    const item = new vscode.TreeItem(n.label, vscode.TreeItemCollapsibleState.None);
    item.description = n.description;
    item.iconPath = new vscode.ThemeIcon(n.icon);
    item.contextValue = "label.placeholder";
    return item;
  }
}