/**
 * ForgeWire VS Code extension entry point.
 *
 * Cross-platform, zero-native-deps. Drives the `forgewire-fabric` Python CLI for
 * "start hub here" / "start runner here" / "install CLI"; talks to the hub
 * REST API directly for read-side views and dispatch.
 */

import * as os from "os";
import * as path from "path";
import * as vscode from "vscode";
import { HubClient } from "./hubClient";
import { HubProvider, RunnersProvider, TasksProvider } from "./treeProviders";

const SECRET_TOKEN_KEY = "forgewireFabric.hubToken";

let outputChannel: vscode.OutputChannel;
let statusItem: vscode.StatusBarItem;
let hubProvider: HubProvider;
let runnersProvider: RunnersProvider;
let tasksProvider: TasksProvider;
let refreshTimer: NodeJS.Timeout | undefined;
let context: vscode.ExtensionContext;

// ---------------------------------------------------------------------------
// activation
// ---------------------------------------------------------------------------

export async function activate(ctx: vscode.ExtensionContext): Promise<void> {
  context = ctx;
  outputChannel = vscode.window.createOutputChannel("ForgeWire Fabric");
  ctx.subscriptions.push(outputChannel);

  statusItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 50);
  statusItem.command = "forgewireFabric.connectHub";
  ctx.subscriptions.push(statusItem);
  updateStatus();

  // Hydrate token from SecretStorage into the live HubClient lookup.
  await hydrateTokenFromSecret();

  hubProvider = new HubProvider(getClient);
  runnersProvider = new RunnersProvider(getClient);
  tasksProvider = new TasksProvider(getClient);
  ctx.subscriptions.push(
    vscode.window.registerTreeDataProvider("forgewireFabric.hub", hubProvider),
    vscode.window.registerTreeDataProvider("forgewireFabric.runners", runnersProvider),
    vscode.window.registerTreeDataProvider("forgewireFabric.tasks", tasksProvider)
  );

  ctx.subscriptions.push(
    vscode.commands.registerCommand("forgewireFabric.installCli", installCli),
    vscode.commands.registerCommand("forgewireFabric.connectHub", connectHub),
    vscode.commands.registerCommand("forgewireFabric.setToken", setToken),
    vscode.commands.registerCommand("forgewireFabric.disconnect", disconnect),
    vscode.commands.registerCommand("forgewireFabric.startHubHere", startHubHere),
    vscode.commands.registerCommand("forgewireFabric.startRunnerHere", startRunnerHere),
    vscode.commands.registerCommand("forgewireFabric.dispatchTask", dispatchTask),
    vscode.commands.registerCommand("forgewireFabric.refresh", refreshAll),
    vscode.commands.registerCommand("forgewireFabric.streamTask", streamTaskCmd),
    vscode.commands.registerCommand("forgewireFabric.cancelTask", cancelTaskCmd),
    vscode.commands.registerCommand("forgewireFabric.showTask", showTaskCmd),
    vscode.commands.registerCommand("forgewireFabric.copyToken", copyToken),
    vscode.commands.registerCommand("forgewireFabric.generateToken", generateToken),
    vscode.commands.registerCommand("forgewireFabric.openSettings", openSettings),
    vscode.commands.registerCommand("forgewireFabric.renameHub", renameHub),
    vscode.commands.registerCommand("forgewireFabric.renameRunner", renameRunner)
  );

  ctx.subscriptions.push(
    vscode.workspace.onDidChangeConfiguration((e) => {
      if (e.affectsConfiguration("forgewireFabric")) {
        updateStatus();
        scheduleRefresh();
        refreshAll();
      }
    })
  );

  scheduleRefresh();
  refreshAll();
}

export function deactivate(): void {
  if (refreshTimer) {
    clearInterval(refreshTimer);
  }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

function getClient(): HubClient | undefined {
  return HubClient.fromConfig();
}

function updateStatus(): void {
  const c = getClient();
  vscode.commands.executeCommand("setContext", "forgewireFabric.connected", !!c);
  if (c) {
    const cfg = vscode.workspace.getConfiguration("forgewireFabric");
    const name = (cfg.get<string>("hubName") ?? "").trim();
    const tag = name ? `${name} (${labelForUrl(c.url)})` : labelForUrl(c.url);
    statusItem.text = `$(plug) ForgeWire Fabric: ${tag}`;
    statusItem.tooltip = new vscode.MarkdownString(
      `Connected to **${c.url}**.\n\nClick to reconnect.`
    );
  } else {
    statusItem.text = "$(debug-disconnect) ForgeWire Fabric";
    statusItem.tooltip = "Click to connect to a ForgeWire hub.";
  }
  statusItem.show();
}

function labelForUrl(url: string): string {
  try {
    const u = new URL(url);
    return u.host;
  } catch {
    return url;
  }
}

function scheduleRefresh(): void {
  if (refreshTimer) {
    clearInterval(refreshTimer);
  }
  const cfg = vscode.workspace.getConfiguration("forgewireFabric");
  const seconds = Math.max(2, cfg.get<number>("refreshIntervalSeconds") ?? 10);
  refreshTimer = setInterval(refreshAll, seconds * 1000);
}

function refreshAll(): void {
  hubProvider?.refresh();
  runnersProvider?.refresh();
  tasksProvider?.refresh();
}

async function hydrateTokenFromSecret(): Promise<void> {
  const cfg = vscode.workspace.getConfiguration("forgewireFabric");
  if ((cfg.get<string>("hubToken") ?? "").trim().length > 0) {
    return;
  }
  const stored = await context.secrets.get(SECRET_TOKEN_KEY);
  if (stored) {
    await cfg.update("hubToken", stored, vscode.ConfigurationTarget.Global);
  }
}

function pythonCommand(): string {
  const cfg = vscode.workspace.getConfiguration("forgewireFabric");
  const explicit = (cfg.get<string>("pythonPath") ?? "").trim();
  if (explicit) {
    return quoteIfNeeded(explicit);
  }
  // Try the official Python extension's selection first.
  const pyCfg = vscode.workspace.getConfiguration("python");
  const fromPy = (pyCfg.get<string>("defaultInterpreterPath") ?? "").trim();
  if (fromPy) {
    return quoteIfNeeded(fromPy);
  }
  return process.platform === "win32" ? "python" : "python3";
}

function quoteIfNeeded(p: string): string {
  return p.includes(" ") && !p.startsWith('"') ? `"${p}"` : p;
}

function getOrCreateTerminal(name: string, env?: Record<string, string>): vscode.Terminal {
  const existing = vscode.window.terminals.find((t) => t.name === name);
  if (existing) {
    return existing;
  }
  return vscode.window.createTerminal({ name, env });
}

// ---------------------------------------------------------------------------
// commands: bootstrap + connection
// ---------------------------------------------------------------------------

async function installCli(): Promise<void> {
  const term = getOrCreateTerminal("ForgeWire Fabric: install");
  term.show();
  term.sendText(`${pythonCommand()} -m pip install --upgrade forgewire-fabric`);
  vscode.window.showInformationMessage(
    "Running `pip install --upgrade forgewire-fabric` in a terminal. Watch progress there."
  );
}

async function connectHub(): Promise<void> {
  const cfg = vscode.workspace.getConfiguration("forgewireFabric");
  const currentUrl = cfg.get<string>("hubUrl") ?? "";
  const url = await vscode.window.showInputBox({
    title: "ForgeWire Fabric Hub URL",
    prompt: "e.g. http://hub.local:8765",
    value: currentUrl,
    ignoreFocusOut: true,
    validateInput: (v) => (/^https?:\/\/.+/i.test(v.trim()) ? null : "Must start with http:// or https://"),
  });
  if (!url) {
    return;
  }
  const token = await vscode.window.showInputBox({
    title: "ForgeWire Fabric Hub Token",
    prompt: "Paste the bearer token (32+ hex chars). Stored in VS Code SecretStorage.",
    password: true,
    ignoreFocusOut: true,
    validateInput: (v) => (v.trim().length >= 16 ? null : "Token must be at least 16 characters"),
  });
  if (!token) {
    return;
  }
  await cfg.update("hubUrl", url.trim(), vscode.ConfigurationTarget.Global);
  await cfg.update("hubToken", token.trim(), vscode.ConfigurationTarget.Global);
  await context.secrets.store(SECRET_TOKEN_KEY, token.trim());

  const client = HubClient.fromConfig();
  if (!client) {
    vscode.window.showErrorMessage("ForgeWire Fabric: failed to construct client.");
    return;
  }
  try {
    const h = await client.healthz();
    vscode.window.showInformationMessage(
      `ForgeWire Fabric: connected (protocol v${h.protocol_version}, hub v${h.version}).`
    );
  } catch (err) {
    vscode.window.showWarningMessage(
      `Saved settings but healthz failed: ${err instanceof Error ? err.message : String(err)}`
    );
  }
  updateStatus();
  refreshAll();
}

async function setToken(): Promise<void> {
  const token = await vscode.window.showInputBox({
    title: "ForgeWire Fabric Hub Token",
    password: true,
    ignoreFocusOut: true,
    validateInput: (v) => (v.trim().length >= 16 ? null : "Token must be at least 16 characters"),
  });
  if (!token) {
    return;
  }
  await vscode.workspace
    .getConfiguration("forgewireFabric")
    .update("hubToken", token.trim(), vscode.ConfigurationTarget.Global);
  await context.secrets.store(SECRET_TOKEN_KEY, token.trim());
  vscode.window.showInformationMessage("ForgeWire Fabric: hub token updated.");
  updateStatus();
  refreshAll();
}

async function disconnect(): Promise<void> {
  const cfg = vscode.workspace.getConfiguration("forgewireFabric");
  await cfg.update("hubUrl", "", vscode.ConfigurationTarget.Global);
  await cfg.update("hubToken", "", vscode.ConfigurationTarget.Global);
  await context.secrets.delete(SECRET_TOKEN_KEY);
  updateStatus();
  refreshAll();
  vscode.window.showInformationMessage("ForgeWire Fabric: disconnected.");
}

async function copyToken(): Promise<void> {
  const cfg = vscode.workspace.getConfiguration("forgewireFabric");
  const t = (cfg.get<string>("hubToken") ?? "").trim();
  if (!t) {
    vscode.window.showWarningMessage("ForgeWire Fabric: no hub token configured.");
    return;
  }
  await vscode.env.clipboard.writeText(t);
  vscode.window.showInformationMessage("ForgeWire Fabric: hub token copied to clipboard.");
}

async function generateToken(): Promise<void> {
  // 32 hex chars (128 bits) via Web Crypto.
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  const tok = Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
  await vscode.env.clipboard.writeText(tok);
  vscode.window.showInformationMessage(
    "ForgeWire Fabric: generated hub token copied to clipboard. Use 'Set Hub Token\u2026' to save it."
  );
}

// ---------------------------------------------------------------------------
// commands: local hub / runner
// ---------------------------------------------------------------------------

async function startHubHere(): Promise<void> {
  const cfg = vscode.workspace.getConfiguration("forgewireFabric");
  const port = await vscode.window.showInputBox({
    title: "Hub port",
    value: String(cfg.get<number>("autoStartHubPort") ?? 8765),
    validateInput: (v) => (/^\d{2,5}$/.test(v) ? null : "Must be a port number"),
  });
  if (!port) {
    return;
  }

  const cfgUrl = (cfg.get<string>("hubUrl") ?? "").trim();
  const cfgToken = (cfg.get<string>("hubToken") ?? "").trim();
  let token = cfgToken;
  if (!token) {
    const ans = await vscode.window.showQuickPick(
      [
        { label: "Generate a new token", value: "gen" },
        { label: "I'll paste one", value: "paste" },
      ],
      { title: "No token configured. How do you want to set one?" }
    );
    if (!ans) {
      return;
    }
    if (ans.value === "gen") {
      const bytes = new Uint8Array(16);
      crypto.getRandomValues(bytes);
      token = Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
      await vscode.env.clipboard.writeText(token);
      vscode.window.showInformationMessage(
        "ForgeWire Fabric: generated token copied to clipboard. Save it somewhere safe."
      );
    } else {
      const t = await vscode.window.showInputBox({
        title: "Hub token",
        password: true,
        validateInput: (v) => (v.trim().length >= 16 ? null : "Min 16 characters"),
      });
      if (!t) {
        return;
      }
      token = t.trim();
    }
  }

  const dbDefault = path.join(os.homedir(), ".forgewire", "hub.sqlite3");
  const dbPath = await vscode.window.showInputBox({
    title: "Hub SQLite path",
    value: dbDefault,
  });
  if (!dbPath) {
    return;
  }

  // Save URL/token so the same VS Code instance can talk to the local hub.
  if (!cfgUrl) {
    await cfg.update(
      "hubUrl",
      `http://127.0.0.1:${port}`,
      vscode.ConfigurationTarget.Global
    );
  }
  await cfg.update("hubToken", token, vscode.ConfigurationTarget.Global);
  await context.secrets.store(SECRET_TOKEN_KEY, token);

  const term = getOrCreateTerminal("ForgeWire Fabric: hub", { FORGEWIRE_HUB_TOKEN: token });
  term.show();
  const py = pythonCommand();
  term.sendText(
    `${py} -m forgewire_fabric.cli hub start --host 0.0.0.0 --port ${port} --db-path "${dbPath}"`
  );
  updateStatus();
  setTimeout(refreshAll, 2500);
}

async function startRunnerHere(): Promise<void> {
  const c = getClient();
  if (!c) {
    vscode.window.showWarningMessage("Connect to a hub first (or use 'Start Hub Here').");
    return;
  }
  const wsRoot = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath ?? os.homedir();
  const workspace = await vscode.window.showInputBox({
    title: "Runner workspace root",
    value: wsRoot,
  });
  if (!workspace) {
    return;
  }
  const tags = await vscode.window.showInputBox({
    title: "Capability tags (comma-separated, optional)",
    placeHolder: "linux,gpu:nvidia,python:3.11",
    value: "",
  });
  const scope = await vscode.window.showInputBox({
    title: "Scope prefixes (comma-separated)",
    placeHolder: "src/,tests/",
    value: "",
  });

  const cfg = vscode.workspace.getConfiguration("forgewireFabric");
  const env: Record<string, string> = {
    FORGEWIRE_HUB_URL: c.url,
    FORGEWIRE_HUB_TOKEN: (cfg.get<string>("hubToken") ?? "").trim(),
  };
  const term = getOrCreateTerminal("ForgeWire Fabric: runner", env);
  term.show();
  const py = pythonCommand();
  const parts = [`${py} -m forgewire_fabric.cli runner start`, `--workspace-root "${workspace}"`];
  if (tags?.trim()) {
    parts.push(`--tags "${tags.trim()}"`);
  }
  if (scope?.trim()) {
    parts.push(`--scope-prefixes "${scope.trim()}"`);
  }
  term.sendText(parts.join(" "));
  setTimeout(refreshAll, 4000);
}

// ---------------------------------------------------------------------------
// commands: dispatch / inspect
// ---------------------------------------------------------------------------

async function dispatchTask(): Promise<void> {
  const c = getClient();
  if (!c) {
    vscode.window.showWarningMessage("Connect to a hub first.");
    return;
  }
  const prompt = await vscode.window.showInputBox({
    title: "ForgeWire Fabric \u00b7 Dispatch \u00b7 prompt",
    prompt: "Shell command (default executor) or sealed brief",
    ignoreFocusOut: true,
  });
  if (!prompt) {
    return;
  }
  const scope = await vscode.window.showInputBox({
    title: "Scope globs (comma-separated)",
    placeHolder: "tests/**,src/foo/**",
    ignoreFocusOut: true,
    validateInput: (v) => (v.trim() ? null : "At least one glob is required"),
  });
  if (!scope) {
    return;
  }
  const branch = await vscode.window.showInputBox({
    title: "Per-task branch",
    value: `agent/${os.hostname().toLowerCase()}/dispatch-${Date.now()}`,
    ignoreFocusOut: true,
  });
  if (!branch) {
    return;
  }
  const baseCommit = await vscode.window.showInputBox({
    title: "Base commit (40-char SHA, or 0\u00d740 for no-op)",
    value: "0".repeat(40),
    ignoreFocusOut: true,
    validateInput: (v) => (/^[0-9a-f]{7,64}$/i.test(v.trim()) ? null : "7\u201364 hex chars"),
  });
  if (!baseCommit) {
    return;
  }
  const title = prompt.length > 60 ? `${prompt.slice(0, 57)}\u2026` : prompt;
  try {
    const t = await c.dispatch({
      title,
      prompt,
      scope_globs: scope.split(",").map((s) => s.trim()).filter(Boolean),
      branch: branch.trim(),
      base_commit: baseCommit.trim(),
    });
    vscode.window
      .showInformationMessage(`Dispatched task #${t.id}.`, "Tail Stream")
      .then((sel) => {
        if (sel === "Tail Stream") {
          streamTaskCmd(t.id);
        }
      });
    refreshAll();
  } catch (err) {
    vscode.window.showErrorMessage(
      `Dispatch failed: ${err instanceof Error ? err.message : String(err)}`
    );
  }
}

async function streamTaskCmd(arg: number | { id: number }): Promise<void> {
  const c = getClient();
  if (!c) {
    return;
  }
  const id = typeof arg === "number" ? arg : arg?.id;
  if (!id) {
    return;
  }
  outputChannel.show(true);
  outputChannel.appendLine(`\n--- streaming task #${id} ---`);
  const ctrl = new AbortController();
  const sub = vscode.workspace.onDidChangeConfiguration(() => {});
  try {
    for await (const ev of c.streamEvents(id, ctrl.signal)) {
      outputChannel.appendLine(`[${ev.event}] ${ev.data}`);
      if (ev.event === "task") {
        try {
          const obj = JSON.parse(ev.data);
          if (obj?.status && ["done", "failed", "cancelled", "timed_out"].includes(obj.status)) {
            outputChannel.appendLine(`--- task #${id} terminal: ${obj.status} ---`);
            break;
          }
        } catch {
          // ignore parse errors; just keep streaming
        }
      }
    }
  } catch (err) {
    outputChannel.appendLine(
      `--- stream error: ${err instanceof Error ? err.message : String(err)} ---`
    );
  } finally {
    sub.dispose();
  }
}

async function cancelTaskCmd(arg: number | { id: number }): Promise<void> {
  const c = getClient();
  if (!c) {
    return;
  }
  const id = typeof arg === "number" ? arg : arg?.id;
  if (!id) {
    return;
  }
  const ok = await vscode.window.showWarningMessage(
    `Cancel task #${id}?`,
    { modal: true },
    "Cancel Task"
  );
  if (ok !== "Cancel Task") {
    return;
  }
  try {
    await c.cancel(id);
    vscode.window.showInformationMessage(`Cancelled task #${id}.`);
    refreshAll();
  } catch (err) {
    vscode.window.showErrorMessage(
      `Cancel failed: ${err instanceof Error ? err.message : String(err)}`
    );
  }
}

async function showTaskCmd(arg: number | { id: number }): Promise<void> {
  const c = getClient();
  if (!c) {
    return;
  }
  const id = typeof arg === "number" ? arg : arg?.id;
  if (!id) {
    return;
  }
  try {
    const t = await c.getTask(id);
    const doc = await vscode.workspace.openTextDocument({
      content: JSON.stringify(t, null, 2),
      language: "json",
    });
    vscode.window.showTextDocument(doc, { preview: true });
  } catch (err) {
    vscode.window.showErrorMessage(
      `Show failed: ${err instanceof Error ? err.message : String(err)}`
    );
  }
}

// ---------------------------------------------------------------------------
// commands: settings panel (role / hub url / token / port / workspace)
// ---------------------------------------------------------------------------

let settingsPanel: vscode.WebviewPanel | undefined;

async function renameHub(): Promise<void> {
  const c = getClient();
  if (!c) {
    vscode.window.showWarningMessage("Connect to a hub first \u2014 hub names are stored on the hub and propagate to all connected nodes.");
    return;
  }
  let current = "";
  try {
    current = (await c.getLabels()).hub_name ?? "";
  } catch {
    /* ignore; allow rename anyway */
  }
  const name = await vscode.window.showInputBox({
    title: "Hub display name (fabric-wide)",
    prompt: "Friendly name for this hub. Leave blank to clear.",
    value: current,
    ignoreFocusOut: true,
    validateInput: (v) => (v.length <= 80 ? null : "Max 80 chars"),
  });
  if (name === undefined) {
    return;
  }
  const trimmed = name.trim();
  const verb = trimmed === "" ? "clear the hub name" : `rename this hub to "${trimmed}"`;
  const ok = await vscode.window.showWarningMessage(
    `This will ${verb} for every node connected to ${labelForUrl(c.url)}.\n\n` +
      `The change is stored on the hub and propagates to all clients on their next refresh. Continue?`,
    { modal: true },
    "Apply Fabric-Wide"
  );
  if (ok !== "Apply Fabric-Wide") {
    return;
  }
  try {
    await c.setHubName(trimmed, os.hostname());
    vscode.window.showInformationMessage(
      trimmed === ""
        ? "Hub name cleared fabric-wide."
        : `Hub renamed to "${trimmed}" fabric-wide.`
    );
    updateStatus();
    refreshAll();
  } catch (err) {
    vscode.window.showErrorMessage(
      `Hub rename failed: ${err instanceof Error ? err.message : String(err)}`
    );
  }
}

async function renameRunner(arg?: { runner_id?: string } | string): Promise<void> {
  const c = getClient();
  if (!c) {
    vscode.window.showWarningMessage("Connect to a hub first \u2014 runner aliases are stored on the hub and propagate to all connected nodes.");
    return;
  }
  let runnerId: string | undefined;
  let runnerHost: string | undefined;
  if (typeof arg === "string") {
    runnerId = arg;
  } else if (arg && typeof arg === "object" && typeof (arg as { runner_id?: string }).runner_id === "string") {
    runnerId = (arg as { runner_id: string }).runner_id;
    runnerHost = (arg as { hostname?: string }).hostname;
  }

  let runners: { runner_id: string; hostname: string }[] = [];
  try {
    runners = (await c.listRunners()) as { runner_id: string; hostname: string }[];
  } catch (err) {
    vscode.window.showErrorMessage(
      `Could not list runners: ${err instanceof Error ? err.message : String(err)}`
    );
    return;
  }
  let labels: Record<string, string> = {};
  try {
    labels = (await c.getLabels()).runner_aliases ?? {};
  } catch {
    /* ignore */
  }

  if (!runnerId) {
    const pick = await vscode.window.showQuickPick(
      runners.map((r) => ({
        label: labels[r.runner_id] || r.hostname || r.runner_id.slice(0, 8),
        description: r.hostname,
        detail: r.runner_id,
        runner_id: r.runner_id,
        hostname: r.hostname,
      })),
      { title: "Pick a runner to rename (fabric-wide)" }
    );
    if (!pick) {
      return;
    }
    runnerId = pick.runner_id;
    runnerHost = pick.hostname;
  } else if (!runnerHost) {
    runnerHost = runners.find((r) => r.runner_id === runnerId)?.hostname;
  }

  const isThisHost = !!runnerHost && runnerHost.toLowerCase() === os.hostname().toLowerCase();
  const currentAlias = labels[runnerId] ?? "";
  const next = await vscode.window.showInputBox({
    title: `Alias for runner ${runnerHost ?? runnerId.slice(0, 8)} (fabric-wide)`,
    prompt: "Friendly name for this runner. Leave blank to clear.",
    value: currentAlias,
    ignoreFocusOut: true,
    validateInput: (v) => (v.length <= 80 ? null : "Max 80 chars"),
  });
  if (next === undefined) {
    return;
  }
  const trimmed = next.trim();
  const target = runnerHost ?? runnerId.slice(0, 8);
  const verb = trimmed === "" ? `clear the alias for ${target}` : `alias ${target} as "${trimmed}"`;
  const sameNodeNote = isThisHost
    ? ""
    : `\n\nNote: you are renaming a runner on a different node (${target}). `;
  const ok = await vscode.window.showWarningMessage(
    `This will ${verb} for every node connected to ${labelForUrl(c.url)}.${sameNodeNote}\n\n` +
      `The change is stored on the hub and propagates to all clients on their next refresh. Continue?`,
    { modal: true },
    "Apply Fabric-Wide"
  );
  if (ok !== "Apply Fabric-Wide") {
    return;
  }
  try {
    await c.setRunnerAlias(runnerId, trimmed, os.hostname());
    vscode.window.showInformationMessage(
      trimmed === ""
        ? `Cleared alias for ${target} fabric-wide.`
        : `Aliased ${target} as "${trimmed}" fabric-wide.`
    );
    refreshAll();
  } catch (err) {
    vscode.window.showErrorMessage(
      `Rename failed: ${err instanceof Error ? err.message : String(err)}`
    );
  }
}

async function openSettings(): Promise<void> {
  if (settingsPanel) {
    settingsPanel.reveal(vscode.ViewColumn.Active);
    return;
  }
  const panel = vscode.window.createWebviewPanel(
    "forgewireFabric.settings",
    "ForgeWire Fabric Settings",
    vscode.ViewColumn.Active,
    { enableScripts: true, retainContextWhenHidden: true }
  );
  settingsPanel = panel;
  panel.onDidDispose(() => {
    settingsPanel = undefined;
  });

  const cfg = vscode.workspace.getConfiguration("forgewireFabric");
  const wsRoot = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath ?? os.homedir();
  const initial = {
    hubUrl: cfg.get<string>("hubUrl") ?? "",
    hubToken: cfg.get<string>("hubToken") ?? "",
    hubTokenFile: cfg.get<string>("hubTokenFile") ?? "",
    pythonPath: cfg.get<string>("pythonPath") ?? "",
    refreshIntervalSeconds: cfg.get<number>("refreshIntervalSeconds") ?? 10,
    autoStartHubPort: cfg.get<number>("autoStartHubPort") ?? 8765,
    workspaceRoot: wsRoot,
  };

  panel.webview.html = settingsHtml(initial);

  panel.webview.onDidReceiveMessage(async (msg) => {
    try {
      if (msg?.type === "save") {
        const c = vscode.workspace.getConfiguration("forgewireFabric");
        await c.update("hubUrl", String(msg.hubUrl ?? "").trim(), vscode.ConfigurationTarget.Global);
        await c.update("hubTokenFile", String(msg.hubTokenFile ?? "").trim(), vscode.ConfigurationTarget.Global);
        await c.update("pythonPath", String(msg.pythonPath ?? "").trim(), vscode.ConfigurationTarget.Global);
        await c.update("refreshIntervalSeconds", Number(msg.refreshIntervalSeconds) || 10, vscode.ConfigurationTarget.Global);
        await c.update("autoStartHubPort", Number(msg.autoStartHubPort) || 8765, vscode.ConfigurationTarget.Global);
        const tok = String(msg.hubToken ?? "").trim();
        if (tok) {
          await c.update("hubToken", tok, vscode.ConfigurationTarget.Global);
          await context.secrets.store(SECRET_TOKEN_KEY, tok);
        }
        vscode.window.showInformationMessage("ForgeWire Fabric: settings saved.");
        updateStatus();
        scheduleRefresh();
        refreshAll();
      } else if (msg?.type === "test") {
        const c = HubClient.fromConfig();
        if (!c) {
          panel.webview.postMessage({ type: "testResult", ok: false, error: "no hub configured" });
          return;
        }
        try {
          const h = await c.healthz();
          panel.webview.postMessage({
            type: "testResult",
            ok: true,
            url: c.url,
            status: h.status,
            version: h.version,
            protocol: h.protocol_version,
          });
        } catch (err) {
          panel.webview.postMessage({
            type: "testResult",
            ok: false,
            error: err instanceof Error ? err.message : String(err),
          });
        }
      } else if (msg?.type === "applySetup") {
        const role = String(msg.role ?? "runner");
        const wsr = String(msg.workspaceRoot ?? "").trim() || (vscode.workspace.workspaceFolders?.[0]?.uri.fsPath ?? os.homedir());
        const url = String(msg.hubUrl ?? "").trim();
        const tok = String(msg.hubToken ?? "").trim();
        const port = Number(msg.autoStartHubPort) || 8765;
        const parts = [
          `${pythonCommand()} -m forgewire_fabric.cli setup`,
          `--role ${role}`,
          `--port ${port}`,
          `--workspace-root "${wsr}"`,
        ];
        if (url) {
          parts.push(`--hub-url "${url}"`);
        }
        if (tok) {
          parts.push(`--hub-token "${tok}"`);
        }
        const term = getOrCreateTerminal("ForgeWire Fabric: setup");
        term.show();
        term.sendText(parts.join(" "));
        vscode.window.showInformationMessage(
          "ForgeWire Fabric: running 'setup' in terminal. Watch for the UAC prompt on Windows."
        );
      } else if (msg?.type === "generateToken") {
        const bytes = new Uint8Array(16);
        crypto.getRandomValues(bytes);
        const tok = Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
        panel.webview.postMessage({ type: "generatedToken", token: tok });
      }
    } catch (err) {
      vscode.window.showErrorMessage(
        `Settings action failed: ${err instanceof Error ? err.message : String(err)}`
      );
    }
  });
}

function settingsHtml(init: Record<string, unknown>): string {
  const json = JSON.stringify(init).replace(/</g, "\\u003c");
  return `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8" />
<style>
  body { font-family: var(--vscode-font-family); color: var(--vscode-foreground); padding: 16px; max-width: 720px; }
  h1 { font-size: 1.4em; margin-top: 0; }
  h2 { font-size: 1.05em; margin-top: 24px; border-bottom: 1px solid var(--vscode-panel-border); padding-bottom: 4px; }
  label { display: block; margin: 12px 0 4px; font-weight: 600; }
  .hint { font-size: 0.85em; color: var(--vscode-descriptionForeground); margin-bottom: 4px; }
  input[type=text], input[type=password], input[type=number], select {
    width: 100%; padding: 6px 8px;
    background: var(--vscode-input-background);
    color: var(--vscode-input-foreground);
    border: 1px solid var(--vscode-input-border, transparent);
    border-radius: 2px;
    box-sizing: border-box;
  }
  .row { display: flex; gap: 8px; }
  .row > * { flex: 1; }
  button {
    margin-top: 12px; padding: 6px 14px;
    background: var(--vscode-button-background);
    color: var(--vscode-button-foreground);
    border: none; border-radius: 2px; cursor: pointer;
  }
  button.secondary {
    background: var(--vscode-button-secondaryBackground);
    color: var(--vscode-button-secondaryForeground);
  }
  button:hover { background: var(--vscode-button-hoverBackground); }
  .actions { margin-top: 20px; display: flex; gap: 8px; flex-wrap: wrap; }
  #result { margin-top: 12px; padding: 8px; border-radius: 2px; white-space: pre-wrap; font-family: var(--vscode-editor-font-family); display: none; }
  #result.ok { background: var(--vscode-testing-iconPassed, #387a3833); }
  #result.err { background: var(--vscode-testing-iconFailed, #aa000033); }
  fieldset { border: 1px solid var(--vscode-panel-border); border-radius: 2px; padding: 8px 12px; }
  fieldset legend { padding: 0 6px; font-weight: 600; }
  .role-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px; }
  .role-grid label { font-weight: 400; display: flex; align-items: center; gap: 6px; margin: 0; }
</style>
</head>
<body>
<h1>ForgeWire Fabric Settings</h1>
<p class="hint">These settings are saved to your VS Code user settings. The token is also written to SecretStorage when you click <strong>Save</strong>.</p>

<h2>Connection</h2>
<label for="hubUrl">Hub URL</label>
<div class="hint">e.g. <code>http://10.120.81.95:8765</code></div>
<input type="text" id="hubUrl" />

<label for="hubToken">Hub token</label>
<div class="hint">32+ hex chars. Saved to SecretStorage. Leave blank to keep the existing one or to read from a token file.</div>
<div class="row">
  <input type="password" id="hubToken" placeholder="(unchanged)" />
  <button class="secondary" id="genBtn" type="button">Generate</button>
</div>

<label for="hubTokenFile">Token file (optional)</label>
<div class="hint">Path read when the token field is empty. Default: <code>~/.forgewire/hub.token</code>.</div>
<input type="text" id="hubTokenFile" />

<h2>Install / role (one-shot setup)</h2>
<p class="hint">Drives <code>forgewire-fabric setup</code> in a terminal. On Windows the installer self-elevates (UAC).</p>
<fieldset>
  <legend>Role</legend>
  <div class="role-grid">
    <label><input type="radio" name="role" value="hub" /> Hub only</label>
    <label><input type="radio" name="role" value="runner" checked /> Runner only</label>
    <label><input type="radio" name="role" value="hub-and-runner" /> Hub + Runner</label>
  </div>
</fieldset>

<label for="workspaceRoot">Runner workspace root</label>
<input type="text" id="workspaceRoot" />

<label for="autoStartHubPort">Hub port</label>
<input type="number" id="autoStartHubPort" min="1" max="65535" />

<h2>Other</h2>
<label for="pythonPath">Python interpreter (optional)</label>
<div class="hint">Empty = auto-detect (uses python.defaultInterpreterPath, then python3, then python).</div>
<input type="text" id="pythonPath" />

<label for="refreshIntervalSeconds">Refresh interval (seconds)</label>
<input type="number" id="refreshIntervalSeconds" min="2" max="600" />

<div class="actions">
  <button id="saveBtn" type="button">Save settings</button>
  <button id="testBtn" class="secondary" type="button">Test connection</button>
  <button id="applyBtn" type="button">Run setup\u2026</button>
</div>

<div id="result"></div>

<script>
  const vscode = acquireVsCodeApi();
  const init = ${json};
  const f = (id) => document.getElementById(id);
  f('hubUrl').value = init.hubUrl || '';
  f('hubTokenFile').value = init.hubTokenFile || '';
  f('pythonPath').value = init.pythonPath || '';
  f('refreshIntervalSeconds').value = init.refreshIntervalSeconds || 10;
  f('autoStartHubPort').value = init.autoStartHubPort || 8765;
  f('workspaceRoot').value = init.workspaceRoot || '';

  function payload() {
    return {
      hubUrl: f('hubUrl').value,
      hubToken: f('hubToken').value,
      hubTokenFile: f('hubTokenFile').value,
      pythonPath: f('pythonPath').value,
      refreshIntervalSeconds: f('refreshIntervalSeconds').value,
      autoStartHubPort: f('autoStartHubPort').value,
      workspaceRoot: f('workspaceRoot').value,
      role: (document.querySelector('input[name=role]:checked') || {}).value || 'runner',
    };
  }

  f('saveBtn').onclick = () => vscode.postMessage(Object.assign({ type: 'save' }, payload()));
  f('testBtn').onclick = () => vscode.postMessage({ type: 'test' });
  f('applyBtn').onclick = () => vscode.postMessage(Object.assign({ type: 'applySetup' }, payload()));
  f('genBtn').onclick = () => vscode.postMessage({ type: 'generateToken' });

  window.addEventListener('message', (ev) => {
    const m = ev.data;
    const r = f('result');
    if (m.type === 'testResult') {
      r.style.display = 'block';
      if (m.ok) {
        r.className = 'ok';
        r.textContent = 'OK \u2014 ' + m.url + '\\nstatus: ' + m.status + '\\nversion: ' + m.version + '\\nprotocol: v' + m.protocol;
      } else {
        r.className = 'err';
        r.textContent = 'Failed: ' + m.error;
      }
    } else if (m.type === 'generatedToken') {
      f('hubToken').value = m.token;
      r.style.display = 'block';
      r.className = 'ok';
      r.textContent = 'Generated 128-bit token. Click Save to persist.';
    }
  });
</script>
</body>
</html>`;
}

