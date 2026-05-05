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
import { RunnersProvider, TasksProvider } from "./treeProviders";

const SECRET_TOKEN_KEY = "forgewireFabric.hubToken";

let outputChannel: vscode.OutputChannel;
let statusItem: vscode.StatusBarItem;
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

  runnersProvider = new RunnersProvider(getClient);
  tasksProvider = new TasksProvider(getClient);
  ctx.subscriptions.push(
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
    vscode.commands.registerCommand("forgewireFabric.generateToken", generateToken)
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
  if (c) {
    statusItem.text = `$(plug) ForgeWire Fabric: ${labelForUrl(c.url)}`;
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
