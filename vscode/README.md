# ForgeWire VS Code Extension

> Drive a [ForgeWire](https://github.com/DigitalHallucinations/forgewire) compute fabric from inside VS Code. Cross-platform (Windows / macOS / Linux). Apache-2.0.

ForgeWire turns any cluster of machines into a signed, scope-bounded task fabric. This extension is the GUI for it: connect to a hub, watch runners and tasks live, dispatch sealed briefs, and tail per-task stream output — all without leaving the editor. It can also start a hub or a runner locally with two clicks, so any computer running VS Code can join a cluster regardless of OS.

## Features

- **Activity-bar sidebar** with two tree views:
  - **Runners**: every registered runner with hostname, OS/arch, capability tags, scope prefixes, current load, and online/draining/offline state.
  - **Tasks**: recent dispatches with status icons, branch, and per-task actions (tail stream, cancel, show JSON).
- **Status bar item** showing the active hub host. Click to (re)connect.
- **Dispatch quick-pick**: prompt → scope globs → branch → base-commit → sent. The hub's terminal status is reported back; the extension can immediately start tailing the SSE stream into an output channel.
- **Local hub control**: *ForgeWire: Start Hub Here* runs `forgewire hub start` in a managed terminal with a freshly generated token (saved to VS Code SecretStorage).
- **Local runner control**: *ForgeWire: Start Runner Here* registers the current machine as a runner against the connected hub. Workspace root, tags, and scope prefixes are prompted with sensible defaults from the open folder.
- **CLI bootstrap**: *ForgeWire: Install / Update CLI* runs `pip install --upgrade forgewire` in your selected Python interpreter — no terminal commands required.
- **Token utilities**: generate cryptographically-random hub tokens; copy the active token to clipboard.
- **Auto-refresh**: runners and tasks views poll every N seconds (configurable, default 10s).

## Requirements

- VS Code **1.85+**.
- A **Python 3.10+** interpreter available on the machine (used only for the local hub/runner commands and the install bootstrap; you can drive a remote hub without any Python on this host).
- For local hub/runner: the `forgewire` Python package. The extension can install it for you on first use.

## Setup (90 seconds)

1. Install the extension.
2. Run **ForgeWire: Connect to Hub** from the command palette and paste the URL + bearer token shared by your hub operator. The token is stored in VS Code [SecretStorage](https://code.visualstudio.com/api/references/vscode-api#SecretStorage).
3. The sidebar now shows live runners and tasks. **ForgeWire: Dispatch Task** sends work; **Tail Task Stream** (right-click on a task) streams its output into the *ForgeWire* output channel.

### Joining a cluster from a fresh machine

If you've just installed VS Code on a new box and want to make it a runner:

1. Install the extension.
2. Run **ForgeWire: Install / Update CLI** (one click). Wait for `pip` to finish in the terminal.
3. Run **ForgeWire: Connect to Hub** with the cluster's URL + token.
4. Run **ForgeWire: Start Runner Here**. Pick the workspace root (defaults to the open folder), tags, and scope prefixes.

That's it — the runner is online and the hub will route matching tasks to it.

### Standing up a hub from a fresh machine

1. Install the extension on the host you want to be the hub.
2. Run **ForgeWire: Install / Update CLI**.
3. Run **ForgeWire: Start Hub Here**. Pick a port (default 8765); the extension generates a random token, copies it to your clipboard, and saves it.
4. Share the URL `http://<this-host>:<port>` and the token with anyone joining the cluster.

## Settings

| Setting | Default | Description |
| --- | --- | --- |
| `forgewire.hubUrl` | `""` | Base URL of the hub. |
| `forgewire.hubToken` | `""` | Bearer token. Prefer the *Set Hub Token* command (uses SecretStorage). |
| `forgewire.pythonPath` | `""` | Python interpreter used for `pip install` / local hub / local runner. Empty = auto-detect. |
| `forgewire.refreshIntervalSeconds` | `10` | Tree-view refresh cadence. |
| `forgewire.autoStartHubPort` | `8765` | Default port for *Start Hub Here*. |

## Commands

All commands are under the **ForgeWire** category in the command palette:

- `Connect to Hub…` / `Set Hub Token…` / `Disconnect`
- `Install / Update CLI`
- `Start Hub Here…` / `Start Runner Here…`
- `Dispatch Task…`
- `Refresh`
- `Tail Task Stream` / `Cancel Task` / `Show Task` (also available from the right-click menu in the Tasks view)
- `Generate New Hub Token` / `Copy Hub Token to Clipboard`

## Security

- The hub token is saved in VS Code SecretStorage when set via *Set Hub Token* or *Connect to Hub*. Avoid putting it in your settings JSON if you sync settings.
- The extension talks to the hub over plain HTTP by default. **Always put a TLS-terminating proxy (Caddy, nginx, Traefik, or a tunnel like Tailscale Funnel) in front of any hub exposed beyond your trusted LAN.**
- Generated tokens are 128-bit and produced via `crypto.getRandomValues()`.

## Limitations

- The extension **does not** verify hub TLS certificates differently from Node's defaults; for self-signed certs, route through a proxy or local DNS that already trusts the cert.
- Auto-discovery via mDNS is not yet wired into the extension; use the URL field directly.
- Task creation does not currently sign the dispatch envelope at the dispatcher level; rely on the hub's bearer-token auth (the runner half of the protocol is fully signed).

## Reporting issues

Please file issues at <https://github.com/DigitalHallucinations/forgewire/issues>. Include the extension version, VS Code version, and the OS of the failing machine.
