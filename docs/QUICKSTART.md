# ForgeWire Quickstart

A 5-minute path from zero to a working hub + runner controlled from your laptop.

ForgeWire is a *signed task fabric*. One machine runs the **hub** (a FastAPI
service that owns the task graph). Any number of other machines run **runners**
that claim tasks scoped to a path glob and stream output back. You **dispatch**
tasks from a CLI (or VS Code, see `docs/vscode.md`) on any machine that can
reach the hub.

> **Threat model**: every register / claim / heartbeat is ed25519-signed by
> the runner; the hub validates a shared bearer token on every request.
> Anyone with the token can dispatch tasks, so treat it like an SSH key.

---

## 1. Install

On every machine that will be a hub, runner, or dispatcher:

```bash
pip install forgewire-fabric
```

(Optional) for LAN auto-discovery, install the `mdns` extra:

```bash
pip install "forgewire[mdns]"
```

(Optional) for the Rust acceleration of the claim router and stream counters,
install the runtime extension:

```bash
pip install forgewire-runtime
```

The pure-Python implementation is the fallback and is always functional —
the runtime only matters once you have hundreds of concurrent runners.

---

## 2. Pick a hub host

This is the machine the others will connect to. Any always-on box on your
network works (a NUC, a homelab box, a desktop that's always plugged in).

### 2a. Generate a token (once)

```bash
forgewire token gen > hub.token
```

Save this somewhere safe and share it (over a secure channel) with anyone
who needs to dispatch tasks or run runners against this hub. **Anyone with
this token can issue tasks to your cluster.**

### 2b. Start the hub

```powershell
# Windows
$env:FORGEWIRE_HUB_TOKEN = (Get-Content hub.token).Trim()
forgewire-fabric hub start --host 0.0.0.0 --port 8765
```

```bash
# Linux / macOS
export FORGEWIRE_HUB_TOKEN=$(cat hub.token)
forgewire-fabric hub start --host 0.0.0.0 --port 8765
```

Verify from another shell:

```bash
export FORGEWIRE_HUB_URL=http://<hub-host>:8765
export FORGEWIRE_HUB_TOKEN=$(cat hub.token)
forgewire-fabric hub healthz
# → {"status":"ok","protocol_version":2,...}
```

To run the hub as a long-lived service, see
[`docs/operations/service-install.md`](operations/service-install.md) for
NSSM (Windows) and systemd (Linux) recipes.

---

## 3. Add runners

On every worker machine, set the same env vars and start the runner:

```bash
export FORGEWIRE_HUB_URL=http://<hub-host>:8765
export FORGEWIRE_HUB_TOKEN=$(cat hub.token)

forgewire-fabric runner start \
  --workspace-root /path/to/your/repo \
  --scope-prefixes "src/,tests/" \
  --tags "linux,gpu:nvidia,python:3.11"
```

The runner generates an ed25519 identity on first launch
(`~/.forgewire/runner_identity.json`, or `%USERPROFILE%\.forgewire\` on
Windows) and registers it with the hub. Subsequent restarts reuse the same
identity, so its `runner_id` stays stable.

`--scope-prefixes` is the safety belt: a runner will only claim tasks whose
`scope_globs` are entirely within one of the prefixes you list. Tasks that
ask for `infra/**` are invisible to a runner that only declared
`scope-prefixes=src/,tests/`.

Confirm registration from anywhere:

```bash
forgewire-fabric runners list
```

---

## 4. Dispatch your first task

From any machine with the token (your laptop is fine):

```bash
forgewire-fabric dispatch "pytest tests/smoke -x" \
  --scope "tests/smoke/**" \
  --branch "agent/laptop/smoke-1" \
  --base-commit $(git rev-parse origin/main)
```

Watch it run:

```bash
forgewire-fabric tasks list                # see queued/running/done
forgewire-fabric tasks show 1              # full envelope incl. result
forgewire-fabric tasks stream 1            # tail SSE: stdout/stderr line by line
```

The default executor runs the prompt as a shell command in the runner's
`--workspace-root`. Custom executors (e.g. driving an orchestrator) plug in
via `forgewire_fabric.runner.run_runner(executor=...)`.

---

## 5. (Optional) Use the VS Code extension

A cross-platform GUI lives in [`vscode/`](../vscode). It works on Windows,
macOS, and Linux — anywhere VS Code runs.

For now, install from the packaged VSIX (marketplace listing pending):

```bash
# from a clone of the repo
cd vscode
npm install
npm run package
code --install-extension forgewire-0.1.0.vsix
```

Then in VS Code:

1. Open the **ForgeWire** activity bar item.
2. Run **ForgeWire: Connect to Hub** (Ctrl+Shift+P) and paste your URL + token.
   The token is stored in VS Code SecretStorage.
3. Browse runners + tasks live in the sidebar. Use **ForgeWire: Dispatch
   Task** to send work; right-click a task to **Tail Task Stream**.

If a machine doesn't have the CLI installed yet, run **ForgeWire: Install /
Update CLI** to `pip install --upgrade forgewire` against the Python
interpreter VS Code already knows about. From there, **Start Hub Here** or
**Start Runner Here** will turn that machine into a hub or runner without
ever leaving the editor.

See [`vscode/README.md`](../vscode/README.md) for the full command and
settings reference.

---

## Next steps

* [Architecture overview](architecture.md) — how dispatch, claim, scope, and
  signed envelopes fit together.
* [Operations: production hub install](operations/service-install.md) —
  NSSM, systemd, TLS termination, backup.
* [Federation](architecture/federation.md) — connecting hubs across NAT
  boundaries (Phase 3 overlay transport).
* [Security model](architecture/security.md) — signed envelopes, replay
  protection, scope enforcement.

---

## Environment variables

| Variable | Purpose |
| --- | --- |
| `FORGEWIRE_HUB_URL` | Hub base URL for clients (`http://host:port`). |
| `FORGEWIRE_HUB_TOKEN` | Bearer token shared with the hub. |
| `FORGEWIRE_HUB_TOKEN_FILE` | Path to a file containing the token (alternative to `_TOKEN`). |
| `FORGEWIRE_HUB_DISCOVER` | If `1`, browse mDNS for a hub when `_URL` is unset (requires `mdns` extra). |
| `FORGEWIRE_HUB_HOST` / `FORGEWIRE_HUB_PORT` | Default bind for `forgewire-fabric hub start`. |
| `FORGEWIRE_HUB_DB_PATH` | SQLite path for the hub's task graph (default `~/.forgewire/hub.sqlite3`). |
| `FORGEWIRE_RUNNER_WORKSPACE_ROOT` | Working tree for the runner. |
| `FORGEWIRE_RUNNER_TAGS` | Comma-separated runner capability tags. |
| `FORGEWIRE_RUNNER_SCOPE_PREFIXES` | Comma-separated path prefixes the runner accepts. |
| `FORGEWIRE_RUNNER_MAX_CONCURRENT` | Max concurrent tasks (default 1). |
| `FORGEWIRE_RUNNER_POLL_INTERVAL` | Seconds between empty-claim polls (default 5.0). |

Legacy `BLACKBOARD_*` aliases are still accepted for backwards compatibility
with PhrenForge integrations.

## Signed dispatch (M2.4)

By default, the hub accepts dispatches authenticated only by the bearer token.
You can additionally require **per-dispatcher ed25519 envelope signatures** so
that even a leaked bearer token cannot impersonate an arbitrary developer.

### One-time dispatcher setup

On each developer workstation:

```bash
forgewire keys init-dispatcher --label "$(hostname)"
```

This writes `~/.forgewire/dispatcher_identity.json` (mode 0o600 on POSIX). The
first `forgewire-fabric dispatch` call after this auto-registers the public key with
the hub; subsequent dispatches sign the immutable fields of the envelope
(`op`, `dispatcher_id`, `title`, `prompt`, `scope_globs`, `base_commit`,
`branch`, `timestamp`, `nonce`) and include the signature.

### Enforcing signed dispatch on the hub

To reject unsigned dispatches entirely, start the hub with:

```bash
forgewire-fabric hub start --require-signed-dispatch
# or
FORGEWIRE_HUB_REQUIRE_SIGNED_DISPATCH=1 forgewire-fabric hub start
```

When this flag is on, `POST /tasks` returns `426 Upgrade Required` and
clients must use `POST /tasks/v2`.

### Inspecting registered dispatchers

```bash
forgewire dispatchers list
```

