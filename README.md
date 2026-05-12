# ForgeWire Fabric

> **Bring-your-own-compute for AI-assisted development.**
>
> ForgeWire Fabric is a work-graph-native control plane for trusted machines,
> local agents, editor workflows, and operator-owned compute.

ForgeWire Fabric turns machines you already control into a private execution
fabric. VS Code, MCP clients, automation, and larger ForgeWire systems can send
scoped work to trusted runners using signed dispatch envelopes, capability-aware
claim routing, structured streams, and auditable results.

The point is not to hide SSH behind a nicer button. The point is to make a pile
of laptops, desktops, GPU boxes, homelab nodes, and future fleet hosts behave
like one accountable execution surface: signed, observable, policy-aware, and
owned by the operator.

This repository is the standalone Fabric implementation: the hub server, runner
agent, CLI, installer scripts, VS Code extension, Python package, and Rust
acceleration crates.

---

## The Bet

Agentic systems are systems first and models second. Model quality matters, but
the hard deployment problems are trust boundaries, routing, failure recovery,
operator control, auditability, and performance across messy real hardware.

ForgeWire Fabric exists for the performance that is already on the table:

- The workstation beside you that can run tests while your laptop stays free.
- The always-on box that can host the hub and keep the fleet alive.
- The GPU machine that should receive GPU-shaped work without hard-coded IPs.
- The office or homelab cluster that should not require Kubernetes just to let
  agents use it safely.
- The future federated fleet that should not depend on a third-party cloud as
  the runtime trust boundary.

Fabric's current job is the control plane: authenticate the work, decide which
runner may claim it, stream what happened, persist the result, and leave an
audit trail. The scheduler, transport, and fleet layers grow from that spine.

---

## What Ships Today

**Status:** `0.11.x` active alpha, pip-installable as `forgewire-fabric`.

| Surface | Current state |
| --- | --- |
| Hub | FastAPI task graph, runner registry, streams, results, labels, approvals, audit endpoints, SQLite WAL by default, optional rqlite backend. |
| Runner | Long-lived claim loop with persistent ed25519 identity, scope prefixes, capability tags, max-concurrency guard, sidecar config, and stable identity import/export. |
| Dispatch | Bearer-token auth plus protocol-v2 signed dispatch envelopes, nonce replay protection, base commit, branch, scope globs, required tags/tools, tenant, priority, timeout. |
| Routing | Capability, tool, tenant, scope, drain, and concurrency gates before claim. Unsatisfied queued work can be inspected with `tasks waiting`. |
| Streams | Append-only task streams over SSE with monotonic sequence counters and terminal `done`, `failed`, `cancelled`, or `timed_out` results. |
| Operator controls | CLI groups for `approvals`, `audit`, `replay`, `labels`, `secrets`, `dispatchers`, `runners`, `tasks`, `hub`, `runner`, `setup`, and `mcp`. |
| Service install | Windows `forgewire-fabric setup` installs NSSM services plus runner and hub watchdogs. Linux systemd and macOS launchd service assets are present as basic installers. |
| VS Code | Cross-OS extension in [`vscode/`](vscode): connect to a hub, browse runners/tasks, dispatch work, tail streams, start a hub or runner locally. |
| Rust acceleration | Optional `forgewire-runtime` PyO3 extension for envelope crypto, claim routing, and stream sequence counters; pure-Python fallback remains supported. |

The public Python package is `forgewire_fabric`. The command-line entry point is
`forgewire-fabric`.

---

## What It Is Not

ForgeWire Fabric is not yet a full distributed compute runtime, DAG scheduler,
GPU residency manager, model-sharding layer, or replacement for Ray, Nomad,
Kubernetes, Slurm, or Dask.

Today it sends one scoped task to one eligible runner. That restraint is
intentional. The project is building the trust, routing, audit, and operator
substrate first, because any real heterogeneous compute layer needs those
properties before it can safely get clever.

---

## Architecture At A Glance

```text
Dispatcher surfaces
  VS Code extension  |  CLI  |  MCP servers  |  automation
          |
          v
ForgeWire Fabric hub
  signed dispatch intake
  task graph + runner registry
  capability/scope/operator gates
  streams + results + audit chain
          |
          v
Trusted runners
  persistent identity
  capability advertisement
  workspace-scoped execution
  structured stream/result reporting
```

The same work-graph vocabulary carries through every profile:

- **Dispatch envelope:** signed task intent: prompt, branch, base commit, scope
  globs, required tags/tools, tenant, timestamp, nonce.
- **Hub:** the state machine that validates auth, records tasks, evaluates who
  may claim, mediates streams, and persists results.
- **Runner:** a trusted worker that advertises capabilities, claims matching
  work, executes inside a configured workspace, and reports back.
- **Scope globs:** the filesystem boundary for a task.
- **Capability tags/tools:** the first routing language: what this task needs,
  and what this runner can honestly do.
- **Audit chain:** tamper-evident hub-side events that make dispatch, replay,
  and operator review concrete instead of vibes.

Inside the larger ForgeWire ecosystem, Fabric is the remote execution bridge.
ForgeWire keeps local planning, blackboard state, tools, personas, and workflow
logic; Fabric carries sealed work to remote runners and returns telemetry and
results. Standalone, Fabric is useful on its own as a private developer compute
fabric controlled from the editor or CLI.

---

## Performance Direction

Fabric is designed around a simple performance rule: fast paths are welcome only
when the portable path stays correct. The Rust crates accelerate hot loops, but
the Python fallback remains part of the contract.

Measured on a Windows 11 Precision 5520 with Python 3.11.15 and Rust 1.95.0,
the optional `forgewire-runtime` path currently shows:

| Hot path | Rust path | Python path | Result |
| --- | ---: | ---: | ---: |
| Canonical envelope bytes | 25.05 us/op | 51.29 us/op | 2.05x faster |
| Sign envelope | 385.99 us/op | 1018.43 us/op | 2.64x faster |
| Verify envelope | 316.98 us/op | 1106.38 us/op | 3.49x faster |
| Claim router, typical queue | 37-45 us/op | 67-72 us/op | 1.6-1.86x faster |
| Stream counter only | ~1.1M ops/sec | ~310k ops/sec | ~3.5x faster |

The honest part: end-to-end stream append is currently SQLite fsync-bound, so
the counter win is hidden until stream batching lands. That is the pattern here:
measure the real bottleneck, keep parity, move the hot path when it matters.

See [PERFORMANCE.md](PERFORMANCE.md) for benchmark methodology and caveats.

---

## Resilience

Windows is the most complete OOTB service profile today. `forgewire-fabric setup`
installs NSSM services and the watchdog stack in one pass:

- **Runner watchdog:** probes the hub's view of this host. If the runner's
  heartbeat goes stale, it restarts the local runner service. This catches the
  class of failures where the process is alive but the asyncio loop is wedged.
- **Hub watchdog:** probes `/healthz`. On the hub host it restarts locally; on
  peer hosts it can restart the hub over SSH with a SYSTEM-owned key.

Validate a Windows node with:

```powershell
Get-ScheduledTaskInfo -TaskName ForgeWireRunnerWatchdog, ForgeWireHubWatchdog |
  Select-Object TaskName, LastRunTime, LastTaskResult
```

`LastTaskResult` should be `0`. Full service detail lives in
[docs/operations/service-install.md](docs/operations/service-install.md) and
the scripts under [`scripts/install/`](scripts/install/).

---

## Quickstart

### Windows service path

```powershell
pip install forgewire-fabric

# Hub host, also running a local runner.
forgewire-fabric setup --role hub-and-runner `
  --port 8765 --bind-host 0.0.0.0

# Peer runner. The SSH triplet enables cross-host hub restart from this node.
forgewire-fabric setup --role runner `
  --hub-url http://<hub>:8765 `
  --workspace-root C:\path\to\repo `
  --hub-ssh-host <hub-host-or-ip> `
  --hub-ssh-user <user-on-hub> `
  --hub-ssh-key-file C:\Users\<you>\.ssh\id_ed25519_forgewire

# Dispatch from any machine with the hub token.
forgewire-fabric dispatch "pytest -x" --scope "tests/**" `
  --branch agent/laptop/smoke --base-commit (git rev-parse origin/main)
forgewire-fabric tasks list
forgewire-fabric tasks stream <id>
```

Pass `--no-hub-watchdog` on a runner that should not participate in hub
failover. On `--role hub-and-runner`, cross-host watchdog setup is suppressed
because the local hub watchdog already covers that host.

### Foreground path on Linux/macOS

```bash
pip install forgewire-fabric
forgewire-fabric token gen > hub.token
export FORGEWIRE_HUB_TOKEN=$(cat hub.token)

forgewire-fabric hub start --host 0.0.0.0 --port 8765
forgewire-fabric runner start --workspace-root /path/to/repo
```

For the longer path, see [docs/QUICKSTART.md](docs/QUICKSTART.md).

---

## VS Code And MCP

The extension in [`vscode/`](vscode) makes VS Code the control surface: connect
to a hub, set the token, view runners and tasks, dispatch work, tail streams,
and start a local hub or runner.

```bash
cd vscode
npm install
npm run package
code --install-extension forgewire-fabric-0.1.7.vsix
```

The CLI can also wire VS Code user-scope MCP entries:

```bash
forgewire-fabric mcp install --hub-url http://127.0.0.1:8765
forgewire-fabric mcp install --hub-url http://127.0.0.1:8765 --with-runner --workspace-root /path/to/repo
```

See [vscode/README.md](vscode/README.md) for extension commands and settings.

---

## Roadmap: From Remote Dispatch To Work-Aware Fabric

The roadmap is deliberately layered. Each phase has to ship standalone value;
later phases deepen the same work graph instead of replacing it.

| Layer | Intent | Status |
| --- | --- | --- |
| Standalone control plane | Hub, runner, signed dispatch, streams, results, service install, VS Code surface. | Active alpha in this repo. |
| Operator control plane | Policy gates, approvals, cost ledger, audit replay, richer capability routing, egress allowlists, secret broker, role-separated identity, dashboard. | Partly landed; continuing milestone work. |
| LAN cluster | 2-20 node local fleet, mDNS/manual join, rqlite-backed state, warm standby, content-addressed blobs, consumer integrations. | Roadmap / in progress in sibling planning docs. |
| Federated overlay | Operator-owned QUIC/Noise transport, hierarchical identity, capability tokens, NAT traversal, hub mesh, air-gap snapshots. | Planned. |
| Work-aware fabric | Capability anycast, scope-bound egress, per-task QoS, by-byte cost accounting. | Planned. |
| Witnessed audit and replay | Signed per-task event logs, external witness co-signing, replay verification, privacy-preserving fleet metrics. | Planned. |

The long-term shape is not "remote shell for agents." It is a private compute
fabric where dispatchers describe the work, policy decides whether it is allowed,
the fabric finds an eligible runner, transport carries the streams, and the log
can prove what happened afterward.

Roadmap source: [DigitalHallucinations/forgewire todo 114](https://github.com/DigitalHallucinations/forgewire/tree/main/todos/114-forgewire-fabric).

---

## Repository Layout

```text
ForgeWire Fabric/
├── Cargo.toml                  # Rust workspace
├── crates/
│   ├── fabric-protocol/        # Signed-envelope schema + ed25519 verify
│   ├── fabric-claim-router/    # Capability-tag matcher + scope filter
│   ├── fabric-streams/         # Monotonic stream sequence counter
│   └── fabric-py/              # PyO3 bindings for forgewire_runtime
├── python/
│   └── forgewire_fabric/
│       ├── hub/                # FastAPI hub, client, MCP servers
│       ├── runner/             # Runner agent, identity, config
│       ├── dispatcher/         # Dispatcher identity and signing
│       └── _installer_assets/  # Wheel-shipped installer mirror
├── scripts/                    # Installers, smoke tests, DR scripts, benches
├── tests/                      # Pytest suite and parity tests
├── vscode/                     # VS Code extension
└── docs/                       # Quickstart and operations docs
```

---

## Build And Test

### Python control plane

```bash
pip install -e .[test]
pytest tests/ -q
```

### Rust runtime extension

```bash
cargo test --workspace
maturin develop --release -m crates/fabric-py/pyproject.toml
```

The Rust extension is optional. Install `forgewire-runtime` for accelerated hot
paths; set `FORGEWIRE_FORCE_PYTHON=1` to force the portable implementation.

---

## Lineage

ForgeWire Fabric began as a practical need: one developer machine was not
enough, and sending Copilot-era work to another trusted machine should not mean
a tangle of SSH sessions, loose scripts, and blind trust.

It grew out of the ForgeWire ecosystem, then split into this standalone project
once the remote-dispatch layer became useful on its own. The thesis remains the
same: survivable agentic systems need graceful degradation, parity paths, audit
trails, ownership boundaries, and replaceable substrates.

---

## License

Apache-2.0. See [LICENSE](LICENSE).
