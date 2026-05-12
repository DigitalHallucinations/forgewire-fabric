# ForgeWire - Fabric

> **Bring-your-own-compute for AI-assisted development.**  
> A secure remote dispatch fabric for AI agents, developer machines, and trusted runners. Apache-2.0.

ForgeWire - Fabric lets VS Code, automation systems, and orchestration layers send scoped work to **machines you already control** using signed dispatch envelopes, capability-bound execution, structured event streams, and auditable results. It turns VS Code into a control surface for a private developer compute fabric — without renting someone else's cloud and without a tangle of SSH sessions and loose scripts.

## Origin

ForgeWire - Fabric began with a simple problem: one developer machine was not enough. The original goal was to let a Copilot-driven VS Code workflow on one machine dispatch work in parallel to another trusted machine, without turning the process into a pile of SSH sessions, loose scripts, and blind trust.

Early versions reused a few pieces from [ForgeWire](https://github.com/DigitalHallucinations/forgewire), then the project evolved through iteration, crossed into the ForgeWire ecosystem, and was extracted back into a standalone project once the remote-dispatch fabric became useful on its own.

## What ForgeWire is (and isn't)

**ForgeWire - Fabric is** a *remote machine and agent dispatch fabric*. It authenticates dispatchers, advertises runner capabilities, ships scoped work over a signed wire, streams events back, and persists results. It is useful as a standalone tool and as a bridge from larger systems (like PhrenForge) to remote workers.

**ForgeWire - Fabric is not** — yet — a full distributed compute runtime, work-graph scheduler, or cluster manager. It is not a drop-in replacement for Ray, Nomad, Kubernetes, Slurm, or Dask. It does not split a single job across nodes, manage GPU residency, or do heterogeneous bin-packing. Those capabilities are on the [roadmap](#roadmap-heterogeneous-private-compute) but the project is honest that today it is the *control plane*, not the compute layer.

### Inside PhrenForge vs. standalone

```text
ForgeWire
├─ Local dispatcher              ← stays in ForgeWire 
├─ Blackboard / shared state    ← stays in PhrenForge
├─ Local tools, agents, workflows
└─ ForgeWire bridge
   ├─ Remote machine dispatch
   ├─ Remote agent dispatch
   ├─ Signed dispatch envelopes
   ├─ Capability-scoped execution
   └─ Event/result reporting back to PhrenForge blackboard
```

**Inside ForgeWire**, ForgeWire - Fabric is the remote execution bridge. ForgeWire keeps its own local dispatcher and blackboard; ForgeWire - Fabric handles authenticated dispatch to remote workers and returns telemetry and results into ForgeWire's coordination layer. ForgeWire - Fabric does not replace those systems.

**Standalone**, ForgeWire - Fabric lets developers wire up trusted machines they already own as remote execution targets for VS Code/Copilot-style workflows: one development machine dispatches scoped work to another in parallel, and the editor watches the stream live.

---

## Status

✅ **M2.1 shipped — pip-installable.** End-to-end smoke verified: hub + runner + dispatch from `pip install forgewire-fabric`. See [`docs/QUICKSTART.md`](docs/QUICKSTART.md) for the 5-minute path. Roadmap in [todo 114-forgewire-fabric](https://github.com/DigitalHallucinations/forgewire/tree/main/todos/114-forgewire-fabric).

| Component | State |
|-----------|-------|
| `crates/fabric-protocol` | ✅ Stable. Protocol-v2 envelopes (ed25519). |
| `crates/fabric-claim-router` | ✅ Stable. Capability-tag matcher + scope filter. |
| `crates/fabric-streams` | ✅ Stable. In-memory monotonic seq counter. |
| `crates/fabric-py` | ✅ Stable. PyO3 bindings — distributed as `forgewire-runtime`. |
| `python/forgewire/hub` | ✅ Pure-Python hub server, `forgewire-fabric hub start`. |
| `python/forgewire/runner` | ✅ Standalone claim-loop agent, `forgewire-fabric runner start`. |
| `forgewire` CLI (Click) | ✅ `hub`, `runner`, `dispatch`, `tasks`, `runners`, `keys`, `token`. |
| `tests/` | ✅ End-to-end + parity tests. |
| VS Code extension | ✅ Cross-OS GUI in [`vscode/`](vscode). Connect, dispatch, tail streams, start a hub or runner with one command. |
| NSSM/systemd/launchd installers | 📋 Planned (M2.3). |

---

## Quickstart

```bash
pip install forgewire-fabric

# Hub host
forgewire token gen > hub.token
export FORGEWIRE_HUB_TOKEN=$(cat hub.token)
forgewire-fabric hub start --host 0.0.0.0 --port 8765

# Each runner
export FORGEWIRE_HUB_URL=http://<hub>:8765 FORGEWIRE_HUB_TOKEN=...
forgewire-fabric runner start --workspace-root /path/to/repo \
    --scope-prefixes "src/,tests/" --tags "linux,python:3.11"

# Dispatch from any machine with the token
forgewire-fabric dispatch "pytest -x" --scope "tests/**" \
    --branch agent/laptop/smoke --base-commit $(git rev-parse origin/main)
forgewire-fabric tasks list
forgewire-fabric tasks stream <id>
```

Full guide: [`docs/QUICKSTART.md`](docs/QUICKSTART.md).

### Or use the VS Code extension

For a cross-platform GUI (Windows / macOS / Linux), install the extension
from [`vscode/`](vscode):

```bash
cd vscode && npm install && npm run package
code --install-extension forgewire-fabric-0.1.0.vsix
```

Then run **ForgeWire: Connect to Hub** from the command palette. The
extension can also `pip install` the CLI, start a hub, or register a
runner on the current machine — useful for joining new boxes to a cluster
without touching a terminal. See [`vscode/README.md`](vscode/README.md).

---

## Layout

```
ForgeWire - Fabric/
├── Cargo.toml                  # Rust workspace
├── crates/
│   ├── fabric-protocol/            # Signed-envelope schema + ed25519 verify
│   ├── fabric-claim-router/        # Capability-tag matcher
│   ├── fabric-streams/             # Monotonic stream-seq counter
│   └── fabric-py/                  # PyO3 bindings for the above
├── python/
│   └── forgewire/
│       ├── hub/                # FastAPI hub: dispatch, claim, streams, results
│       └── runner/             # Identity, capability discovery, worktree helpers
├── scripts/                    # NSSM start/stop, bench harnesses, smoke tests
├── tests/                      # Pytest suite (parity tests against Python fallback)
└── docs/                       # Overview + protocol notes
```

---

## Build & test

### Python control plane (recommended)

```bash
pip install -e .[test]
pytest tests/ -q
```

### Rust runtime extension

```bash
cargo test --workspace
# Or build the Python binding:
maturin develop --release -m crates/fabric-py/pyproject.toml
```

The Rust extension (`forgewire-runtime`) is *optional*. The pure-Python hub
and runner work without it; install it for the accelerated claim-router and
stream counters when running large fleets.

---

## Concepts

- **Dispatch envelope** — signed JSON from a *dispatcher* (e.g. an editor session, a CI runner, an MCP client) to the hub. Defines task, scope globs, required capability tags, base commit.
- **Hub** — the FastAPI process at the centre. Persists envelopes, validates signatures, issues claims, mediates streams + results. SQLite WAL by default.
- **Runner** — long-lived agent registered with capability tags (`tool:browser`, `gpu:nvidia`, `phrenforge:1`, …). Polls for claimable work. Reports streams + a signed result.
- **Capability tags** — strings on a runner. A dispatch's `required_tags` must all be present on a runner for it to claim.
- **Scope globs** — path patterns on a dispatch. Restrict what a runner is allowed to read/write inside its workspace.
- **Stream** — append-only `(task_id, channel, line)` log persisted by the hub. Backed by a monotonic seq counter (`fabric-streams`).
- **Result** — terminal envelope for a task: `done | failed | cancelled | timed_out`.

---

## Roadmap: Heterogeneous Private Compute

ForgeWire is **not currently** a full distributed compute runtime or cluster scheduler. The current focus is secure remote machine and agent dispatch, event streaming, and result reporting.

However, ForgeWire lays the **control-plane foundation** for heterogeneous private compute: a future layer where trusted machines can advertise capabilities, receive scoped work, execute in parallel, stream state, and report results back to an originating controller such as PhrenForge.

### Today — remote dispatch fabric

- Send jobs to remote machines and agents
- Authenticate dispatch (ed25519 + bearer token)
- Check capability tags + scope globs before claim
- Stream events / results over SSE
- Report back to PhrenForge or another controller

### Future — heterogeneous private compute fabric

- Runner capability registry (CPU / GPU / RAM / OS / arch / toolchain / network location / trust level)
- Heartbeats and health scoring
- Runner pools and tags
- Task affinity ("send GPU work to nodes with `gpu:nvidia` and high health")
- Work-graph scheduling
- Parallel dispatch groups
- Retry and failover policies
- Result aggregation across nodes
- Local-network discovery (mDNS, partial today)
- Optional PhrenForge blackboard reporting
- Optional VS Code visualization of runner state

Full plan: [DigitalHallucinations/PhrenForge → todos/114-forgewire-fabric](https://github.com/DigitalHallucinations/PhrenForge/tree/main/todos/114-forgewire-fabric).

---

## License

Apache-2.0. See [LICENSE](LICENSE).
