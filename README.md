# ForgeWire

> **Work-graph-aware compute fabric.** Signed dispatch envelopes, scope-bound capability tokens, structured event streams, federated transport. Apache-2.0.

ForgeWire is the control plane that lets a dispatcher hand a task to a remote runner over an authenticated wire — a capability-routed, audit-friendly substitute for "ssh + tmux + good intentions" used by AI agents, CI workers, and distributed inference fleets.

This repository is **extracted from [PhrenForge](https://github.com/DigitalHallucinations/PhrenForge)**. PhrenForge's runner integration, full operator docs, and mature test fleet still live there; this repo is the standalone, embeddable, third-party-consumable surface.

---

## Status

✅ **M2.1 shipped — pip-installable.** End-to-end smoke verified: hub + runner + dispatch from `pip install forgewire`. See [`docs/QUICKSTART.md`](docs/QUICKSTART.md) for the 5-minute path. Roadmap in [todo 114-forgewire-fabric](https://github.com/DigitalHallucinations/PhrenForge/tree/main/todos/114-forgewire-fabric).

| Component | State |
|-----------|-------|
| `crates/fw-protocol` | ✅ Stable. Protocol-v2 envelopes (ed25519). |
| `crates/fw-claim-router` | ✅ Stable. Capability-tag matcher + scope filter. |
| `crates/fw-streams` | ✅ Stable. In-memory monotonic seq counter. |
| `crates/fw-py` | ✅ Stable. PyO3 bindings — distributed as `forgewire-runtime`. |
| `python/forgewire/hub` | ✅ Pure-Python hub server, `forgewire hub start`. |
| `python/forgewire/runner` | ✅ Standalone claim-loop agent, `forgewire runner start`. |
| `forgewire` CLI (Click) | ✅ `hub`, `runner`, `dispatch`, `tasks`, `runners`, `keys`, `token`. |
| `tests/` | ✅ End-to-end + parity tests. |
| VS Code extension | ✅ Cross-OS GUI in [`vscode/`](vscode). Connect, dispatch, tail streams, start a hub or runner with one command. |
| NSSM/systemd/launchd installers | 📋 Planned (M2.3). |

---

## Quickstart

```bash
pip install forgewire

# Hub host
forgewire token gen > hub.token
export FORGEWIRE_HUB_TOKEN=$(cat hub.token)
forgewire hub start --host 0.0.0.0 --port 8765

# Each runner
export FORGEWIRE_HUB_URL=http://<hub>:8765 FORGEWIRE_HUB_TOKEN=...
forgewire runner start --workspace-root /path/to/repo \
    --scope-prefixes "src/,tests/" --tags "linux,python:3.11"

# Dispatch from any machine with the token
forgewire dispatch "pytest -x" --scope "tests/**" \
    --branch agent/laptop/smoke --base-commit $(git rev-parse origin/main)
forgewire tasks list
forgewire tasks stream <id>
```

Full guide: [`docs/QUICKSTART.md`](docs/QUICKSTART.md).

### Or use the VS Code extension

For a cross-platform GUI (Windows / macOS / Linux), install the extension
from [`vscode/`](vscode):

```bash
cd vscode && npm install && npm run package
code --install-extension forgewire-0.1.0.vsix
```

Then run **ForgeWire: Connect to Hub** from the command palette. The
extension can also `pip install` the CLI, start a hub, or register a
runner on the current machine — useful for joining new boxes to a cluster
without touching a terminal. See [`vscode/README.md`](vscode/README.md).

---

## Layout

```
forgewire/
├── Cargo.toml                  # Rust workspace
├── crates/
│   ├── fw-protocol/            # Signed-envelope schema + ed25519 verify
│   ├── fw-claim-router/        # Capability-tag matcher
│   ├── fw-streams/             # Monotonic stream-seq counter
│   └── fw-py/                  # PyO3 bindings for the above
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
maturin develop --release -m crates/fw-py/pyproject.toml
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
- **Stream** — append-only `(task_id, channel, line)` log persisted by the hub. Backed by a monotonic seq counter (`fw-streams`).
- **Result** — terminal envelope for a task: `done | failed | cancelled | timed_out`.

---

## Roadmap

This snapshot covers Phase 0 (foundations) and is the staging ground for Phase 2 (PyPI extraction, VS Code extension, installers). Phases 3–6 (federated overlay, work-aware fabric, audit/witnessing/replay, ecosystem) will be developed here once the extraction is clean.

Full plan: [DigitalHallucinations/PhrenForge → todos/114-forgewire-fabric](https://github.com/DigitalHallucinations/PhrenForge/tree/main/todos/114-forgewire-fabric).

---

## License

Apache-2.0. See [LICENSE](LICENSE).
