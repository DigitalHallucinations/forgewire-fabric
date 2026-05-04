# ForgeWire

> **Work-graph-aware compute fabric.** Signed dispatch envelopes, scope-bound capability tokens, structured event streams, federated transport. Apache-2.0.

ForgeWire is the control plane that lets a dispatcher hand a task to a remote runner over an authenticated wire — a capability-routed, audit-friendly substitute for "ssh + tmux + good intentions" used by AI agents, CI workers, and distributed inference fleets.

This repository is **extracted from [PhrenForge](https://github.com/DigitalHallucinations/PhrenForge)**. PhrenForge's runner integration, full operator docs, and mature test fleet still live there; this repo is the standalone, embeddable, third-party-consumable surface.

---

## Status

🚧 **Phase 0 → Phase 2 in progress.** This is the initial extraction snapshot. See [todo 114-forgewire-fabric](https://github.com/DigitalHallucinations/PhrenForge/tree/main/todos/114-forgewire-fabric) for the full roadmap (Phase 0 shipped in PhrenForge; Phase 2 is the extraction work happening here).

| Component | State |
|-----------|-------|
| `crates/fw-protocol` | ✅ Stable. Protocol-v2 envelopes (ed25519). |
| `crates/fw-claim-router` | ✅ Stable. Capability-tag matcher + scope filter. |
| `crates/fw-streams` | ✅ Stable. In-memory monotonic seq counter. |
| `crates/fw-py` | ✅ Stable. PyO3 bindings for the three crates. |
| `python/forgewire/hub` | 🚧 Imports still reference original PhrenForge namespaces in places; rename to top-level `forgewire.*` is tracked as M2.1. |
| `python/forgewire/runner` | 🚧 Same as above. |
| `tests/` | ✅ End-to-end + parity tests. |
| Standalone PyPI package | 📋 Planned (M2.1). |
| VS Code extension | 📋 Planned (M2.2). |
| NSSM/systemd/launchd installers | 📋 Planned (M2.3). |

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

### Rust workspace

```powershell
cargo test --workspace
```

### Python (via Rust extension)

```powershell
# from repo root
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install maturin pytest pytest-asyncio fastapi httpx uvicorn pydantic pynacl
maturin develop --release -m crates/fw-py/Cargo.toml
pytest tests/ -q
```

> **Note:** During this extraction phase some Python module imports (e.g. `scripts.remote.hub.*`) still mirror the original PhrenForge layout. They will be normalized to `forgewire.*` in M2.1. Until then, the recommended consumer surface is the Rust crates and the FastAPI hub server entry point.

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
