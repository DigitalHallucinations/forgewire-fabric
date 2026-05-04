# Extraction status

This repository was created on 2026-05-03 by extracting the following files from [PhrenForge](https://github.com/DigitalHallucinations/PhrenForge) at commit `195ea6fc`:

| Source (PhrenForge) | Destination (forgewire) |
|---------------------|-------------------------|
| `forgewire-runtime/Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `rustfmt.toml`, `pyproject.toml`, `PERFORMANCE.md`, `README.md`, `.gitignore` | repo root (renamed `README.md` → `README-runtime.md`) |
| `forgewire-runtime/crates/{fw-protocol, fw-claim-router, fw-streams, fw-py}/` | `crates/` |
| `scripts/remote/hub/*.{py,sql}` | `python/forgewire/hub/` |
| `scripts/remote/runner/*.py` | `python/forgewire/runner/` |
| `scripts/remote/{bench_*,smoke_test}.py`, `scripts/remote/*.ps1` | `scripts/` |
| `tests/remote/test_forgewire_*.py`, `__init__.py` | `tests/` |

## What is NOT yet renamed

The following carry-overs from the PhrenForge namespace will be normalised in **M2.1** (see [todo 114 Phase 2](https://github.com/DigitalHallucinations/PhrenForge/blob/main/todos/114-forgewire-fabric/phase-2-extraction-and-tooling.md)):

- Python imports referencing `scripts.remote.hub.*` or `scripts.remote.runner.*` need to point at `forgewire.hub.*` / `forgewire.runner.*`.
- Environment variables (`BLACKBOARD_URL`, `PHRENFORGE_*`) need a `FORGEWIRE_*` rename with a transitional alias period.
- Class names (`Blackboard`, `BlackboardClient`) need `Hub`, `HubClient`. The `Blackboard` term is kept as a separate concept for PhrenForge's *cognitive* state.
- `forgewire-runtime` references in inline comments / pyproject metadata need to point at the new repo URL.
- Service/process names (`PhrenForgeHub` NSSM service) need a `ForgeWireHub` alternative shipped via the M2.3 installers.

## How to consume the snapshot today

1. **Use the Rust crates directly.** They are self-contained and ready: `cargo test --workspace` passes.
2. **Use the FastAPI hub.** Run `python -m uvicorn forgewire.hub.server:app` after fixing import paths — or run it from a PhrenForge checkout where the imports resolve naturally.
3. **Wait for M2.1** if you want a clean PyPI-installable `pip install forgewire` story; that lands in this repo as the next milestone.

## Contributing

Open an issue or PR. The roadmap and acceptance gates live in PhrenForge's `todos/114-forgewire-fabric/` until extraction is complete; from M2.1 onward they migrate into this repo as the canonical home.
