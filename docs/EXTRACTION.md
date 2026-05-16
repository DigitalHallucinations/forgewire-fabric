# Extraction and migration notes

## Historical archive (read-only context)

This repository was created on **2026-05-03** by extracting Fabric-focused components from the parent platform (then PhrenForge, later ForgeWire) at commit `195ea6fc`.

| Source (historical parent repo) | Destination (forgewire-fabric) |
|---------------------|-------------------------|
| `forgewire-runtime/Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `rustfmt.toml`, `pyproject.toml`, `PERFORMANCE.md`, `README.md`, `.gitignore` | repo root (with runtime README moved to `README-runtime.md`) |
| `forgewire-runtime/crates/{fabric-protocol, fabric-claim-router, fabric-streams, fabric-py}/` | `crates/` |
| `scripts/remote/hub/*.{py,sql}` | `python/forgewire_fabric/hub/` |
| `scripts/remote/runner/*.py` | `python/forgewire_fabric/runner/` |
| `scripts/remote/{bench_*,smoke_test}.py`, `scripts/remote/*.ps1` | `scripts/` |
| `tests/remote/test_forgewire_*.py`, `__init__.py` | `tests/` |

The historical extraction lineage is retained for auditability and provenance only.

## Current operator guidance (authoritative)

For day-to-day usage and release operations, treat the following as source of truth:

- Product identity and scope: [`README.md`](../README.md)
- Positioning and boundary with parent platform: [`docs/POSITIONING.md`](POSITIONING.md)
- Install and run flows: [`docs/QUICKSTART.md`](QUICKSTART.md)
- Service supervision/install details: [`docs/operations/service-install.md`](operations/service-install.md)
- Release cut checklist: [`docs/release-checklist.md`](release-checklist.md)

### Migration compatibility notes still relevant

These are legacy compatibility surfaces still intentionally present in the codebase:

- Legacy `BLACKBOARD_*` aliases alongside canonical `FORGEWIRE_*` variables for transitional integrations.
- Some inline lineage comments referencing pre-extraction milestone IDs.
- Backward-compatibility aliases for earlier client naming where explicitly documented.

These are compatibility shims, not indications that this repository is the full parent platform.

## Contribution note

Open issues/PRs against this repo for Fabric behavior, docs, and releases. Historical roadmap references are acceptable as lineage context, but new release and operator guidance should be documented in this repository.
