# ForgeWire Fabric

> ForgeWire Fabric is a work-graph-aware compute fabric for authenticated task dispatch to remote runners.

ForgeWire Fabric turns machines you already control into a private execution fabric. VS Code, CLI, MCP clients, and automation can send scoped work to trusted runners using signed dispatch envelopes, scope-bound capability tokens, structured event streams, and auditable results.

## What this is

ForgeWire Fabric is a work-graph-aware compute fabric for dispatching authenticated tasks to remote runners.

It focuses on:

- signed dispatch envelopes
- scope-bound capability tokens
- runner registration and trust
- structured event streams
- hub/runner execution
- federated transport
- VS Code and agent workflow integration

## What this is not

ForgeWire Fabric is not the full ForgeWire/PhrenForge platform.

It does not provide the full desktop shell, persona ecosystem, memory system, local blackboard, or broader assistant runtime. Those belong to the parent platform and may integrate with Fabric, but they are not Fabric itself.

## Project lineage

ForgeWire Fabric began as the remote dispatch layer inside the larger ForgeWire platform, formerly PhrenForge. It was extracted into a standalone project so developers can use the remote runner fabric independently while still allowing the parent platform to integrate with it.

## What problem Fabric solves

Most teams with trusted machines (laptops, workstations, and homelab nodes) still dispatch work with ad hoc scripts and manual SSH orchestration. ForgeWire Fabric provides an authenticated control plane so dispatchers can issue scoped work, route it to eligible runners, and retain auditable execution history.

## What is included in this repo

This repository contains the standalone Fabric implementation:

- Hub server (FastAPI)
- Runner agent
- `forgewire-fabric` CLI
- Installer scripts (including Windows NSSM/watchdog setup)
- VS Code extension (`vscode/`)
- Python package (`forgewire_fabric`)
- Rust acceleration crates under `crates/`

## Install

```bash
pip install forgewire-fabric
```

The package import is `forgewire_fabric` and the CLI entry point is `forgewire-fabric`.

## Hub/runner smoke test

```bash
pip install forgewire-fabric
forgewire-fabric token gen > hub.token
export FORGEWIRE_HUB_TOKEN=$(cat hub.token)

# Terminal 1
forgewire-fabric hub start --host 127.0.0.1 --port 8765

# Terminal 2
forgewire-fabric runner start --workspace-root /path/to/repo --hub-url http://127.0.0.1:8765

# Terminal 3
forgewire-fabric dispatch "pytest -q" --scope "tests/**"
forgewire-fabric tasks list
forgewire-fabric tasks stream <task-id>
```

Windows service-based setup and watchdog details are documented in [docs/operations/service-install.md](docs/operations/service-install.md).

## VS Code and agent workflows

The VS Code extension in [`vscode/`](vscode) provides hub connectivity, runner/task browsing, dispatch, and stream tailing.

```bash
cd vscode
npm install
npm run package
code --install-extension forgewire-fabric-0.1.7.vsix
```

CLI MCP setup for editor/agent workflows:

```bash
forgewire-fabric mcp install --hub-url http://127.0.0.1:8765
forgewire-fabric mcp install --hub-url http://127.0.0.1:8765 --with-runner --workspace-root /path/to/repo
```

See [vscode/README.md](vscode/README.md) for extension commands and settings.

## Stable today

- Authenticated hub/runner dispatch flow
- Signed protocol-v2 envelopes with nonce replay protection
- Scope/capability-aware claim routing
- Structured stream events and persisted terminal results
- Runner identity persistence and trust registration
- Core operator CLI surfaces (`tasks`, `runners`, `audit`, `approvals`, `secrets`, `dispatchers`, `setup`)
- Windows OOTB service install with watchdog supervision

## Experimental / evolving surfaces

- Federated transport layers and overlay networking
- Extended multi-node cluster orchestration roadmap
- Some installer paths outside Windows (Linux/macOS are less complete)
- Performance-sensitive Rust acceleration paths remain optional and parity-bound to Python fallback behavior

## Current limitations

ForgeWire Fabric is focused on remote dispatch and runner coordination. It does not attempt to replace the parent platform’s local orchestration, persona system, memory layer, or UI. Some integration surfaces may still evolve as the extracted boundary hardens.

## Additional documentation

- Quickstart: [docs/QUICKSTART.md](docs/QUICKSTART.md)
- Positioning: [docs/POSITIONING.md](docs/POSITIONING.md)
- Extraction notes: [docs/EXTRACTION.md](docs/EXTRACTION.md)
- Performance: [PERFORMANCE.md](PERFORMANCE.md)
