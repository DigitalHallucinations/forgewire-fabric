# ForgeWire Fabric Positioning

> What ForgeWire Fabric is, what it is not, and how it relates to the parent ForgeWire/PhrenForge platform.

## One-line description

ForgeWire Fabric is a work-graph-aware compute fabric for authenticated task dispatch to remote runners.

## What ForgeWire Fabric is

A standalone remote dispatch/control-plane layer that ships:

- Hub service (task intake, trust checks, routing gates, stream/result persistence)
- Runner service (identity-bearing worker, scoped execution, event/result reporting)
- CLI (`forgewire-fabric`) for operators and dispatchers
- VS Code integration surface for dispatch and task observation
- Optional Rust accelerators with Python parity fallback

## What ForgeWire Fabric is not

ForgeWire Fabric is not the complete ForgeWire/PhrenForge assistant platform.

It does not own the parent platform's local orchestration runtime, desktop shell, persona ecosystem, memory layer, or full assistant behavior.

## Relationship to parent platform

Historically, Fabric started as the remote dispatch layer inside ForgeWire (formerly PhrenForge). In parent-platform deployments, Fabric acts as the remote execution substrate while local orchestration remains outside this repo.

## Standalone value

ForgeWire Fabric can be adopted independently by teams that want authenticated dispatch from editor/automation surfaces to trusted remote runners without outsourcing execution control to third-party compute.
