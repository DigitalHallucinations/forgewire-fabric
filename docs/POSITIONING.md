# Positioning

> **What ForgeWire is, what it isn't, and how it fits next to PhrenForge.**

## One-line description

> ForgeWire is a secure remote dispatch fabric for AI agents, developer machines, and trusted runners. It lets VS Code, automation systems, and orchestration layers send scoped work to machines you control using signed dispatch envelopes, capability-bound execution, structured event streams, and auditable results.

## Taglines

- **Bring-your-own-compute for AI-assisted development.** *(primary)*
- Turn trusted machines into a private execution fabric.
- VS Code as the control surface for your own private developer cloud.
- Remote dispatch for developers and agents without handing execution to someone else's cloud.

## What ForgeWire *is*

A **remote machine and agent dispatch fabric**. Concretely, today the project ships:

- A **hub** (FastAPI + SQLite) that owns the task graph, validates signatures, mediates claims, and persists streams + results.
- **Runners** (long-lived agents) that advertise capability tags and scope prefixes, claim matching work, execute it, and stream output back over SSE.
- A **CLI** (`forgewire`) for dispatchers, hub operators, and runner operators on any OS.
- A **VS Code extension** that wraps the same flows in a GUI: connect, dispatch, tail streams, install/start a hub or runner on the local box.

## What ForgeWire is *not* (yet)

ForgeWire is **not** a full distributed compute runtime or cluster scheduler. It is **not** a drop-in replacement for Ray, Nomad, Kubernetes, Slurm, or Dask.

Specifically, it does **not** currently:

- Schedule a single job across multiple nodes.
- Bin-pack work across heterogeneous CPU/GPU/RAM constraints.
- Maintain a fleet-wide work graph or DAG-of-tasks scheduler.
- Manage GPU residency, model sharding, or KV-cache pinning.
- Provide retry/failover policies above the per-task level.

These belong on the [roadmap](../README.md#roadmap-heterogeneous-private-compute), not in the current capability description.

## Where ForgeWire fits next to PhrenForge

```text
PhrenForge
├─ Local dispatcher              ← stays in PhrenForge
├─ Blackboard / shared state     ← stays in PhrenForge
├─ Local tools, agents, workflows
└─ ForgeWire bridge
   ├─ Remote machine dispatch
   ├─ Remote agent dispatch
   ├─ Signed dispatch envelopes
   ├─ Capability-scoped execution
   └─ Event/result reporting back to PhrenForge blackboard
```

In PhrenForge, ForgeWire acts as the **remote execution bridge**. Local planning, scheduling, and blackboard coordination remain inside PhrenForge; ForgeWire handles authenticated dispatch to remote workers and returns remote-execution telemetry and results into the PhrenForge coordination layer.

ForgeWire does **not**:

- Replace PhrenForge's local dispatcher.
- Replace PhrenForge's blackboard.
- Own planning, intent capture, or persona/agent state.

ForgeWire **does**:

- Carry a sealed brief from the local dispatcher to a remote runner.
- Enforce per-task scope globs and capability tags.
- Surface stream events and a terminal result back to a local subscriber (the PhrenForge blackboard, a VS Code instance, or a CLI).

## Standalone use

Outside PhrenForge, ForgeWire is the missing piece for "I have three machines I trust, I want my editor to dispatch work to all of them, and I do not want to write a fleet of bash scripts." It is **especially well suited to VS Code/Copilot workflows**, where a single development machine wants to fan out scoped work to other trusted machines in parallel while keeping the editor as the control surface.

## Layer separation (today vs. future)

| | **ForgeWire today** | **ForgeWire future** |
|---|---|---|
| Goal | Secure remote dispatch | Heterogeneous private compute |
| Scope | One task → one runner | Work graph → many runners |
| Selection | Capability tags + scope globs | Affinity by CPU/GPU/RAM/OS/tools/network/trust |
| Failure | Per-task retry | Pool-level failover |
| State | Per-task envelopes | Fleet state + health scoring |
| Integration | PhrenForge bridge / VS Code GUI / CLI | Above + scheduler API for orchestrators |

The point of being honest about this layering is to avoid promising scheduler-class behavior the project does not yet implement, while making it clear that the **control plane** built today is exactly the substrate the future scheduler will sit on top of.
