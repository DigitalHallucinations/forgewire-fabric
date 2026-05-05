"""MCP server (driver/dispatcher side) exposing dispatch-side blackboard tools.

v2 topology: the hub lives on the always-on OptiPlex; this MCP server runs on
whatever host is acting as dispatcher (typically the laptop / workstation).

Tools surfaced to the main agent:

* ``dispatch_task``       -- enqueue a sealed task
* ``await_result``        -- block until the task reaches a terminal state
* ``get_task``            -- fetch current state
* ``list_tasks``          -- observability
* ``cancel_task``         -- request cancellation
* ``post_note``           -- back-channel from main agent to runner
* ``read_notes``          -- read runner -> main agent notes
* ``stream_progress``     -- pull progress entries since a sequence id

Run via stdio from VS Code's ``.vscode/mcp.json``::

    {
      "servers": {
        "forgewire-dispatcher": {
          "command": "python",
          "args": ["-m", "forgewire_fabric.hub.dispatcher_mcp"],
          "env": {
            "FORGEWIRE_HUB_URL": "http://10.220.190.95:8765",
            "FORGEWIRE_HUB_TOKEN_FILE": "${userHome}/.forgewire/hub.token"
          }
        }
      }
    }
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server

from forgewire_fabric.hub.client import (
    BlackboardClient,
    BlackboardError,
    load_client_from_env,
)
from forgewire_fabric.hub.mcp_common import ToolRegistry

LOGGER = logging.getLogger("forgewire_fabric.dispatcher_mcp")

TERMINAL_STATES = {"done", "failed", "cancelled", "timed_out"}


def _register_tools(registry: ToolRegistry, client: BlackboardClient) -> None:
    registry.register(
        name="dispatch_task",
        description=(
            "Enqueue a sealed task for the remote runner. Returns the task "
            "record including its assigned id and 'queued' status."
        ),
        input_schema={
            "type": "object",
            "required": ["title", "prompt", "scope_globs", "base_commit", "branch"],
            "properties": {
                "title": {"type": "string"},
                "prompt": {"type": "string"},
                "scope_globs": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 1,
                },
                "base_commit": {"type": "string"},
                "branch": {"type": "string"},
                "todo_id": {"type": ["string", "null"]},
                "timeout_minutes": {"type": "integer", "minimum": 1, "maximum": 720},
                "priority": {"type": "integer"},
                "metadata": {"type": "object"},
                "required_tools": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Runner must advertise all of these tools to claim.",
                },
                "required_tags": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Runner must advertise all of these tags to claim.",
                },
                "tenant": {
                    "type": ["string", "null"],
                    "description": "Restrict claims to runners on this tenant.",
                },
                "workspace_root": {
                    "type": ["string", "null"],
                    "description": "Restrict claims to runners with this workspace_root.",
                },
                "require_base_commit": {
                    "type": "boolean",
                    "description": (
                        "When true, the runner's last_known_commit must match "
                        "base_commit before the task is handed out."
                    ),
                },
            },
        },
        handler=lambda args: client.dispatch_task(args),
    )

    registry.register(
        name="get_task",
        description="Fetch the current state of a task by id.",
        input_schema={
            "type": "object",
            "required": ["task_id"],
            "properties": {"task_id": {"type": "integer"}},
        },
        handler=lambda args: client.get_task(int(args["task_id"])),
    )

    registry.register(
        name="list_tasks",
        description="List tasks, optionally filtered by status.",
        input_schema={
            "type": "object",
            "properties": {
                "status": {
                    "type": ["string", "null"],
                    "enum": [
                        None,
                        "queued",
                        "claimed",
                        "running",
                        "done",
                        "failed",
                        "cancelled",
                        "timed_out",
                    ],
                },
                "limit": {"type": "integer", "minimum": 1, "maximum": 500},
            },
        },
        handler=lambda args: client.list_tasks(
            status=args.get("status"), limit=int(args.get("limit", 100))
        ),
    )

    registry.register(
        name="cancel_task",
        description="Request cancellation of a task. Queued tasks terminate immediately; running tasks set a flag.",
        input_schema={
            "type": "object",
            "required": ["task_id"],
            "properties": {"task_id": {"type": "integer"}},
        },
        handler=lambda args: client.cancel_task(int(args["task_id"])),
    )

    registry.register(
        name="post_note",
        description="Post a free-form note onto a task (dispatcher -> runner channel).",
        input_schema={
            "type": "object",
            "required": ["task_id", "body"],
            "properties": {
                "task_id": {"type": "integer"},
                "author": {"type": "string", "default": "dispatcher"},
                "body": {"type": "string"},
            },
        },
        handler=lambda args: client.post_note(
            int(args["task_id"]),
            {"author": args.get("author", "dispatcher"), "body": args["body"]},
        ),
    )

    registry.register(
        name="read_notes",
        description="Read notes for a task posted after the given id (default 0).",
        input_schema={
            "type": "object",
            "required": ["task_id"],
            "properties": {
                "task_id": {"type": "integer"},
                "after_id": {"type": "integer", "default": 0},
            },
        },
        handler=lambda args: client.read_notes(
            int(args["task_id"]), after_id=int(args.get("after_id", 0))
        ),
    )

    async def stream_progress(args: dict[str, Any]) -> dict[str, Any]:
        task_id = int(args["task_id"])
        after_seq = int(args.get("after_seq", 0))
        # Pull current state + any progress since after_seq.
        task = await client.get_task(task_id)
        # Reuse get_task; progress entries are part of progress, not task -> hit blackboard.
        # We expose a simple poll: read latest task state + walk SSE briefly.
        events: list[dict[str, Any]] = []
        try:
            async for event_name, raw in client.stream_events(task_id):
                if event_name == "progress":
                    import json as _json  # local to avoid top-level import noise

                    payload = _json.loads(raw)
                    if payload.get("seq", 0) > after_seq:
                        events.append(payload)
                if event_name == "task":
                    break
        except Exception as exc:  # pragma: no cover - SSE best-effort
            LOGGER.warning("stream_progress fallthrough: %s", exc)
        return {"task": task, "progress": events}

    registry.register(
        name="stream_progress",
        description=(
            "Poll any new progress entries (and current task state) for a task. "
            "Returns immediately after one event-stream pass."
        ),
        input_schema={
            "type": "object",
            "required": ["task_id"],
            "properties": {
                "task_id": {"type": "integer"},
                "after_seq": {"type": "integer", "default": 0},
            },
        },
        handler=stream_progress,
    )

    async def await_result(args: dict[str, Any]) -> dict[str, Any]:
        task_id = int(args["task_id"])
        timeout_seconds = int(args.get("timeout_seconds", 1800))
        poll_seconds = float(args.get("poll_seconds", 2.0))
        deadline = asyncio.get_event_loop().time() + timeout_seconds
        while True:
            task = await client.get_task(task_id)
            if task["status"] in TERMINAL_STATES:
                return task
            if asyncio.get_event_loop().time() >= deadline:
                return {
                    "task": task,
                    "timed_out_waiting": True,
                    "elapsed_seconds": timeout_seconds,
                }
            await asyncio.sleep(poll_seconds)

    registry.register(
        name="await_result",
        description=(
            "Block (server-side) until a task reaches a terminal state or "
            "the timeout elapses. Returns the final task record."
        ),
        input_schema={
            "type": "object",
            "required": ["task_id"],
            "properties": {
                "task_id": {"type": "integer"},
                "timeout_seconds": {"type": "integer", "default": 1800},
                "poll_seconds": {"type": "number", "default": 2.0},
            },
        },
        handler=await_result,
    )

    async def list_runners(_args: dict[str, Any]) -> dict[str, Any]:
        return await client.list_runners()

    registry.register(
        name="list_runners",
        description=(
            "Return the current runner registry: hub protocol_version + all "
            "registered runners with capabilities, derived state "
            "(online|degraded|offline|draining), and current load. Use this "
            "to confirm a target runner is online before dispatching, and "
            "as the response when claim returns no_eligible_runner."
        ),
        input_schema={"type": "object", "properties": {}},
        handler=list_runners,
    )

    async def drain_runner(args: dict[str, Any]) -> dict[str, Any]:
        return await client.drain_runner_by_dispatcher(str(args["runner_id"]))

    registry.register(
        name="drain_runner",
        description=(
            "Dispatcher-initiated drain. Marks the runner as draining; the hub "
            "stops handing out new tasks. In-flight tasks complete normally."
        ),
        input_schema={
            "type": "object",
            "required": ["runner_id"],
            "properties": {"runner_id": {"type": "string"}},
        },
        handler=drain_runner,
    )

    async def read_stream(args: dict[str, Any]) -> dict[str, Any]:
        lines = await client.read_stream(
            int(args["task_id"]),
            after_seq=int(args.get("after_seq", 0)),
            limit=int(args.get("limit", 500)),
        )
        return {"lines": lines}

    registry.register(
        name="read_stream",
        description=(
            "Read structured stdout/stderr/info lines emitted by the runner "
            "for a task, since after_seq. Use to follow long-running build/test "
            "output without re-pulling progress beats."
        ),
        input_schema={
            "type": "object",
            "required": ["task_id"],
            "properties": {
                "task_id": {"type": "integer"},
                "after_seq": {"type": "integer", "default": 0},
                "limit": {"type": "integer", "default": 500},
            },
        },
        handler=read_stream,
    )

    async def discover_hub(args: dict[str, Any]) -> dict[str, Any]:
        from forgewire_fabric.hub.discovery import discover_hubs

        timeout = float(args.get("timeout_seconds", 3.0))
        hits = discover_hubs(timeout=timeout)
        return {"hubs": hits}

    registry.register(
        name="discover_hub",
        description=(
            "Browse the local LAN via mDNS for ForgeWire hubs advertising "
            "_forgewire-hub._tcp. Returns hub host/port/protocol_version. "
            "Useful for first-run dispatcher bootstrap when FORGEWIRE_HUB_URL "
            "is not yet configured. Requires the optional 'zeroconf' package."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "timeout_seconds": {"type": "number", "default": 3.0},
            },
        },
        handler=discover_hub,
    )


async def _run() -> None:
    logging.basicConfig(level=logging.INFO)
    client = load_client_from_env()
    server = Server("forgewire-dispatcher")
    registry = ToolRegistry()
    _register_tools(registry, client)
    registry.bind(server)
    try:
        async with stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                server.create_initialization_options(),
            )
    except BlackboardError as exc:
        LOGGER.error("blackboard error: %s", exc)
        raise
    finally:
        await client.aclose()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
