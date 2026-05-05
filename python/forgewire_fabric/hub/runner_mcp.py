"""MCP server (runner side) exposing runner-side blackboard tools.

Co-located with the hub on the always-on host. On startup the runner:

1. loads (or generates) its persistent ed25519 identity,
2. probes the host for capabilities,
3. registers with the hub via ``POST /runners/register`` (signed),
4. starts a background heartbeat loop.

Tools surfaced to the local Copilot agent in the runner chat mode:

* ``claim_next_task``    -- capability-aware claim via ``/tasks/claim-v2``
* ``mark_running``       -- transition the claimed task to running
* ``report_progress``    -- append a progress beat
* ``report_stream``      -- append a single stdout/stderr/info line
* ``report_result``      -- terminal report
* ``post_note``          -- runner -> dispatcher back-channel
* ``read_notes``         -- read dispatcher -> runner notes
* ``get_task``           -- inspect current task state
* ``request_drain``      -- ask the hub to mark this runner draining
* ``self_update``        -- git fetch + ff-merge the runner workspace
* ``runner_identity``    -- inspect runner_id, hostname, capabilities, state

``FORGEWIRE_HUB_URL`` should be ``http://127.0.0.1:<port>`` since runner and hub
are colocated.

Optional env knobs (canonical names; ``PHRENFORGE_RUNNER_*`` legacy aliases
are also honoured for one minor cycle):
  FORGEWIRE_RUNNER_TAGS=foo,bar          -- runner-declared tags
  FORGEWIRE_RUNNER_SCOPE_PREFIXES=...    -- comma-separated path prefixes the
                                            runner is allowed to write
  FORGEWIRE_RUNNER_TENANT=name           -- tenant id for multi-tenant routing
  FORGEWIRE_RUNNER_WORKSPACE_ROOT=path   -- absolute path of the runner's clone
  FORGEWIRE_RUNNER_MAX_CONCURRENT=N      -- task concurrency cap (default 1)
  FORGEWIRE_RUNNER_VERSION=str           -- override version string
  FORGEWIRE_RUNNER_AUTOUPDATE=1          -- git fetch+ff-merge before claim
  FORGEWIRE_RUNNER_AUTOUPDATE_BRANCH=main -- branch to fast-forward (default main)
"""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server

from forgewire_fabric.hub.client import (
    BlackboardClient,
    BlackboardError,
    load_client_from_env,
)
from forgewire_fabric.hub.mcp_common import ToolRegistry
from forgewire_fabric.runner.identity import RunnerIdentity, load_or_create
from forgewire_fabric.runner.runner_capabilities import (
    describe_host,
    detect_tools,
    fresh_nonce,
    now_ts,
    sample_resources,
    sign_payload,
)


LOGGER = logging.getLogger("forgewire_fabric.runner_mcp")

PROTOCOL_VERSION = 2
HEARTBEAT_INTERVAL_SECONDS = 20
DEFAULT_VERSION = "0.1.0"
SELF_UPDATE_MIN_INTERVAL_SECONDS = 60


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def _git_run(workspace_root: str, args: list[str], timeout: float = 30.0) -> tuple[int, str]:
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=workspace_root,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (subprocess.SubprocessError, FileNotFoundError, OSError) as exc:
        return (-1, f"{type(exc).__name__}: {exc}")
    out = (proc.stdout or "") + (proc.stderr or "")
    return (proc.returncode, out.strip())


def self_update_workspace(workspace_root: str | None, branch: str) -> dict[str, Any]:
    """Run ``git fetch`` + ``git merge --ff-only origin/<branch>`` in-place.

    Returns a structured dict suitable for logging or surfacing as MCP output.
    Never raises -- failure modes are reported via ``status`` and ``error``.
    """

    if not workspace_root or not os.path.isdir(workspace_root):
        return {"status": "skipped", "reason": "no_workspace_root"}
    old_head = _detect_workspace_commit(workspace_root)
    rc, fetch_out = _git_run(workspace_root, ["fetch", "--quiet", "origin", branch], timeout=60.0)
    if rc != 0:
        return {
            "status": "failed",
            "stage": "fetch",
            "branch": branch,
            "old_head": old_head,
            "error": fetch_out,
        }
    rc, merge_out = _git_run(
        workspace_root,
        ["merge", "--ff-only", f"origin/{branch}"],
        timeout=30.0,
    )
    new_head = _detect_workspace_commit(workspace_root)
    if rc != 0:
        return {
            "status": "failed",
            "stage": "merge",
            "branch": branch,
            "old_head": old_head,
            "new_head": new_head,
            "error": merge_out,
        }
    return {
        "status": "ok" if old_head != new_head else "unchanged",
        "branch": branch,
        "old_head": old_head,
        "new_head": new_head,
    }


def _parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [s.strip() for s in value.split(",") if s.strip()]


def _detect_workspace_commit(workspace_root: str | None) -> str | None:
    if not workspace_root or not os.path.isdir(workspace_root):
        return None
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=workspace_root,
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=3,
        )
        return out.strip() or None
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        return None


class RunnerSession:
    """In-memory state shared between MCP tools and the heartbeat loop."""

    def __init__(
        self,
        client: BlackboardClient,
        identity: RunnerIdentity,
        *,
        workspace_root: str | None,
        tenant: str | None,
        tags: list[str],
        scope_prefixes: list[str],
        max_concurrent: int,
        runner_version: str,
    ) -> None:
        self.client = client
        self.identity = identity
        self.workspace_root = workspace_root
        self.tenant = tenant
        self.tags = tags
        self.scope_prefixes = scope_prefixes
        self.max_concurrent = max_concurrent
        self.runner_version = runner_version
        self.tools = detect_tools()
        self.host = describe_host()
        self.last_resources: dict[str, Any] = {}
        self.last_heartbeat_ok: bool = False
        self.last_register_response: dict[str, Any] | None = None
        self.autoupdate_enabled: bool = _truthy(
            os.environ.get("FORGEWIRE_RUNNER_AUTOUPDATE")
            or os.environ.get("PHRENFORGE_RUNNER_AUTOUPDATE")
        )
        self.autoupdate_branch: str = (
            os.environ.get("FORGEWIRE_RUNNER_AUTOUPDATE_BRANCH")
            or os.environ.get("PHRENFORGE_RUNNER_AUTOUPDATE_BRANCH")
            or "main"
        )
        self.last_autoupdate_ts: float = 0.0
        self.last_autoupdate_result: dict[str, Any] | None = None

    @property
    def runner_id(self) -> str:
        return self.identity.runner_id

    def _signed_envelope(self, op: str) -> tuple[int, str, str]:
        ts = now_ts()
        nonce = fresh_nonce()
        signed = {
            "op": op,
            "runner_id": self.runner_id,
            "timestamp": ts,
            "nonce": nonce,
        }
        return ts, nonce, sign_payload(self.identity, signed)

    def claim_payload(self) -> dict[str, Any]:
        resources = sample_resources()
        self.last_resources = resources
        ts, nonce, signature = self._signed_envelope("claim")
        return {
            "runner_id": self.runner_id,
            "timestamp": ts,
            "nonce": nonce,
            "signature": signature,
            "scope_prefixes": self.scope_prefixes,
            "tools": self.tools,
            "tags": self.tags,
            "tenant": self.tenant,
            "workspace_root": self.workspace_root,
            "last_known_commit": _detect_workspace_commit(self.workspace_root),
            **resources,
        }

    def heartbeat_payload(self) -> dict[str, Any]:
        resources = sample_resources()
        self.last_resources = resources
        ts, nonce, signature = self._signed_envelope("heartbeat")
        return {
            "runner_id": self.runner_id,
            "timestamp": ts,
            "nonce": nonce,
            "signature": signature,
            "last_known_commit": _detect_workspace_commit(self.workspace_root),
            **resources,
        }

    def drain_payload(self) -> dict[str, Any]:
        ts, nonce, signature = self._signed_envelope("drain")
        return {
            "runner_id": self.runner_id,
            "timestamp": ts,
            "nonce": nonce,
            "signature": signature,
        }

    def register_payload(self) -> dict[str, Any]:
        ts = now_ts()
        nonce = fresh_nonce()
        signed = {
            "op": "register",
            "runner_id": self.runner_id,
            "public_key": self.identity.public_key_hex,
            "protocol_version": PROTOCOL_VERSION,
            "timestamp": ts,
            "nonce": nonce,
        }
        signature = sign_payload(self.identity, signed)
        body = {
            "runner_id": self.runner_id,
            "public_key": self.identity.public_key_hex,
            "protocol_version": PROTOCOL_VERSION,
            "runner_version": self.runner_version,
            "tools": self.tools,
            "tags": self.tags,
            "scope_prefixes": self.scope_prefixes,
            "tenant": self.tenant,
            "workspace_root": self.workspace_root,
            "max_concurrent": self.max_concurrent,
            "metadata": {},
            "timestamp": ts,
            "nonce": nonce,
            "signature": signature,
        }
        body.update(self.host)
        return body


def _build_session(client: BlackboardClient) -> RunnerSession:
    identity = load_or_create()
    workspace_root = (
        os.environ.get("FORGEWIRE_RUNNER_WORKSPACE_ROOT")
        or os.environ.get("PHRENFORGE_RUNNER_WORKSPACE_ROOT")
        or os.getcwd()
    )
    tags = _parse_csv(
        os.environ.get("FORGEWIRE_RUNNER_TAGS")
        or os.environ.get("PHRENFORGE_RUNNER_TAGS")
    )
    scope_prefixes = _parse_csv(
        os.environ.get("FORGEWIRE_RUNNER_SCOPE_PREFIXES")
        or os.environ.get("PHRENFORGE_RUNNER_SCOPE_PREFIXES")
    )
    tenant = (
        os.environ.get("FORGEWIRE_RUNNER_TENANT")
        or os.environ.get("PHRENFORGE_RUNNER_TENANT")
        or None
    )
    max_concurrent = int(
        os.environ.get("FORGEWIRE_RUNNER_MAX_CONCURRENT")
        or os.environ.get("PHRENFORGE_RUNNER_MAX_CONCURRENT", "1")
    )
    runner_version = (
        os.environ.get("FORGEWIRE_RUNNER_VERSION")
        or os.environ.get("PHRENFORGE_RUNNER_VERSION", DEFAULT_VERSION)
    )
    return RunnerSession(
        client,
        identity,
        workspace_root=workspace_root,
        tenant=tenant,
        tags=tags,
        scope_prefixes=scope_prefixes,
        max_concurrent=max_concurrent,
        runner_version=runner_version,
    )


async def _register_with_retries(session: RunnerSession) -> dict[str, Any]:
    delay = 1.0
    while True:
        try:
            response = await session.client.register_runner(session.register_payload())
            session.last_register_response = response
            LOGGER.info(
                "registered with hub as runner_id=%s (host=%s, tools=%s)",
                session.runner_id,
                session.host["hostname"],
                ",".join(session.tools) or "<none>",
            )
            return response
        except BlackboardError as exc:
            LOGGER.warning("registration failed: %s; retrying in %.1fs", exc, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)


async def _heartbeat_loop(session: RunnerSession) -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
        try:
            await session.client.heartbeat(
                session.runner_id, session.heartbeat_payload()
            )
            session.last_heartbeat_ok = True
        except BlackboardError as exc:
            session.last_heartbeat_ok = False
            LOGGER.warning("heartbeat failed: %s", exc)
            if exc.status_code == 404:
                LOGGER.info("hub forgot us; re-registering")
                await _register_with_retries(session)


def _register_tools(registry: ToolRegistry, session: RunnerSession) -> None:
    client = session.client

    async def claim_next_task(_args: dict[str, Any]) -> dict[str, Any]:
        # Optional self-update before every claim, throttled so a hot loop
        # does not pull on every poll.
        if session.autoupdate_enabled and session.workspace_root:
            import time as _time

            now = _time.time()
            if now - session.last_autoupdate_ts >= SELF_UPDATE_MIN_INTERVAL_SECONDS:
                session.last_autoupdate_ts = now
                result = await asyncio.to_thread(
                    self_update_workspace,
                    session.workspace_root,
                    session.autoupdate_branch,
                )
                session.last_autoupdate_result = result
                if result.get("status") == "failed":
                    LOGGER.warning(
                        "runner self-update failed (%s): %s",
                        result.get("stage"),
                        result.get("error"),
                    )
                elif result.get("status") == "ok":
                    LOGGER.info(
                        "runner self-updated %s -> %s",
                        result.get("old_head"),
                        result.get("new_head"),
                    )
        return await client.claim_task_v2(session.claim_payload())

    registry.register(
        name="claim_next_task",
        description=(
            "Claim the highest-priority queued task that matches this runner's "
            "capabilities (scope prefixes, tools, tags, tenant, repo state). "
            "Returns ``{task: <record|null>, info: {...}}`` where ``info.reason`` "
            "explains why no task was handed out."
        ),
        input_schema={"type": "object", "properties": {}},
        handler=claim_next_task,
    )

    registry.register(
        name="mark_running",
        description="Transition a claimed task to running (call after worktree is prepared).",
        input_schema={
            "type": "object",
            "required": ["task_id"],
            "properties": {"task_id": {"type": "integer"}},
        },
        handler=lambda args: client.mark_running(int(args["task_id"])),
    )

    registry.register(
        name="get_task",
        description="Fetch current task state (including cancel_requested flag).",
        input_schema={
            "type": "object",
            "required": ["task_id"],
            "properties": {"task_id": {"type": "integer"}},
        },
        handler=lambda args: client.get_task(int(args["task_id"])),
    )

    registry.register(
        name="report_progress",
        description=(
            "Append a progress beat. Call this after each meaningful step so "
            "the dispatcher can stream context. files_touched is optional."
        ),
        input_schema={
            "type": "object",
            "required": ["task_id", "message"],
            "properties": {
                "task_id": {"type": "integer"},
                "message": {"type": "string"},
                "files_touched": {"type": "array", "items": {"type": "string"}},
            },
        },
        handler=lambda args: client.append_progress(
            int(args["task_id"]),
            {
                "worker_id": session.runner_id,
                "message": args["message"],
                "files_touched": args.get("files_touched"),
            },
        ),
    )

    registry.register(
        name="report_stream",
        description=(
            "Append a single line of structured stdout/stderr/info output. "
            "Use this to forward subprocess output (pytest, linters, build) "
            "to the dispatcher in real time."
        ),
        input_schema={
            "type": "object",
            "required": ["task_id", "channel", "line"],
            "properties": {
                "task_id": {"type": "integer"},
                "channel": {"type": "string", "enum": ["stdout", "stderr", "info"]},
                "line": {"type": "string"},
            },
        },
        handler=lambda args: client.append_stream(
            int(args["task_id"]),
            {
                "worker_id": session.runner_id,
                "channel": args["channel"],
                "line": args["line"],
            },
        ),
    )

    registry.register(
        name="report_result",
        description=(
            "Terminal report. status must be one of done|failed|timed_out|"
            "cancelled. Always include head_commit and commits when status="
            "done. Always include error when status!=done."
        ),
        input_schema={
            "type": "object",
            "required": ["task_id", "status"],
            "properties": {
                "task_id": {"type": "integer"},
                "status": {
                    "type": "string",
                    "enum": ["done", "failed", "timed_out", "cancelled"],
                },
                "head_commit": {"type": ["string", "null"]},
                "commits": {"type": "array", "items": {"type": "string"}},
                "files_touched": {"type": "array", "items": {"type": "string"}},
                "test_summary": {"type": ["string", "null"]},
                "log_tail": {"type": ["string", "null"]},
                "error": {"type": ["string", "null"]},
            },
        },
        handler=lambda args: client.submit_result(
            int(args["task_id"]),
            {
                "worker_id": session.runner_id,
                "status": args["status"],
                "head_commit": args.get("head_commit"),
                "commits": args.get("commits", []),
                "files_touched": args.get("files_touched", []),
                "test_summary": args.get("test_summary"),
                "log_tail": args.get("log_tail"),
                "error": args.get("error"),
            },
        ),
    )

    registry.register(
        name="post_note",
        description="Post a free-form note (runner -> dispatcher channel).",
        input_schema={
            "type": "object",
            "required": ["task_id", "body"],
            "properties": {
                "task_id": {"type": "integer"},
                "body": {"type": "string"},
            },
        },
        handler=lambda args: client.post_note(
            int(args["task_id"]),
            {"author": session.runner_id, "body": args["body"]},
        ),
    )

    registry.register(
        name="read_notes",
        description="Read dispatcher notes for a task posted after the given id.",
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

    async def request_drain(_args: dict[str, Any]) -> dict[str, Any]:
        return await client.drain_runner_signed(
            session.runner_id, session.drain_payload()
        )

    registry.register(
        name="request_drain",
        description=(
            "Ask the hub to mark this runner as draining. The hub stops handing "
            "out new tasks; in-flight tasks complete normally."
        ),
        input_schema={"type": "object", "properties": {}},
        handler=request_drain,
    )

    async def self_update_tool(args: dict[str, Any]) -> dict[str, Any]:
        branch = str(args.get("branch") or session.autoupdate_branch or "main")
        result = await asyncio.to_thread(
            self_update_workspace, session.workspace_root, branch
        )
        session.last_autoupdate_result = result
        return result

    registry.register(
        name="self_update",
        description=(
            "Run ``git fetch`` + ``git merge --ff-only origin/<branch>`` in "
            "the runner workspace. Returns the old/new head commits. Auto-"
            "invoked before every claim when FORGEWIRE_RUNNER_AUTOUPDATE=1."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "branch": {"type": "string", "default": "main"},
            },
        },
        handler=self_update_tool,
    )

    async def runner_identity_tool(_args: dict[str, Any]) -> dict[str, Any]:
        return {
            "runner_id": session.runner_id,
            "hostname": session.host["hostname"],
            "os": session.host["os"],
            "arch": session.host["arch"],
            "tools": session.tools,
            "tags": session.tags,
            "scope_prefixes": session.scope_prefixes,
            "tenant": session.tenant,
            "workspace_root": session.workspace_root,
            "runner_version": session.runner_version,
            "protocol_version": PROTOCOL_VERSION,
            "max_concurrent": session.max_concurrent,
            "last_resources": session.last_resources,
            "last_heartbeat_ok": session.last_heartbeat_ok,
        }

    registry.register(
        name="runner_identity",
        description="Return this runner's identity, advertised capabilities, and last sampled resources.",
        input_schema={"type": "object", "properties": {}},
        handler=runner_identity_tool,
    )


async def _run() -> None:
    logging.basicConfig(level=logging.INFO)
    client = load_client_from_env()
    session = _build_session(client)
    LOGGER.info(
        "forgewire-fabric runner MCP starting as runner_id=%s host=%s",
        session.runner_id,
        session.host["hostname"],
    )
    await _register_with_retries(session)
    heartbeat_task = asyncio.create_task(_heartbeat_loop(session))
    server = Server("forgewire-runner")
    registry = ToolRegistry()
    _register_tools(registry, session)
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
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        await client.aclose()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
