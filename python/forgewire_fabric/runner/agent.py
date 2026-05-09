"""Standalone ForgeWire runner agent.

Minimal claim-loop runner: register → heartbeat → claim → execute → submit.

The default executor is a **shell command runner** that runs the task's
``prompt`` field as a shell command in the configured workspace, streaming
stdout/stderr back to the hub line by line. Embedding applications (e.g.
PhrenForge) plug in a richer executor by passing ``executor=`` to
:func:`run_runner`; see ``modules/orchestration/forgewire_runner.py`` upstream
for an orchestrator-backed example.

Environment knobs (all optional):

- ``FORGEWIRE_HUB_URL`` / ``FORGEWIRE_HUB_TOKEN`` — hub connection.
- ``FORGEWIRE_RUNNER_TENANT`` — tenant slug.
- ``FORGEWIRE_RUNNER_WORKSPACE_ROOT`` — absolute path to the working tree
  (default: cwd).
- ``FORGEWIRE_RUNNER_TAGS`` — comma-separated capability tags.
- ``FORGEWIRE_RUNNER_SCOPE_PREFIXES`` — comma-separated path prefixes the
  runner will accept tasks for.
- ``FORGEWIRE_RUNNER_MAX_CONCURRENT`` — task concurrency cap (default 1).
- ``FORGEWIRE_RUNNER_POLL_INTERVAL`` — seconds between empty-claim polls
  (default 5.0).
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

from forgewire_fabric.hub.client import (
    BlackboardClient,
    BlackboardError,
    load_client_from_env,
)
from forgewire_fabric.runner.identity import RunnerIdentity, load_or_create
from forgewire_fabric.runner.runner_capabilities import (
    describe_host,
    detect_tools,
    fresh_nonce,
    now_ts,
    sample_resources,
    sign_payload,
)

LOGGER = logging.getLogger("forgewire_fabric.runner.agent")

PROTOCOL_VERSION = 3
HEARTBEAT_INTERVAL_SECONDS = 20.0
DEFAULT_POLL_INTERVAL_SECONDS = 5.0
DEFAULT_RUNNER_VERSION = "0.4.1"


def _parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [s.strip() for s in value.split(",") if s.strip()]


@dataclass
class RunnerConfig:
    workspace_root: str
    tenant: str | None
    tags: list[str]
    scope_prefixes: list[str]
    max_concurrent: int = 1
    runner_version: str = DEFAULT_RUNNER_VERSION
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS

    @classmethod
    def from_env(cls) -> "RunnerConfig":
        workspace_root = (
            os.environ.get("FORGEWIRE_RUNNER_WORKSPACE_ROOT") or os.getcwd()
        )
        # Fail fast: the runner spawns every shell task with
        # ``cwd=workspace_root``. If the directory does not exist the
        # subprocess call dies with ``WinError 267 (directory name is
        # invalid)`` *after* the hub has already routed work to us, which
        # the operator sees as a successful runner that fails every task.
        # Surface the misconfiguration before we register.
        if not os.path.isdir(workspace_root):
            raise RuntimeError(
                f"FORGEWIRE_RUNNER_WORKSPACE_ROOT does not exist: {workspace_root!r}. "
                "Create the directory or correct the env var before starting "
                "the runner."
            )
        return cls(
            workspace_root=workspace_root,
            tenant=os.environ.get("FORGEWIRE_RUNNER_TENANT") or None,
            tags=_parse_csv(os.environ.get("FORGEWIRE_RUNNER_TAGS")),
            scope_prefixes=_parse_csv(
                os.environ.get("FORGEWIRE_RUNNER_SCOPE_PREFIXES")
            ),
            max_concurrent=int(os.environ.get("FORGEWIRE_RUNNER_MAX_CONCURRENT", "1")),
            runner_version=os.environ.get(
                "FORGEWIRE_RUNNER_VERSION", DEFAULT_RUNNER_VERSION
            ),
            poll_interval_seconds=float(
                os.environ.get(
                    "FORGEWIRE_RUNNER_POLL_INTERVAL",
                    str(DEFAULT_POLL_INTERVAL_SECONDS),
                )
            ),
        )


@dataclass
class RunnerSession:
    client: BlackboardClient
    identity: RunnerIdentity
    config: RunnerConfig
    tools: list[str] = field(default_factory=list)
    host: dict[str, Any] = field(default_factory=dict)
    # Self-reported reliability counters. Reset to 0 on the next successful
    # call. Surfaced to the hub via heartbeat so /runners can show it.
    claim_failures_total: int = 0
    claim_failures_consecutive: int = 0
    last_claim_error: str | None = None
    heartbeat_failures_total: int = 0
    heartbeat_failures_consecutive: int = 0
    last_heartbeat_error: str | None = None

    def __post_init__(self) -> None:
        if not self.tools:
            self.tools = detect_tools()
        if not self.host:
            self.host = describe_host()

    @property
    def runner_id(self) -> str:
        return self.identity.runner_id

    def _signed(self, op: str) -> tuple[int, str, str]:
        ts = now_ts()
        nonce = fresh_nonce()
        sig = sign_payload(
            self.identity,
            {"op": op, "runner_id": self.runner_id, "timestamp": ts, "nonce": nonce},
        )
        return ts, nonce, sig

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
        body: dict[str, Any] = {
            "runner_id": self.runner_id,
            "public_key": self.identity.public_key_hex,
            "protocol_version": PROTOCOL_VERSION,
            "runner_version": self.config.runner_version,
            "tools": self.tools,
            "tags": self.config.tags,
            "scope_prefixes": self.config.scope_prefixes,
            "tenant": self.config.tenant,
            "workspace_root": self.config.workspace_root,
            "max_concurrent": self.config.max_concurrent,
            "metadata": {"flavor": "forgewire-runner"},
            "timestamp": ts,
            "nonce": nonce,
            "signature": signature,
        }
        body.update(self.host)
        return body

    def heartbeat_payload(self) -> dict[str, Any]:
        ts, nonce, signature = self._signed("heartbeat")
        return {
            "runner_id": self.runner_id,
            "timestamp": ts,
            "nonce": nonce,
            "signature": signature,
            "claim_failures_total": self.claim_failures_total,
            "claim_failures_consecutive": self.claim_failures_consecutive,
            "last_claim_error": self.last_claim_error,
            "heartbeat_failures_total": self.heartbeat_failures_total,
            **sample_resources(),
        }

    def claim_payload(self) -> dict[str, Any]:
        ts, nonce, signature = self._signed("claim")
        return {
            "runner_id": self.runner_id,
            "timestamp": ts,
            "nonce": nonce,
            "signature": signature,
            "scope_prefixes": self.config.scope_prefixes,
            "tools": self.tools,
            "tags": self.config.tags,
            "tenant": self.config.tenant,
            "workspace_root": self.config.workspace_root,
            **sample_resources(),
        }

    def drain_payload(self) -> dict[str, Any]:
        ts, nonce, signature = self._signed("drain")
        return {
            "runner_id": self.runner_id,
            "timestamp": ts,
            "nonce": nonce,
            "signature": signature,
        }


TaskExecutor = Callable[[dict[str, Any], RunnerSession], Awaitable[dict[str, Any]]]


async def shell_executor(task: dict[str, Any], session: RunnerSession) -> dict[str, Any]:
    """Default executor: run ``task['prompt']`` as a shell command.

    The command runs with ``cwd=session.config.workspace_root``. Stdout and
    stderr are streamed line-by-line to the hub. The terminal status is
    ``done`` on exit code 0, ``failed`` otherwise.
    """
    task_id = int(task["id"])
    prompt = str(task.get("prompt", "")).strip()
    if not prompt:
        return {
            "worker_id": session.runner_id,
            "status": "failed",
            "error": "empty prompt",
        }

    # Windows: cmd /c <prompt>; POSIX: bash -lc <prompt>
    if sys.platform == "win32":
        argv = ["cmd", "/c", prompt]
    else:
        argv = ["bash", "-lc", prompt]

    log_lines: list[str] = []
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            cwd=session.config.workspace_root,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except OSError as exc:
        return {
            "worker_id": session.runner_id,
            "status": "failed",
            "error": f"spawn failed: {exc}",
        }

    async def _pump(stream: asyncio.StreamReader, channel: str) -> None:
        while True:
            raw = await stream.readline()
            if not raw:
                return
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            log_lines.append(f"[{channel}] {line}")
            try:
                await session.client.append_stream(
                    task_id,
                    {"worker_id": session.runner_id, "channel": channel, "line": line},
                )
            except BlackboardError as exc:
                LOGGER.warning("append_stream failed: %s", exc)

    assert proc.stdout is not None and proc.stderr is not None
    await asyncio.gather(
        _pump(proc.stdout, "stdout"),
        _pump(proc.stderr, "stderr"),
    )
    rc = await proc.wait()
    status = "done" if rc == 0 else "failed"
    return {
        "worker_id": session.runner_id,
        "status": status,
        "log_tail": "\n".join(log_lines[-50:]),
        "error": None if rc == 0 else f"exit code {rc}",
    }


async def _register_with_retries(session: RunnerSession) -> dict[str, Any]:
    delay = 1.0
    while True:
        try:
            response = await session.client.register_runner(session.register_payload())
            LOGGER.info(
                "registered runner_id=%s tags=%s",
                session.runner_id,
                ",".join(session.config.tags) or "<none>",
            )
            return response
        except BlackboardError as exc:
            LOGGER.warning("register failed: %s; retry in %.1fs", exc, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)


async def _heartbeat_loop(session: RunnerSession, *, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=HEARTBEAT_INTERVAL_SECONDS)
            return
        except asyncio.TimeoutError:
            pass
        try:
            await session.client.heartbeat(session.runner_id, session.heartbeat_payload())
            session.heartbeat_failures_consecutive = 0
            session.last_heartbeat_error = None
        except BlackboardError as exc:
            session.heartbeat_failures_total += 1
            session.heartbeat_failures_consecutive += 1
            session.last_heartbeat_error = str(exc)
            LOGGER.warning("heartbeat failed: %s", exc)
            if exc.status_code == 404:
                LOGGER.warning(
                    "hub does not know runner_id=%s; re-registering",
                    session.runner_id,
                )
                await _register_with_retries(session)


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


async def _run_one_task(
    session: RunnerSession, executor: TaskExecutor, task: dict[str, Any]
) -> None:
    task_id = int(task["id"])
    LOGGER.info("claimed task %s (todo=%s)", task_id, task.get("todo_id"))
    try:
        await session.client.mark_running(task_id)
    except BlackboardError as exc:
        LOGGER.warning("mark_running failed: %s", exc)
    try:
        result = await executor(task, session)
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.exception("executor crashed for task %s", task_id)
        result = {
            "worker_id": session.runner_id,
            "status": "failed",
            "error": f"executor crashed: {type(exc).__name__}: {exc}",
        }
    try:
        await session.client.submit_result(task_id, result)
        LOGGER.info("submitted task %s status=%s", task_id, result.get("status"))
    except BlackboardError as exc:
        LOGGER.error("submit_result failed for task %s: %s", task_id, exc)


async def _claim_loop(
    session: RunnerSession, executor: TaskExecutor, *, stop: asyncio.Event
) -> None:
    while not stop.is_set():
        try:
            response = await session.client.claim_task_v2(session.claim_payload())
            session.claim_failures_consecutive = 0
            session.last_claim_error = None
        except BlackboardError as exc:
            session.claim_failures_total += 1
            session.claim_failures_consecutive += 1
            session.last_claim_error = str(exc)
            LOGGER.warning("claim failed: %s", exc)
            # Symmetric to _heartbeat_loop: a 404 here means the hub forgot
            # this runner_id (e.g. hub state reset, snapshot import, rqlite
            # quorum loss replayed). Without re-registering the claim loop
            # spins forever even though heartbeats may already be healing.
            if exc.status_code == 404:
                LOGGER.warning(
                    "hub does not know runner_id=%s on claim; re-registering",
                    session.runner_id,
                )
                try:
                    await _register_with_retries(session)
                except Exception:  # pragma: no cover - defensive
                    LOGGER.exception("re-register on claim 404 failed")
            await _sleep_or_stop(stop, session.config.poll_interval_seconds)
            continue
        task = response.get("task")
        if not task:
            await _sleep_or_stop(stop, session.config.poll_interval_seconds)
            continue
        await _run_one_task(session, executor, task)


async def run_runner(
    *,
    config: Optional[RunnerConfig] = None,
    client: Optional[BlackboardClient] = None,
    executor: Optional[TaskExecutor] = None,
    stop_event: Optional[asyncio.Event] = None,
    identity: Optional[RunnerIdentity] = None,
) -> None:
    """Run the registration + heartbeat + claim loops until ``stop_event`` fires."""
    cfg = config or RunnerConfig.from_env()
    owns_client = client is None
    if client is None:
        client = load_client_from_env()
    executor = executor or shell_executor
    stop_event = stop_event or asyncio.Event()
    identity = identity or load_or_create()
    session = RunnerSession(client=client, identity=identity, config=cfg)

    try:
        await _register_with_retries(session)
        heartbeat_task = asyncio.create_task(
            _heartbeat_loop(session, stop=stop_event), name="forgewire-runner-heartbeat"
        )
        claim_task = asyncio.create_task(
            _claim_loop(session, executor, stop=stop_event), name="forgewire-runner-claim"
        )
        await stop_event.wait()
        for t in (heartbeat_task, claim_task):
            t.cancel()
        for t in (heartbeat_task, claim_task):
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        try:
            await session.client.drain_runner_signed(
                session.runner_id, session.drain_payload()
            )
        except BlackboardError as exc:
            LOGGER.warning("drain on shutdown failed: %s", exc)
    finally:
        if owns_client:
            await client.aclose()
