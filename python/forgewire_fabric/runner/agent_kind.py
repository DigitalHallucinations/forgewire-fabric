"""Standalone ForgeWire ``kind:agent`` runner.

A sibling to :mod:`forgewire_fabric.runner.agent` (which is the shell-exec
``kind:command`` runner). This binary advertises ``kind:agent`` and a
deterministic harness executor that satisfies sealed approval-roundtrip
briefs without pulling in the full ForgeWire orchestrator stack.

The executor is intentionally narrow: it understands a sealed brief of the
shape::

    scope_globs = ["<single relative path under workspace_root>"]
    prompt      = "... approval-roundtrip-marker: <marker> ..."

When matched, it writes the marker line to the target file inside its
sandbox workspace and returns ``status=done`` with ``files_touched`` set.
Anything that does not match the harness shape is reported ``failed`` with
a clear ``error`` so the dispatcher can re-route to a real Copilot Chat
session or to the orchestrator-backed bridge upstream.

Environment knobs (all optional unless noted):

- ``FORGEWIRE_HUB_URL`` / ``FORGEWIRE_HUB_TOKEN`` -- hub connection
  (required).
- ``FORGEWIRE_AGENT_RUNNER_IDENTITY_PATH`` -- identity file for this
  binary. Default: ``%PROGRAMDATA%\\forgewire\\agent_runner_identity.json``
  (Windows) or ``/var/lib/forgewire/agent_runner_identity.json`` (POSIX).
  Picked so the agent runner gets a distinct ``runner_id`` from the
  command runner on the same host.
- ``FORGEWIRE_AGENT_RUNNER_WORKSPACE_ROOT`` -- sandbox directory the
  executor writes into. Default: ``%PROGRAMDATA%\\forgewire\\agent-sandbox``
  on Windows; ``/var/lib/forgewire/agent-sandbox`` elsewhere.
- ``FORGEWIRE_AGENT_RUNNER_TAGS`` -- comma-separated extra tags. The
  ``kind:agent`` tag is appended automatically and any operator-supplied
  ``kind:*`` is stripped.
- ``FORGEWIRE_AGENT_RUNNER_POLL_INTERVAL`` -- seconds between empty
  claim polls (default 5.0).
- ``FORGEWIRE_AGENT_RUNNER_TENANT`` -- tenant slug.
- ``FORGEWIRE_AGENT_RUNNER_SCOPE_PREFIXES`` -- comma-separated path
  prefixes; default: empty (accept any scope).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
import signal
import sys
from pathlib import Path
from typing import Any

from forgewire_fabric.hub.client import BlackboardError, load_client_from_env
from forgewire_fabric.runner.agent import (
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_RUNNER_VERSION,
    RunnerConfig,
    RunnerSession,
    run_runner,
)
from forgewire_fabric.runner.identity import load_or_create
from forgewire_fabric.runner.runner_capabilities import apply_kind_tag

LOGGER = logging.getLogger("forgewire_fabric.runner.agent_kind")

_MARKER_PATTERNS = (
    re.compile(r"approval-?roundtrip-?marker[:=]\s*([A-Za-z0-9._-]+)", re.IGNORECASE),
    re.compile(r"\bmarker[:=]\s*([A-Za-z0-9._-]+)", re.IGNORECASE),
)


def _parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [s.strip() for s in value.split(",") if s.strip()]


def _default_identity_path() -> Path:
    override = os.environ.get("FORGEWIRE_AGENT_RUNNER_IDENTITY_PATH")
    if override:
        return Path(override).expanduser()
    if sys.platform == "win32":
        program_data = os.environ.get("PROGRAMDATA") or r"C:\ProgramData"
        return Path(program_data) / "forgewire" / "agent_runner_identity.json"
    return Path("/var/lib/forgewire/agent_runner_identity.json")


def _default_workspace_root() -> Path:
    override = os.environ.get("FORGEWIRE_AGENT_RUNNER_WORKSPACE_ROOT")
    if override:
        return Path(override).expanduser()
    if sys.platform == "win32":
        program_data = os.environ.get("PROGRAMDATA") or r"C:\ProgramData"
        return Path(program_data) / "forgewire" / "agent-sandbox"
    return Path("/var/lib/forgewire/agent-sandbox")


def _extract_marker(prompt: str) -> str | None:
    for pat in _MARKER_PATTERNS:
        match = pat.search(prompt)
        if match:
            return match.group(1)
    return None


def _build_config() -> RunnerConfig:
    workspace = _default_workspace_root()
    workspace.mkdir(parents=True, exist_ok=True)
    extra = _parse_csv(os.environ.get("FORGEWIRE_AGENT_RUNNER_TAGS"))
    # apply_kind_tag strips any operator-supplied ``kind:*`` and appends the
    # canonical one. The agent runner's kind is the binary, not config.
    tags = apply_kind_tag(extra, default_kind="agent")
    return RunnerConfig(
        workspace_root=str(workspace),
        tenant=os.environ.get("FORGEWIRE_AGENT_RUNNER_TENANT") or None,
        tags=tags,
        scope_prefixes=_parse_csv(
            os.environ.get("FORGEWIRE_AGENT_RUNNER_SCOPE_PREFIXES")
        ),
        max_concurrent=int(
            os.environ.get("FORGEWIRE_AGENT_RUNNER_MAX_CONCURRENT", "1")
        ),
        runner_version=os.environ.get(
            "FORGEWIRE_AGENT_RUNNER_VERSION", DEFAULT_RUNNER_VERSION
        ),
        poll_interval_seconds=float(
            os.environ.get(
                "FORGEWIRE_AGENT_RUNNER_POLL_INTERVAL",
                str(DEFAULT_POLL_INTERVAL_SECONDS),
            )
        ),
    )


async def _stream(session: RunnerSession, task_id: int, channel: str, line: str) -> None:
    try:
        await session.client.append_stream(
            task_id,
            {"worker_id": session.runner_id, "channel": channel, "line": line},
        )
    except BlackboardError as exc:
        LOGGER.warning("append_stream failed for task %s: %s", task_id, exc)


async def harness_executor(task: dict[str, Any], session: RunnerSession) -> dict[str, Any]:
    """Deterministic kind:agent executor for sealed approval-roundtrip briefs.

    Writes the brief's marker line to ``scope_globs[0]`` inside the runner
    sandbox and returns ``done``. Anything that doesn't match the harness
    shape is reported ``failed`` with a precise reason so the operator can
    see exactly why the brief was not understood.
    """
    task_id = int(task["id"])
    worker_id = session.runner_id
    workspace = Path(session.config.workspace_root)
    scope_globs = list(task.get("scope_globs") or [])
    prompt = str(task.get("prompt") or "")

    if len(scope_globs) != 1:
        await _stream(
            session,
            task_id,
            "stderr",
            f"[forgewire-agent] expected exactly one scope_globs entry, got {len(scope_globs)}",
        )
        return {
            "worker_id": worker_id,
            "status": "failed",
            "error": "harness executor expects exactly one scope_globs entry",
        }

    rel = scope_globs[0]
    if "*" in rel or "?" in rel:
        return {
            "worker_id": worker_id,
            "status": "failed",
            "error": (
                "harness executor expects a literal relative path in "
                f"scope_globs[0]; got glob {rel!r}"
            ),
        }

    target = (workspace / rel).resolve()
    try:
        target.relative_to(workspace.resolve())
    except ValueError:
        return {
            "worker_id": worker_id,
            "status": "failed",
            "error": f"target path escapes runner sandbox: {target}",
        }

    marker = _extract_marker(prompt)
    if marker is None:
        return {
            "worker_id": worker_id,
            "status": "failed",
            "error": (
                "harness executor could not find an "
                "'approval-roundtrip-marker: <value>' line in the prompt"
            ),
        }

    target.parent.mkdir(parents=True, exist_ok=True)
    body = f"approval-roundtrip-marker: {marker}\n"
    target.write_text(body, encoding="utf-8")

    await _stream(
        session,
        task_id,
        "stdout",
        f"[forgewire-agent] wrote marker={marker} to {target}",
    )

    return {
        "worker_id": worker_id,
        "status": "done",
        "head_commit": None,
        "commits": [],
        "files_touched": [rel],
        "test_summary": None,
        "log_tail": f"marker={marker} -> {target}",
        "error": None,
    }


async def run_agent_runner(*, stop_event: asyncio.Event | None = None) -> None:
    """Boot the kind:agent runner with the harness executor."""
    config = _build_config()
    identity = load_or_create(_default_identity_path())
    LOGGER.info(
        "starting kind:agent runner runner_id=%s workspace=%s",
        identity.runner_id,
        config.workspace_root,
    )
    client = load_client_from_env()
    try:
        await run_runner(
            config=config,
            client=client,
            executor=harness_executor,
            stop_event=stop_event,
            identity=identity,
        )
    finally:
        await client.aclose()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    stop = asyncio.Event()

    def _handler(*_a: Any) -> None:  # pragma: no cover - signal wiring
        stop.set()

    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        sigs = (signal.SIGINT, signal.SIGTERM) if sys.platform != "win32" else (signal.SIGINT,)
        for sig in sigs:
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, _handler)
        loop.run_until_complete(run_agent_runner(stop_event=stop))
    finally:
        loop.close()
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
