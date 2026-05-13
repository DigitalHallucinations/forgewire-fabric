from __future__ import annotations

# Mechanical M2.6.5 split from the former monolithic cli.py.
# Each command module imports a broad helper surface while this phase keeps behavior unchanged.
# ruff: noqa: F401

import asyncio
import contextlib
import json
import os
import secrets
import signal
import sys
from pathlib import Path
from typing import Any

import click

from . import cli
from ._helpers import _P, _P_home, _async, _candidates_from_env, _client, _load_token_for_probe, _print_json

# ---------------------------------------------------------------------------
# dispatch
# ---------------------------------------------------------------------------


@cli.command(help="Dispatch a task envelope to the hub.")
@click.argument("prompt")
@click.option("--title", default=None, help="Short title (default: first 60 chars of prompt).")
@click.option("--scope", "scope_globs", multiple=True, required=True, help="Repeatable scope glob.")
@click.option("--branch", required=True, help="Per-task branch name (e.g. agent/host/todo-slice).")
@click.option("--base-commit", required=True, help="Base commit SHA the runner will branch from.")
@click.option("--todo-id", default=None)
@click.option("--timeout-minutes", type=int, default=60)
@click.option("--priority", type=int, default=100)
@click.option("--required-tag", "required_tags", multiple=True)
@click.option("--required-tool", "required_tools", multiple=True)
@click.option("--tenant", default=None)
@click.option(
    "--signed/--unsigned",
    "signed",
    default=None,
    help=(
        "Force signed (POST /tasks/v2) or unsigned (POST /tasks) dispatch. "
        "Default: signed if a dispatcher identity file exists, else unsigned."
    ),
)
@click.option(
    "--identity",
    "identity_path",
    default=None,
    help="Path to a dispatcher_identity.json (default: ~/.forgewire/dispatcher_identity.json).",
)
def dispatch(
    prompt: str,
    title: str | None,
    scope_globs: tuple[str, ...],
    branch: str,
    base_commit: str,
    todo_id: str | None,
    timeout_minutes: int,
    priority: int,
    required_tags: tuple[str, ...],
    required_tools: tuple[str, ...],
    tenant: str | None,
    signed: bool | None,
    identity_path: str | None,
) -> None:
    payload = {
        "title": title or prompt[:60],
        "prompt": prompt,
        "scope_globs": list(scope_globs),
        "base_commit": base_commit,
        "branch": branch,
        "todo_id": todo_id,
        "timeout_minutes": timeout_minutes,
        "priority": priority,
        "required_tags": list(required_tags) or None,
        "required_tools": list(required_tools) or None,
        "tenant": tenant,
    }
    payload = {k: v for k, v in payload.items() if v is not None}

    # Decide signed vs unsigned. Auto: signed iff an identity file exists.
    from forgewire_fabric.dispatcher.identity import (
        DEFAULT_IDENTITY_PATH,
        load_or_create,
    )

    target_path = Path(identity_path) if identity_path else DEFAULT_IDENTITY_PATH
    use_signed = signed if signed is not None else target_path.exists()

    if use_signed:
        ident = load_or_create(target_path)
        _async(_dispatch_signed(ident, payload))
    else:
        async def _go() -> None:
            async with _client() as c:
                _print_json(await c.dispatch_task(payload))

        _async(_go())


async def _dispatch_signed(ident: Any, payload: dict[str, Any]) -> None:
    """Sign and POST to /tasks/v2, auto-registering the dispatcher on 404."""
    import json as _json
    import secrets as _secrets
    import socket as _socket
    import time as _time

    timestamp = int(_time.time())
    nonce = _secrets.token_hex(16)
    signed_body = {
        "op": "dispatch",
        "dispatcher_id": ident.dispatcher_id,
        "title": payload["title"],
        "prompt": payload["prompt"],
        "scope_globs": list(payload["scope_globs"]),
        "base_commit": payload["base_commit"],
        "branch": payload["branch"],
        "timestamp": timestamp,
        "nonce": nonce,
    }
    canonical = _json.dumps(signed_body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    sig = ident.sign(canonical)
    full = dict(payload)
    full.update(
        {
            "dispatcher_id": ident.dispatcher_id,
            "timestamp": timestamp,
            "nonce": nonce,
            "signature": sig,
        }
    )
    async with _client() as c:
        try:
            _print_json(await c.dispatch_task_signed(full))
            return
        except Exception as exc:  # noqa: BLE001 - we re-raise non-404
            status = getattr(exc, "status_code", None)
            if status != 404:
                raise
        # Auto-register on first signed dispatch and retry once.
        click.echo("Registering dispatcher with hub on first use...", err=True)
        await _register_dispatcher_with_client(c, ident, hostname=_socket.gethostname())
        # Re-sign with a fresh nonce/timestamp and retry the dispatch.
        timestamp = int(_time.time())
        nonce = _secrets.token_hex(16)
        signed_body["timestamp"] = timestamp
        signed_body["nonce"] = nonce
        canonical = _json.dumps(signed_body, sort_keys=True, separators=(",", ":")).encode("utf-8")
        full["timestamp"] = timestamp
        full["nonce"] = nonce
        full["signature"] = ident.sign(canonical)
        _print_json(await c.dispatch_task_signed(full))


async def _register_dispatcher_with_client(
    client: Any,
    ident: Any,
    *,
    hostname: str,
) -> dict[str, Any]:
    import json as _json
    import secrets as _secrets
    import time as _time

    timestamp = int(_time.time())
    nonce = _secrets.token_hex(16)
    body = {
        "op": "register-dispatcher",
        "dispatcher_id": ident.dispatcher_id,
        "public_key": ident.public_key_hex,
        "timestamp": timestamp,
        "nonce": nonce,
    }
    canonical = _json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    payload = {
        "dispatcher_id": ident.dispatcher_id,
        "public_key": ident.public_key_hex,
        "label": ident.label,
        "hostname": hostname,
        "metadata": {"dispatch_enabled": True},
        "timestamp": timestamp,
        "nonce": nonce,
        "signature": ident.sign(canonical),
    }
    result = await client.register_dispatcher(payload)
    # Dispatcher registration is the source of truth for signed dispatch.
    # Host-role reporting is UI enrichment; do not fail dispatch because
    # an older hub lacks /hosts/roles.
    with contextlib.suppress(Exception):
        await client.set_host_role(
            {
                "hostname": hostname,
                "role": "dispatch",
                "enabled": True,
                "status": "registered",
                "metadata": {
                    "dispatcher_id": ident.dispatcher_id,
                    "label": ident.label,
                },
            }
        )
    return result

# ---------------------------------------------------------------------------
# tasks
# ---------------------------------------------------------------------------


@cli.group(help="Inspect tasks.")
def tasks() -> None:
    pass


@tasks.command("list", help="List recent tasks.")
@click.option("--status", default=None, help="Filter by status (queued/running/done/failed/cancelled/timed_out).")
@click.option("--limit", type=int, default=50)
def tasks_list(status: str | None, limit: int) -> None:
    async def _go() -> None:
        async with _client() as c:
            _print_json(await c.list_tasks(status=status, limit=limit))

    _async(_go())


@tasks.command("show", help="Show one task.")
@click.argument("task_id", type=int)
def tasks_show(task_id: int) -> None:
    async def _go() -> None:
        async with _client() as c:
            _print_json(await c.get_task(task_id))

    _async(_go())


@tasks.command(
    "waiting",
    help="List queued tasks no online runner can satisfy (M2.5.4).",
)
def tasks_waiting() -> None:
    async def _go() -> None:
        async with _client() as c:
            _print_json(await c.list_waiting_tasks())

    _async(_go())


@tasks.command("stream", help="Tail a task's stream output (SSE).")
@click.argument("task_id", type=int)
def tasks_stream(task_id: int) -> None:
    async def _go() -> None:
        async with _client() as c:
            async for event, data in c.stream_events(task_id):
                click.echo(f"{event}: {data}")

    _async(_go())
