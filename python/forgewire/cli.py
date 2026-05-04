"""ForgeWire CLI — ``forgewire <subcommand>``.

Subcommands:

* ``forgewire hub start``     — start the FastAPI hub (signed dispatch / claim / streams).
* ``forgewire runner start``  — register a runner and run the claim loop.
* ``forgewire dispatch``      — POST a sealed task to the hub.
* ``forgewire tasks list``    — list tasks.
* ``forgewire tasks show``    — show a single task.
* ``forgewire tasks stream``  — tail a task's stream output.
* ``forgewire runners list``  — list registered runners.
* ``forgewire keys init``     — generate a dispatcher ed25519 keypair.
* ``forgewire token gen``     — generate a random hub token (32 hex chars).

Connection envs (canonical / legacy):

* ``FORGEWIRE_HUB_URL``   (alias: ``BLACKBOARD_URL``)
* ``FORGEWIRE_HUB_TOKEN`` (alias: ``BLACKBOARD_TOKEN``)
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import signal
import sys
from pathlib import Path
from typing import Any

import click

from forgewire import __version__


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _client():  # pragma: no cover - thin wrapper
    from forgewire.hub.client import load_client_from_env

    return load_client_from_env()


def _print_json(obj: Any) -> None:
    click.echo(json.dumps(obj, indent=2, sort_keys=True, default=str))


def _async(coro: Any) -> Any:
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# top-level group
# ---------------------------------------------------------------------------


@click.group(help="ForgeWire control-plane CLI.")
@click.version_option(__version__, prog_name="forgewire")
def cli() -> None:
    pass


# ---------------------------------------------------------------------------
# hub
# ---------------------------------------------------------------------------


@cli.group(help="Hub server commands.")
def hub() -> None:
    pass


@hub.command("start", help="Start the ForgeWire hub (uvicorn).")
@click.option("--host", default=None, help="Bind host (default: 127.0.0.1 or $FORGEWIRE_HUB_HOST).")
@click.option("--port", type=int, default=None, help="Bind port (default: 8765 or $FORGEWIRE_HUB_PORT).")
@click.option("--db-path", default=None, help="SQLite DB path.")
@click.option("--token-file", default=None, help="File containing the hub token.")
@click.option("--mdns", is_flag=True, default=False, help="Advertise via mDNS on the LAN.")
@click.option("--log-level", default="info")
def hub_start(
    host: str | None,
    port: int | None,
    db_path: str | None,
    token_file: str | None,
    mdns: bool,
    log_level: str,
) -> None:
    from forgewire.hub.server import main as hub_main

    argv: list[str] = []
    if host:
        argv += ["--host", host]
    if port is not None:
        argv += ["--port", str(port)]
    if db_path:
        argv += ["--db-path", db_path]
    if token_file:
        argv += ["--token-file", token_file]
    if mdns:
        argv += ["--mdns"]
    argv += ["--log-level", log_level]
    hub_main(argv)


@hub.command("healthz", help="Ping the hub /healthz endpoint.")
def hub_healthz() -> None:
    async def _go() -> None:
        async with _client() as c:
            _print_json(await c.healthz())

    _async(_go())


# ---------------------------------------------------------------------------
# runner
# ---------------------------------------------------------------------------


@cli.group(help="Runner agent commands.")
def runner() -> None:
    pass


@runner.command("start", help="Run the claim loop for this host.")
@click.option("--workspace-root", default=None, help="Working tree the runner operates in.")
@click.option("--tags", default=None, help="Comma-separated capability tags.")
@click.option("--scope-prefixes", default=None, help="Comma-separated path prefixes.")
@click.option("--tenant", default=None)
@click.option("--max-concurrent", type=int, default=None)
@click.option("--poll-interval", type=float, default=None, help="Seconds between empty-claim polls.")
def runner_start(
    workspace_root: str | None,
    tags: str | None,
    scope_prefixes: str | None,
    tenant: str | None,
    max_concurrent: int | None,
    poll_interval: float | None,
) -> None:
    if workspace_root:
        os.environ["FORGEWIRE_RUNNER_WORKSPACE_ROOT"] = workspace_root
    if tags is not None:
        os.environ["FORGEWIRE_RUNNER_TAGS"] = tags
    if scope_prefixes is not None:
        os.environ["FORGEWIRE_RUNNER_SCOPE_PREFIXES"] = scope_prefixes
    if tenant:
        os.environ["FORGEWIRE_RUNNER_TENANT"] = tenant
    if max_concurrent is not None:
        os.environ["FORGEWIRE_RUNNER_MAX_CONCURRENT"] = str(max_concurrent)
    if poll_interval is not None:
        os.environ["FORGEWIRE_RUNNER_POLL_INTERVAL"] = str(poll_interval)

    from forgewire.runner.agent import run_runner

    stop = asyncio.Event()

    def _handler(*_a: Any) -> None:  # pragma: no cover - signal wiring
        stop.set()

    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        for sig in (signal.SIGINT, signal.SIGTERM) if sys.platform != "win32" else (signal.SIGINT,):
            try:
                loop.add_signal_handler(sig, _handler)
            except NotImplementedError:
                pass
        loop.run_until_complete(run_runner(stop_event=stop))
    finally:
        loop.close()


@runner.command("identity", help="Print this runner's persistent identity.")
@click.option("--path", default=None, help="Override identity file path.")
def runner_identity(path: str | None) -> None:
    from forgewire.runner.identity import load_or_create

    p = Path(path) if path else None
    ident = load_or_create(p)
    _print_json(
        {
            "runner_id": ident.runner_id,
            "public_key": ident.public_key_hex,
        }
    )


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
    from forgewire.dispatcher.identity import (
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
        reg_ts = int(_time.time())
        reg_nonce = _secrets.token_hex(16)
        reg_body = {
            "op": "register-dispatcher",
            "dispatcher_id": ident.dispatcher_id,
            "public_key": ident.public_key_hex,
            "timestamp": reg_ts,
            "nonce": reg_nonce,
        }
        reg_canon = _json.dumps(reg_body, sort_keys=True, separators=(",", ":")).encode("utf-8")
        reg_sig = ident.sign(reg_canon)
        await c.register_dispatcher(
            {
                "dispatcher_id": ident.dispatcher_id,
                "public_key": ident.public_key_hex,
                "label": ident.label,
                "hostname": _socket.gethostname(),
                "timestamp": reg_ts,
                "nonce": reg_nonce,
                "signature": reg_sig,
            }
        )
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


@tasks.command("stream", help="Tail a task's stream output (SSE).")
@click.argument("task_id", type=int)
def tasks_stream(task_id: int) -> None:
    async def _go() -> None:
        async with _client() as c:
            async for event, data in c.stream_events(task_id):
                click.echo(f"{event}: {data}")

    _async(_go())


# ---------------------------------------------------------------------------
# runners
# ---------------------------------------------------------------------------


@cli.group("runners", help="Inspect registered runners.")
def runners_group() -> None:
    pass


@runners_group.command("list", help="List currently-registered runners.")
def runners_list() -> None:
    async def _go() -> None:
        async with _client() as c:
            _print_json(await c.list_runners())

    _async(_go())


# ---------------------------------------------------------------------------
# keys / token
# ---------------------------------------------------------------------------


@cli.group(help="Identity / key utilities.")
def keys() -> None:
    pass


@keys.command("init", help="Generate (or load) the local runner identity file.")
@click.option("--path", default=None)
def keys_init(path: str | None) -> None:
    from forgewire.runner.identity import load_or_create

    ident = load_or_create(Path(path) if path else None)
    _print_json(
        {
            "runner_id": ident.runner_id,
            "public_key": ident.public_key_hex,
        }
    )


@keys.command(
    "init-dispatcher",
    help="Generate (or load) the dispatcher identity file used for signed dispatch.",
)
@click.option("--path", default=None)
@click.option("--label", default=None, help="Freeform label (default: hostname).")
def keys_init_dispatcher(path: str | None, label: str | None) -> None:
    from forgewire.dispatcher.identity import load_or_create

    ident = load_or_create(Path(path) if path else None, label=label)
    _print_json(
        {
            "dispatcher_id": ident.dispatcher_id,
            "public_key": ident.public_key_hex,
            "label": ident.label,
        }
    )


@cli.group("dispatchers", help="Inspect registered dispatchers.")
def dispatchers_group() -> None:
    pass


@dispatchers_group.command("list", help="List dispatchers known to the hub.")
def dispatchers_list() -> None:
    async def _go() -> None:
        async with _client() as c:
            _print_json(await c.list_dispatchers())

    _async(_go())


@cli.group(help="Token utilities.")
def token() -> None:
    pass


@token.command("gen", help="Generate a random 32-char hub token.")
@click.option("--length", type=int, default=32, show_default=True)
def token_gen(length: int) -> None:
    if length < 16:
        raise click.BadParameter("length must be >= 16")
    click.echo(secrets.token_hex(length // 2))


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
