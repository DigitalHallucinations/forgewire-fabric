from __future__ import annotations

# Mechanical M2.6.5 split from the former monolithic cli.py.
# Each command module imports a broad helper surface while this phase keeps behavior unchanged.
# ruff: noqa: F401,F811

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
from .dispatch import _register_dispatcher_with_client
from ._helpers import _P, _P_home, _async, _candidates_from_env, _client, _load_token_for_probe, _print_json

# ---------------------------------------------------------------------------
# keys / token
# ---------------------------------------------------------------------------


@cli.group(help="Identity / key utilities.")
def keys() -> None:
    pass
@keys.command("init", help="Generate (or load) the local runner identity file.")
@click.option("--path", default=None)
def keys_init(path: str | None) -> None:
    from forgewire_fabric.runner.identity import load_or_create

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
    from forgewire_fabric.dispatcher.identity import load_or_create

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


@dispatchers_group.command(
    "register",
    help="Create/load a dispatcher identity, register it with the hub, and mark this host dispatch-enabled.",
)
@click.option(
    "--identity",
    "identity_path",
    default=None,
    help="Path to dispatcher_identity.json (default: ~/.forgewire/dispatcher_identity.json).",
)
@click.option("--label", default=None, help="Dispatcher label (default: hostname).")
@click.option("--hostname", default=None, help="Hostname to report (default: socket.gethostname()).")
def dispatchers_register(identity_path: str | None, label: str | None, hostname: str | None) -> None:
    import socket as _socket

    from forgewire_fabric.dispatcher.identity import load_or_create

    ident = load_or_create(Path(identity_path) if identity_path else None, label=label)
    reported_hostname = hostname or _socket.gethostname()

    async def _go() -> None:
        async with _client() as c:
            result = await _register_dispatcher_with_client(
                c,
                ident,
                hostname=reported_hostname,
            )
        _print_json(result)

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
