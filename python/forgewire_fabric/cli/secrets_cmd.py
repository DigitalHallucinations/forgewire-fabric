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
# M2.5.5a: secret broker
# ---------------------------------------------------------------------------


@cli.group("secrets", help="Sealed secret broker (put/list/rotate/delete).")
def secrets_group() -> None:
    pass


@secrets_group.command("list", help="List secret metadata (names + versions only).")
def secrets_list() -> None:
    async def _go() -> None:
        async with _client() as c:
            _print_json(await c.list_secrets())

    _async(_go())


def _read_secret_value(
    value: str | None, value_file: str | None, value_env: str | None
) -> str:
    """Resolve a secret value from one of --value / --value-file / --value-env.

    Inline ``--value`` is convenient for one-off ops but lands the
    plaintext in shell history; the file/env paths exist for operators
    who want to avoid that. Exactly one source must be set.
    """
    sources = [s for s in (value, value_file, value_env) if s is not None]
    if len(sources) != 1:
        raise click.UsageError(
            "exactly one of --value / --value-file / --value-env is required"
        )
    if value is not None:
        return value
    if value_file is not None:
        return Path(value_file).read_text(encoding="utf-8").rstrip("\r\n")
    assert value_env is not None
    val = os.environ.get(value_env)
    if not val:
        raise click.UsageError(f"env var {value_env} is unset or empty")
    return val


@secrets_group.command("put", help="Create-or-rotate a sealed secret.")
@click.argument("name")
@click.option("--value", default=None, help="Inline plaintext value (shell-history hazard).")
@click.option("--value-file", default=None, help="Read plaintext from this file (trailing newline stripped).")
@click.option("--value-env", default=None, help="Read plaintext from this environment variable.")
def secrets_put(
    name: str, value: str | None, value_file: str | None, value_env: str | None
) -> None:
    plaintext = _read_secret_value(value, value_file, value_env)

    async def _go() -> None:
        async with _client() as c:
            _print_json(await c.put_secret(name=name, value=plaintext))

    _async(_go())


@secrets_group.command("rotate", help=(
    "Rotate an existing sealed secret. Equivalent to `put` on an existing name; "
    "errors out if the name has not been registered yet."
))
@click.argument("name")
@click.option("--value", default=None, help="Inline plaintext value (shell-history hazard).")
@click.option("--value-file", default=None, help="Read plaintext from this file (trailing newline stripped).")
@click.option("--value-env", default=None, help="Read plaintext from this environment variable.")
def secrets_rotate(
    name: str, value: str | None, value_file: str | None, value_env: str | None
) -> None:
    plaintext = _read_secret_value(value, value_file, value_env)

    async def _go() -> None:
        async with _client() as c:
            existing = {row["name"] for row in await c.list_secrets()}
            if name not in existing:
                raise click.ClickException(
                    f"secret {name!r} does not exist; use `secrets put` to create it"
                )
            _print_json(await c.put_secret(name=name, value=plaintext))

    _async(_go())


@secrets_group.command("delete", help="Delete a sealed secret.")
@click.argument("name")
@click.confirmation_option(prompt="Delete this secret?")
def secrets_delete(name: str) -> None:
    async def _go() -> None:
        async with _client() as c:
            _print_json(await c.delete_secret(name))

    _async(_go())


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------
