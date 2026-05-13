"""ForgeWire CLI package."""

from __future__ import annotations

import sys

import click

from forgewire_fabric import __version__, runtime_version


def _version_triple() -> str:
    """Render the full compatibility triple shown by ``--version``."""
    from forgewire_fabric.hub.server import (
        DEFAULT_MIN_RUNNER_VERSION,
        PROTOCOL_VERSION,
        SCHEMA_VERSION,
    )

    rt = runtime_version() or "(pure-python)"
    return (
        f"forgewire-fabric {__version__}\n"
        f"  runtime    = {rt}\n"
        f"  protocol   = {PROTOCOL_VERSION}\n"
        f"  schema     = {SCHEMA_VERSION}\n"
        f"  min_runner = {DEFAULT_MIN_RUNNER_VERSION}"
    )


def _print_version(ctx: click.Context, _param: click.Parameter, value: bool) -> None:
    if not value or ctx.resilient_parsing:
        return
    click.echo(_version_triple())
    ctx.exit()


@click.group(help="ForgeWire control-plane CLI.")
@click.option(
    "--version",
    is_flag=True,
    expose_value=False,
    is_eager=True,
    callback=_print_version,
    help="Show fabric/runtime/protocol/schema/min_runner versions and exit.",
)
def cli() -> None:
    pass


from . import dispatch as _dispatch  # noqa: E402,F401
from . import approvals as _approvals  # noqa: E402,F401
from . import audit as _audit  # noqa: E402,F401
from . import hub as _hub  # noqa: E402,F401
from . import identity as _identity  # noqa: E402,F401
from . import runner as _runner  # noqa: E402,F401
from . import runners_cmd as _runners_cmd  # noqa: E402,F401
from . import secrets_cmd as _secrets_cmd  # noqa: E402,F401
from . import setup as _setup  # noqa: E402,F401
from . import vscode as _vscode  # noqa: E402,F401
from ._helpers import _async, _client, _load_token_for_probe, _print_json  # noqa: E402,F401
from .vscode import _write_vscode_user_mcp, _write_vscode_user_settings  # noqa: E402,F401


def main() -> None:
    cli()


__all__ = [
    "cli",
    "main",
    "sys",
    "_async",
    "_client",
    "_load_token_for_probe",
    "_print_json",
    "_write_vscode_user_mcp",
    "_write_vscode_user_settings",
]
