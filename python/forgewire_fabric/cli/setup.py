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
from ._helpers import _P, _P_home, _async, _candidates_from_env, _client, _load_token_for_probe, _print_json

from .vscode import _write_vscode_user_mcp, _write_vscode_user_settings

# ---------------------------------------------------------------------------
# setup (one-shot)
# ---------------------------------------------------------------------------


@cli.command(
    "setup",
    help=(
        "One-shot install for this host. Picks a role and installs the "
        "matching OS service(s). On Windows the underlying scripts self-elevate."
    ),
)
@click.option(
    "--role",
    type=click.Choice(["hub", "runner", "hub-and-runner"]),
    required=True,
    help="hub: this box hosts the hub. runner: this box only runs jobs. "
    "hub-and-runner: this box does both (single-box dev fabric).",
)
@click.option("--hub-url", default=None, help="Hub URL the runner connects to. "
              "Required when --role includes 'runner' and not 'hub'. For "
              "'hub-and-runner' defaults to http://127.0.0.1:<port>.")
@click.option("--hub-token", default=None, help="Bearer token. For roles that "
              "include 'hub' a fresh 32-hex token is generated when omitted; "
              "for 'runner' it is read from FORGEWIRE_HUB_TOKEN or the "
              "default hub.token file.")
@click.option("--port", type=int, default=8765, show_default=True)
@click.option("--bind-host", default="0.0.0.0", show_default=True,
              help="Hub bind host. Use 0.0.0.0 to accept LAN runners.")
@click.option("--workspace-root", default=None,
              help="Runner workspace root. Defaults to the current directory.")
@click.option("--hub-ssh-host", default=None,
              help="(runner role) Hub SSH target. Enables cross-host hub watchdog.")
@click.option("--hub-ssh-user", default=None,
              help="(runner role) SSH user on the hub host.")
@click.option("--hub-ssh-key-file", default=None,
              help="(runner role) Path to the SSH private key for the hub watchdog.")
@click.option("--no-hub-watchdog", is_flag=True,
              help="(runner role) Skip the cross-host hub watchdog even when SSH credentials are supplied.")
def setup(
    role: str,
    hub_url: str | None,
    hub_token: str | None,
    port: int,
    bind_host: str,
    workspace_root: str | None,
    hub_ssh_host: str | None,
    hub_ssh_user: str | None,
    hub_ssh_key_file: str | None,
    no_hub_watchdog: bool,
) -> None:
    from pathlib import Path as _P

    from forgewire_fabric.install import install_hub, install_runner

    install_role_hub = role in ("hub", "hub-and-runner")
    install_role_runner = role in ("runner", "hub-and-runner")

    # --- token resolution -------------------------------------------------
    token_file_default = _P(r"C:\ProgramData\forgewire\hub.token") if sys.platform.startswith("win") else _P("/etc/forgewire/hub.token")
    if install_role_hub and not hub_token:
        hub_token = secrets.token_hex(16)
    if install_role_runner and not hub_token:
        env_tok = os.environ.get("FORGEWIRE_HUB_TOKEN")
        if env_tok:
            hub_token = env_tok
        elif token_file_default.exists():
            hub_token = token_file_default.read_text(encoding="utf-8").strip()
        else:
            raise SystemExit(
                "Runner role requires --hub-token (or FORGEWIRE_HUB_TOKEN, "
                f"or {token_file_default} from a hub install)."
            )

    # --- hub url resolution ----------------------------------------------
    if install_role_runner and not hub_url:
        if install_role_hub:
            hub_url = f"http://127.0.0.1:{port}"
        else:
            raise SystemExit("Runner role requires --hub-url.")

    # --- workspace --------------------------------------------------------
    if install_role_runner and not workspace_root:
        workspace_root = str(_P.cwd())

    # --- install hub first so the runner has something to claim from ----
    if install_role_hub:
        click.echo(f"Installing hub on {bind_host}:{port}...")
        install_hub(port=port, host=bind_host, token=hub_token)

    if install_role_runner:
        assert hub_url is not None and hub_token is not None and workspace_root is not None
        click.echo(f"Installing runner -> {hub_url} (workspace: {workspace_root})...")
        install_runner(
            hub_url=hub_url,
            hub_token=hub_token,
            workspace_root=workspace_root,
            hub_ssh_host=hub_ssh_host,
            hub_ssh_user=hub_ssh_user,
            hub_ssh_key_file=hub_ssh_key_file,
            no_hub_watchdog=no_hub_watchdog or install_role_hub,
        )

    # --- VS Code wiring (user-readable token + extension settings) ---
    # The system-wide token at C:\ProgramData\forgewire\hub.token is locked to
    # SYSTEM + Administrators (correct for a service), but the VS Code
    # extension runs as the user. Drop a user-readable copy and point the
    # extension at it via VS Code user settings.json so the sidebar populates
    # without asking the user to paste anything.
    try:
        _write_vscode_user_settings(hub_url=hub_url or f"http://127.0.0.1:{port}",
                                    hub_token=hub_token)
        click.echo("Wired VS Code extension (forgewireFabric.hubUrl + token file).")
    except Exception as exc:  # pragma: no cover - best-effort
        click.echo(f"Note: could not auto-wire VS Code settings: {exc}", err=True)

    # --- VS Code MCP wiring (forgewire-dispatcher / forgewire-runner) ---
    # Same idea but for the MCP control plane. We always wire the dispatcher
    # entry (every host is potentially a driver). The runner entry is only
    # wired when this host actually runs a runner.
    try:
        _write_vscode_user_mcp(
            hub_url=hub_url or f"http://127.0.0.1:{port}",
            install_runner=install_role_runner,
            workspace_root=workspace_root,
        )
        click.echo("Wired VS Code MCP servers (forgewire-dispatcher"
                   + (" + forgewire-runner)" if install_role_runner else ")"))
    except Exception as exc:  # pragma: no cover - best-effort
        click.echo(f"Note: could not auto-wire VS Code MCP: {exc}", err=True)

    click.echo("Setup complete.")
