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

@cli.command(
    "grant-service-control",
    help=(
        "Grant the invoking user (or --account) start/stop/pause rights on the "
        "named Windows services so future bounces don't need elevation. "
        "Per-service ACL only; no system-wide UAC change."
    ),
)
@click.option(
    "--service",
    "services",
    multiple=True,
    default=(
        "ForgeWireHub",
        "ForgeWireRunner",
        "ForgeWireRqliteNode1",
        "ForgeWireRqliteNode2",
        "ForgeWireRqliteNode3",
    ),
    help="Service short name. Repeatable. Missing services are skipped.",
)
@click.option(
    "--account",
    default=None,
    help="DOMAIN\\user to grant rights to. Defaults to the invoking user.",
)
def grant_service_control_cmd(services: tuple[str, ...], account: str | None) -> None:
    if not sys.platform.startswith("win"):
        click.echo("grant-service-control is a Windows-only operation; nothing to do.")
        return
    from forgewire_fabric.install import grant_service_control

    grant_service_control(list(services), account=account)


# ---------------------------------------------------------------------------
# mcp (VS Code MCP server registration)
# ---------------------------------------------------------------------------


@cli.group(help="MCP control-plane wiring (VS Code mcp.json).")
def mcp() -> None:
    pass


@mcp.command("install", help=(
    "Register forgewire-dispatcher (and optionally forgewire-runner) in the "
    "VS Code user-scope mcp.json. Re-runs idempotently and prunes legacy "
    "scripts.remote.hub entries."
))
@click.option("--hub-url", default=None,
              help="Hub URL the dispatcher MCP server connects to. "
              "Defaults to forgewireFabric.hubUrl from VS Code settings, "
              "else http://127.0.0.1:8765.")
@click.option("--with-runner", is_flag=True, default=False,
              help="Also register forgewire-runner (only for hosts that run a runner).")
@click.option("--workspace-root", default=None,
              help="Runner workspace root for the runner MCP entry (when --with-runner).")
def mcp_install(hub_url: str | None, with_runner: bool, workspace_root: str | None) -> None:
    if not hub_url:
        # Try to read from existing user settings
        try:
            settings_path = _vscode_user_dir() / "settings.json"
            if settings_path.exists():
                cur = json.loads(settings_path.read_text(encoding="utf-8") or "{}")
                hub_url = cur.get("forgewireFabric.hubUrl") or None
        except Exception:
            hub_url = None
    if not hub_url:
        hub_url = "http://127.0.0.1:8765"
    _write_vscode_user_mcp(
        hub_url=hub_url,
        install_runner=with_runner,
        workspace_root=workspace_root,
    )
    click.echo(f"Wired VS Code MCP servers (hub_url={hub_url}, runner={with_runner}).")


@mcp.command("uninstall", help="Remove ForgeWire MCP servers from the VS Code user-scope mcp.json.")
def mcp_uninstall() -> None:
    mcp_path = _vscode_user_dir() / "mcp.json"
    if not mcp_path.exists():
        click.echo("No user-scope mcp.json found; nothing to do.")
        return
    try:
        cur = json.loads(mcp_path.read_text(encoding="utf-8") or "{}")
    except json.JSONDecodeError:
        raise SystemExit(f"{mcp_path} is not valid JSON; refusing to edit.") from None
    servers = cur.get("servers") or {}
    removed = []
    for k in ("forgewire-dispatcher", "forgewire-runner"):
        if k in servers:
            servers.pop(k)
            removed.append(k)
    cur["servers"] = servers
    mcp_path.write_text(json.dumps(cur, indent=4), encoding="utf-8")
    click.echo(f"Removed: {', '.join(removed) if removed else '(none)'}")

def _write_vscode_user_settings(*, hub_url: str, hub_token: str) -> None:
    """Best-effort: drop a user-readable hub token and update VS Code user
    settings.json so the ForgeWire Fabric extension can discover the hub
    without manual configuration. Idempotent: leaves unrelated keys alone.
    """
    import json
    from pathlib import Path as _P

    home = _P.home()
    user_token_dir = home / ".forgewire"
    user_token_dir.mkdir(parents=True, exist_ok=True)
    user_token = user_token_dir / "hub.token"
    user_token.write_text(hub_token.strip(), encoding="utf-8")

    if sys.platform.startswith("win"):
        settings_path = _P(os.environ.get("APPDATA", str(home))) / "Code" / "User" / "settings.json"
    elif sys.platform == "darwin":
        settings_path = home / "Library" / "Application Support" / "Code" / "User" / "settings.json"
    else:
        settings_path = home / ".config" / "Code" / "User" / "settings.json"

    settings_path.parent.mkdir(parents=True, exist_ok=True)
    if settings_path.exists():
        try:
            current = json.loads(settings_path.read_text(encoding="utf-8") or "{}")
        except json.JSONDecodeError:
            # Don't clobber a hand-broken file; bail out.
            raise
    else:
        current = {}

    # Drop any pre-rename keys so they cannot shadow the new ones.
    current.pop("forgewire.hubUrl", None)
    current.pop("forgewire.hubToken", None)
    current.pop("forgewire.hubTokenFile", None)
    current["forgewireFabric.hubUrl"] = hub_url
    current["forgewireFabric.hubTokenFile"] = str(user_token)
    settings_path.write_text(json.dumps(current, indent=4), encoding="utf-8")


def _vscode_user_dir() -> "Path":
    from pathlib import Path as _P

    home = _P.home()
    if sys.platform.startswith("win"):
        return _P(os.environ.get("APPDATA", str(home))) / "Code" / "User"
    if sys.platform == "darwin":
        return home / "Library" / "Application Support" / "Code" / "User"
    return home / ".config" / "Code" / "User"


def _write_vscode_user_mcp(
    *,
    hub_url: str,
    install_runner: bool,
    workspace_root: str | None,
) -> None:
    """Best-effort: register the ForgeWire MCP servers in VS Code's user-scope
    ``mcp.json`` so any window picks them up without per-workspace config.

    * ``forgewire-dispatcher`` always wired -- every box might drive.
        * ``forgewire-runner`` only wired when this host installs a runner; it
            uses the same hub URL passed to setup/install so non-hub workstations
            do not hang trying to register against localhost.

    Stale entries from the legacy ``forgewire`` repo (``BLACKBOARD_*`` env,
    ``scripts.remote.hub.*`` modules) are pruned so the OptiPlex stops
    spawning the old version.
    """
    import json
    from pathlib import Path as _P

    home = _P.home()
    user_token = home / ".forgewire" / "hub.token"

    py = _python_for_mcp()

    dispatcher_entry = {
        "command": py,
        "args": ["-m", "forgewire_fabric.hub.dispatcher_mcp"],
        "env": {
            "FORGEWIRE_HUB_URL": hub_url,
            "FORGEWIRE_HUB_TOKEN_FILE": str(user_token),
        },
    }

    mcp_path = _vscode_user_dir() / "mcp.json"
    mcp_path.parent.mkdir(parents=True, exist_ok=True)
    if mcp_path.exists():
        try:
            current = json.loads(mcp_path.read_text(encoding="utf-8") or "{}")
        except json.JSONDecodeError:
            # Don't clobber a hand-broken file.
            raise
    else:
        current = {"$schema": "https://aka.ms/vscode-mcp-schema"}

    servers = current.setdefault("servers", {})

    # Drop legacy entries so we never run two versions side-by-side.
    for stale_key in ("forgewire-dispatcher", "forgewire-runner"):
        existing = servers.get(stale_key)
        if isinstance(existing, dict):
            args = existing.get("args") or []
            if any("scripts.remote.hub" in str(a) for a in args):
                servers.pop(stale_key, None)

    servers["forgewire-dispatcher"] = dispatcher_entry

    if install_runner:
        runner_env = {
            "FORGEWIRE_HUB_URL": hub_url,
            "FORGEWIRE_HUB_TOKEN_FILE": str(user_token),
        }
        if workspace_root:
            runner_env["FORGEWIRE_RUNNER_WORKSPACE_ROOT"] = workspace_root
        servers["forgewire-runner"] = {
            "command": py,
            "args": ["-m", "forgewire_fabric.hub.runner_mcp"],
            "env": runner_env,
        }
    else:
        # If we are not running a runner here, drop any stale runner entry so
        # the dispatcher doesn't try to start one on a box that has no hub.
        servers.pop("forgewire-runner", None)

    mcp_path.write_text(json.dumps(current, indent=4), encoding="utf-8")


def _python_for_mcp() -> str:
    """Return a python interpreter path for the MCP server entries.

    We prefer the *current* interpreter (the one that just installed
    ``forgewire-fabric``), since that is guaranteed to have the package
    importable. Fall back to ``python`` on PATH.
    """
    exe = sys.executable
    if exe and Path(exe).exists():
        return exe
    return "python"
