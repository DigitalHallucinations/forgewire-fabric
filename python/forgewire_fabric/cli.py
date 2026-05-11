"""ForgeWire CLI — ``forgewire <subcommand>``.

Subcommands:

* ``forgewire-fabric hub start``     — start the FastAPI hub (signed dispatch / claim / streams).
* ``forgewire-fabric runner start``  — register a runner and run the claim loop.
* ``forgewire-fabric dispatch``      — POST a sealed task to the hub.
* ``forgewire-fabric tasks list``    — list tasks.
* ``forgewire-fabric tasks show``    — show a single task.
* ``forgewire-fabric tasks stream``  — tail a task's stream output.
* ``forgewire-fabric runners list``  — list registered runners.
* ``forgewire-fabric keys init``     — generate a dispatcher ed25519 keypair.
* ``forgewire-fabric token gen``     — generate a random hub token (32 hex chars).

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

from forgewire_fabric import __version__


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _client():  # pragma: no cover - thin wrapper
    from forgewire_fabric.hub.client import load_client_from_env

    return load_client_from_env()


def _print_json(obj: Any) -> None:
    click.echo(json.dumps(obj, indent=2, sort_keys=True, default=str))


def _async(coro: Any) -> Any:
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# top-level group
# ---------------------------------------------------------------------------


@click.group(help="ForgeWire control-plane CLI.")
@click.version_option(__version__, prog_name="forgewire-fabric")
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
@click.option("--backend", type=click.Choice(["sqlite", "rqlite"]), default=None,
              help="State backend. 'sqlite' = legacy single-node WAL (default). "
                   "'rqlite' = Raft-replicated cluster.")
@click.option("--rqlite-host", default=None, help="rqlite cluster member host (any node).")
@click.option("--rqlite-port", type=int, default=None, help="rqlite HTTP API port (default 4001).")
@click.option("--rqlite-consistency",
              type=click.Choice(["none", "weak", "strong", "linearizable"]),
              default=None, help="rqlite read consistency level for SELECTs.")
def hub_start(
    host: str | None,
    port: int | None,
    db_path: str | None,
    token_file: str | None,
    mdns: bool,
    log_level: str,
    backend: str | None,
    rqlite_host: str | None,
    rqlite_port: int | None,
    rqlite_consistency: str | None,
) -> None:
    from forgewire_fabric.hub.server import main as hub_main

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
    if backend:
        argv += ["--backend", backend]
    if rqlite_host:
        argv += ["--rqlite-host", rqlite_host]
    if rqlite_port is not None:
        argv += ["--rqlite-port", str(rqlite_port)]
    if rqlite_consistency:
        argv += ["--rqlite-consistency", rqlite_consistency]
    hub_main(argv)


@hub.command("healthz", help="Ping the hub /healthz endpoint.")
def hub_healthz() -> None:
    async def _go() -> None:
        async with _client() as c:
            _print_json(await c.healthz())

    _async(_go())


@hub.command("install", help="Install the hub as an OS service (NSSM/systemd/launchd).")
@click.option("--port", type=int, default=8765, show_default=True)
@click.option("--host", default="0.0.0.0", show_default=True)
@click.option("--token", default=None, help="Bearer token. If omitted a fresh 32-hex token is generated (Windows only).")
def hub_install(port: int, host: str, token: str | None) -> None:
    from forgewire_fabric.install import install_hub

    install_hub(port=port, host=host, token=token)


@hub.command("uninstall", help="Remove the hub OS service.")
def hub_uninstall() -> None:
    from forgewire_fabric.install import uninstall_hub

    uninstall_hub()


# ----- failover / replication ---------------------------------------------


@hub.command("status", help=(
    "Probe each candidate hub and print which is currently active, plus "
    "uptime and snapshot age. Candidates come from --candidate (repeatable) "
    "or FORGEWIRE_HUB_CANDIDATES (comma-separated)."
))
@click.option("--candidate", "candidates", multiple=True,
              help="Candidate hub URL (repeatable). Probed in order.")
@click.option("--token-file", default=None, help="Token file for probes.")
def hub_status(candidates: tuple[str, ...], token_file: str | None) -> None:
    import time as _time
    from forgewire_fabric.hub.client import BlackboardClient as _BC, BlackboardError as _BE

    cands = list(candidates) or _candidates_from_env()
    if not cands:
        raise SystemExit("No candidates. Pass --candidate URL or set FORGEWIRE_HUB_CANDIDATES.")
    token = _load_token_for_probe(token_file)
    rows: list[dict[str, Any]] = []
    active_url: str | None = None
    for url in cands:
        info: dict[str, Any] = {"url": url, "ok": False}
        try:
            async def _probe(u: str = url) -> dict[str, Any]:
                async with _BC(u, token) as c:
                    return await c.healthz()
            h = _async(_probe())
            info.update(ok=True, uptime_seconds=h.get("uptime_seconds"),
                        version=h.get("version"))
            if active_url is None:
                active_url = url
        except _BE as exc:
            info["error"] = str(exc)
        except Exception as exc:  # pragma: no cover
            info["error"] = repr(exc)
        rows.append(info)
    snap_path = _P_home() / ".forgewire" / "snapshots" / "latest.sqlite3"
    snap_meta = _P_home() / ".forgewire" / "snapshots" / "latest.meta.json"
    snap_age: float | None = None
    if snap_meta.exists():
        try:
            meta = json.loads(snap_meta.read_text(encoding="utf-8"))
            snap_age = _time.time() - float(meta.get("generated_at") or 0)
        except Exception:
            pass
    _print_json({
        "active": active_url,
        "candidates": rows,
        "local_snapshot": {
            "path": str(snap_path) if snap_path.exists() else None,
            "age_seconds": snap_age,
        },
    })


@hub.command("snapshot-pull", help="Pull a snapshot from the active hub and store it locally.")
@click.option("--candidate", "candidates", multiple=True)
@click.option("--token-file", default=None)
def hub_snapshot_pull(candidates: tuple[str, ...], token_file: str | None) -> None:
    import time as _time
    from forgewire_fabric.hub.client import BlackboardClient as _BC, BlackboardError as _BE

    cands = list(candidates) or _candidates_from_env()
    if not cands:
        raise SystemExit("No candidates configured.")
    token = _load_token_for_probe(token_file)
    snap_dir = _P_home() / ".forgewire" / "snapshots"
    snap_dir.mkdir(parents=True, exist_ok=True)

    last_err: str | None = None
    for url in cands:
        try:
            async def _do(u: str = url) -> tuple[bytes, dict[str, str]]:
                async with _BC(u, token) as c:
                    return await c.fetch_snapshot()
            blob, headers = _async(_do())
            (snap_dir / "latest.sqlite3").write_bytes(blob)
            (snap_dir / "latest.meta.json").write_text(
                json.dumps({
                    "source_url": url,
                    "generated_at": float(headers.get("x-snapshot-generated-at", _time.time())),
                    "hub_started_at": float(headers.get("x-hub-started-at", 0) or 0),
                    "bytes": len(blob),
                }, indent=2),
                encoding="utf-8",
            )
            click.echo(f"Pulled {len(blob)} bytes from {url} -> {snap_dir / 'latest.sqlite3'}")
            return
        except _BE as exc:
            last_err = f"{url}: {exc}"
            continue
        except Exception as exc:  # pragma: no cover
            last_err = f"{url}: {exc!r}"
            continue
    raise SystemExit(f"All candidates failed. Last error: {last_err}")


@hub.command("promote", help=(
    "Promote this node to active hub. Pre-flights: refuses if another hub on "
    "the candidate list is already responding (split-brain guard) unless "
    "--force. If a local snapshot is present and --import-snapshot is set, "
    "imports it before starting the service."
))
@click.option("--candidate", "candidates", multiple=True,
              help="Candidate hub URLs to probe for split-brain. Defaults to FORGEWIRE_HUB_CANDIDATES.")
@click.option("--port", type=int, default=8765, show_default=True)
@click.option("--bind-host", default="0.0.0.0", show_default=True)
@click.option("--token", default=None, help="Hub token. Uses existing one if a hub.token file exists.")
@click.option("--import-snapshot/--no-import-snapshot", default=True, show_default=True,
              help="Import ~/.forgewire/snapshots/latest.sqlite3 before starting (atomic).")
@click.option("--force", is_flag=True, default=False, help="Skip split-brain guard.")
def hub_promote(
    candidates: tuple[str, ...],
    port: int,
    bind_host: str,
    token: str | None,
    import_snapshot: bool,
    force: bool,
) -> None:
    from forgewire_fabric.hub.client import BlackboardClient as _BC

    cands = list(candidates) or _candidates_from_env()
    probe_token = _load_token_for_probe(None)
    # Split-brain guard
    if not force:
        for url in cands:
            try:
                async def _ping(u: str = url) -> dict[str, Any]:
                    async with _BC(u, probe_token) as c:
                        return await c.healthz()
                _async(_ping())
                raise SystemExit(
                    f"Refusing to promote: another hub on {url} is already serving. "
                    f"Demote it first, or pass --force."
                )
            except SystemExit:
                raise
            except Exception:
                continue
    # Token resolution
    token_file = _P(r"C:\ProgramData\forgewire\hub.token") if sys.platform.startswith("win") else _P("/etc/forgewire/hub.token")
    if not token and token_file.exists():
        token = token_file.read_text(encoding="utf-8").strip()
    if not token:
        token = secrets.token_hex(16)
    # Snapshot import (offline, file-level: copy to db_path before service starts)
    if import_snapshot:
        snap = _P_home() / ".forgewire" / "snapshots" / "latest.sqlite3"
        if snap.exists():
            db_path = _P_home() / ".forgewire" / "hub.sqlite3"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            db_path.write_bytes(snap.read_bytes())
            click.echo(f"Imported snapshot: {snap} -> {db_path}")
        else:
            click.echo("No local snapshot to import (expected at ~/.forgewire/snapshots/latest.sqlite3); promoting empty.")
    # Install + start the hub service.
    from forgewire_fabric.install import install_hub
    install_hub(port=port, host=bind_host, token=token)
    click.echo("Promoted: hub service running on this node.")


@hub.command("demote", help=(
    "Demote this node from active hub. Drains all runners, pushes a final "
    "snapshot to all peers in the candidate list, then stops the local "
    "hub service. After this the next-priority candidate should --promote."
))
@click.option("--peer", "peers", multiple=True,
              help="Peer URLs to push the final snapshot to (repeatable).")
@click.option("--token-file", default=None)
@click.option("--skip-push", is_flag=True, default=False, help="Don't push the final snapshot.")
def hub_demote(peers: tuple[str, ...], token_file: str | None, skip_push: bool) -> None:
    from forgewire_fabric.hub.client import BlackboardClient as _BC, BlackboardError as _BE
    from forgewire_fabric.install import uninstall_hub

    token = _load_token_for_probe(token_file)
    local_url = f"http://127.0.0.1:8765"

    async def _drain_all() -> None:
        async with _BC(local_url, token) as c:
            try:
                rs = (await c.list_runners()).get("runners") or []
            except _BE:
                rs = []
            for r in rs:
                rid = r.get("runner_id")
                if not rid:
                    continue
                try:
                    await c.drain_runner_by_dispatcher(rid)
                except _BE:
                    pass
    try:
        _async(_drain_all())
        click.echo("Drained runners.")
    except Exception as exc:  # pragma: no cover
        click.echo(f"Drain step failed (continuing): {exc}", err=True)

    # Pull final snapshot locally first.
    if not skip_push:
        async def _pull() -> bytes:
            async with _BC(local_url, token) as c:
                blob, _ = await c.fetch_snapshot()
                return blob
        try:
            final = _async(_pull())
        except Exception as exc:
            click.echo(f"Could not fetch final snapshot from local hub: {exc}", err=True)
            final = b""

        peer_list = list(peers) or [u for u in _candidates_from_env() if "127.0.0.1" not in u and "localhost" not in u]
        for peer in peer_list:
            try:
                async def _push(u: str = peer, blob: bytes = final) -> dict[str, Any]:
                    async with _BC(u, token) as c:
                        return await c.import_snapshot(blob, force=True)
                res = _async(_push())
                click.echo(f"Pushed snapshot to {peer}: {res}")
            except Exception as exc:
                click.echo(f"Push to {peer} failed: {exc}", err=True)

    # Finally stop the service.
    uninstall_hub()
    click.echo("Demoted: hub service stopped on this node.")


def _candidates_from_env() -> list[str]:
    raw = os.environ.get("FORGEWIRE_HUB_CANDIDATES", "").strip()
    if not raw:
        return []
    return [s.strip() for s in raw.split(",") if s.strip()]


def _load_token_for_probe(token_file: str | None) -> str:
    if token_file:
        return _P(token_file).read_text(encoding="utf-8").strip()
    env_tok = os.environ.get("FORGEWIRE_HUB_TOKEN")
    if env_tok:
        return env_tok.strip()
    user_tok = _P_home() / ".forgewire" / "hub.token"
    if user_tok.exists():
        return user_tok.read_text(encoding="utf-8").strip()
    raise SystemExit("No hub token. Set --token-file, FORGEWIRE_HUB_TOKEN, or ~/.forgewire/hub.token.")


def _P_home() -> "Path":
    return Path.home()


def _P(p: str) -> "Path":
    return Path(p)


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

    from forgewire_fabric.runner.agent import run_runner

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
    from forgewire_fabric.runner.identity import load_or_create

    p = Path(path) if path else None
    ident = load_or_create(p)
    _print_json(
        {
            "runner_id": ident.runner_id,
            "public_key": ident.public_key_hex,
        }
    )


@runner.command(
    "identity-export",
    help=(
        "Export this runner's identity (incl. private key) to a portable "
        "JSON file. Use to preserve runner_id across hardware migration: "
        "export from the retiring host, then 'runner identity-import' on "
        "the replacement before installing the service."
    ),
)
@click.option("--output", "output", default=None,
              help="Destination file. Omit to print to stdout.")
@click.option("--source", default=None,
              help="Source identity file (default: machine-wide).")
def runner_identity_export(output: str | None, source: str | None) -> None:
    from forgewire_fabric.runner.identity import export_identity

    record = export_identity(
        destination=Path(output) if output else None,
        source=Path(source) if source else None,
    )
    if output:
        _print_json({"exported_to": output, "runner_id": record["runner_id"]})
    else:
        _print_json(record)


@runner.command(
    "identity-import",
    help=(
        "Install a previously-exported identity as this machine's runner "
        "identity. Refuses to overwrite a different existing runner_id "
        "unless --force."
    ),
)
@click.argument("source", type=click.Path(exists=True, dir_okay=False))
@click.option("--target", default=None,
              help="Destination identity file (default: machine-wide).")
@click.option("--force", is_flag=True, default=False,
              help="Overwrite an existing different runner_id.")
def runner_identity_import(source: str, target: str | None, force: bool) -> None:
    from forgewire_fabric.runner.identity import import_identity

    ident = import_identity(
        Path(source),
        target=Path(target) if target else None,
        force=force,
    )
    _print_json(
        {
            "runner_id": ident.runner_id,
            "public_key": ident.public_key_hex,
            "imported_from": source,
        }
    )


@runner.command("install", help="Install the runner as an OS service (NSSM/systemd/launchd).")
@click.option("--hub-url", required=True, envvar="FORGEWIRE_HUB_URL")
@click.option("--hub-token", required=True, envvar="FORGEWIRE_HUB_TOKEN")
@click.option("--workspace-root", required=True, help="Per-runner workspace root.")
def runner_install(hub_url: str, hub_token: str, workspace_root: str) -> None:
    from forgewire_fabric.install import install_runner

    install_runner(hub_url=hub_url, hub_token=hub_token, workspace_root=workspace_root)


@runner.command("uninstall", help="Remove the runner OS service.")
def runner_uninstall() -> None:
    from forgewire_fabric.install import uninstall_runner

    uninstall_runner()


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
        raise SystemExit(f"{mcp_path} is not valid JSON; refusing to edit.")
    servers = cur.get("servers") or {}
    removed = []
    for k in ("forgewire-dispatcher", "forgewire-runner"):
        if k in servers:
            servers.pop(k)
            removed.append(k)
    cur["servers"] = servers
    mcp_path.write_text(json.dumps(cur, indent=4), encoding="utf-8")
    click.echo(f"Removed: {', '.join(removed) if removed else '(none)'}")


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
def setup(
    role: str,
    hub_url: str | None,
    hub_token: str | None,
    port: int,
    bind_host: str,
    workspace_root: str | None,
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
    if install_role_runner:
        if not hub_url:
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
        install_runner(hub_url=hub_url, hub_token=hub_token, workspace_root=workspace_root)

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
      points at the local hub (127.0.0.1) since they are colocated.

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
            "FORGEWIRE_HUB_URL": "http://127.0.0.1:8765",
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


@runners_group.command(
    "caps",
    help="Show advertised capability blob for one or all runners (M2.5.4).",
)
@click.option("--runner", "runner_filter", default=None, help="Limit to one runner_id.")
def runners_caps(runner_filter: str | None) -> None:
    async def _go() -> None:
        async with _client() as c:
            payload = await c.list_runners()
        out = []
        for r in payload.get("runners", []):
            if runner_filter and r.get("runner_id") != runner_filter:
                continue
            out.append(
                {
                    "runner_id": r.get("runner_id"),
                    "hostname": r.get("hostname"),
                    "state": r.get("state"),
                    "capabilities": r.get("capabilities") or {},
                }
            )
        _print_json({"runners": out})

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
# M2.5.1: approval queue
# ---------------------------------------------------------------------------


@cli.group(help="Approval inbox for HubDispatchGate REQUIRE_APPROVAL holds.")
def approvals() -> None:
    pass


@approvals.command("list", help="List approvals (default: pending only).")
@click.option(
    "--status",
    type=click.Choice(["pending", "approved", "denied", "consumed", "all"]),
    default="pending",
    show_default=True,
)
@click.option("--limit", type=int, default=200, show_default=True)
def approvals_list(status: str, limit: int) -> None:
    async def _go() -> None:
        async with _client() as c:
            rows = await c.list_approvals(
                status=None if status == "all" else status, limit=limit
            )
            _print_json(rows)

    _async(_go())


@approvals.command("get", help="Fetch a single approval row by id.")
@click.argument("approval_id")
def approvals_get(approval_id: str) -> None:
    async def _go() -> None:
        async with _client() as c:
            _print_json(await c.get_approval(approval_id))

    _async(_go())


@approvals.command("approve", help="Approve a pending dispatch.")
@click.argument("approval_id")
@click.option("--approver", default=None, help="Operator identifier (defaults to $USER).")
@click.option("--reason", default=None, help="Free-text justification.")
def approvals_approve(
    approval_id: str, approver: str | None, reason: str | None
) -> None:
    approver = approver or os.environ.get("USER") or os.environ.get("USERNAME")
    async def _go() -> None:
        async with _client() as c:
            _print_json(
                await c.approve_approval(
                    approval_id, approver=approver, reason=reason
                )
            )

    _async(_go())


@approvals.command("deny", help="Deny a pending dispatch.")
@click.argument("approval_id")
@click.option("--reason", required=True, help="Required: why was this denied.")
@click.option("--approver", default=None, help="Operator identifier (defaults to $USER).")
def approvals_deny(
    approval_id: str, reason: str, approver: str | None
) -> None:
    approver = approver or os.environ.get("USER") or os.environ.get("USERNAME")
    async def _go() -> None:
        async with _client() as c:
            _print_json(
                await c.deny_approval(
                    approval_id, approver=approver, reason=reason
                )
            )

    _async(_go())


@approvals.command("watch", help="Poll for new pending approvals.")
@click.option("--interval", type=float, default=5.0, show_default=True)
def approvals_watch(interval: float) -> None:
    async def _go() -> None:
        seen: set[str] = set()
        async with _client() as c:
            while True:
                rows = await c.list_approvals(status="pending")
                fresh = [r for r in rows if r["approval_id"] not in seen]
                for row in fresh:
                    seen.add(row["approval_id"])
                    _print_json(row)
                    click.echo("---")
                await asyncio.sleep(interval)

    try:
        _async(_go())
    except KeyboardInterrupt:
        pass


# ---------------------------------------------------------------------------
# M2.5.3: audit log + replay
# ---------------------------------------------------------------------------


@cli.group(help="Hub-side hash-chained audit log.")
def audit() -> None:
    pass


@audit.command("show", help="Show the full audit chain for one task.")
@click.argument("task_id", type=int)
def audit_show(task_id: int) -> None:
    async def _go() -> None:
        async with _client() as c:
            _print_json(await c.audit_for_task(task_id))

    _async(_go())


@audit.command("tail", help="Print the current chain head hash.")
def audit_tail_cmd() -> None:
    async def _go() -> None:
        async with _client() as c:
            _print_json(await c.audit_tail())

    _async(_go())


@audit.command(
    "export",
    help="Export one day of audit events to a self-verifying .jsonl.gz file.",
)
@click.option("--day", required=True, help="Calendar day in YYYY-MM-DD form.")
@click.option(
    "--out",
    "out_path",
    default=None,
    help="Output path (default: ./audit-YYYYMMDD.jsonl.gz).",
)
@click.option(
    "--verify-only",
    is_flag=True,
    default=False,
    help="Re-verify the chain without writing a file (chain ok / break details).",
)
def audit_export(day: str, out_path: str | None, verify_only: bool) -> None:
    import gzip

    async def _go() -> None:
        async with _client() as c:
            doc = await c.audit_for_day(day)
        events = doc["events"]
        if not doc["verified"]:
            click.echo(f"CHAIN BREAK: {doc['error']}", err=True)
            raise SystemExit(2)
        click.echo(
            f"verified {len(events)} events for {day} "
            f"(chain ok)", err=True,
        )
        if verify_only:
            return
        target = Path(out_path) if out_path else Path(
            f"audit-{day.replace('-', '')}.jsonl.gz"
        )
        # JSONL with a trailing manifest line so a downstream verifier can
        # check the chain without trusting our filename or extension.
        with gzip.open(target, "wt", encoding="utf-8") as fh:
            for ev in events:
                fh.write(json.dumps(ev, sort_keys=True))
                fh.write("\n")
            manifest = {
                "_manifest": True,
                "day": day,
                "count": len(events),
                "first_prev_hash": events[0]["prev_event_id_hash"] if events else None,
                "last_event_hash": events[-1]["event_id_hash"] if events else None,
            }
            fh.write(json.dumps(manifest, sort_keys=True))
            fh.write("\n")
        click.echo(str(target))

    _async(_go())


@audit.command(
    "verify",
    help="Re-verify a previously exported audit-YYYYMMDD.jsonl.gz file offline.",
)
@click.argument("path", type=click.Path(exists=True, dir_okay=False))
def audit_verify(path: str) -> None:
    import gzip

    from forgewire_fabric.hub.server import Blackboard

    events: list[dict[str, Any]] = []
    manifest: dict[str, Any] | None = None
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if obj.get("_manifest") is True:
                manifest = obj
                continue
            events.append(obj)
    ok, err = Blackboard.verify_audit_chain(events)
    summary = {
        "path": path,
        "events": len(events),
        "verified": ok,
        "error": err,
        "manifest": manifest,
    }
    _print_json(summary)
    if not ok:
        raise SystemExit(2)


@cli.command(
    "replay",
    help=(
        "Re-dispatch a recorded task using the original sealed brief from the "
        "hub audit log. Requires the original dispatch event to be present and "
        "the chain to verify."
    ),
)
@click.argument("task_id", type=int)
@click.option(
    "--branch",
    default=None,
    help="Override the target branch (default: derive '<orig>-replay-<task_id>').",
)
@click.option(
    "--base-commit",
    default=None,
    help="Override base_commit (default: original).",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Print the reconstructed payload without dispatching.",
)
@click.option(
    "--signed/--unsigned",
    default=None,
    help="Force signed/unsigned dispatch (default: auto, like `dispatch`).",
)
@click.option(
    "--identity",
    "identity_path",
    default=None,
    help="Dispatcher identity file (default: ~/.forgewire/dispatcher_identity.json).",
)
def replay(
    task_id: int,
    branch: str | None,
    base_commit: str | None,
    dry_run: bool,
    signed: bool | None,
    identity_path: str | None,
) -> None:
    async def _fetch() -> dict[str, Any]:
        async with _client() as c:
            return await c.audit_for_task(task_id)

    doc = _async(_fetch())
    if not doc["verified"]:
        click.echo(f"CHAIN BREAK on audit for task {task_id}: {doc['error']}", err=True)
        raise SystemExit(2)
    dispatch_evs = [e for e in doc["events"] if e["kind"] == "dispatch"]
    if not dispatch_evs:
        click.echo(f"no dispatch event found for task {task_id}", err=True)
        raise SystemExit(2)
    orig = dispatch_evs[0]["payload"]
    new_branch = branch or f"{orig['branch']}-replay-{task_id}"
    payload = {
        "title": f"[replay {task_id}] {orig['title']}",
        # NOTE: prompt is not stored in the audit payload — sealed_brief_hash
        # is the integrity anchor. We fetch the live task row to recover it.
        "prompt": "",
        "scope_globs": list(orig.get("scope_globs") or []),
        "base_commit": base_commit or orig["base_commit"],
        "branch": new_branch,
        "todo_id": orig.get("todo_id"),
        "timeout_minutes": orig.get("timeout_minutes") or 60,
        "priority": orig.get("priority") or 100,
        "required_tags": list(orig.get("required_tags") or []) or None,
        "required_tools": list(orig.get("required_tools") or []) or None,
        "tenant": orig.get("tenant"),
    }

    async def _hydrate() -> None:
        async with _client() as c:
            task = await c.get_task(task_id)
            payload["prompt"] = task.get("prompt") or ""

    _async(_hydrate())

    payload = {k: v for k, v in payload.items() if v is not None}

    if dry_run:
        _print_json({"replay_payload": payload, "from_audit": orig})
        return

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


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
