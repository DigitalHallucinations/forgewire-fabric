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
    local_url = "http://127.0.0.1:8765"

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
                with contextlib.suppress(_BE):
                    await c.drain_runner_by_dispatcher(rid)
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
