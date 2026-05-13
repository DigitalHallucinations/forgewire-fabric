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
from .dispatch import _dispatch_signed
from ._helpers import _P, _P_home, _async, _candidates_from_env, _client, _load_token_for_probe, _print_json

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
