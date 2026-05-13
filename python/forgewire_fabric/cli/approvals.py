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

    with contextlib.suppress(KeyboardInterrupt):
        _async(_go())
