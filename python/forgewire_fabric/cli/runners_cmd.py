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
                    "alias": r.get("alias") or "",
                    "hostname": r.get("hostname"),
                    "state": r.get("state"),
                    "capabilities": r.get("capabilities") or {},
                }
            )
        _print_json({"hub_name": payload.get("hub_name", ""), "runners": out})

    _async(_go())


@runners_group.command(
    "names",
    help=(
        "Compact view: hub_name + (alias, hostname, runner_id, state) per "
        "runner. Use this to confirm a target machine by its operator-set "
        "name before dispatching."
    ),
)
def runners_names() -> None:
    async def _go() -> None:
        async with _client() as c:
            payload = await c.list_runners()
        rows = []
        for r in payload.get("runners", []):
            rows.append(
                {
                    "alias": r.get("alias") or "",
                    "hostname": r.get("hostname"),
                    "runner_id": r.get("runner_id"),
                    "state": r.get("state"),
                }
            )
        _print_json({"hub_name": payload.get("hub_name", ""), "runners": rows})

    _async(_go())


# ---------------------------------------------------------------------------
# hosts
# ---------------------------------------------------------------------------


@cli.group("hosts", help="Inspect host-level role summaries.")
def hosts_group() -> None:
    pass


@hosts_group.command("list", help="List hosts with hub/control/dispatch/runner roles.")
def hosts_list() -> None:
    async def _go() -> None:
        async with _client() as c:
            _print_json(await c.list_hosts())

    _async(_go())


@hosts_group.command("set-role", help="Report or override a host role enablement fact.")
@click.argument("hostname")
@click.argument(
    "role",
    type=click.Choice(["hub_head", "control", "dispatch", "command_runner", "agent_runner"]),
)
@click.option("--enabled/--disabled", default=True, show_default=True)
@click.option("--status", default=None, help="Freeform status, e.g. installed, registered, skipped.")
@click.option(
    "--metadata",
    default="{}",
    help="JSON metadata object to store with the role fact.",
)
def hosts_set_role(hostname: str, role: str, enabled: bool, status: str | None, metadata: str) -> None:
    try:
        metadata_obj = json.loads(metadata)
    except json.JSONDecodeError as exc:
        raise click.ClickException(f"metadata must be JSON: {exc}") from exc
    if not isinstance(metadata_obj, dict):
        raise click.ClickException("metadata must be a JSON object")

    async def _go() -> None:
        async with _client() as c:
            _print_json(
                await c.set_host_role(
                    {
                        "hostname": hostname,
                        "role": role,
                        "enabled": enabled,
                        "status": status,
                        "metadata": metadata_obj,
                    }
                )
            )

    _async(_go())


# ---------------------------------------------------------------------------
# labels  --  fabric-wide cosmetic state (hub_name + machine aliases).
#
# Aliases live in the hub's ``labels`` table, keyed by either
# ``host_alias:<hostname>`` for machine labels or the legacy
# ``runner_alias:<runner_id>`` for runner-identity labels. They are durable
# by design:
#
#   * **Updates** (code redeploy, hub restart) -- table is sqlite-backed.
#   * **Upgrades** (schema bumps) -- ``CREATE TABLE IF NOT EXISTS labels``
#     is idempotent; existing rows are preserved.
#   * **Migrations** (hardware swap of a runner) -- aliases are keyed by
#     ``runner_id``, not by ``hostname``, so when an operator carries the
#     runner identity to a new host via ``runner identity-import`` the
#     alias automatically follows.
#
# The ``labels export`` / ``labels import`` commands give operators an
# out-of-band backup channel that does not depend on the hub's snapshot
# pipeline -- useful when rebuilding a hub from scratch.
# ---------------------------------------------------------------------------


@cli.group("labels", help="Fabric-wide labels (hub name + machine aliases).")
def labels_group() -> None:
    pass


@labels_group.command("list", help="Show the current hub_name and runner_aliases.")
def labels_list() -> None:
    async def _go() -> None:
        async with _client() as c:
            _print_json(await c.get_labels())

    _async(_go())


@labels_group.command("set-hub-name", help="Persist the fabric-wide hub display name.")
@click.argument("name")
@click.option("--updated-by", default=None, help="Operator handle for the audit trail.")
def labels_set_hub_name(name: str, updated_by: str | None) -> None:
    async def _go() -> None:
        async with _client() as c:
            _print_json(await c.set_hub_name(name, updated_by=updated_by))

    _async(_go())


@labels_group.command(
    "set-host-alias",
    help="Persist a friendly label for a hostname/machine.",
)
@click.argument("hostname")
@click.argument("alias")
@click.option("--updated-by", default=None, help="Operator handle for the audit trail.")
def labels_set_host_alias(
    hostname: str, alias: str, updated_by: str | None
) -> None:
    async def _go() -> None:
        async with _client() as c:
            _print_json(
                await c.set_host_alias(hostname, alias, updated_by=updated_by)
            )

    _async(_go())


@labels_group.command(
    "clear-host-alias",
    help="Remove a host label. Empty-string upsert deletes the row hub-side.",
)
@click.argument("hostname")
@click.option("--updated-by", default=None)
def labels_clear_host_alias(hostname: str, updated_by: str | None) -> None:
    async def _go() -> None:
        async with _client() as c:
            _print_json(
                await c.set_host_alias(hostname, "", updated_by=updated_by)
            )

    _async(_go())


@labels_group.command(
    "set-runner-alias",
    help="Persist a legacy friendly alias for a runner_id.",
)
@click.argument("runner_id")
@click.argument("alias")
@click.option("--updated-by", default=None, help="Operator handle for the audit trail.")
def labels_set_runner_alias(
    runner_id: str, alias: str, updated_by: str | None
) -> None:
    async def _go() -> None:
        async with _client() as c:
            _print_json(
                await c.set_runner_alias(runner_id, alias, updated_by=updated_by)
            )

    _async(_go())


@labels_group.command(
    "clear-runner-alias",
    help="Remove an alias entry. Empty-string upsert deletes the row hub-side.",
)
@click.argument("runner_id")
@click.option("--updated-by", default=None)
def labels_clear_runner_alias(runner_id: str, updated_by: str | None) -> None:
    async def _go() -> None:
        async with _client() as c:
            _print_json(
                await c.set_runner_alias(runner_id, "", updated_by=updated_by)
            )

    _async(_go())


@labels_group.command(
    "export",
    help=(
        "Write the current labels payload to a JSON file (or stdout). "
        "Use this as an out-of-band backup that survives a full hub "
        "rebuild even when no snapshot is available."
    ),
)
@click.option("--output", default=None,
              help="Destination file. Omit to print to stdout.")
def labels_export(output: str | None) -> None:
    async def _go() -> None:
        async with _client() as c:
            payload = await c.get_labels()
        envelope = {
            "schema": "forgewire-labels-export/1",
            "labels": payload,
        }
        if output:
            Path(output).expanduser().write_text(
                json.dumps(envelope, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            _print_json(
                {
                    "exported_to": output,
                    "hub_name": payload.get("hub_name", ""),
                    "alias_count": len(payload.get("runner_aliases") or {}),
                    "host_alias_count": len(payload.get("host_aliases") or {}),
                }
            )
        else:
            _print_json(envelope)

    _async(_go())


@labels_group.command(
    "import",
    help=(
        "Restore a previously-exported labels payload to the hub. Each "
        "row is upserted via PUT /labels/{hub,hosts/{name},runners/{id}} so the call "
        "is idempotent. Empty values delete the corresponding row."
    ),
)
@click.argument("source", type=click.Path(exists=True, dir_okay=False))
@click.option("--updated-by", default=None, help="Operator handle for the audit trail.")
def labels_import(source: str, updated_by: str | None) -> None:
    data = json.loads(Path(source).expanduser().read_text(encoding="utf-8"))
    if isinstance(data, dict) and "labels" in data:
        schema = str(data.get("schema") or "")
        if schema and not schema.startswith("forgewire-labels-export/"):
            raise click.ClickException(f"unknown labels schema {schema!r}")
        payload = data["labels"]
    else:
        # Tolerate a bare {hub_name, runner_aliases, host_aliases} blob.
        payload = data
    if not isinstance(payload, dict):
        raise click.ClickException("labels payload must be a JSON object")
    hub_name = str(payload.get("hub_name", ""))
    aliases = payload.get("runner_aliases") or {}
    if not isinstance(aliases, dict):
        raise click.ClickException("runner_aliases must be an object")
    host_aliases = payload.get("host_aliases") or {}
    if not isinstance(host_aliases, dict):
        raise click.ClickException("host_aliases must be an object")

    async def _go() -> None:
        applied = {"hub_name": False, "aliases": 0, "host_aliases": 0}
        async with _client() as c:
            await c.set_hub_name(hub_name, updated_by=updated_by)
            applied["hub_name"] = True
            for hostname, alias in host_aliases.items():
                await c.set_host_alias(
                    str(hostname), str(alias), updated_by=updated_by
                )
                applied["host_aliases"] += 1
            for runner_id, alias in aliases.items():
                await c.set_runner_alias(
                    str(runner_id), str(alias), updated_by=updated_by
                )
                applied["aliases"] += 1
            final = await c.get_labels()
        _print_json({"applied": applied, "labels": final})

    _async(_go())
