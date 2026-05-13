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
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, _handler)
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
@click.option(
    "--bundle/--no-bundle",
    default=True,
    show_default=True,
    help=(
        "When set, write a migration bundle that also carries the "
        "runner-config sidecar (tags / workspace_root / scope_prefixes / "
        "tenant / max_concurrent). Disable to emit only the identity "
        "record (backward-compatible with pre-bundle releases)."
    ),
)
def runner_identity_export(
    output: str | None, source: str | None, bundle: bool
) -> None:
    from forgewire_fabric.runner.identity import (
        export_identity,
        export_runner_bundle,
    )

    if bundle:
        record = export_runner_bundle(
            destination=Path(output) if output else None,
            identity_source=Path(source) if source else None,
        )
        if output:
            _print_json(
                {
                    "exported_to": output,
                    "runner_id": record["identity"]["runner_id"],
                    "config_keys": sorted(record["config"].keys()),
                }
            )
        else:
            _print_json(record)
        return
    # Legacy bare-identity export (no config sidecar).
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
        "Install a previously-exported identity (or migration bundle) as "
        "this machine's runner identity. Bundles also restore the "
        "runner-config sidecar. Refuses to overwrite a different "
        "existing runner_id unless --force."
    ),
)
@click.argument("source", type=click.Path(exists=True, dir_okay=False))
@click.option("--target", default=None,
              help="Destination identity file (default: machine-wide).")
@click.option("--force", is_flag=True, default=False,
              help="Overwrite an existing different runner_id.")
def runner_identity_import(source: str, target: str | None, force: bool) -> None:
    from forgewire_fabric.runner.identity import import_runner_bundle

    # import_runner_bundle accepts both new-style bundles and the legacy
    # bare-identity format produced by ``--no-bundle`` exports.
    result = import_runner_bundle(
        Path(source),
        identity_target=Path(target) if target else None,
        force=force,
    )
    _print_json(
        {
            "runner_id": result["runner_id"],
            "public_key": result["public_key"],
            "config_keys": sorted(result["config"].keys()),
            "imported_from": source,
        }
    )


# ---------------------------------------------------------------------------
# runner config sidecar (machine-wide routing knobs that survive service
# reinstalls and travel with ``identity-export --bundle``)
# ---------------------------------------------------------------------------


@runner.group("config", help="Manage the machine-wide runner-config sidecar.")
def runner_config_group() -> None:
    pass


@runner_config_group.command("show", help="Print the effective sidecar content.")
@click.option("--path", default=None, help="Override the sidecar file path.")
def runner_config_show(path: str | None) -> None:
    from forgewire_fabric.runner.identity import (
        DEFAULT_RUNNER_CONFIG_PATH,
        load_runner_config_overrides,
    )

    target = Path(path).expanduser() if path else DEFAULT_RUNNER_CONFIG_PATH
    _print_json(
        {
            "path": str(target),
            "exists": target.exists(),
            "values": load_runner_config_overrides(target),
        }
    )


@runner_config_group.command("set", help="Persist one or more sidecar values.")
@click.option("--workspace-root", default=None)
@click.option("--tags", default=None, help="Comma-separated capability tags.")
@click.option("--scope-prefixes", default=None, help="Comma-separated path prefixes.")
@click.option("--tenant", default=None)
@click.option("--max-concurrent", type=int, default=None)
@click.option("--poll-interval", type=float, default=None,
              help="Seconds between empty-claim polls.")
@click.option("--runner-version", default=None)
@click.option("--path", default=None, help="Override the sidecar file path.")
@click.option("--replace/--merge", default=False, show_default=True,
              help="Replace the file outright vs. merge with existing values.")
def runner_config_set(
    workspace_root: str | None,
    tags: str | None,
    scope_prefixes: str | None,
    tenant: str | None,
    max_concurrent: int | None,
    poll_interval: float | None,
    runner_version: str | None,
    path: str | None,
    replace: bool,
) -> None:
    from forgewire_fabric.runner.identity import (
        DEFAULT_RUNNER_CONFIG_PATH,
        save_runner_config_overrides,
    )

    overrides: dict[str, Any] = {}
    if workspace_root is not None:
        overrides["workspace_root"] = workspace_root
    if tags is not None:
        overrides["tags"] = tags
    if scope_prefixes is not None:
        overrides["scope_prefixes"] = scope_prefixes
    if tenant is not None:
        overrides["tenant"] = tenant
    if max_concurrent is not None:
        overrides["max_concurrent"] = max_concurrent
    if poll_interval is not None:
        overrides["poll_interval_seconds"] = poll_interval
    if runner_version is not None:
        overrides["runner_version"] = runner_version
    if not overrides and not replace:
        raise click.ClickException(
            "no values supplied; pass --workspace-root / --tags / ... or "
            "use --replace to clear the sidecar."
        )
    target = Path(path).expanduser() if path else DEFAULT_RUNNER_CONFIG_PATH
    saved = save_runner_config_overrides(overrides, path=target, merge=not replace)
    _print_json({"path": str(target), "values": saved})


@runner_config_group.command("clear", help="Delete the sidecar file if present.")
@click.option("--path", default=None, help="Override the sidecar file path.")
def runner_config_clear(path: str | None) -> None:
    from forgewire_fabric.runner.identity import (
        DEFAULT_RUNNER_CONFIG_PATH,
        clear_runner_config_overrides,
    )

    target = Path(path).expanduser() if path else DEFAULT_RUNNER_CONFIG_PATH
    clear_runner_config_overrides(target)
    _print_json({"path": str(target), "cleared": True})


@runner.command("install", help="Install the runner as an OS service (NSSM/systemd/launchd).")
@click.option("--hub-url", required=True, envvar="FORGEWIRE_HUB_URL")
@click.option("--hub-token", required=True, envvar="FORGEWIRE_HUB_TOKEN")
@click.option("--workspace-root", required=True, help="Per-runner workspace root.")
@click.option("--tags", default=None,
              help="Comma-separated capability tags. Seeds the runner-config sidecar.")
@click.option("--scope-prefixes", default=None,
              help="Comma-separated path prefixes the runner accepts. Seeds the sidecar.")
@click.option("--tenant", default=None, help="Tenant slug. Seeds the sidecar.")
@click.option("--max-concurrent", type=int, default=None,
              help="Concurrency cap. Seeds the sidecar.")
@click.option("--poll-interval", type=float, default=None,
              help="Empty-claim poll interval (seconds). Seeds the sidecar.")
@click.option("--hub-ssh-host", default=None,
              help="Hub SSH target (DNS/IP). Enables cross-host hub watchdog from this node.")
@click.option("--hub-ssh-user", default=None,
              help="SSH user on the hub host (paired with --hub-ssh-host/--hub-ssh-key-file).")
@click.option("--hub-ssh-key-file", default=None,
              help="Path to the SSH private key. Copied to a SYSTEM-readable location at install time.")
@click.option("--hub-service-name", default="ForgeWireHub",
              help="Remote hub service name to restart on liveness failure (default: ForgeWireHub).")
@click.option("--hub-healthz-url", default=None,
              help="Health probe URL. Defaults to <hub-url>/healthz.")
@click.option("--no-hub-watchdog", is_flag=True,
              help="Skip the cross-host hub watchdog even when SSH credentials are supplied.")
def runner_install(
    hub_url: str,
    hub_token: str,
    workspace_root: str,
    tags: str | None,
    scope_prefixes: str | None,
    tenant: str | None,
    max_concurrent: int | None,
    poll_interval: float | None,
    hub_ssh_host: str | None,
    hub_ssh_user: str | None,
    hub_ssh_key_file: str | None,
    hub_service_name: str,
    hub_healthz_url: str | None,
    no_hub_watchdog: bool,
) -> None:
    from forgewire_fabric.install import install_runner

    install_runner(
        hub_url=hub_url,
        hub_token=hub_token,
        workspace_root=workspace_root,
        tags=tags,
        scope_prefixes=scope_prefixes,
        tenant=tenant,
        max_concurrent=max_concurrent,
        poll_interval=poll_interval,
        hub_ssh_host=hub_ssh_host,
        hub_ssh_user=hub_ssh_user,
        hub_ssh_key_file=hub_ssh_key_file,
        hub_service_name=hub_service_name,
        hub_healthz_url=hub_healthz_url,
        no_hub_watchdog=no_hub_watchdog,
    )


@runner.command("uninstall", help="Remove the runner OS service.")
def runner_uninstall() -> None:
    from forgewire_fabric.install import uninstall_runner

    uninstall_runner()
