from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

import click


def _client():  # pragma: no cover - thin wrapper
    public_cli = sys.modules.get("forgewire_fabric.cli")
    public_client = getattr(public_cli, "_client", None) if public_cli is not None else None
    if public_client is not None and public_client is not _client:
        return public_client()

    from forgewire_fabric.hub.client import load_client_from_env

    return load_client_from_env()


def _print_json(obj: Any) -> None:
    click.echo(json.dumps(obj, indent=2, sort_keys=True, default=str))


def _async(coro: Any) -> Any:
    return asyncio.run(coro)


def _candidates_from_env() -> list[str]:
    raw = os.environ.get("FORGEWIRE_HUB_CANDIDATES", "")
    return [p.strip() for p in raw.split(",") if p.strip()]


def _load_token_for_probe(token_file: str | None) -> str:
    if token_file:
        return Path(token_file).expanduser().read_text(encoding="utf-8").strip()
    env = os.environ.get("FORGEWIRE_HUB_TOKEN") or os.environ.get("BLACKBOARD_TOKEN")
    if env:
        return env
    default = Path.home() / ".forgewire" / "hub_token"
    if default.exists():
        return default.read_text(encoding="utf-8").strip()
    return ""


def _P_home() -> Path:
    return Path.home()


def _P(p: str) -> Path:
    return Path(p).expanduser()
