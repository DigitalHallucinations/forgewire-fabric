"""Task-kind taxonomy contract.

Two task kinds live on the same hub queue but must stay routable-disjoint:

  * ``agent``   -- sealed brief for a Copilot-Chat agent runner. Default for
                   every legacy dispatch and the only kind a legacy
                   ``/tasks/claim`` (worker-id-only) request will hand out.
  * ``command`` -- shell/script payload for a non-agent (cmd) runner.
                   Routable only via the v2 claim path to a runner whose
                   tags include ``kind:command``.

These tests pin both ends of that contract using the real hub against an
in-process ASGI transport. No mocking.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import httpx
import pytest

from forgewire_fabric.hub.client import BlackboardClient
from forgewire_fabric.hub.server import BlackboardConfig, create_app


HUB_TOKEN = "y" * 32


def _make_client() -> tuple[BlackboardClient, httpx.AsyncClient]:
    tmp = Path(tempfile.mkdtemp(prefix="fw-kind-"))
    cfg = BlackboardConfig(
        db_path=tmp / "blackboard.db",
        token=HUB_TOKEN,
        host="127.0.0.1",
        port=0,
    )
    app = create_app(cfg)
    transport = httpx.ASGITransport(app=app)
    raw = httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
        headers={"authorization": f"Bearer {HUB_TOKEN}"},
    )
    client = BlackboardClient.__new__(BlackboardClient)
    client._base = "http://testserver"
    client._client = raw
    return client, raw


def _payload(title: str, **extra: object) -> dict[str, object]:
    base: dict[str, object] = {
        "title": title,
        "prompt": "noop",
        "scope_globs": ["docs/x.md"],
        "base_commit": "a" * 40,
        "branch": f"feature/{title}",
    }
    base.update(extra)
    return base


@pytest.mark.asyncio
async def test_dispatch_defaults_to_agent_kind() -> None:
    client, raw = _make_client()
    try:
        task = await client.dispatch_task(_payload("kind-default"))
        assert task["kind"] == "agent"
        fetched = await client.get_task(int(task["id"]))
        assert fetched["kind"] == "agent"
    finally:
        await raw.aclose()


@pytest.mark.asyncio
async def test_dispatch_command_kind_persists() -> None:
    client, raw = _make_client()
    try:
        task = await client.dispatch_task(_payload("kind-command", kind="command"))
        assert task["kind"] == "command"
        fetched = await client.get_task(int(task["id"]))
        assert fetched["kind"] == "command"
    finally:
        await raw.aclose()


@pytest.mark.asyncio
async def test_legacy_claim_only_returns_agent_tasks() -> None:
    """A legacy ``/tasks/claim`` (worker-id only, no v2 tags) must never
    hand out a ``kind='command'`` task -- shell-exec runners that haven't
    upgraded to v2 cannot be trusted with command work.
    """
    client, raw = _make_client()
    try:
        cmd = await client.dispatch_task(_payload("only-command", kind="command"))
        # No agent task in the queue; legacy claim should return None.
        first = await client.claim_task({"worker_id": "legacy-worker"})
        assert first is None, f"legacy claim must skip command tasks, got {first!r}"

        # Add an agent task; legacy claim should now pick *that* one,
        # leaving the command task untouched.
        agent = await client.dispatch_task(_payload("then-agent"))
        second = await client.claim_task({"worker_id": "legacy-worker"})
        assert second is not None
        assert int(second["id"]) == int(agent["id"])
        assert second["kind"] == "agent"

        # Command task is still queued.
        cmd_now = await client.get_task(int(cmd["id"]))
        assert cmd_now["status"] == "queued"
    finally:
        await raw.aclose()
