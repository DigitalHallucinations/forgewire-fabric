"""M2.6.1 regression: BlackboardClient.submit_result and append_progress
must return the parsed response dict (not implicit ``None``).

These tests exercise the real client against an in-process hub via
``httpx.ASGITransport``. No mocking.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import httpx
import pytest

from forgewire_fabric.hub.client import BlackboardClient
from forgewire_fabric.hub.server import BlackboardConfig, create_app


HUB_TOKEN = "x" * 32


def _make_client() -> tuple[BlackboardClient, httpx.AsyncClient]:
    """Build a BlackboardClient wired to an in-process ASGI hub."""
    tmp = Path(tempfile.mkdtemp(prefix="fw-m261-"))
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


async def _dispatch_and_claim(client: BlackboardClient, worker_id: str) -> int:
    dispatched = await client.dispatch_task(
        {
            "title": "m261-contract",
            "prompt": "noop",
            "scope_globs": ["docs/x.md"],
            "base_commit": "a" * 40,
            "branch": "feature/m261-contract",
        }
    )
    task_id = int(dispatched["id"])
    claim = await client.claim_task({"worker_id": worker_id})
    assert claim is not None, "expected to claim the task we just dispatched"
    assert int(claim["id"]) == task_id
    return task_id


@pytest.mark.asyncio
async def test_submit_result_returns_dict() -> None:
    client, raw = _make_client()
    try:
        worker_id = "worker-m261-result"
        task_id = await _dispatch_and_claim(client, worker_id)
        result = await client.submit_result(
            task_id,
            {
                "worker_id": worker_id,
                "status": "done",
                "files_touched": ["docs/x.md"],
                "test_summary": "ok",
            },
        )
        assert result is not None
        assert isinstance(result, dict)
        assert int(result["id"]) == task_id
        assert result["status"] == "done"
    finally:
        await raw.aclose()


@pytest.mark.asyncio
async def test_append_progress_returns_dict() -> None:
    client, raw = _make_client()
    try:
        worker_id = "worker-m261-progress"
        task_id = await _dispatch_and_claim(client, worker_id)
        result = await client.append_progress(
            task_id,
            {
                "worker_id": worker_id,
                "message": "halfway there",
                "files_touched": ["docs/x.md"],
            },
        )
        assert result is not None
        assert isinstance(result, dict)
    finally:
        await raw.aclose()
