from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from fastapi import HTTPException

from forgewire_fabric.hub.server import BlackboardConfig, DispatchTaskRequest, create_app


HUB_TOKEN = "test-hub-token-exception-chain"


def _post_endpoint(app: Any, path: str) -> Callable[..., Any]:
    for route in app.routes:
        if getattr(route, "path", None) == path and "POST" in getattr(route, "methods", set()):
            return route.endpoint
    raise AssertionError(f"POST route not found: {path}")


def test_dispatch_task_preserves_storage_http_error_cause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = create_app(
        BlackboardConfig(
            db_path=tmp_path / "hub.sqlite3",
            token=HUB_TOKEN,
            host="127.0.0.1",
            port=0,
        )
    )
    endpoint = _post_endpoint(app, "/tasks")
    blackboard = app.state.blackboard

    storage_error = httpx.ConnectError("rqlite offline")

    def raise_storage_error(**_: Any) -> dict[str, Any]:
        raise storage_error

    monkeypatch.setattr(blackboard, "create_task", raise_storage_error)

    payload = DispatchTaskRequest(
        title="offline rqlite probe",
        prompt="echo ok",
        scope_globs=["python/forgewire_fabric/**"],
        base_commit="abcdef0",
        branch="agent/test/exception-chain",
    )

    with pytest.raises(HTTPException) as caught:
        endpoint(SimpleNamespace(app=app), payload)

    assert caught.value.status_code == 502
    assert "rqlite unreachable" in str(caught.value.detail)
    assert caught.value.__cause__ is storage_error
