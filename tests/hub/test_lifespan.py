"""M2.6.4 regression coverage for FastAPI lifespan startup.

The hub used to register startup hooks with deprecated ``@app.on_event``.
Booting the app through ``TestClient`` should no longer emit FastAPI's
``on_event`` deprecation warning.
"""

from __future__ import annotations

import warnings
from pathlib import Path

from fastapi.testclient import TestClient

from forgewire_fabric.hub.server import BlackboardConfig, create_app


HUB_TOKEN = "test-hub-token-lifespan-aaaaaaaa"


def _make_app(tmp_path: Path):
    cfg = BlackboardConfig(
        db_path=tmp_path / "hub.sqlite3",
        token=HUB_TOKEN,
        host="127.0.0.1",
        port=0,
    )
    return create_app(cfg)


def test_create_app_lifespan_emits_no_on_event_deprecation(tmp_path: Path) -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        for _ in range(2):
            app = _make_app(tmp_path)
            with TestClient(app) as client:
                response = client.get("/healthz")
                assert response.status_code == 200

    on_event_warnings = [
        warning
        for warning in caught
        if issubclass(warning.category, DeprecationWarning)
        and "on_event" in str(warning.message)
    ]
    assert on_event_warnings == []
