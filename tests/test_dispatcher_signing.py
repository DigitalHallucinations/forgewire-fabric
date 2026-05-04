"""M2.4: dispatcher-side envelope signing tests.

These tests exercise the hub server end-to-end via FastAPI's TestClient.
No mocking: a real SQLite blackboard, a real ed25519 key pair, and the real
HTTP routing layer.
"""

from __future__ import annotations

import json
import secrets
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from forgewire.dispatcher.identity import DispatcherIdentity, load_or_create
from forgewire.hub.server import BlackboardConfig, create_app


HUB_TOKEN = "test-hub-token-aaaaaaaaaaaaaaaaa"


def _make_app(tmp_path: Path, *, require_signed: bool = False):
    cfg = BlackboardConfig(
        db_path=tmp_path / "hub.sqlite3",
        token=HUB_TOKEN,
        host="127.0.0.1",
        port=0,
        require_signed_dispatch=require_signed,
    )
    return create_app(cfg)


def _ident(tmp_path: Path, label: str = "test-dispatcher") -> DispatcherIdentity:
    return load_or_create(tmp_path / "dispatcher_identity.json", label=label)


def _canonical(body: dict) -> bytes:
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sign_register(ident: DispatcherIdentity) -> dict:
    ts = int(time.time())
    nonce = secrets.token_hex(16)
    body = {
        "op": "register-dispatcher",
        "dispatcher_id": ident.dispatcher_id,
        "public_key": ident.public_key_hex,
        "timestamp": ts,
        "nonce": nonce,
    }
    return {
        "dispatcher_id": ident.dispatcher_id,
        "public_key": ident.public_key_hex,
        "label": ident.label,
        "hostname": "test-host",
        "timestamp": ts,
        "nonce": nonce,
        "signature": ident.sign(_canonical(body)),
    }


def _sign_dispatch(
    ident: DispatcherIdentity,
    *,
    title: str = "task",
    prompt: str = "do work",
    scope_globs=("docs/**",),
    base_commit: str = "deadbeef",
    branch: str = "agent/test/slice",
    nonce: str | None = None,
    timestamp: int | None = None,
) -> dict:
    ts = int(time.time()) if timestamp is None else timestamp
    n = nonce or secrets.token_hex(16)
    body = {
        "op": "dispatch",
        "dispatcher_id": ident.dispatcher_id,
        "title": title,
        "prompt": prompt,
        "scope_globs": list(scope_globs),
        "base_commit": base_commit,
        "branch": branch,
        "timestamp": ts,
        "nonce": n,
    }
    return {
        "title": title,
        "prompt": prompt,
        "scope_globs": list(scope_globs),
        "base_commit": base_commit,
        "branch": branch,
        "dispatcher_id": ident.dispatcher_id,
        "timestamp": ts,
        "nonce": n,
        "signature": ident.sign(_canonical(body)),
    }


def _auth() -> dict:
    return {"authorization": f"Bearer {HUB_TOKEN}"}


def test_register_then_signed_dispatch_happy_path(tmp_path):
    app = _make_app(tmp_path)
    ident = _ident(tmp_path)
    with TestClient(app) as client:
        r = client.post(
            "/dispatchers/register", json=_sign_register(ident), headers=_auth()
        )
        assert r.status_code == 200, r.text
        assert r.json()["dispatcher"]["dispatcher_id"] == ident.dispatcher_id

        r = client.post(
            "/tasks/v2", json=_sign_dispatch(ident), headers=_auth()
        )
        assert r.status_code == 200, r.text
        task = r.json()
        assert task["dispatcher_id"] == ident.dispatcher_id
        assert task["title"] == "task"

        r = client.get("/dispatchers", headers=_auth())
        assert r.status_code == 200
        ids = [d["dispatcher_id"] for d in r.json()["dispatchers"]]
        assert ident.dispatcher_id in ids


def test_replay_nonce_is_rejected(tmp_path):
    app = _make_app(tmp_path)
    ident = _ident(tmp_path)
    with TestClient(app) as client:
        client.post("/dispatchers/register", json=_sign_register(ident), headers=_auth())
        body = _sign_dispatch(ident)
        r1 = client.post("/tasks/v2", json=body, headers=_auth())
        assert r1.status_code == 200
        r2 = client.post("/tasks/v2", json=body, headers=_auth())
        assert r2.status_code == 403


def test_tampered_signature_is_rejected(tmp_path):
    app = _make_app(tmp_path)
    ident = _ident(tmp_path)
    with TestClient(app) as client:
        client.post("/dispatchers/register", json=_sign_register(ident), headers=_auth())
        body = _sign_dispatch(ident)
        body["title"] = "tampered title"
        r = client.post("/tasks/v2", json=body, headers=_auth())
        assert r.status_code == 403


def test_register_collision_rejects_new_pubkey(tmp_path):
    app = _make_app(tmp_path)
    ident = _ident(tmp_path)
    other = load_or_create(tmp_path / "other_identity.json", label="other")
    # Force same dispatcher_id with a different public key.
    other_dict = _sign_register(other)
    other_dict["dispatcher_id"] = ident.dispatcher_id
    # The signature was made with `other`'s key over `other`'s id, so we need
    # to re-sign with the colliding id to actually exercise the rebind path.
    ts = int(time.time())
    nonce = secrets.token_hex(16)
    body = {
        "op": "register-dispatcher",
        "dispatcher_id": ident.dispatcher_id,
        "public_key": other.public_key_hex,
        "timestamp": ts,
        "nonce": nonce,
    }
    other_dict.update(
        {
            "dispatcher_id": ident.dispatcher_id,
            "public_key": other.public_key_hex,
            "timestamp": ts,
            "nonce": nonce,
            "signature": other.sign(_canonical(body)),
        }
    )
    with TestClient(app) as client:
        r = client.post("/dispatchers/register", json=_sign_register(ident), headers=_auth())
        assert r.status_code == 200
        r = client.post("/dispatchers/register", json=other_dict, headers=_auth())
        assert r.status_code == 409


def test_unsigned_dispatch_blocked_when_required(tmp_path):
    app = _make_app(tmp_path, require_signed=True)
    with TestClient(app) as client:
        r = client.post(
            "/tasks",
            json={
                "title": "x",
                "prompt": "y",
                "scope_globs": ["docs/**"],
                "base_commit": "deadbeef",
                "branch": "agent/test/slice",
            },
            headers=_auth(),
        )
        assert r.status_code == 426


def test_stale_timestamp_rejected(tmp_path):
    app = _make_app(tmp_path)
    ident = _ident(tmp_path)
    with TestClient(app) as client:
        # Register normally so we have a valid pubkey on file.
        client.post("/dispatchers/register", json=_sign_register(ident), headers=_auth())
        # Now send a dispatch with a timestamp far in the past.
        stale = _sign_dispatch(ident, timestamp=int(time.time()) - 86400)
        r = client.post("/tasks/v2", json=stale, headers=_auth())
        assert r.status_code == 401


def test_unknown_dispatcher_404(tmp_path):
    app = _make_app(tmp_path)
    ident = _ident(tmp_path)
    with TestClient(app) as client:
        # Skip registration entirely.
        r = client.post("/tasks/v2", json=_sign_dispatch(ident), headers=_auth())
        assert r.status_code == 404
