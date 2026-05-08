"""v0.4: hub-side observability for runner claim/heartbeat failures.

Exercises the additive heartbeat fields (claim_failures_total,
claim_failures_consecutive, last_claim_error, heartbeat_failures_total)
and the derived /runners ``state="degraded"`` when a runner heartbeats
healthy but reports a stuck claim loop.
"""

from __future__ import annotations

import json
import secrets
import time
from pathlib import Path

from fastapi.testclient import TestClient

from forgewire_fabric.hub.server import BlackboardConfig, create_app
from forgewire_fabric.runner.identity import load_or_create
from forgewire_fabric.runner.runner_capabilities import sign_payload


HUB_TOKEN = "test-hub-token-aaaaaaaaaaaaaaaaa"


def _canonical(body: dict) -> bytes:
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _make_app(tmp_path: Path):
    cfg = BlackboardConfig(
        db_path=tmp_path / "hub.sqlite3",
        token=HUB_TOKEN,
        host="127.0.0.1",
        port=0,
    )
    return create_app(cfg)


def _auth() -> dict:
    return {"authorization": f"Bearer {HUB_TOKEN}"}


def _register(client: TestClient, ident, *, ts: int, nonce: str) -> None:
    body = {
        "op": "register",
        "runner_id": ident.runner_id,
        "public_key": ident.public_key_hex,
        "protocol_version": 2,
        "timestamp": ts,
        "nonce": nonce,
    }
    sig = sign_payload(ident, body)
    payload = {
        "runner_id": ident.runner_id,
        "public_key": ident.public_key_hex,
        "protocol_version": 2,
        "runner_version": "0.4.0",
        "hostname": "test-host",
        "os": "test-os",
        "arch": "x86_64",
        "tools": ["py"],
        "tags": [],
        "scope_prefixes": [],
        "metadata": {},
        "timestamp": ts,
        "nonce": nonce,
        "signature": sig,
    }
    r = client.post("/runners/register", json=payload, headers=_auth())
    assert r.status_code == 200, r.text


def _heartbeat(
    client: TestClient,
    ident,
    *,
    claim_failures_total: int | None = None,
    claim_failures_consecutive: int | None = None,
    last_claim_error: str | None = None,
    heartbeat_failures_total: int | None = None,
    ts_offset: int = 1,
):
    ts = int(time.time()) + ts_offset
    nonce = secrets.token_hex(16)
    body = {
        "op": "heartbeat",
        "runner_id": ident.runner_id,
        "timestamp": ts,
        "nonce": nonce,
    }
    sig = sign_payload(ident, body)
    payload = {
        "runner_id": ident.runner_id,
        "timestamp": ts,
        "nonce": nonce,
        "signature": sig,
        "claim_failures_total": claim_failures_total,
        "claim_failures_consecutive": claim_failures_consecutive,
        "last_claim_error": last_claim_error,
        "heartbeat_failures_total": heartbeat_failures_total,
    }
    r = client.post(
        f"/runners/{ident.runner_id}/heartbeat", json=payload, headers=_auth()
    )
    assert r.status_code == 200, r.text
    return r.json()


def test_claim_failure_counters_persist_and_degrade_state(tmp_path: Path) -> None:
    app = _make_app(tmp_path)
    ident = load_or_create(tmp_path / "ri.json")
    with TestClient(app) as c:
        _register(c, ident, ts=int(time.time()), nonce=secrets.token_hex(16))

        rec = _heartbeat(
            c,
            ident,
            claim_failures_total=7,
            claim_failures_consecutive=5,
            last_claim_error='HTTP 404 {"detail":"runner not registered"}',
            heartbeat_failures_total=0,
        )
        assert rec["claim_failures_total"] == 7
        assert rec["claim_failures_consecutive"] == 5
        assert rec["last_claim_error"].startswith("HTTP 404")
        assert rec["last_claim_error_at"] is not None

        # /runners surfaces degraded even though the heartbeat is fresh.
        r = c.get("/runners", headers=_auth())
        assert r.status_code == 200
        runners = r.json()["runners"]
        assert len(runners) == 1
        assert runners[0]["state"] == "degraded"
        assert runners[0]["last_claim_error"].startswith("HTTP 404")


def test_recovered_heartbeat_clears_consecutive_but_keeps_last_error_at(
    tmp_path: Path,
) -> None:
    app = _make_app(tmp_path)
    ident = load_or_create(tmp_path / "ri.json")
    with TestClient(app) as c:
        _register(c, ident, ts=int(time.time()), nonce=secrets.token_hex(16))
        _heartbeat(
            c,
            ident,
            claim_failures_total=4,
            claim_failures_consecutive=4,
            last_claim_error="boom",
            ts_offset=1,
        )
        rec = _heartbeat(
            c,
            ident,
            claim_failures_total=4,
            claim_failures_consecutive=0,
            last_claim_error=None,
            ts_offset=2,
        )
        assert rec["claim_failures_consecutive"] == 0
        assert rec["last_claim_error"] is None
        # Historical incident timestamp is preserved for postmortem.
        assert rec["last_claim_error_at"] is not None

        r = c.get("/runners", headers=_auth())
        assert r.json()["runners"][0]["state"] == "online"


def test_register_resets_consecutive(tmp_path: Path) -> None:
    """Re-registering (e.g. after a 404 on claim) should clear the
    consecutive counter so a recovered runner doesn't show degraded
    forever.
    """
    app = _make_app(tmp_path)
    ident = load_or_create(tmp_path / "ri.json")
    with TestClient(app) as c:
        _register(c, ident, ts=int(time.time()), nonce=secrets.token_hex(16))
        _heartbeat(
            c,
            ident,
            claim_failures_total=10,
            claim_failures_consecutive=10,
            last_claim_error="HTTP 404",
            ts_offset=1,
        )
        # Re-register (different ts/nonce).
        _register(c, ident, ts=int(time.time()) + 5, nonce=secrets.token_hex(16))

        r = c.get("/runners", headers=_auth())
        rec = r.json()["runners"][0]
        # Total preserved as audit history; consecutive cleared.
        assert rec["claim_failures_total"] == 10
        assert rec["claim_failures_consecutive"] == 0
        assert rec["last_claim_error"] is None
        assert rec["state"] == "online"


def test_legacy_runner_omits_fields_no_change(tmp_path: Path) -> None:
    """A v0.1 runner that doesn't know about reliability fields keeps
    working; the hub stores zeros and does not flip state to degraded.
    """
    app = _make_app(tmp_path)
    ident = load_or_create(tmp_path / "ri.json")
    with TestClient(app) as c:
        _register(c, ident, ts=int(time.time()), nonce=secrets.token_hex(16))
        # Heartbeat with NO new fields.
        ts = int(time.time()) + 1
        nonce = secrets.token_hex(16)
        body = {
            "op": "heartbeat",
            "runner_id": ident.runner_id,
            "timestamp": ts,
            "nonce": nonce,
        }
        sig = sign_payload(ident, body)
        payload = {
            "runner_id": ident.runner_id,
            "timestamp": ts,
            "nonce": nonce,
            "signature": sig,
        }
        r = c.post(
            f"/runners/{ident.runner_id}/heartbeat", json=payload, headers=_auth()
        )
        assert r.status_code == 200, r.text
        rec = r.json()
        assert rec["claim_failures_total"] == 0
        assert rec["claim_failures_consecutive"] == 0
        assert rec["last_claim_error"] is None
        assert rec["state"] == "online"
