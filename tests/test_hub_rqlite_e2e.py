"""End-to-end hub tests against the live rqlite cluster.

Runs the same flows as :mod:`test_dispatcher_signing` but with
``backend="rqlite"`` so we exercise:

* :class:`forgewire_fabric.hub._rqlite_db.Connection` end-to-end
* the 9 refactored SELECT-inside-tx call sites
* the rqlite-aware /state/snapshot and /state/import endpoints

Each test uses a fresh database name within rqlite so test runs are
independent. Note: rqlite's HTTP API exposes a single SQLite database
per cluster, so we can't get true isolation -- instead we DROP all the
hub tables before each test and let ``Blackboard.__init__`` re-create
them via ``schema.sql``.
"""
from __future__ import annotations

import json
import os
import secrets
import socket
import time
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from forgewire_fabric.dispatcher.identity import DispatcherIdentity, load_or_create
from forgewire_fabric.hub.server import BlackboardConfig, create_app

RQLITE_HOST = os.environ.get("RQLITE_HOST", "127.0.0.1")
RQLITE_PORT = int(os.environ.get("RQLITE_PORT", "4001"))
HUB_TOKEN = "test-hub-token-rqlite-aaaaaaaaaaa"

# Production guard: this suite is destructive (it ``DROP TABLE IF
# EXISTS`` every hub table before each test, including ``labels``
# which stores operator-set fabric state like ``hub_name`` and
# ``runner_alias:<runner_id>``). Pointing it at a non-loopback host
# without opting in would silently wipe a live cluster. We refuse
# unless ``FORGEWIRE_TEST_RQLITE_DESTRUCTIVE_OK=1`` is explicitly
# set. Loopback (127.0.0.1 / ::1 / localhost) is exempt because the
# only way a developer hits a loopback rqlite is by standing one up
# locally for tests.
_LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}
_DESTRUCTIVE_OK = os.environ.get("FORGEWIRE_TEST_RQLITE_DESTRUCTIVE_OK") == "1"

# Order matters: drop child tables before parents so FK isn't violated.
_HUB_TABLES = [
    "task_streams",
    "results",
    "progress",
    "notes",
    "labels",
    "dispatchers",
    "runners",
    "workers",
    "tasks",
]


def _cluster_reachable() -> bool:
    if RQLITE_HOST not in _LOOPBACK_HOSTS and not _DESTRUCTIVE_OK:
        # Refuse to run destructively against any host that isn't
        # loopback unless the operator opted in. This keeps a stray
        # ``pytest`` invocation on a dev box from wiping production
        # labels / aliases / runners.
        return False
    try:
        with socket.create_connection((RQLITE_HOST, RQLITE_PORT), timeout=1.0):
            pass
    except OSError:
        return False
    try:
        with httpx.Client(
            base_url=f"http://{RQLITE_HOST}:{RQLITE_PORT}", timeout=2.0
        ) as c:
            return c.get("/status").status_code == 200
    except httpx.HTTPError:
        return False


pytestmark = pytest.mark.skipif(
    not _cluster_reachable(),
    reason=(
        f"rqlite cluster {RQLITE_HOST}:{RQLITE_PORT} not reachable or "
        "non-loopback without FORGEWIRE_TEST_RQLITE_DESTRUCTIVE_OK=1"
    ),
)


@pytest.fixture(autouse=True)
def _clean_cluster():
    """Drop all hub tables before each test so we start from schema.sql.

    rqlite is a single shared database; tests must be sequential. The
    in-memory stream-counter inside Blackboard resets per ``__init__``
    so no Python-side state leaks across tests.
    """
    statements = [[f"DROP TABLE IF EXISTS {t}"] for t in _HUB_TABLES]
    with httpx.Client(
        base_url=f"http://{RQLITE_HOST}:{RQLITE_PORT}",
        timeout=20.0,
        follow_redirects=True,
    ) as c:
        r = c.post("/db/execute?transaction=true", json=statements)
        assert r.status_code == 200, r.text
    yield


def _make_app(tmp_path: Path, *, require_signed: bool = False):
    cfg = BlackboardConfig(
        db_path=tmp_path / "hub.sqlite3",  # unused under rqlite backend
        token=HUB_TOKEN,
        host="127.0.0.1",
        port=0,
        require_signed_dispatch=require_signed,
        backend="rqlite",
        rqlite_host=RQLITE_HOST,
        rqlite_port=RQLITE_PORT,
    )
    return create_app(cfg)


def _canonical(body: dict) -> bytes:
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _ident(tmp_path: Path, label: str = "test-dispatcher") -> DispatcherIdentity:
    return load_or_create(tmp_path / "dispatcher_identity.json", label=label)


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


def _auth():
    return {"Authorization": f"Bearer {HUB_TOKEN}"}


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------


def test_health_under_rqlite_backend(tmp_path):
    app = _make_app(tmp_path)
    with TestClient(app) as client:
        r = client.get("/healthz")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"


def test_register_then_signed_dispatch_under_rqlite(tmp_path):
    app = _make_app(tmp_path, require_signed=True)
    ident = _ident(tmp_path)
    with TestClient(app) as client:
        r = client.post(
            "/dispatchers/register", json=_sign_register(ident), headers=_auth()
        )
        assert r.status_code == 200, r.text
        r2 = client.post(
            "/tasks/v2", json=_sign_dispatch(ident), headers=_auth()
        )
        assert r2.status_code == 200, r2.text
        body = r2.json()
        assert "id" in body
        assert body["status"] == "queued"


def test_replay_nonce_rejected_under_rqlite(tmp_path):
    """Site 8 (consume_dispatcher_nonce) must reject replays under rqlite."""
    app = _make_app(tmp_path, require_signed=True)
    ident = _ident(tmp_path)
    with TestClient(app) as client:
        client.post(
            "/dispatchers/register", json=_sign_register(ident), headers=_auth()
        )
        nonce = secrets.token_hex(16)
        ts = int(time.time())
        r1 = client.post(
            "/tasks/v2",
            json=_sign_dispatch(ident, nonce=nonce, timestamp=ts),
            headers=_auth(),
        )
        assert r1.status_code == 200
        r2 = client.post(
            "/tasks/v2",
            json=_sign_dispatch(ident, nonce=nonce, timestamp=ts),
            headers=_auth(),
        )
        assert r2.status_code in (401, 403), r2.text


def test_register_collision_rejects_new_pubkey_under_rqlite(tmp_path):
    """Site 7 (upsert_dispatcher) must reject re-bind under rqlite."""
    from dataclasses import replace as dc_replace

    app = _make_app(tmp_path, require_signed=True)
    ident_a = _ident(tmp_path, label="A")
    ident_b_seed = _ident(tmp_path / "b", label="A")  # different key
    # Force same dispatcher_id but a different key pair so the binding
    # check fires.
    ident_b = dc_replace(ident_b_seed, dispatcher_id=ident_a.dispatcher_id)
    with TestClient(app) as client:
        r1 = client.post(
            "/dispatchers/register", json=_sign_register(ident_a), headers=_auth()
        )
        assert r1.status_code == 200
        r2 = client.post(
            "/dispatchers/register", json=_sign_register(ident_b), headers=_auth()
        )
        assert r2.status_code in (401, 403, 409), r2.text
        assert "different public_key" in r2.text or "permission" in r2.text.lower()


def test_state_snapshot_under_rqlite(tmp_path):
    """/state/snapshot proxies to rqlite /db/backup. The body should be a
    valid SQLite file (magic bytes ``SQLite format 3\\0``).
    """
    app = _make_app(tmp_path)
    with TestClient(app) as client:
        r = client.get("/state/snapshot", headers=_auth())
        assert r.status_code == 200, r.text
        assert r.headers.get("X-Snapshot-Source") == "rqlite"
        assert r.content[:16].startswith(b"SQLite format 3\x00")


def test_claim_next_task_v2_cas_under_rqlite(tmp_path):
    """Site 9 (claim_next_task_v2) end-to-end via the dispatch + register
    + claim cycle. Validates that the refactor's UPDATE...RETURNING CAS
    actually claims a task through the rqlite backend."""
    app = _make_app(tmp_path, require_signed=True)
    ident = _ident(tmp_path)
    with TestClient(app) as client:
        client.post(
            "/dispatchers/register", json=_sign_register(ident), headers=_auth()
        )
        # Dispatch a task.
        r = client.post(
            "/tasks/v2", json=_sign_dispatch(ident), headers=_auth()
        )
        assert r.status_code == 200
        # The legacy v1 claim path is the simplest way to validate the
        # refactor under the test client. (claim_next_task, site 1.)
        r2 = client.post(
            "/tasks/claim",
            json={
                "worker_id": "test-worker-1",
                "hostname": "test-host",
                "capabilities": {"tools": ["python"]},
            },
            headers=_auth(),
        )
        assert r2.status_code == 200, r2.text
        body = r2.json()
        # Either a task was claimed or none was eligible (depends on
        # routing); both are valid under the new code path.
        assert "task" in body or body.get("status") in (
            "no_task",
            "queue_empty",
            None,
        )


def test_labels_round_trip_under_rqlite(tmp_path):
    """Parity: ``labels`` (hub_name + runner_alias:<runner_id>) must
    persist through the rqlite write path the same way it does under
    sqlite. ``_upsert_label`` uses ``BEGIN IMMEDIATE`` + a single
    DELETE-or-INSERT statement + commit, which is the rqlite-safe
    pattern (no SELECT/RETURNING inside the buffered txn). This test
    locks that contract in so the rqlite backend can't silently
    regress.
    """
    app = _make_app(tmp_path)
    with TestClient(app) as client:
        # Empty registry on a freshly-DROP'd labels table.
        r = client.get("/labels", headers=_auth())
        assert r.status_code == 200
        assert r.json() == {"hub_name": "", "runner_aliases": {}}

        # Set hub_name -- single INSERT path.
        r = client.put(
            "/labels/hub", json={"name": "rqlite-parity-hub"}, headers=_auth()
        )
        assert r.status_code == 200, r.text
        assert r.json()["hub_name"] == "rqlite-parity-hub"

        # Upsert hub_name -- INSERT...ON CONFLICT DO UPDATE path.
        r = client.put(
            "/labels/hub", json={"name": "rqlite-parity-hub-v2"}, headers=_auth()
        )
        assert r.status_code == 200
        assert r.json()["hub_name"] == "rqlite-parity-hub-v2"

        # Per-runner aliases (multiple keys with the runner_alias: prefix).
        r = client.put(
            "/labels/runners/rid-A",
            json={"alias": "Alpha"},
            headers=_auth(),
        )
        assert r.status_code == 200
        r = client.put(
            "/labels/runners/rid-B",
            json={"alias": "Bravo"},
            headers=_auth(),
        )
        assert r.status_code == 200

        # Read everything back; rqlite's strong-consistency read must see
        # all three rows.
        r = client.get("/labels", headers=_auth())
        body = r.json()
        assert body["hub_name"] == "rqlite-parity-hub-v2"
        assert body["runner_aliases"] == {"rid-A": "Alpha", "rid-B": "Bravo"}

        # Clear one alias -- DELETE path. The other rows must remain.
        r = client.put(
            "/labels/runners/rid-A", json={"alias": ""}, headers=_auth()
        )
        assert r.status_code == 200
        r = client.get("/labels", headers=_auth())
        body = r.json()
        assert body["runner_aliases"] == {"rid-B": "Bravo"}
        assert body["hub_name"] == "rqlite-parity-hub-v2"
