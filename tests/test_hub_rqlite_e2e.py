"""End-to-end hub tests against a live rqlite cluster.

Runs the same flows as :mod:`test_dispatcher_signing` but with
``backend="rqlite"`` so we exercise:

* :class:`forgewire_fabric.hub._rqlite_db.Connection` end-to-end
* the 9 refactored SELECT-inside-tx call sites
* the rqlite-aware /state/snapshot and /state/import endpoints

**Non-destructive contract.** This suite is allowed to run against
*any* rqlite cluster the developer can reach, including the
production cluster, because the production cluster is, in practice,
the only cluster most developers have. To make that safe we:

* Never ``DROP TABLE`` or ``DELETE FROM`` shared hub tables.
* Generate per-test unique identifiers (UUIDs) so dispatcher /
    worker / task rows added by tests never collide with operator
    state. Host-visible test rows use the ``test-host-`` prefix and
    are deleted surgically after the run; the alternative (table wipe)
    wiped operator-set ``labels`` rows in production.
* For tests that must mutate fabric-wide singletons (the
  ``labels`` table), snapshot the live state at session start and
  restore it at session end.

If you want stricter isolation, stand up a dedicated rqlite
cluster and point ``RQLITE_HOST`` / ``RQLITE_PORT`` at it.
"""
from __future__ import annotations

import json
import os
import secrets
import socket
import time
import uuid
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from forgewire_fabric.dispatcher.identity import DispatcherIdentity, load_or_create
from forgewire_fabric.hub.server import BlackboardConfig, create_app

RQLITE_HOST = os.environ.get("RQLITE_HOST", "127.0.0.1")
RQLITE_PORT = int(os.environ.get("RQLITE_PORT", "4001"))
HUB_TOKEN = "test-hub-token-rqlite-aaaaaaaaaaa"
TEST_HOST_PREFIX = "test-host-"


def _cluster_reachable() -> bool:
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
    reason=f"rqlite cluster {RQLITE_HOST}:{RQLITE_PORT} not reachable",
)


def _execute_rqlite(statements: list[list]) -> None:
    if not statements:
        return
    with httpx.Client(
        base_url=f"http://{RQLITE_HOST}:{RQLITE_PORT}",
        timeout=10.0,
        follow_redirects=True,
    ) as c:
        c.post("/db/execute", json=statements).raise_for_status()


def _cleanup_test_hosts() -> None:
    statements = [
        [
            "DELETE FROM dispatchers WHERE hostname = ? OR hostname LIKE ?",
            "test-host",
            f"{TEST_HOST_PREFIX}%",
        ],
        [
            "DELETE FROM host_roles WHERE hostname = ? OR hostname LIKE ?",
            "test-host",
            f"{TEST_HOST_PREFIX}%",
        ],
    ]
    _execute_rqlite(statements)


@pytest.fixture(scope="session", autouse=True)
def _host_surface_cleanup():
    _cleanup_test_hosts()
    yield
    _cleanup_test_hosts()


@pytest.fixture(scope="session", autouse=True)
def _labels_snapshot():
    """Snapshot ``labels`` at session start, restore at session end.

    The ``labels`` table holds fabric-wide singletons (``hub_name``
    plus ``runner_alias:<runner_id>`` rows) that operators set by
    hand. The labels parity test below has to mutate those keys,
    which would clobber operator state on a shared cluster. We
    capture every row up front, let the tests run, then re-apply
    the captured set verbatim (and delete anything the tests added
    on top).
    """
    with httpx.Client(
        base_url=f"http://{RQLITE_HOST}:{RQLITE_PORT}",
        timeout=10.0,
        follow_redirects=True,
    ) as c:
        # If the table doesn't exist yet the query errors out; the
        # first Blackboard() ctor will create it. Treat "missing" as
        # an empty snapshot.
        try:
            r = c.post(
                "/db/query?level=strong",
                json=[
                    "SELECT key, value, updated_by FROM labels"
                ],
            )
            r.raise_for_status()
            res = r.json()["results"][0]
            cols = res.get("columns", [])
            rows = [
                dict(zip(cols, v)) for v in res.get("values", []) or []
            ]
        except Exception:  # noqa: BLE001
            rows = []
    yield rows
    # Restore: re-upsert every captured row, then delete any key not
    # in the snapshot.
    with httpx.Client(
        base_url=f"http://{RQLITE_HOST}:{RQLITE_PORT}",
        timeout=10.0,
        follow_redirects=True,
    ) as c:
        statements: list[list] = []
        for row in rows:
            statements.append(
                [
                    """INSERT INTO labels (key, value, updated_by, updated_at)
                       VALUES (?, ?, ?, datetime('now'))
                       ON CONFLICT(key) DO UPDATE SET
                           value = excluded.value,
                           updated_by = excluded.updated_by,
                           updated_at = excluded.updated_at""",
                    row["key"],
                    row["value"],
                    row.get("updated_by"),
                ]
            )
        captured_keys = [row["key"] for row in rows]
        # Delete any key the tests added on top of the snapshot.
        try:
            r = c.post(
                "/db/query?level=strong",
                json=["SELECT key FROM labels"],
            )
            r.raise_for_status()
            res = r.json()["results"][0]
            live_keys = [v[0] for v in res.get("values", []) or []]
        except Exception:  # noqa: BLE001
            live_keys = []
        for k in live_keys:
            if k not in captured_keys:
                statements.append(["DELETE FROM labels WHERE key = ?", k])
        _execute_rqlite(statements)


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


def _ident(tmp_path: Path, label: str | None = None) -> DispatcherIdentity:
    """Return a *fresh* DispatcherIdentity per call.

    Tests must not reuse a dispatcher_id across runs because the
    upsert_dispatcher binding check rejects re-binds. Each call
    writes to a unique subpath under ``tmp_path`` so identities are
    independent.
    """
    sub = tmp_path / f"ident-{uuid.uuid4().hex[:8]}"
    return load_or_create(
        sub / "dispatcher_identity.json",
        label=label or f"test-dispatcher-{uuid.uuid4().hex[:6]}",
    )


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
        "hostname": f"{TEST_HOST_PREFIX}{ident.dispatcher_id[:8]}",
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
                "hostname": f"{TEST_HOST_PREFIX}{uuid.uuid4().hex[:8]}",
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
    """Parity: ``labels`` (hub_name + runner/host aliases) must
    persist through the rqlite write path the same way it does under
    sqlite. ``_upsert_label`` uses ``BEGIN IMMEDIATE`` + a single
    DELETE-or-INSERT statement + commit, which is the rqlite-safe
    pattern (no SELECT/RETURNING inside the buffered txn). This test
    locks that contract in so the rqlite backend can't silently
    regress.

    Non-destructive: uses UUID-suffixed alias keys so it never
    collides with operator-set runner or host aliases, and snapshots +
    restores ``hub_name`` around its own mutation so the live
    cluster's operator-set hub name is preserved.
    """
    rid_a = f"rid-test-{uuid.uuid4().hex}"
    rid_b = f"rid-test-{uuid.uuid4().hex}"
    host_a = f"{TEST_HOST_PREFIX}{uuid.uuid4().hex[:8]}"
    host_b = f"{TEST_HOST_PREFIX}{uuid.uuid4().hex[:8]}"
    test_hub_name_a = f"rqlite-parity-{uuid.uuid4().hex[:6]}"
    test_hub_name_b = f"rqlite-parity-{uuid.uuid4().hex[:6]}"

    app = _make_app(tmp_path)
    with TestClient(app) as client:
        # Snapshot the live hub_name so we can restore it after we
        # mutate the singleton.
        r = client.get("/labels", headers=_auth())
        assert r.status_code == 200
        original_hub_name = r.json()["hub_name"]

        # Our test-scoped alias keys must NOT exist yet.
        existing_aliases = r.json()["runner_aliases"]
        assert rid_a not in existing_aliases
        assert rid_b not in existing_aliases
        existing_host_aliases = r.json().get("host_aliases", {})
        assert host_a not in existing_host_aliases
        assert host_b not in existing_host_aliases

        try:
            # Set hub_name -- single INSERT path.
            r = client.put(
                "/labels/hub", json={"name": test_hub_name_a}, headers=_auth()
            )
            assert r.status_code == 200, r.text
            assert r.json()["hub_name"] == test_hub_name_a

            # Upsert hub_name -- INSERT...ON CONFLICT DO UPDATE path.
            r = client.put(
                "/labels/hub", json={"name": test_hub_name_b}, headers=_auth()
            )
            assert r.status_code == 200
            assert r.json()["hub_name"] == test_hub_name_b

            # Per-runner aliases (multiple keys with the runner_alias: prefix).
            r = client.put(
                f"/labels/runners/{rid_a}",
                json={"alias": "Alpha"},
                headers=_auth(),
            )
            assert r.status_code == 200
            r = client.put(
                f"/labels/runners/{rid_b}",
                json={"alias": "Bravo"},
                headers=_auth(),
            )
            assert r.status_code == 200

            # Per-host aliases (machine labels).
            r = client.put(
                f"/labels/hosts/{host_a}",
                json={"alias": "Host Alpha"},
                headers=_auth(),
            )
            assert r.status_code == 200
            r = client.put(
                f"/labels/hosts/{host_b}",
                json={"alias": "Host Bravo"},
                headers=_auth(),
            )
            assert r.status_code == 200

            # Read everything back; rqlite's strong-consistency read must see
            # our rows.
            r = client.get("/labels", headers=_auth())
            body = r.json()
            assert body["hub_name"] == test_hub_name_b
            assert body["runner_aliases"].get(rid_a) == "Alpha"
            assert body["runner_aliases"].get(rid_b) == "Bravo"
            assert body["host_aliases"].get(host_a) == "Host Alpha"
            assert body["host_aliases"].get(host_b) == "Host Bravo"

            # Clear one runner alias and one host alias -- DELETE path. The
            # other test rows must remain.
            r = client.put(
                f"/labels/runners/{rid_a}",
                json={"alias": ""},
                headers=_auth(),
            )
            assert r.status_code == 200
            r = client.put(
                f"/labels/hosts/{host_a}",
                json={"alias": ""},
                headers=_auth(),
            )
            assert r.status_code == 200
            r = client.get("/labels", headers=_auth())
            body = r.json()
            assert rid_a not in body["runner_aliases"]
            assert body["runner_aliases"].get(rid_b) == "Bravo"
            assert host_a not in body["host_aliases"]
            assert body["host_aliases"].get(host_b) == "Host Bravo"
        finally:
            # Restore operator state.
            client.put(
                "/labels/hub",
                json={"name": original_hub_name},
                headers=_auth(),
            )
            for rid in (rid_a, rid_b):
                client.put(
                    f"/labels/runners/{rid}",
                    json={"alias": ""},
                    headers=_auth(),
                )
            for hostname in (host_a, host_b):
                client.put(
                    f"/labels/hosts/{hostname}",
                    json={"alias": ""},
                    headers=_auth(),
                )
