"""Tests for the M2.5.1 approval queue.

When the hub policy says ``REQUIRE_APPROVAL`` (e.g. dispatch targeting a
protected branch) the gate now seeds a pending approval row, returns 428
with the ``approval_id`` and ``envelope_hash``, and only lets the dispatch
through on a re-POST that carries an ``approval_id`` matching an
**approved** row whose envelope hash equals the canonical hash of the
re-dispatched intent.

Mocking policy: none. We exercise the real FastAPI app over a real on-disk
SQLite blackboard against a real policy.yaml, just like
``test_policy_gate.py``.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import yaml
from fastapi.testclient import TestClient

from forgewire_fabric.hub.server import BlackboardConfig, create_app


HUB_TOKEN = "x" * 32
BEARER = {"Authorization": f"Bearer {HUB_TOKEN}"}
BASE = {"title": "t", "prompt": "p", "base_commit": "a" * 40}


def _build_client(policy: dict) -> TestClient:
    tmp = Path(tempfile.mkdtemp(prefix="fw-appr-"))
    policy_path = tmp / "policy.yaml"
    policy_path.write_text(yaml.safe_dump(policy), encoding="utf-8")
    cfg = BlackboardConfig(
        db_path=tmp / "blackboard.db",
        token=HUB_TOKEN,
        host="127.0.0.1",
        port=0,
        policy_path=policy_path,
    )
    return TestClient(create_app(cfg))


def _dispatch(client: TestClient, *, branch: str, scope_globs: list[str], todo_id: str,
              approval_id: str | None = None) -> dict:
    body = {**BASE, "scope_globs": scope_globs, "branch": branch, "todo_id": todo_id}
    if approval_id is not None:
        body["approval_id"] = approval_id
    return client.post("/tasks", json=body, headers=BEARER)  # type: ignore[return-value]


def test_protected_branch_returns_428_with_approval_id() -> None:
    client = _build_client({"protected_branches": ["main"]})
    resp = _dispatch(client, branch="main", scope_globs=["docs/x.md"], todo_id="42")
    assert resp.status_code == 428, resp.text
    detail = resp.json()["detail"]
    assert detail["decision"] == "require_approval"
    assert "approval_id" in detail
    assert "envelope_hash" in detail
    assert detail["approval_id"]


def test_pending_dedup_on_repeated_intent() -> None:
    """Re-POSTing the same intent without an approval_id re-uses the row."""
    client = _build_client({"protected_branches": ["main"]})
    a = _dispatch(client, branch="main", scope_globs=["docs/x.md"], todo_id="42").json()["detail"]
    b = _dispatch(client, branch="main", scope_globs=["docs/x.md"], todo_id="42").json()["detail"]
    assert a["approval_id"] == b["approval_id"]
    # Different envelope (different scope) must spawn a different row.
    c = _dispatch(client, branch="main", scope_globs=["docs/y.md"], todo_id="42").json()["detail"]
    assert c["approval_id"] != a["approval_id"]


def test_approve_then_redispatch_succeeds() -> None:
    client = _build_client({"protected_branches": ["main"]})
    detail = _dispatch(
        client, branch="main", scope_globs=["docs/x.md"], todo_id="42"
    ).json()["detail"]
    approval_id = detail["approval_id"]

    # Operator approves via the new endpoint.
    resp = client.post(
        f"/approvals/{approval_id}/approve",
        json={"approver": "alice", "reason": "reviewed"},
        headers=BEARER,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "approved"

    # Dispatcher re-POSTs the *same intent* with the approval_id field.
    resp = _dispatch(
        client,
        branch="main",
        scope_globs=["docs/x.md"],
        todo_id="42",
        approval_id=approval_id,
    )
    assert resp.status_code == 200, resp.text

    # Approval is single-use — second re-dispatch must re-enter the gate.
    resp2 = _dispatch(
        client,
        branch="main",
        scope_globs=["docs/x.md"],
        todo_id="42",
        approval_id=approval_id,
    )
    assert resp2.status_code == 428


def test_envelope_hash_mismatch_refuses_consumption() -> None:
    """An approval for one envelope must not unlock a different envelope."""
    client = _build_client({"protected_branches": ["main"]})
    a = _dispatch(
        client, branch="main", scope_globs=["docs/x.md"], todo_id="42"
    ).json()["detail"]
    client.post(
        f"/approvals/{a['approval_id']}/approve",
        json={"approver": "alice"},
        headers=BEARER,
    )

    # Re-dispatch with a *different* scope but the approved approval_id.
    resp = _dispatch(
        client,
        branch="main",
        scope_globs=["docs/y.md"],   # mismatched
        todo_id="42",
        approval_id=a["approval_id"],
    )
    assert resp.status_code == 428


def test_deny_path_locks_intent() -> None:
    client = _build_client({"protected_branches": ["main"]})
    a = _dispatch(
        client, branch="main", scope_globs=["docs/x.md"], todo_id="42"
    ).json()["detail"]
    resp = client.post(
        f"/approvals/{a['approval_id']}/deny",
        json={"approver": "alice", "reason": "no"},
        headers=BEARER,
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "denied"

    # Re-dispatch with the denied id must not consume.
    resp = _dispatch(
        client,
        branch="main",
        scope_globs=["docs/x.md"],
        todo_id="42",
        approval_id=a["approval_id"],
    )
    assert resp.status_code == 428
    new_id = resp.json()["detail"]["approval_id"]
    assert new_id != a["approval_id"], "denied row must not be reused"


def test_resolve_twice_returns_409() -> None:
    client = _build_client({"protected_branches": ["main"]})
    a = _dispatch(
        client, branch="main", scope_globs=["docs/x.md"], todo_id="42"
    ).json()["detail"]
    client.post(
        f"/approvals/{a['approval_id']}/approve",
        json={"approver": "alice"},
        headers=BEARER,
    )
    resp = client.post(
        f"/approvals/{a['approval_id']}/deny",
        json={"approver": "bob"},
        headers=BEARER,
    )
    assert resp.status_code == 409, resp.text


def test_list_and_get_endpoints() -> None:
    client = _build_client({"protected_branches": ["main"]})
    a = _dispatch(
        client, branch="main", scope_globs=["docs/x.md"], todo_id="42"
    ).json()["detail"]
    listing = client.get("/approvals?status=pending", headers=BEARER).json()
    assert any(r["approval_id"] == a["approval_id"] for r in listing["approvals"])
    one = client.get(f"/approvals/{a['approval_id']}", headers=BEARER).json()
    assert one["status"] == "pending"
    assert one["branch"] == "main"
    assert client.get("/approvals/does-not-exist", headers=BEARER).status_code == 404


def test_hard_deny_is_not_approvable() -> None:
    """A 403 forbidden_paths deny must not be turned into an approval row."""
    client = _build_client({"forbidden_paths": ["secrets/**"]})
    resp = _dispatch(
        client, branch="feature/x", scope_globs=["secrets/k.txt"], todo_id="42"
    )
    assert resp.status_code == 403
    detail = resp.json()["detail"]
    assert "approval_id" not in detail
    listing = client.get("/approvals", headers=BEARER).json()
    assert listing["approvals"] == []
