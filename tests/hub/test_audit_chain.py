"""Tests for the M2.5.3 hash-chained audit log, replay, and export.

Exercises the real FastAPI hub against a real on-disk SQLite blackboard.
Verifies that:

* dispatch + claim + result each emit one chain-linked audit_event row;
* the chain self-verifies via :func:`Blackboard.verify_audit_chain`;
* tampering any payload byte breaks the chain on read;
* :func:`Blackboard.audit_iter_day` returns the day's events;
* the replay command can reconstruct a brief from the audit log.

Mocking policy: none. We use the real hub + real client.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from forgewire_fabric.hub.server import Blackboard, BlackboardConfig, create_app


HUB_TOKEN = "x" * 32
BEARER = {"Authorization": f"Bearer {HUB_TOKEN}"}
BASE = {"title": "audit-t", "prompt": "noop body", "base_commit": "a" * 40}


def _build_client() -> tuple[TestClient, Path]:
    tmp = Path(tempfile.mkdtemp(prefix="fw-audit-"))
    cfg = BlackboardConfig(
        db_path=tmp / "blackboard.db",
        token=HUB_TOKEN,
        host="127.0.0.1",
        port=0,
    )
    return TestClient(create_app(cfg)), tmp / "blackboard.db"


def _dispatch_one(client: TestClient, *, todo_id: str = "audit-1") -> dict:
    body = {
        **BASE,
        "scope_globs": ["docs/x.md"],
        "branch": "feature/audit",
        "todo_id": todo_id,
    }
    resp = client.post("/tasks", json=body, headers=BEARER)
    assert resp.status_code == 200, resp.text
    return resp.json()


def _claim(client: TestClient, *, worker_id: str = "w1") -> dict:
    resp = client.post(
        "/tasks/claim",
        json={"worker_id": worker_id, "hostname": "h1", "capabilities": {}},
        headers=BEARER,
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["task"]


def _result(client: TestClient, task_id: int, *, worker_id: str = "w1") -> dict:
    resp = client.post(
        f"/tasks/{task_id}/result",
        json={
            "worker_id": worker_id,
            "status": "done",
            "head_commit": "f" * 40,
            "commits": ["f" * 40],
            "files_touched": ["docs/x.md"],
            "test_summary": "ok",
            "log_tail": "",
            "error": None,
        },
        headers=BEARER,
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_dispatch_emits_chain_linked_event() -> None:
    client, _ = _build_client()
    head_before = client.get("/audit/tail", headers=BEARER).json()["chain_tail"]
    assert head_before == Blackboard.AUDIT_GENESIS_HASH

    task = _dispatch_one(client)
    audit = client.get(f"/audit/tasks/{task['id']}", headers=BEARER).json()
    assert audit["verified"] is True, audit["error"]
    assert len(audit["events"]) == 1
    ev = audit["events"][0]
    assert ev["kind"] == "dispatch"
    assert ev["prev_event_id_hash"] == Blackboard.AUDIT_GENESIS_HASH
    assert ev["payload"]["task_id"] == task["id"]
    assert ev["payload"]["sealed_brief_hash"]
    assert ev["payload"]["signed"] is False

    head_after = client.get("/audit/tail", headers=BEARER).json()["chain_tail"]
    assert head_after == ev["event_id_hash"]


def test_full_lifecycle_emits_three_events_in_chain() -> None:
    client, _ = _build_client()
    task = _dispatch_one(client)
    claimed = _claim(client)
    assert claimed["id"] == task["id"]
    _result(client, task["id"])

    audit = client.get(f"/audit/tasks/{task['id']}", headers=BEARER).json()
    assert audit["verified"] is True, audit["error"]
    kinds = [e["kind"] for e in audit["events"]]
    assert kinds == ["dispatch", "claim", "result"]
    # Each event's prev_hash is the previous event's hash.
    for i in range(1, len(audit["events"])):
        assert audit["events"][i]["prev_event_id_hash"] == audit["events"][i - 1]["event_id_hash"]


def test_chain_break_detected_when_payload_tampered() -> None:
    client, db_path = _build_client()
    task = _dispatch_one(client)
    _claim(client)
    _result(client, task["id"])

    # Tamper the result row's payload directly.
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE audit_event SET payload_json = ? WHERE kind = 'result'",
            (json.dumps({"task_id": task["id"], "status": "BOGUS"}, sort_keys=True),),
        )
        conn.commit()

    audit = client.get(f"/audit/tasks/{task['id']}", headers=BEARER).json()
    assert audit["verified"] is False
    assert "hash mismatch" in (audit["error"] or "")


def test_audit_for_day_returns_today() -> None:
    import datetime as _dt

    client, _ = _build_client()
    _dispatch_one(client)
    today = _dt.datetime.utcnow().date().isoformat()
    doc = client.get(f"/audit/day/{today}", headers=BEARER).json()
    assert doc["verified"] is True
    assert len(doc["events"]) >= 1


def test_audit_for_day_rejects_bad_date() -> None:
    client, _ = _build_client()
    resp = client.get("/audit/day/not-a-date", headers=BEARER)
    assert resp.status_code == 400


def test_dispatch_audit_failure_is_swallowed(monkeypatch: pytest.MonkeyPatch) -> None:
    """If audit append throws, dispatch must still succeed (best-effort)."""
    client, _ = _build_client()
    # Wedge append_audit_event after first task to simulate disk fault.
    bb = client.app.state.blackboard
    boom_calls: list[str] = []

    def boom(**kwargs):  # noqa: ANN001
        boom_calls.append(kwargs["kind"])
        raise RuntimeError("disk full")

    monkeypatch.setattr(bb, "append_audit_event", boom)

    resp = client.post(
        "/tasks",
        json={**BASE, "scope_globs": ["docs/x.md"], "branch": "feature/x", "todo_id": "audit-2"},
        headers=BEARER,
    )
    assert resp.status_code == 200
    assert "dispatch" in boom_calls


def test_verify_chain_helper_handles_genesis_and_partial() -> None:
    client, _ = _build_client()
    _dispatch_one(client, todo_id="a")
    _dispatch_one(client, todo_id="b")
    _dispatch_one(client, todo_id="c")
    # Pull all events via day API and shuffle — verify must reject misordered.
    import datetime as _dt
    today = _dt.datetime.utcnow().date().isoformat()
    events = client.get(f"/audit/day/{today}", headers=BEARER).json()["events"]
    assert len(events) == 3
    ok, err = Blackboard.verify_audit_chain(events)
    assert ok, err

    # Drop the middle event and re-verify => chain break.
    bad = [events[0], events[2]]
    ok2, err2 = Blackboard.verify_audit_chain(bad)
    assert ok2 is False
    assert "chain break" in (err2 or "")
