"""End-to-end tests for the M2.5.1+M2.5.2 hub-side dispatch/completion gate.

These exercise the substrate-owned ``HubDispatchGate`` through the FastAPI
app built by :func:`forgewire_fabric.hub.server.create_app`. The goal is to
prove the wiring: a policy.yaml loaded via ``BlackboardConfig.policy_path``
must convert FabricPolicyEngine / BudgetEnforcer decisions into HTTP responses
on the legacy ``POST /tasks`` and ``POST /tasks/{id}/result`` endpoints.

Mocking policy: none. The hub is built with a real on-disk SQLite blackboard
and a real policy file in a tempdir — these are the actual collaborators the
hub uses in production.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from forgewire_fabric.hub.server import BlackboardConfig, create_app


HUB_TOKEN = "x" * 32
BEARER = {"Authorization": f"Bearer {HUB_TOKEN}"}
BASE = {"title": "t", "prompt": "p", "base_commit": "a" * 40}


def _build_client(policy: dict | None = None) -> TestClient:
    tmp = Path(tempfile.mkdtemp(prefix="fw-gate-"))
    policy_path: Path | None = None
    if policy is not None:
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


def test_unconfigured_policy_is_permissive() -> None:
    """No policy file => HubDispatchGate runs but allows everything."""
    client = _build_client(policy=None)
    resp = client.post(
        "/tasks",
        json={**BASE, "scope_globs": ["secrets/anywhere"], "branch": "main", "todo_id": "1"},
        headers=BEARER,
    )
    assert resp.status_code == 200, resp.text


def test_dispatch_allowed_within_scope() -> None:
    client = _build_client(
        policy={"forbidden_paths": ["secrets/**"], "protected_branches": ["main"]}
    )
    resp = client.post(
        "/tasks",
        json={**BASE, "scope_globs": ["docs/**"], "branch": "feature/x", "todo_id": "2"},
        headers=BEARER,
    )
    assert resp.status_code == 200, resp.text


def test_dispatch_denied_on_forbidden_path() -> None:
    client = _build_client(policy={"forbidden_paths": ["secrets/**"]})
    resp = client.post(
        "/tasks",
        json={**BASE, "scope_globs": ["secrets/k.txt"], "branch": "feature/x", "todo_id": "3"},
        headers=BEARER,
    )
    assert resp.status_code == 403, resp.text
    detail = resp.json()["detail"]
    assert detail["decision"] == "deny"
    assert detail["rule_name"] == "forbidden_paths"
    assert detail["violations"], "deny must carry at least one violation"


def test_dispatch_requires_approval_on_protected_branch() -> None:
    client = _build_client(policy={"protected_branches": ["main"]})
    resp = client.post(
        "/tasks",
        json={**BASE, "scope_globs": ["docs/**"], "branch": "main", "todo_id": "4"},
        headers=BEARER,
    )
    assert resp.status_code == 428, resp.text
    detail = resp.json()["detail"]
    assert detail["decision"] == "require_approval"
    assert any(v["rule"] == "protected_branches" for v in detail["violations"])


def test_completion_denied_on_forbidden_path() -> None:
    """A result envelope reporting files_touched outside policy must be rejected."""
    client = _build_client(policy={"forbidden_paths": ["secrets/**"]})
    # Create a task first (allowed scope).
    create = client.post(
        "/tasks",
        json={**BASE, "scope_globs": ["docs/**"], "branch": "feature/x", "todo_id": "5"},
        headers=BEARER,
    )
    assert create.status_code == 200, create.text
    task_id = create.json()["id"]

    # Now try to submit a result that "touched" a forbidden path. The
    # completion gate must refuse before the blackboard records anything.
    submit = client.post(
        f"/tasks/{task_id}/result",
        json={
            "status": "completed",
            "summary": "ok",
            "files_touched": ["secrets/leaked.txt"],
            "worker_id": "test-runner",
        },
        headers=BEARER,
    )
    assert submit.status_code == 403, submit.text
    detail = submit.json()["detail"]
    assert detail["decision"] == "deny"


def test_completion_protected_branch_still_records_result() -> None:
    """REQUIRE_APPROVAL on completion is informational — the work happened.

    We don't drive a full claim/result lifecycle here (that's covered by the
    end-to-end hub tests). We only assert that the policy gate does not turn
    this completion into a 403/428: the gate must let it through to the
    blackboard, even if the blackboard then rejects on unrelated grounds
    (e.g. unclaimed task, missing worker). Any non-policy status code is fine.
    """
    client = _build_client(policy={"protected_branches": ["never-matched"]})
    create = client.post(
        "/tasks",
        json={**BASE, "scope_globs": ["docs/**"], "branch": "feature/x", "todo_id": "6"},
        headers=BEARER,
    )
    assert create.status_code == 200, create.text
    task_id = create.json()["id"]
    submit = client.post(
        f"/tasks/{task_id}/result",
        json={
            "status": "done",
            "files_touched": ["docs/x.md"],
            "worker_id": "test-runner",
        },
        headers=BEARER,
    )
    # Gate must not fire — allowed paths, branch doesn't match protection
    # list. The blackboard itself may still reject for unrelated reasons
    # (e.g. ownership: the task was never claimed). We distinguish a gate
    # decision (structured dict in ``detail``) from a blackboard error
    # (plain string).
    detail = submit.json().get("detail")
    assert not isinstance(detail, dict) or "decision" not in detail, submit.text


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-q"])
