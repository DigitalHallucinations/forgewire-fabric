"""Runner-kind tagging.

`apply_kind_tag` is the choke point that pins a runner's task-kind
affinity into its register/claim tags. The shell-exec runner must
advertise ``kind:command``; the Copilot-Chat MCP runner must advertise
``kind:agent``. The hub uses that tag (and only that tag) to keep the
two queues disjoint.

These tests pin:
1. The helper itself (default, env override, idempotency, validation).
2. The end-to-end consequence: a runner that registers + claims with
   ``kind:command`` only ever gets ``kind='command'`` tasks, and vice
   versa.
"""

from __future__ import annotations

import secrets
import tempfile
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from forgewire_fabric.hub.server import BlackboardConfig, create_app
from forgewire_fabric.runner.identity import load_or_create
from forgewire_fabric.runner.runner_capabilities import (
    apply_kind_tag,
    resolve_kind_env,
    sign_payload,
)


HUB_TOKEN = "k" * 32
BEARER = {"authorization": f"Bearer {HUB_TOKEN}"}


# ---------------------------------------------------------------- helper unit


def test_apply_kind_tag_appends_default_when_missing() -> None:
    assert apply_kind_tag(["gpu:cuda"], default_kind="command") == [
        "gpu:cuda",
        "kind:command",
    ]


def test_apply_kind_tag_preserves_existing_kind() -> None:
    out = apply_kind_tag(["kind:agent", "rust"], default_kind="command")
    # Canonicalised but kept.
    assert out.count("kind:agent") == 1
    assert "kind:command" not in out
    assert "rust" in out


def test_apply_kind_tag_normalises_equals_form() -> None:
    out = apply_kind_tag(["KIND=Command"], default_kind="agent")
    assert out == ["kind:command"]


def test_apply_kind_tag_env_override_replaces_existing() -> None:
    out = apply_kind_tag(
        ["kind:agent", "gpu:cuda"],
        default_kind="agent",
        env_override="command",
    )
    assert "kind:agent" not in out
    assert "kind:command" in out
    assert "gpu:cuda" in out


def test_apply_kind_tag_rejects_invalid_env_override() -> None:
    with pytest.raises(ValueError):
        apply_kind_tag([], default_kind="agent", env_override="banana")


def test_apply_kind_tag_idempotent() -> None:
    once = apply_kind_tag(["x"], default_kind="command")
    twice = apply_kind_tag(once, default_kind="command")
    assert once == twice


def test_resolve_kind_env_reads_canonical_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FORGEWIRE_RUNNER_KIND", "command")
    monkeypatch.delenv("PHRENFORGE_RUNNER_KIND", raising=False)
    assert resolve_kind_env() == "command"


def test_resolve_kind_env_falls_back_to_legacy_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FORGEWIRE_RUNNER_KIND", raising=False)
    monkeypatch.setenv("PHRENFORGE_RUNNER_KIND", "agent")
    assert resolve_kind_env() == "agent"


# ---------------------------------------------------------------- e2e routing


def _build_client() -> TestClient:
    tmp = Path(tempfile.mkdtemp(prefix="fw-runnerkind-"))
    cfg = BlackboardConfig(
        db_path=tmp / "blackboard.db",
        token=HUB_TOKEN,
        host="127.0.0.1",
        port=0,
    )
    return TestClient(create_app(cfg))


def _register(client: TestClient, ident, *, tags: list[str]) -> None:
    ts = int(time.time())
    nonce = secrets.token_hex(16)
    signed = {
        "op": "register",
        "runner_id": ident.runner_id,
        "public_key": ident.public_key_hex,
        "protocol_version": 3,
        "timestamp": ts,
        "nonce": nonce,
    }
    sig = sign_payload(ident, signed)
    payload = {
        "runner_id": ident.runner_id,
        "public_key": ident.public_key_hex,
        "protocol_version": 3,
        "runner_version": "0.10.0",
        "hostname": f"host-{ident.runner_id[:8]}",
        "os": "test-os",
        "arch": "x86_64",
        "tools": [],
        "tags": tags,
        "scope_prefixes": [],
        "metadata": {},
        "capabilities": {},
        "timestamp": ts,
        "nonce": nonce,
        "signature": sig,
    }
    r = client.post("/runners/register", json=payload, headers=BEARER)
    assert r.status_code == 200, r.text


def _claim_v2(client: TestClient, ident, *, tags: list[str]) -> tuple[int, dict]:
    ts = int(time.time())
    nonce = secrets.token_hex(16)
    signed = {"op": "claim", "runner_id": ident.runner_id, "timestamp": ts, "nonce": nonce}
    sig = sign_payload(ident, signed)
    payload = {
        "runner_id": ident.runner_id,
        "timestamp": ts,
        "nonce": nonce,
        "signature": sig,
        "scope_prefixes": [],
        "tools": [],
        "tags": tags,
    }
    r = client.post("/tasks/claim-v2", json=payload, headers=BEARER)
    return r.status_code, r.json()


def _dispatch(client: TestClient, *, title: str, kind: str) -> dict:
    body = {
        "title": title,
        "prompt": "noop",
        "scope_globs": ["docs/x.md"],
        "base_commit": "a" * 40,
        "branch": f"feature/{title}",
        "kind": kind,
    }
    r = client.post("/tasks", json=body, headers=BEARER)
    assert r.status_code == 200, r.text
    return r.json()


def test_command_runner_only_claims_command_tasks(tmp_path: Path) -> None:
    """A runner tagged ``kind:command`` must not be handed an
    ``kind='agent'`` task, and must successfully claim its own."""
    client = _build_client()
    cmd_ident = load_or_create(tmp_path / "id-cmd.json")
    cmd_tags = apply_kind_tag([], default_kind="command")
    _register(client, cmd_ident, tags=cmd_tags)

    # Only an agent task is queued -> claim must miss.
    agent_task = _dispatch(client, title="for-agent", kind="agent")
    status, body = _claim_v2(client, cmd_ident, tags=cmd_tags)
    assert status == 200
    assert body.get("task") is None, body

    # Now add a command task -> the command runner picks it up.
    cmd_task = _dispatch(client, title="for-cmd", kind="command")
    status, body = _claim_v2(client, cmd_ident, tags=cmd_tags)
    assert status == 200
    assert body.get("task") is not None, body
    assert int(body["task"]["id"]) == int(cmd_task["id"])
    assert body["task"]["kind"] == "command"

    # The agent task is still queued for an agent runner.
    r = client.get(f"/tasks/{agent_task['id']}", headers=BEARER)
    assert r.status_code == 200
    assert r.json()["status"] == "queued"


def test_agent_runner_only_claims_agent_tasks(tmp_path: Path) -> None:
    client = _build_client()
    ag_ident = load_or_create(tmp_path / "id-agent.json")
    ag_tags = apply_kind_tag([], default_kind="agent")
    _register(client, ag_ident, tags=ag_tags)

    # Only a command task queued -> agent runner misses.
    cmd_task = _dispatch(client, title="cmd-only", kind="command")
    status, body = _claim_v2(client, ag_ident, tags=ag_tags)
    assert status == 200
    assert body.get("task") is None, body

    # Now add an agent task -> the agent runner picks it up.
    ag_task = _dispatch(client, title="ag-only", kind="agent")
    status, body = _claim_v2(client, ag_ident, tags=ag_tags)
    assert status == 200
    assert body.get("task") is not None, body
    assert int(body["task"]["id"]) == int(ag_task["id"])
    assert body["task"]["kind"] == "agent"

    # Command task untouched.
    r = client.get(f"/tasks/{cmd_task['id']}", headers=BEARER)
    assert r.json()["status"] == "queued"
