"""M2.5.4: capability matcher + smart routing.

Two layers:

* ``test_matcher_*`` — pure unit tests for the parser/evaluator.
* ``test_route_*`` — end-to-end against the hub: register two runners
  with different capability blobs, dispatch a task with
  ``required_capabilities``, verify the right runner claims it and the
  wrong one gets ``waiting_for_capability``.

Mocking policy: none. Real hub, real on-disk SQLite.
"""

from __future__ import annotations

import secrets
import tempfile
import time
from pathlib import Path

from fastapi.testclient import TestClient

from forgewire_fabric.hub.capability_matcher import match
from forgewire_fabric.hub.server import BlackboardConfig, create_app
from forgewire_fabric.runner.identity import load_or_create
from forgewire_fabric.runner.runner_capabilities import sign_payload


HUB_TOKEN = "x" * 32
BEARER = {"Authorization": f"Bearer {HUB_TOKEN}"}


# ---------------------------------------------------------------- matcher


def test_matcher_presence_predicate() -> None:
    ok, missing = match(["toolchains.rust"], {"toolchains": {"rust": True, "node": True}})
    assert ok and missing == []
    ok, missing = match(["toolchains.go"], {"toolchains": {"rust": True}})
    assert not ok and any("go" in m for m in missing)


def test_matcher_list_membership_shorthand() -> None:
    caps = {"toolchains": ["rust", "node"]}
    ok, _ = match(["toolchains.rust"], caps)
    assert ok
    ok, missing = match(["toolchains.go"], caps)
    assert not ok and "go" in " ".join(missing)


def test_matcher_version_comparisons() -> None:
    caps = {"python": "3.13.1", "ram_gb": 64, "cpu": {"cores": 16}}
    ok, _ = match(["python ~= 3.12", "ram_gb >= 32", "cpu.cores >= 8"], caps)
    assert ok
    ok, missing = match(["python ~= 3.14"], caps)
    assert not ok
    ok, missing = match(["ram_gb >= 128"], caps)
    assert not ok and "ram_gb" in missing[0]


def test_matcher_compatible_release_rejects_major_jump() -> None:
    ok, _ = match(["python ~= 3.12"], {"python": "4.0.0"})
    assert not ok


def test_matcher_equality_and_string() -> None:
    caps = {"os": "windows-11", "region": "homelab"}
    ok, _ = match(['os == "windows-11"', "region == homelab"], caps)
    assert ok
    ok, _ = match(['os == "linux"'], caps)
    assert not ok


def test_matcher_substring_subkey_on_gpu_label() -> None:
    caps = {"gpu": "nvidia:cuda:12.4"}
    ok, _ = match(["gpu.cuda >= 12"], caps)
    assert ok
    ok, _ = match(["gpu.cuda >= 13"], caps)
    assert not ok


def test_matcher_empty_required_passes_anything() -> None:
    ok, missing = match([], {"anything": "goes"})
    assert ok and missing == []


# ------------------------------------------------------------ live routing


def _build_client() -> TestClient:
    tmp = Path(tempfile.mkdtemp(prefix="fw-cap-"))
    cfg = BlackboardConfig(
        db_path=tmp / "blackboard.db", token=HUB_TOKEN, host="127.0.0.1", port=0,
    )
    return TestClient(create_app(cfg))


def _register(client: TestClient, ident, *, capabilities: dict) -> None:
    ts = int(time.time())
    nonce = secrets.token_hex(16)
    body = {
        "op": "register",
        "runner_id": ident.runner_id,
        "public_key": ident.public_key_hex,
        "protocol_version": 3,
        "timestamp": ts,
        "nonce": nonce,
    }
    sig = sign_payload(ident, body)
    payload = {
        "runner_id": ident.runner_id,
        "public_key": ident.public_key_hex,
        "protocol_version": 3,
        "runner_version": "0.10.0",
        "hostname": f"host-{ident.runner_id[:8]}",
        "os": "test-os",
        "arch": "x86_64",
        "tools": [],
        "tags": [],
        "scope_prefixes": [],
        "metadata": {},
        "capabilities": capabilities,
        "timestamp": ts,
        "nonce": nonce,
        "signature": sig,
    }
    r = client.post("/runners/register", json=payload, headers=BEARER)
    assert r.status_code == 200, r.text


def _claim_v2(client: TestClient, ident) -> tuple[int, dict]:
    ts = int(time.time())
    nonce = secrets.token_hex(16)
    body = {"op": "claim", "runner_id": ident.runner_id, "timestamp": ts, "nonce": nonce}
    sig = sign_payload(ident, body)
    payload = {
        "runner_id": ident.runner_id,
        "timestamp": ts,
        "nonce": nonce,
        "signature": sig,
        "scope_prefixes": [],
        "tools": [],
        "tags": [],
    }
    r = client.post("/tasks/claim-v2", json=payload, headers=BEARER)
    return r.status_code, r.json()


def _dispatch(client: TestClient, *, required_capabilities: list[str]) -> dict:
    body = {
        "title": "cap-routed",
        "prompt": "noop",
        "scope_globs": ["docs/x.md"],
        "base_commit": "a" * 40,
        "branch": "feature/cap-route",
        "required_capabilities": required_capabilities,
    }
    r = client.post("/tasks", json=body, headers=BEARER)
    assert r.status_code == 200, r.text
    return r.json()


def test_route_capable_runner_claims_task(tmp_path: Path) -> None:
    client = _build_client()
    cap_ident = load_or_create(tmp_path / "id-cap.json")
    weak_ident = load_or_create(tmp_path / "id-weak.json")
    _register(client, cap_ident, capabilities={"toolchains": {"rust": True}, "ram_gb": 64})
    _register(client, weak_ident, capabilities={"toolchains": {"node": True}, "ram_gb": 8})
    task = _dispatch(client, required_capabilities=["toolchains.rust", "ram_gb >= 32"])

    # Weak runner sees waiting_for_capability.
    status, body = _claim_v2(client, weak_ident)
    assert status == 200, body
    assert body.get("task") is None
    assert body["info"]["reason"] == "waiting_for_capability"
    miss_paths = " ".join(
        m for entry in body["info"]["missing"] for m in entry["missing"]
    )
    assert "rust" in miss_paths or "ram_gb" in miss_paths

    # Capable runner claims it.
    status, body = _claim_v2(client, cap_ident)
    assert status == 200, body
    assert body["task"] is not None
    assert body["task"]["id"] == task["id"]


def test_route_no_match_when_no_runner_qualifies(tmp_path: Path) -> None:
    client = _build_client()
    weak_ident = load_or_create(tmp_path / "id-weak2.json")
    _register(client, weak_ident, capabilities={"toolchains": {"node": True}, "ram_gb": 4})
    task = _dispatch(client, required_capabilities=["toolchains.rust"])

    status, body = _claim_v2(client, weak_ident)
    assert status == 200
    assert body.get("task") is None
    assert body["info"]["reason"] == "waiting_for_capability"

    waiting = client.get("/tasks/waiting", headers=BEARER).json()
    waiting_ids = [t["task_id"] for t in waiting["tasks"]]
    assert task["id"] in waiting_ids
    entry = [t for t in waiting["tasks"] if t["task_id"] == task["id"]][0]
    assert weak_ident.runner_id in entry["missing_per_runner"]


def test_route_legacy_claim_skips_capability_gated_tasks(tmp_path: Path) -> None:
    """Legacy ``/tasks/claim`` (worker_id, no signed identity) must not
    pick up tasks with ``required_capabilities`` because it has no
    capability blob to evaluate against. The hub keeps it queued."""
    client = _build_client()
    _dispatch(client, required_capabilities=["toolchains.rust"])
    # Plain task that any worker can take.
    body = {
        "title": "plain", "prompt": "noop", "scope_globs": ["docs/y.md"],
        "base_commit": "b" * 40, "branch": "feature/plain",
    }
    plain = client.post("/tasks", json=body, headers=BEARER).json()

    r = client.post(
        "/tasks/claim",
        json={"worker_id": "legacy-1", "hostname": "h", "capabilities": {}},
        headers=BEARER,
    )
    assert r.status_code == 200, r.text
    claimed = r.json()["task"]
    assert claimed is not None
    # Legacy worker must have grabbed the plain task, not the rust-gated one.
    assert claimed["id"] == plain["id"]


def test_waiting_endpoint_omits_satisfiable_tasks(tmp_path: Path) -> None:
    client = _build_client()
    cap_ident = load_or_create(tmp_path / "id-cap-w.json")
    _register(client, cap_ident, capabilities={"toolchains": {"rust": True}, "ram_gb": 64})
    _dispatch(client, required_capabilities=["toolchains.rust"])
    waiting = client.get("/tasks/waiting", headers=BEARER).json()
    assert waiting["tasks"] == []
