"""M2.5.5a: sealed secret broker — broker unit tests + HTTP integration.

The broker is *new* code so we cover it from two angles:

* ``test_broker_*`` — direct calls against ``SecretBroker`` over a real
  on-disk SQLite. Verifies seal/open, version bumps, rotation,
  deletion, redaction, and tamper detection.
* ``test_http_*`` and ``test_e2e_*`` — the FastAPI surface
  (``POST/GET/DELETE /secrets``) plus an end-to-end dispatch → claim-v2
  → submit_result round-trip showing secret injection on claim,
  audit-name-only on the chain, and value redaction in
  ``log_tail`` / ``error`` / progress / stream payloads.

Mocking policy: none. Real AES-GCM, real SQLite, real FastAPI app.
"""

from __future__ import annotations

import json
import secrets as _stdlib_secrets
import sqlite3
import tempfile
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from forgewire_fabric.hub.secret_broker import (
    REDACTION_MARKER,
    EnvKeyProvider,
    FileKeyProvider,
    SecretBroker,
)
from forgewire_fabric.hub.server import BlackboardConfig, create_app
from forgewire_fabric.runner.identity import load_or_create
from forgewire_fabric.runner.runner_capabilities import sign_payload


HUB_TOKEN = "z" * 32
BEARER = {"Authorization": f"Bearer {HUB_TOKEN}"}


# ---------------------------------------------------------------- helpers


def _conn(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _now_iso(offset: int = 0) -> str:
    return time.strftime(
        "%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + offset)
    )


def _broker(tmp_path: Path) -> tuple[SecretBroker, Path]:
    db = tmp_path / "secrets.db"
    conn = _conn(db)
    SecretBroker.init_schema(conn)
    conn.commit()
    conn.close()
    key_path = tmp_path / "master.key"
    return SecretBroker(FileKeyProvider(path=key_path)), db


# ---------------------------------------------------------------- unit


def test_broker_put_resolve_roundtrip(tmp_path: Path) -> None:
    broker, db = _broker(tmp_path)
    with _conn(db) as conn:
        broker.put(conn, name="GITHUB_TOKEN", value="ghp_abc123", now_iso=_now_iso())
        conn.commit()
    with _conn(db) as conn:
        resolved = broker.resolve(conn, names=["GITHUB_TOKEN"])
    assert resolved == {"GITHUB_TOKEN": "ghp_abc123"}


def test_broker_put_bumps_version_on_overwrite(tmp_path: Path) -> None:
    broker, db = _broker(tmp_path)
    with _conn(db) as conn:
        broker.put(conn, name="API_KEY", value="v1", now_iso=_now_iso())
        broker.put(conn, name="API_KEY", value="v2", now_iso=_now_iso(1))
        conn.commit()
    with _conn(db) as conn:
        rows = SecretBroker.list_metadata(conn)
        resolved = broker.resolve(conn, names=["API_KEY"])
    assert len(rows) == 1
    assert rows[0]["name"] == "API_KEY"
    assert rows[0]["version"] == 2
    assert resolved["API_KEY"] == "v2"


def test_broker_rotate_requires_existing(tmp_path: Path) -> None:
    broker, db = _broker(tmp_path)
    with _conn(db) as conn:
        with pytest.raises(KeyError):
            broker.rotate(conn, name="NOPE", value="v1", now_iso=_now_iso())


def test_broker_rotate_sets_last_rotated(tmp_path: Path) -> None:
    broker, db = _broker(tmp_path)
    with _conn(db) as conn:
        broker.put(conn, name="ROTATABLE", value="v1", now_iso=_now_iso())
        broker.rotate(conn, name="ROTATABLE", value="v2", now_iso=_now_iso(1))
        conn.commit()
    with _conn(db) as conn:
        rows = SecretBroker.list_metadata(conn)
    assert rows[0]["last_rotated_at"] is not None
    assert rows[0]["version"] == 2


def test_broker_delete_removes_row(tmp_path: Path) -> None:
    broker, db = _broker(tmp_path)
    with _conn(db) as conn:
        broker.put(conn, name="DELME", value="x", now_iso=_now_iso())
        conn.commit()
    with _conn(db) as conn:
        assert broker.delete(conn, name="DELME") is True
        conn.commit()
        assert broker.delete(conn, name="DELME") is False
        assert SecretBroker.list_metadata(conn) == []


def test_broker_resolve_skips_missing_names(tmp_path: Path) -> None:
    broker, db = _broker(tmp_path)
    with _conn(db) as conn:
        broker.put(conn, name="PRESENT", value="v", now_iso=_now_iso())
        conn.commit()
    with _conn(db) as conn:
        out = broker.resolve(conn, names=["PRESENT", "GHOST"])
    assert out == {"PRESENT": "v"}


def test_broker_list_metadata_never_exposes_ciphertext(tmp_path: Path) -> None:
    broker, db = _broker(tmp_path)
    with _conn(db) as conn:
        broker.put(conn, name="SHHH", value="topsecret", now_iso=_now_iso())
        conn.commit()
    with _conn(db) as conn:
        meta = SecretBroker.list_metadata(conn)
    keys = set(meta[0].keys())
    assert "ciphertext" not in keys
    assert {"name", "version", "created_at", "updated_at", "last_rotated_at"} <= keys
    blob = json.dumps(meta).encode("utf-8")
    assert b"topsecret" not in blob


def test_broker_tampered_ciphertext_raises(tmp_path: Path) -> None:
    broker, db = _broker(tmp_path)
    with _conn(db) as conn:
        broker.put(conn, name="GUARDED", value="hunter2", now_iso=_now_iso())
        conn.commit()
    # Flip a byte in the stored ciphertext (past the 12-byte nonce so we
    # corrupt the ciphertext+tag and force an AES-GCM verification fail).
    # Ciphertext is stored base64-encoded for rqlite wire compatibility.
    import base64 as _b64
    with _conn(db) as conn:
        row = conn.execute("SELECT ciphertext FROM secrets WHERE name = ?", ("GUARDED",)).fetchone()
        blob = bytearray(_b64.b64decode(row["ciphertext"]))
        blob[15] ^= 0xFF
        tampered = _b64.b64encode(bytes(blob)).decode("ascii")
        conn.execute("UPDATE secrets SET ciphertext = ? WHERE name = ?", (tampered, "GUARDED"))
        conn.commit()
    with _conn(db) as conn:
        with pytest.raises(PermissionError):
            broker.resolve(conn, names=["GUARDED"])


def test_broker_wrong_master_key_fails_decrypt(tmp_path: Path) -> None:
    broker, db = _broker(tmp_path)
    with _conn(db) as conn:
        broker.put(conn, name="ENC", value="value", now_iso=_now_iso())
        conn.commit()
    # New broker pointed at a fresh, unrelated key file.
    other = SecretBroker(FileKeyProvider(path=tmp_path / "other.key"))
    with _conn(db) as conn:
        with pytest.raises(PermissionError):
            other.resolve(conn, names=["ENC"])


def test_broker_redact_replaces_values_and_skips_when_empty(tmp_path: Path) -> None:
    broker, db = _broker(tmp_path)
    with _conn(db) as conn:
        broker.put(conn, name="TOK", value="shhh-9999", now_iso=_now_iso())
        broker.put(conn, name="SHORT", value="abc", now_iso=_now_iso(1))
        conn.commit()

    def factory() -> sqlite3.Connection:
        return _conn(db)

    out = broker.redact("log: shhh-9999 leaked; also abc", conn_factory=factory)
    assert "shhh-9999" not in out
    assert REDACTION_MARKER.format(name="TOK") in out
    assert REDACTION_MARKER.format(name="SHORT") in out
    # None and empty pass through.
    assert broker.redact(None, conn_factory=factory) is None
    assert broker.redact("", conn_factory=factory) == ""


def test_broker_redact_invalidated_after_rotation(tmp_path: Path) -> None:
    broker, db = _broker(tmp_path)

    def factory() -> sqlite3.Connection:
        return _conn(db)

    with _conn(db) as conn:
        broker.put(conn, name="ROT", value="old-value", now_iso=_now_iso())
        conn.commit()
    assert "old-value" not in (broker.redact("contains old-value", conn_factory=factory) or "")
    with _conn(db) as conn:
        broker.rotate(conn, name="ROT", value="new-value", now_iso=_now_iso(1))
        conn.commit()
    # rotate() should have dropped the cache; new value redacts, old
    # value no longer matches.
    out = broker.redact("contains new-value and old-value", conn_factory=factory)
    assert "new-value" not in out
    assert "old-value" in out  # old value is no longer in the store


def test_env_key_provider_validates_hex_length() -> None:
    import os
    p = EnvKeyProvider(env_var="FW_TEST_KEY")
    os.environ["FW_TEST_KEY"] = "ab" * 32  # 64 hex chars = 32 bytes
    try:
        assert len(p.load()) == 32
        os.environ["FW_TEST_KEY"] = "deadbeef"
        with pytest.raises(ValueError):
            p.load()
    finally:
        os.environ.pop("FW_TEST_KEY", None)


# ---------------------------------------------------------------- http


def _build_client() -> tuple[TestClient, Path]:
    tmp = Path(tempfile.mkdtemp(prefix="fw-sec-"))
    cfg = BlackboardConfig(
        db_path=tmp / "blackboard.db",
        token=HUB_TOKEN,
        host="127.0.0.1",
        port=0,
    )
    return TestClient(create_app(cfg)), tmp


def test_http_put_creates_then_rotates(tmp_path: Path) -> None:
    client, _ = _build_client()
    r = client.post(
        "/secrets",
        json={"name": "GITHUB_TOKEN", "value": "ghp_one"},
        headers=BEARER,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["rotated"] is False
    assert body["secret"]["name"] == "GITHUB_TOKEN"
    assert body["secret"]["version"] == 1

    r = client.post(
        "/secrets",
        json={"name": "GITHUB_TOKEN", "value": "ghp_two"},
        headers=BEARER,
    )
    body = r.json()
    assert body["rotated"] is True
    assert body["secret"]["version"] == 2


def test_http_put_rejects_bad_name() -> None:
    client, _ = _build_client()
    # lowercase + dot — fails the pattern
    r = client.post(
        "/secrets",
        json={"name": "lower.bad", "value": "x"},
        headers=BEARER,
    )
    assert r.status_code == 422


def test_http_put_requires_auth() -> None:
    client, _ = _build_client()
    r = client.post("/secrets", json={"name": "X", "value": "y"})
    assert r.status_code in (401, 403)


def test_http_list_returns_metadata_only() -> None:
    client, _ = _build_client()
    client.post(
        "/secrets",
        json={"name": "API_KEY", "value": "do-not-leak"},
        headers=BEARER,
    )
    r = client.get("/secrets", headers=BEARER)
    assert r.status_code == 200
    payload = r.json()
    assert "secrets" in payload
    blob = json.dumps(payload).encode("utf-8")
    assert b"do-not-leak" not in blob


def test_http_delete_missing_is_404() -> None:
    client, _ = _build_client()
    r = client.delete("/secrets/NEVER", headers=BEARER)
    assert r.status_code == 404


def test_http_delete_happy_path() -> None:
    client, _ = _build_client()
    client.post(
        "/secrets",
        json={"name": "DOOMED", "value": "ttl-zero"},
        headers=BEARER,
    )
    r = client.delete("/secrets/DOOMED", headers=BEARER)
    assert r.status_code == 200
    assert r.json()["deleted"] is True
    listing = client.get("/secrets", headers=BEARER).json()["secrets"]
    assert all(s["name"] != "DOOMED" for s in listing)


# ---------------------------------------------------------------- e2e


def _register(client: TestClient, ident) -> None:
    ts = int(time.time())
    nonce = _stdlib_secrets.token_hex(16)
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
        "runner_version": "0.11.0",
        "hostname": f"host-{ident.runner_id[:8]}",
        "os": "test-os",
        "arch": "x86_64",
        "tools": [],
        "tags": [],
        "scope_prefixes": [],
        "metadata": {},
        "capabilities": {},
        "timestamp": ts,
        "nonce": nonce,
        "signature": sig,
    }
    r = client.post("/runners/register", json=payload, headers=BEARER)
    assert r.status_code == 200, r.text


def _claim_v2(client: TestClient, ident) -> dict:
    ts = int(time.time())
    nonce = _stdlib_secrets.token_hex(16)
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
    assert r.status_code == 200, r.text
    return r.json()


def test_e2e_dispatch_claim_injects_secrets_and_audits_names_only(
    tmp_path: Path,
) -> None:
    client, _ = _build_client()
    ident = load_or_create(tmp_path / "id.json")
    _register(client, ident)
    # Plant the secret.
    client.post(
        "/secrets",
        json={"name": "GITHUB_TOKEN", "value": "ghp_e2e_value"},
        headers=BEARER,
    )
    # Dispatch a task that declares secrets_needed.
    dispatch_body = {
        "title": "secret-claim",
        "prompt": "noop",
        "scope_globs": ["docs/x.md"],
        "base_commit": "a" * 40,
        "branch": "feature/secret-claim",
        "secrets_needed": ["GITHUB_TOKEN"],
    }
    task = client.post("/tasks", json=dispatch_body, headers=BEARER).json()

    claim = _claim_v2(client, ident)
    assert claim["task"] is not None
    assert claim["task"]["id"] == task["id"]
    # Secret value lands in the claim response and only there.
    assert claim["task"]["secrets"] == {"GITHUB_TOKEN": "ghp_e2e_value"}

    # Audit chain: dispatch records the names, claim records dispatched
    # names; nowhere does the value appear.
    audit = client.get(f"/audit/tasks/{task['id']}", headers=BEARER).json()
    assert audit["verified"] is True, audit["error"]
    kinds = [ev["kind"] for ev in audit["events"]]
    assert "dispatch" in kinds and "claim" in kinds
    dispatch_ev = next(ev for ev in audit["events"] if ev["kind"] == "dispatch")
    claim_ev = next(ev for ev in audit["events"] if ev["kind"] == "claim")
    assert dispatch_ev["payload"]["secrets_needed"] == ["GITHUB_TOKEN"]
    assert claim_ev["payload"]["secrets_dispatched"] == ["GITHUB_TOKEN"]
    assert b"ghp_e2e_value" not in json.dumps(audit).encode("utf-8")


def test_e2e_submit_result_redacts_secret_value(tmp_path: Path) -> None:
    client, _ = _build_client()
    ident = load_or_create(tmp_path / "id-redact.json")
    _register(client, ident)
    client.post(
        "/secrets",
        json={"name": "OPENAI_API_KEY", "value": "sk-leakvalue123"},
        headers=BEARER,
    )
    dispatch_body = {
        "title": "redact-me",
        "prompt": "noop",
        "scope_globs": ["docs/y.md"],
        "base_commit": "b" * 40,
        "branch": "feature/redact-me",
        "secrets_needed": ["OPENAI_API_KEY"],
    }
    task = client.post("/tasks", json=dispatch_body, headers=BEARER).json()
    _claim_v2(client, ident)

    # Runner submits a result whose log_tail + error contain the secret
    # value. The hub must redact both before persisting.
    result_body = {
        "worker_id": ident.runner_id,
        "status": "done",
        "log_tail": "boot ok. token=sk-leakvalue123 done.",
        "error": "trace: sk-leakvalue123 surfaced in error",
        "head_commit": "c" * 40,
        "commits": ["c" * 40],
        "files_touched": ["docs/y.md"],
    }
    r = client.post(f"/tasks/{task['id']}/result", json=result_body, headers=BEARER)
    assert r.status_code == 200, r.text
    persisted = r.json()
    log_tail = persisted["result"]["log_tail"]
    error = persisted["result"]["error"]
    assert "sk-leakvalue123" not in log_tail
    assert "sk-leakvalue123" not in error
    assert REDACTION_MARKER.format(name="OPENAI_API_KEY") in log_tail
    assert REDACTION_MARKER.format(name="OPENAI_API_KEY") in error


def test_e2e_progress_and_stream_redact_secret_value(tmp_path: Path) -> None:
    client, _ = _build_client()
    ident = load_or_create(tmp_path / "id-progress.json")
    _register(client, ident)
    client.post(
        "/secrets",
        json={"name": "DB_PASS", "value": "pa55w0rd-leak"},
        headers=BEARER,
    )
    dispatch_body = {
        "title": "redact-stream",
        "prompt": "noop",
        "scope_globs": ["docs/z.md"],
        "base_commit": "d" * 40,
        "branch": "feature/redact-stream",
        "secrets_needed": ["DB_PASS"],
    }
    task = client.post("/tasks", json=dispatch_body, headers=BEARER).json()
    _claim_v2(client, ident)

    # Progress
    pr = client.post(
        f"/tasks/{task['id']}/progress",
        json={
            "worker_id": ident.runner_id,
            "message": "step 1 used pa55w0rd-leak",
        },
        headers=BEARER,
    )
    assert pr.status_code == 200, pr.text

    # Stream single
    sr = client.post(
        f"/tasks/{task['id']}/stream",
        json={
            "worker_id": ident.runner_id,
            "channel": "stdout",
            "line": "connect with pa55w0rd-leak",
        },
        headers=BEARER,
    )
    assert sr.status_code == 200, sr.text

    # Stream bulk
    br = client.post(
        f"/tasks/{task['id']}/stream/bulk",
        json={
            "worker_id": ident.runner_id,
            "entries": [
                {"channel": "stderr", "line": "leak1 pa55w0rd-leak"},
                {"channel": "stdout", "line": "leak2 pa55w0rd-leak"},
            ],
        },
        headers=BEARER,
    )
    assert br.status_code == 200, br.text

    # Pull progress + stream back and confirm redaction.
    fetched = client.get(f"/tasks/{task['id']}", headers=BEARER).json()
    progress_blob = json.dumps(fetched.get("progress") or []).encode("utf-8")
    assert b"pa55w0rd-leak" not in progress_blob

    stream = client.get(
        f"/tasks/{task['id']}/stream", headers=BEARER
    ).json()
    stream_blob = json.dumps(stream).encode("utf-8")
    assert b"pa55w0rd-leak" not in stream_blob
    assert REDACTION_MARKER.format(name="DB_PASS").encode("utf-8") in stream_blob
