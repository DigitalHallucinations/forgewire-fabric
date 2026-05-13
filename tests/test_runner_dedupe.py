"""Regression: one physical host must map to exactly one runner row.

Covers two structural defenses that together make the "duplicate runner
per host" symptom impossible:

1. The runner ``DEFAULT_IDENTITY_PATH`` is machine-wide (under
   ``%PROGRAMDATA%`` on Windows or ``/var/lib/forgewire`` on POSIX), so
   the same host produces the same ``runner_id`` regardless of which OS
   user starts the runner (NSSM ``LocalSystem`` service vs interactive
   ``forgewire-fabric runner start`` under a logged-in user).
2. ``Blackboard.upsert_runner`` prunes any ghost ``runners`` row from the
   same ``hostname`` whose ``last_heartbeat`` is older than
   ``HEARTBEAT_OFFLINE_SECONDS`` before inserting the new registration,
   so a stale identity-rotation cannot leave a phantom entry behind.
"""

from __future__ import annotations

import importlib
import json
import os
import secrets
import sys
import time
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from forgewire_fabric.hub.server import (
    BlackboardConfig,
    HEARTBEAT_OFFLINE_SECONDS,
    create_app,
)
from forgewire_fabric.runner import identity as identity_mod
from forgewire_fabric.runner.identity import load_or_create
from forgewire_fabric.runner.runner_capabilities import sign_payload


HUB_TOKEN = "test-hub-token-aaaaaaaaaaaaaaaaa"


def _auth() -> dict[str, str]:
    return {"authorization": f"Bearer {HUB_TOKEN}"}


def _make_app(tmp_path: Path):
    cfg = BlackboardConfig(
        db_path=tmp_path / "hub.sqlite3",
        token=HUB_TOKEN,
        host="127.0.0.1",
        port=0,
    )
    return create_app(cfg)


def _register(
    client: TestClient,
    ident,
    *,
    hostname: str,
    ts: int | None = None,
) -> None:
    ts = ts if ts is not None else int(time.time())
    nonce = secrets.token_hex(16)
    body = {
        "op": "register",
        "runner_id": ident.runner_id,
        "public_key": ident.public_key_hex,
        "protocol_version": 2,
        "timestamp": ts,
        "nonce": nonce,
    }
    sig = sign_payload(ident, body)
    payload: dict[str, Any] = {
        "runner_id": ident.runner_id,
        "public_key": ident.public_key_hex,
        "protocol_version": 2,
        "runner_version": "0.11.0",
        "hostname": hostname,
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


# ---------------------------------------------------------------------------
# Hub-side dedupe on register
# ---------------------------------------------------------------------------


def _backdate_heartbeat(db_path: Path, runner_id: str, seconds_ago: int) -> None:
    """Force a runner row's ``last_heartbeat`` into the past."""
    import sqlite3

    iso = time.strftime(
        "%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - seconds_ago)
    )
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE runners SET last_heartbeat = ? WHERE runner_id = ?",
            (iso, runner_id),
        )
        conn.commit()
    finally:
        conn.close()


def test_register_prunes_stale_same_host_runner(tmp_path: Path) -> None:
    app = _make_app(tmp_path)
    db_path = tmp_path / "hub.sqlite3"
    ghost = load_or_create(tmp_path / "ghost.json")
    fresh = load_or_create(tmp_path / "fresh.json")
    assert ghost.runner_id != fresh.runner_id

    with TestClient(app) as c:
        _register(c, ghost, hostname="HOST-A")
        # Backdate the ghost's heartbeat past the offline threshold.
        _backdate_heartbeat(db_path, ghost.runner_id, HEARTBEAT_OFFLINE_SECONDS + 30)

        _register(c, fresh, hostname="HOST-A")

        r = c.get("/runners", headers=_auth())
        assert r.status_code == 200
        runners = r.json()["runners"]
        ids = {row["runner_id"] for row in runners}
        # Ghost row from same hostname is gone; only the fresh one survives.
        assert ghost.runner_id not in ids
        assert fresh.runner_id in ids


def test_register_keeps_fresh_same_host_runner(tmp_path: Path) -> None:
    """A still-heartbeating runner on the same hostname must NOT be pruned;
    only stale ghosts are eligible for cleanup.
    """
    app = _make_app(tmp_path)
    other = load_or_create(tmp_path / "other.json")
    new = load_or_create(tmp_path / "new.json")
    assert other.runner_id != new.runner_id

    with TestClient(app) as c:
        _register(c, other, hostname="HOST-A")
        # No backdating: other is fresh.
        _register(c, new, hostname="HOST-A")

        r = c.get("/runners", headers=_auth())
        ids = {row["runner_id"] for row in r.json()["runners"]}
        assert other.runner_id in ids
        assert new.runner_id in ids


def test_register_does_not_prune_different_host(tmp_path: Path) -> None:
    app = _make_app(tmp_path)
    db_path = tmp_path / "hub.sqlite3"
    a = load_or_create(tmp_path / "a.json")
    b = load_or_create(tmp_path / "b.json")

    with TestClient(app) as c:
        _register(c, a, hostname="HOST-A")
        _backdate_heartbeat(db_path, a.runner_id, HEARTBEAT_OFFLINE_SECONDS + 30)
        _register(c, b, hostname="HOST-B")

        r = c.get("/runners", headers=_auth())
        ids = {row["runner_id"] for row in r.json()["runners"]}
        # Stale runner on HOST-A is untouched because the new registrant
        # came from a different hostname.
        assert a.runner_id in ids
        assert b.runner_id in ids


# ---------------------------------------------------------------------------
# Machine-wide identity path
# ---------------------------------------------------------------------------


def _reload_identity_module(monkeypatch: pytest.MonkeyPatch, override: Path) -> Any:
    """Reload ``identity`` so ``DEFAULT_IDENTITY_PATH`` picks up the env."""
    monkeypatch.setenv("FORGEWIRE_RUNNER_IDENTITY_PATH", str(override))
    return importlib.reload(identity_mod)


def test_default_identity_path_honors_env_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    override = tmp_path / "machine" / "runner_identity.json"
    mod = _reload_identity_module(monkeypatch, override)
    try:
        assert override == mod.DEFAULT_IDENTITY_PATH
        ident = mod.load_or_create()
        assert override.exists()
        again = mod.load_or_create()
        assert again.runner_id == ident.runner_id
    finally:
        monkeypatch.delenv("FORGEWIRE_RUNNER_IDENTITY_PATH", raising=False)
        importlib.reload(identity_mod)


def test_default_identity_path_is_machine_scoped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On Windows the default must live under ``%PROGRAMDATA%`` — never
    under a per-user home — so NSSM ``LocalSystem`` and an interactive user
    resolve to the same file.
    """
    monkeypatch.delenv("FORGEWIRE_RUNNER_IDENTITY_PATH", raising=False)
    mod = importlib.reload(identity_mod)
    try:
        path = mod.DEFAULT_IDENTITY_PATH
        if sys.platform == "win32":
            program_data = os.environ.get("PROGRAMDATA") or r"C:\ProgramData"
            assert str(path).lower().startswith(program_data.lower())
            # Must not be under any user home.
            assert "users" not in str(path).lower().split(os.sep)
        else:
            assert str(path).startswith(("/var/lib/forgewire", "/etc/forgewire"))
    finally:
        importlib.reload(identity_mod)


def _make_identity_payload(runner_id: str | None = None) -> dict[str, str]:
    """Forge a structurally-valid identity record (matched keypair)."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    sk = Ed25519PrivateKey.generate()
    sk_hex = sk.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    ).hex()
    pk_hex = sk.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    ).hex()
    import uuid as _uuid

    return {
        "runner_id": runner_id or str(_uuid.uuid4()),
        "public_key": pk_hex,
        "private_key": sk_hex,
        "created_at": "2026-01-01T00:00:00Z",
    }


def test_load_or_create_migrates_legacy_user_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A pre-existing per-user identity must be migrated into the
    machine-wide path so the ``runner_id`` is preserved across the upgrade.
    """
    machine_path = tmp_path / "machine" / "runner_identity.json"
    legacy_path = tmp_path / "user_home" / ".forgewire" / "runner_identity.json"
    legacy_path.parent.mkdir(parents=True)
    legacy_payload = _make_identity_payload(
        "11111111-2222-3333-4444-555555555555"
    )
    legacy_path.write_text(json.dumps(legacy_payload), encoding="utf-8")

    monkeypatch.setenv("FORGEWIRE_RUNNER_IDENTITY_PATH", str(machine_path))
    mod = importlib.reload(identity_mod)
    try:
        # Point the legacy lookup at our temp user home.
        monkeypatch.setattr(mod, "_LEGACY_USER_IDENTITY_PATH", legacy_path)
        ident = mod.load_or_create()
        assert ident.runner_id == legacy_payload["runner_id"]
        assert machine_path.exists()
        # Machine-wide file is now seeded with the migrated content.
        migrated = json.loads(machine_path.read_text(encoding="utf-8"))
        assert migrated["runner_id"] == legacy_payload["runner_id"]
    finally:
        monkeypatch.delenv("FORGEWIRE_RUNNER_IDENTITY_PATH", raising=False)
        importlib.reload(identity_mod)


# ---------------------------------------------------------------------------
# Cross-machine migration (export / import)
# ---------------------------------------------------------------------------


def test_export_import_round_trip_preserves_runner_id(tmp_path: Path) -> None:
    """The export/import pair is how operators preserve a runner_id when
    physically replacing a host. The round-trip MUST be lossless.
    """
    src_path = tmp_path / "src.json"
    src_ident = identity_mod.load_or_create(src_path)

    exported = tmp_path / "exported.json"
    record = identity_mod.export_identity(exported, source=src_path)
    assert exported.exists()
    assert record["runner_id"] == src_ident.runner_id

    dst_path = tmp_path / "dst.json"
    imported = identity_mod.import_identity(exported, target=dst_path)
    assert imported.runner_id == src_ident.runner_id
    assert imported.public_key_hex == src_ident.public_key_hex


def test_import_refuses_to_overwrite_different_runner_id(tmp_path: Path) -> None:
    src_path = tmp_path / "src.json"
    dst_path = tmp_path / "dst.json"
    src_ident = identity_mod.load_or_create(src_path)
    dst_ident = identity_mod.load_or_create(dst_path)
    assert src_ident.runner_id != dst_ident.runner_id

    with pytest.raises(RuntimeError, match="refusing to overwrite"):
        identity_mod.import_identity(src_path, target=dst_path)

    # Existing identity untouched.
    still = identity_mod.load_or_create(dst_path)
    assert still.runner_id == dst_ident.runner_id


def test_import_force_overwrites_different_runner_id(tmp_path: Path) -> None:
    src_path = tmp_path / "src.json"
    dst_path = tmp_path / "dst.json"
    src_ident = identity_mod.load_or_create(src_path)
    identity_mod.load_or_create(dst_path)

    imported = identity_mod.import_identity(src_path, target=dst_path, force=True)
    assert imported.runner_id == src_ident.runner_id


def test_import_is_idempotent_for_same_runner_id(tmp_path: Path) -> None:
    src_path = tmp_path / "src.json"
    dst_path = tmp_path / "dst.json"
    identity_mod.load_or_create(src_path)
    # First import seeds dst.
    identity_mod.import_identity(src_path, target=dst_path)
    # Re-importing the same identity is a no-op-style success.
    identity_mod.import_identity(src_path, target=dst_path)


def test_import_rejects_mismatched_keypair(tmp_path: Path) -> None:
    """A tampered or hand-crafted identity file whose private key does not
    derive its public key is rejected outright.
    """
    bad_path = tmp_path / "bad.json"
    payload = _make_identity_payload()
    payload["public_key"] = "0" * 64
    bad_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="public_key does not match"):
        identity_mod.import_identity(bad_path, target=tmp_path / "dst.json")


def test_import_rejects_missing_fields(tmp_path: Path) -> None:
    bad_path = tmp_path / "bad.json"
    bad_path.write_text(json.dumps({"runner_id": "abc"}), encoding="utf-8")
    with pytest.raises(ValueError, match="missing required fields"):
        identity_mod.import_identity(bad_path, target=tmp_path / "dst.json")


def test_imported_identity_round_trips_through_register(tmp_path: Path) -> None:
    """End-to-end: a runner_id carried from machine A to machine B via
    export/import registers cleanly against the hub and the resulting row
    matches the imported id.
    """
    src_path = tmp_path / "src.json"
    dst_path = tmp_path / "dst.json"
    src_ident = identity_mod.load_or_create(src_path)
    identity_mod.export_identity(tmp_path / "carry.json", source=src_path)
    dst_ident = identity_mod.import_identity(
        tmp_path / "carry.json", target=dst_path
    )
    assert dst_ident.runner_id == src_ident.runner_id

    app = _make_app(tmp_path)
    with TestClient(app) as c:
        _register(c, dst_ident, hostname="NEW-HARDWARE")
        r = c.get("/runners", headers=_auth())
        runners = r.json()["runners"]
        assert {row["runner_id"] for row in runners} == {src_ident.runner_id}


# ---------------------------------------------------------------------------
# Install-time bootstrap
# ---------------------------------------------------------------------------


def test_ensure_identity_dir_creates_machine_wide_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "fresh-machine" / "forgewire" / "runner_identity.json"
    monkeypatch.setenv("FORGEWIRE_RUNNER_IDENTITY_PATH", str(target))
    mod = importlib.reload(identity_mod)
    try:
        result = mod.ensure_identity_dir()
        assert result == target.parent
        assert target.parent.is_dir()
        # Idempotent.
        mod.ensure_identity_dir()
        assert target.parent.is_dir()
    finally:
        monkeypatch.delenv("FORGEWIRE_RUNNER_IDENTITY_PATH", raising=False)
        importlib.reload(identity_mod)

