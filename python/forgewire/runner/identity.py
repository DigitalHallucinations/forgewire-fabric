"""Persistent runner identity (UUID + ed25519 keypair).

The runner generates this once on first startup and persists it at
``~/.phrenforge/runner_identity.json``. The hub stores the public key on
``/runners/register`` and verifies signed payloads on every state-changing
runner call (registration, heartbeat, drain ack).

File format::

    {
        "runner_id":  "<uuid4 lowercase hex with dashes>",
        "public_key": "<32-byte hex>",
        "private_key": "<32-byte hex>",
        "created_at": "<iso8601 utc>"
    }

The file is written 0600 on POSIX. On Windows we fall back to default ACLs
because chmod is a no-op there; the file lives under ``%USERPROFILE%`` which
is per-user already.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)


DEFAULT_IDENTITY_PATH = Path.home() / ".phrenforge" / "runner_identity.json"


@dataclass(frozen=True, slots=True)
class RunnerIdentity:
    runner_id: str
    public_key_hex: str
    _private_key_hex: str

    @property
    def public_key(self) -> Ed25519PublicKey:
        return Ed25519PublicKey.from_public_bytes(bytes.fromhex(self.public_key_hex))

    def sign(self, payload: bytes) -> str:
        sk = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(self._private_key_hex))
        return sk.sign(payload).hex()


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_or_create(path: Path | None = None) -> RunnerIdentity:
    """Return the persisted identity, creating it on first use."""
    target = (path or DEFAULT_IDENTITY_PATH).expanduser()
    if target.exists():
        data = json.loads(target.read_text(encoding="utf-8"))
        return RunnerIdentity(
            runner_id=str(data["runner_id"]),
            public_key_hex=str(data["public_key"]),
            _private_key_hex=str(data["private_key"]),
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    sk = Ed25519PrivateKey.generate()
    sk_bytes = sk.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    )
    pk_bytes = sk.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    record = {
        "runner_id": str(uuid.uuid4()),
        "public_key": pk_bytes.hex(),
        "private_key": sk_bytes.hex(),
        "created_at": _now_iso(),
    }
    target.write_text(json.dumps(record, indent=2), encoding="utf-8")
    try:
        os.chmod(target, 0o600)
    except OSError:
        pass
    return RunnerIdentity(
        runner_id=record["runner_id"],
        public_key_hex=record["public_key"],
        _private_key_hex=record["private_key"],
    )


def verify_signature(public_key_hex: str, payload: bytes, signature_hex: str) -> bool:
    """Server-side signature verification helper.

    Returns False on any failure (bad hex, length mismatch, invalid signature).
    """
    try:
        pk = Ed25519PublicKey.from_public_bytes(bytes.fromhex(public_key_hex))
        pk.verify(bytes.fromhex(signature_hex), payload)
        return True
    except Exception:
        return False
