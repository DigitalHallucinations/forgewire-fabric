"""Dispatcher-side cryptographic identity.

Dispatchers (CLI users, MCP clients, VS Code extension hosts, automation
scripts) own an ed25519 keypair persisted on disk. The hub records the
public key on first registration and verifies a signature on every signed
dispatch (``POST /tasks/v2``).

This is the dispatcher-half mirror of :mod:`forgewire_fabric.runner.identity`. The
fields and on-disk format are identical except the file is named
``dispatcher_identity.json`` and the canonical id field is
``dispatcher_id``.

File format::

    {
        "dispatcher_id": "<uuid4>",
        "public_key":   "<32-byte hex>",
        "private_key":  "<32-byte hex>",
        "label":        "<freeform host/account label>",
        "created_at":   "<iso8601 utc>"
    }

The file is written 0600 on POSIX and lives under ``%USERPROFILE%`` on
Windows so default ACLs already restrict it to the user.
"""

from __future__ import annotations

import json
import os
import socket
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)


DEFAULT_IDENTITY_PATH = Path.home() / ".forgewire" / "dispatcher_identity.json"


@dataclass(frozen=True, slots=True)
class DispatcherIdentity:
    dispatcher_id: str
    public_key_hex: str
    label: str
    _private_key_hex: str

    @property
    def public_key(self) -> Ed25519PublicKey:
        return Ed25519PublicKey.from_public_bytes(bytes.fromhex(self.public_key_hex))

    def sign(self, payload: bytes) -> str:
        sk = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(self._private_key_hex))
        return sk.sign(payload).hex()


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_or_create(
    path: Path | None = None,
    *,
    label: str | None = None,
) -> DispatcherIdentity:
    """Return the persisted dispatcher identity, creating it on first use.

    ``label`` is only honored on creation; subsequent calls return the
    persisted label even if a different one is passed.
    """

    target = (path or DEFAULT_IDENTITY_PATH).expanduser()
    if target.exists():
        data = json.loads(target.read_text(encoding="utf-8"))
        return DispatcherIdentity(
            dispatcher_id=str(data["dispatcher_id"]),
            public_key_hex=str(data["public_key"]),
            label=str(data.get("label") or socket.gethostname()),
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
        "dispatcher_id": str(uuid.uuid4()),
        "public_key": pk_bytes.hex(),
        "private_key": sk_bytes.hex(),
        "label": (label or socket.gethostname()),
        "created_at": _now_iso(),
    }
    target.write_text(json.dumps(record, indent=2), encoding="utf-8")
    try:
        os.chmod(target, 0o600)
    except OSError:
        pass
    return DispatcherIdentity(
        dispatcher_id=record["dispatcher_id"],
        public_key_hex=record["public_key"],
        label=record["label"],
        _private_key_hex=record["private_key"],
    )
