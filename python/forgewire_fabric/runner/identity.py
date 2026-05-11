"""Persistent runner identity (UUID + ed25519 keypair).

Identity is **machine-scoped**, not user-scoped: the same physical host must
register under a single ``runner_id`` regardless of which OS user starts the
runner (NSSM ``LocalSystem`` service vs. interactive ``forgewire-fabric
runner start``). Anchoring the file under the user's home directory caused
duplicate ``runner_id`` rows in the hub registry for the same host. The
canonical resolution order is:

1. ``$FORGEWIRE_RUNNER_IDENTITY_PATH`` if set.
2. ``%PROGRAMDATA%\\forgewire\\runner_identity.json`` on Windows
   (default ``C:\\ProgramData\\forgewire\\runner_identity.json``).
3. ``/var/lib/forgewire/runner_identity.json`` on POSIX if the parent
   exists and is writable; else ``/etc/forgewire/runner_identity.json``;
   else fall back to ``~/.forgewire/runner_identity.json`` for dev.

On first read, if the machine-wide target does not exist but a legacy
per-user path (``~/.forgewire/runner_identity.json`` or
``~/.phrenforge/runner_identity.json``) does, the content is migrated into
the machine-wide path so the same ``runner_id`` is preserved across the
upgrade.

File format::

    {
        "runner_id":  "<uuid4 lowercase hex with dashes>",
        "public_key": "<32-byte hex>",
        "private_key": "<32-byte hex>",
        "created_at": "<iso8601 utc>"
    }

The file is written 0600 on POSIX. On Windows we fall back to default ACLs
because chmod is a no-op there; ``%PROGRAMDATA%`` is per-machine and ACL'd
to ``SYSTEM``/Administrators by default.
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)


_IDENTITY_FILENAME = "runner_identity.json"


def _machine_identity_path() -> Path:
    """Return the canonical machine-wide identity path for this OS."""
    override = os.environ.get("FORGEWIRE_RUNNER_IDENTITY_PATH")
    if override:
        return Path(override).expanduser()
    if sys.platform == "win32":
        program_data = os.environ.get("PROGRAMDATA") or r"C:\ProgramData"
        return Path(program_data) / "forgewire" / _IDENTITY_FILENAME
    # POSIX: prefer /var/lib/forgewire, fall back to /etc/forgewire.
    for base in ("/var/lib/forgewire", "/etc/forgewire"):
        parent = Path(base)
        if parent.exists() and os.access(parent, os.W_OK):
            return parent / _IDENTITY_FILENAME
    return Path("/var/lib/forgewire") / _IDENTITY_FILENAME


DEFAULT_IDENTITY_PATH = _machine_identity_path()
_LEGACY_USER_IDENTITY_PATH = Path.home() / ".forgewire" / _IDENTITY_FILENAME
_LEGACY_PHRENFORGE_IDENTITY_PATH = (
    Path.home() / ".phrenforge" / _IDENTITY_FILENAME
)


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
    """Return the persisted identity, creating it on first use.

    When ``path`` is ``None`` we resolve to the machine-wide default
    (``DEFAULT_IDENTITY_PATH``). On first read, content from any legacy
    per-user identity file is migrated into the machine-wide location so
    the same ``runner_id`` is preserved across upgrades.
    """
    explicit = path is not None
    target = (path or DEFAULT_IDENTITY_PATH).expanduser()
    if not target.exists() and not explicit:
        for legacy in (
            _LEGACY_USER_IDENTITY_PATH,
            _LEGACY_PHRENFORGE_IDENTITY_PATH,
        ):
            if legacy.exists():
                try:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_text(
                        legacy.read_text(encoding="utf-8"), encoding="utf-8"
                    )
                except OSError:
                    # If we can't write to the machine-wide path (no perms),
                    # fall back to the legacy path so we don't mint a fresh
                    # runner_id on every restart. The deployer is expected
                    # to fix permissions; we degrade gracefully meanwhile.
                    target = legacy
                break
    if target.exists():
        data = json.loads(target.read_text(encoding="utf-8"))
        return RunnerIdentity(
            runner_id=str(data["runner_id"]),
            public_key_hex=str(data["public_key"]),
            _private_key_hex=str(data["private_key"]),
        )
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        # Can't create the machine-wide dir (e.g. unprivileged dev box):
        # fall back to a per-user path so the runner can still come up.
        target = _LEGACY_USER_IDENTITY_PATH
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
