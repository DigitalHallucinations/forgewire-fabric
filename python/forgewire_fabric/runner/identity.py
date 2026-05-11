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


_IDENTITY_FIELDS = frozenset({"runner_id", "public_key", "private_key"})


def _validate_identity_record(data: object) -> dict[str, str]:
    """Validate the on-disk identity JSON and return a normalized dict.

    Identity files are operator-portable (used by ``runner identity import``
    when migrating a runner role to a new machine); we therefore validate
    structure and key lengths rather than trusting the bytes blindly.
    """
    if not isinstance(data, dict):
        raise ValueError("identity file must contain a JSON object")
    missing = _IDENTITY_FIELDS - data.keys()
    if missing:
        raise ValueError(f"identity file missing required fields: {sorted(missing)}")
    runner_id = str(data["runner_id"]).strip()
    public_key = str(data["public_key"]).strip().lower()
    private_key = str(data["private_key"]).strip().lower()
    # Parse as UUID to reject obviously malformed ids.
    uuid.UUID(runner_id)
    if len(public_key) != 64 or len(private_key) != 64:
        raise ValueError("public/private key must be 32 raw bytes (64 hex chars)")
    bytes.fromhex(public_key)
    bytes.fromhex(private_key)
    # Cross-check that the private key actually derives the public key.
    sk = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(private_key))
    derived = sk.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    ).hex()
    if derived != public_key:
        raise ValueError("identity file public_key does not match private_key")
    return {
        "runner_id": runner_id,
        "public_key": public_key,
        "private_key": private_key,
        "created_at": str(data.get("created_at") or _now_iso()),
    }


def _atomic_write_identity(target: Path, record: dict[str, str]) -> None:
    """Write the identity record to ``target`` atomically with 0600 perms."""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".tmp.{os.getpid()}")
    tmp.write_text(json.dumps(record, indent=2), encoding="utf-8")
    try:
        os.chmod(tmp, 0o600)
    except OSError:
        # Windows: chmod is a no-op; ACL inheritance from ProgramData
        # already restricts write to SYSTEM/Administrators.
        pass
    os.replace(tmp, target)


def ensure_identity_dir(path: Path | None = None) -> Path:
    """Create the machine-wide identity directory if missing.

    Called by ``install_runner`` so a service installed under a different
    OS account than the original installer still resolves to a writable,
    machine-scoped directory. Returns the resolved directory path.

    On Windows, ``%PROGRAMDATA%\\forgewire`` inherits ACLs that grant
    SYSTEM and Administrators full control plus authenticated users read,
    which is the right shape for a service identity store.
    """
    target = (path or DEFAULT_IDENTITY_PATH).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    return target.parent


def export_identity(
    destination: Path | None = None,
    *,
    source: Path | None = None,
) -> dict[str, str]:
    """Return (and optionally write) the current runner identity.

    Used during hardware migration: export from the retiring machine,
    transfer to the replacement, then ``import_identity`` there. The
    private key is included by design — this file is the runner's
    cryptographic identity and is meaningless without it.

    ``destination`` is written atomically with 0600 perms when provided.
    Returns the identity dict regardless.
    """
    src = (source or DEFAULT_IDENTITY_PATH).expanduser()
    if not src.exists():
        # Allow exporting a freshly-minted identity by triggering creation.
        load_or_create(src if source is not None else None)
    data = json.loads(src.read_text(encoding="utf-8"))
    record = _validate_identity_record(data)
    if destination is not None:
        _atomic_write_identity(destination.expanduser(), record)
    return record


def import_identity(
    source: Path,
    *,
    target: Path | None = None,
    force: bool = False,
) -> RunnerIdentity:
    """Install an exported identity file as this machine's runner identity.

    Refuses to overwrite an existing identity whose ``runner_id`` differs
    from the incoming one unless ``force=True``; an identical ``runner_id``
    is treated as idempotent and overwritten silently (covers re-runs of
    the migration step). Always atomic.
    """
    src = source.expanduser()
    if not src.exists():
        raise FileNotFoundError(f"identity source not found: {src}")
    record = _validate_identity_record(json.loads(src.read_text(encoding="utf-8")))
    dst = (target or DEFAULT_IDENTITY_PATH).expanduser()
    if dst.exists() and not force:
        existing = _validate_identity_record(
            json.loads(dst.read_text(encoding="utf-8"))
        )
        if existing["runner_id"] != record["runner_id"]:
            raise RuntimeError(
                "refusing to overwrite existing runner identity "
                f"{existing['runner_id']!r} with {record['runner_id']!r}; "
                "rerun with --force to confirm"
            )
    _atomic_write_identity(dst, record)
    return RunnerIdentity(
        runner_id=record["runner_id"],
        public_key_hex=record["public_key"],
        _private_key_hex=record["private_key"],
    )


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
                    record = _validate_identity_record(
                        json.loads(legacy.read_text(encoding="utf-8"))
                    )
                    _atomic_write_identity(target, record)
                except (OSError, ValueError):
                    # If we can't write to the machine-wide path (no perms)
                    # or the legacy file is corrupt, fall back to using
                    # the legacy path so we don't mint a fresh runner_id
                    # on every restart. The deployer is expected to fix
                    # permissions; we degrade gracefully meanwhile.
                    target = legacy
                break
    if target.exists():
        record = _validate_identity_record(
            json.loads(target.read_text(encoding="utf-8"))
        )
        return RunnerIdentity(
            runner_id=record["runner_id"],
            public_key_hex=record["public_key"],
            _private_key_hex=record["private_key"],
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
    _atomic_write_identity(target, record)
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
