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

import contextlib
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)


_IDENTITY_FILENAME = "runner_identity.json"
_RUNNER_CONFIG_FILENAME = "runner_config.json"

# Routing/operational knobs that survive ``runner identity-export`` and are
# read as a fallback by ``RunnerConfig.from_env`` when the corresponding
# env var is unset. Env vars always win so an operator can override on the
# command line without editing the sidecar.
_RUNNER_CONFIG_FIELDS: tuple[str, ...] = (
    "workspace_root",
    "tenant",
    "tags",
    "scope_prefixes",
    "max_concurrent",
    "poll_interval_seconds",
    "runner_version",
)


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
DEFAULT_RUNNER_CONFIG_PATH = DEFAULT_IDENTITY_PATH.parent / _RUNNER_CONFIG_FILENAME
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
    # Windows: chmod is a no-op; ACL inheritance from ProgramData already restricts write.
    with contextlib.suppress(OSError):
        os.chmod(tmp, 0o600)
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


# ---------------------------------------------------------------------------
# Runner config sidecar (routing/operational knobs)
# ---------------------------------------------------------------------------
#
# The runner's routing attributes (workspace_root, tenant, tags,
# scope_prefixes, max_concurrent, poll_interval_seconds, runner_version)
# historically came exclusively from environment variables, which the NSSM
# / systemd / launchd service unit injected at start. That model has two
# survivability holes:
#
#   1. **Updates**: a service reinstall that forgets one of the env vars
#      silently downgrades a runner (e.g. drops a tag, narrows
#      scope_prefixes), and the hub starts routing the wrong work to it.
#   2. **Migration**: ``runner identity-export`` carries the keypair to
#      the replacement host but not the operator's intent about *what
#      that runner is for*. The operator has to remember every flag.
#
# The sidecar at ``<identity_dir>/runner_config.json`` is a machine-wide
# JSON file that records the operator's intent next to the identity. It
# is read as a **fallback** by ``RunnerConfig.from_env`` — environment
# variables always win, so existing deployments are unaffected and
# operators can still override on the command line. ``identity-export``
# bundles it; ``identity-import`` restores it.


_CSV_FIELDS = frozenset({"tags", "scope_prefixes"})
_INT_FIELDS = frozenset({"max_concurrent"})
_FLOAT_FIELDS = frozenset({"poll_interval_seconds"})


def _validate_runner_config(data: object) -> dict[str, Any]:
    """Validate and normalize a runner_config.json payload.

    Returns only the keys we recognise; unknown keys are dropped so a
    forward-compatible sidecar written by a newer CLI doesn't crash an
    older runner. List fields are coerced to ``list[str]``; numeric
    fields are coerced to their target type or raise ``ValueError``.
    """
    if not isinstance(data, dict):
        raise ValueError("runner_config.json must contain a JSON object")
    out: dict[str, Any] = {}
    for key in _RUNNER_CONFIG_FIELDS:
        if key not in data:
            continue
        value = data[key]
        if value is None:
            continue
        if key in _CSV_FIELDS:
            if isinstance(value, str):
                items = [s.strip() for s in value.split(",") if s.strip()]
            elif isinstance(value, list):
                items = [str(s).strip() for s in value if str(s).strip()]
            else:
                raise ValueError(f"{key!r} must be a list or comma-string")
            out[key] = items
        elif key in _INT_FIELDS:
            out[key] = int(value)
        elif key in _FLOAT_FIELDS:
            out[key] = float(value)
        else:
            out[key] = str(value)
    return out


def _runner_config_path_for_identity(identity_path: Path) -> Path:
    """Resolve the sidecar path adjacent to a given identity file."""
    return identity_path.expanduser().parent / _RUNNER_CONFIG_FILENAME


def load_runner_config_overrides(path: Path | None = None) -> dict[str, Any]:
    """Return the persisted runner-config sidecar, or an empty dict.

    A missing or unreadable file degrades to ``{}`` so a fresh install
    behaves identically to today (env-only). Validation errors are not
    swallowed; they signal operator misconfiguration that should fail
    loudly at runner start.
    """
    target = (path or DEFAULT_RUNNER_CONFIG_PATH).expanduser()
    if not target.exists():
        return {}
    raw = json.loads(target.read_text(encoding="utf-8"))
    return _validate_runner_config(raw)


def save_runner_config_overrides(
    overrides: dict[str, Any],
    *,
    path: Path | None = None,
    merge: bool = True,
) -> dict[str, Any]:
    """Persist runner-config overrides atomically.

    ``merge=True`` (default) reads the existing sidecar and overlays the
    new keys, so an operator can update a single field without retyping
    the whole config. ``merge=False`` replaces the file outright. An
    empty ``overrides`` with ``merge=False`` deletes the sidecar.
    """
    target = (path or DEFAULT_RUNNER_CONFIG_PATH).expanduser()
    current = load_runner_config_overrides(target) if merge else {}
    current.update(_validate_runner_config(overrides))
    target.parent.mkdir(parents=True, exist_ok=True)
    if not current and not merge:
        if target.exists():
            target.unlink()
        return {}
    tmp = target.with_suffix(target.suffix + f".tmp.{os.getpid()}")
    tmp.write_text(json.dumps(current, indent=2, sort_keys=True), encoding="utf-8")
    with contextlib.suppress(OSError):
        os.chmod(tmp, 0o600)
    os.replace(tmp, target)
    return current


def clear_runner_config_overrides(path: Path | None = None) -> None:
    """Remove the runner-config sidecar if present."""
    target = (path or DEFAULT_RUNNER_CONFIG_PATH).expanduser()
    if target.exists():
        target.unlink()


# ---------------------------------------------------------------------------
# Migration bundles (identity + config)
# ---------------------------------------------------------------------------


def export_runner_bundle(
    destination: Path | None = None,
    *,
    identity_source: Path | None = None,
    config_source: Path | None = None,
) -> dict[str, Any]:
    """Return (and optionally write) a full migration bundle.

    A bundle is a JSON object with two keys::

        {"identity": {<runner_identity.json>}, "config": {<runner_config.json>}}

    ``config`` is present even when the sidecar is missing (as ``{}``) so
    a re-import is deterministic. This is the file an operator carries
    between hosts to preserve both the cryptographic identity *and* the
    routing/operational intent.
    """
    id_src = (identity_source or DEFAULT_IDENTITY_PATH).expanduser()
    if not id_src.exists():
        load_or_create(id_src if identity_source is not None else None)
    identity = _validate_identity_record(
        json.loads(id_src.read_text(encoding="utf-8"))
    )
    cfg_src = (
        config_source
        if config_source is not None
        else _runner_config_path_for_identity(id_src)
    )
    config = load_runner_config_overrides(cfg_src)
    bundle = {
        "schema": "forgewire-runner-bundle/1",
        "exported_at": _now_iso(),
        "identity": identity,
        "config": config,
    }
    if destination is not None:
        dst = destination.expanduser()
        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = dst.with_suffix(dst.suffix + f".tmp.{os.getpid()}")
        tmp.write_text(json.dumps(bundle, indent=2, sort_keys=True), encoding="utf-8")
        with contextlib.suppress(OSError):
            os.chmod(tmp, 0o600)
        os.replace(tmp, dst)
    return bundle


def import_runner_bundle(
    source: Path,
    *,
    identity_target: Path | None = None,
    config_target: Path | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Install a previously-exported bundle as this machine's identity+config.

    Validates the schema marker, then delegates to ``import_identity``
    (which enforces the runner_id refuse-overwrite guard) and writes the
    sidecar atomically. Returns the restored bundle.

    A plain identity-only JSON (no ``schema`` / ``identity`` keys) is
    accepted for backward compatibility with files produced by the
    earlier ``runner identity-export`` (which exported the bare
    identity record). In that case no sidecar is written.
    """
    src = source.expanduser()
    if not src.exists():
        raise FileNotFoundError(f"bundle source not found: {src}")
    data = json.loads(src.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("bundle must contain a JSON object")
    if "identity" in data:
        schema = str(data.get("schema") or "")
        if schema and not schema.startswith("forgewire-runner-bundle/"):
            raise ValueError(f"unknown bundle schema {schema!r}")
        identity_payload = data["identity"]
        config_payload = data.get("config") or {}
    else:
        # Legacy bare-identity export.
        identity_payload = data
        config_payload = {}
    # Materialize the identity through a tmp file so we can reuse the
    # validation + refuse-overwrite logic in import_identity().
    id_tmp = (identity_target or DEFAULT_IDENTITY_PATH).expanduser()
    id_tmp.parent.mkdir(parents=True, exist_ok=True)
    staging = id_tmp.with_suffix(id_tmp.suffix + f".bundle.{os.getpid()}")
    staging.write_text(json.dumps(identity_payload), encoding="utf-8")
    try:
        ident = import_identity(staging, target=identity_target, force=force)
    finally:
        with contextlib.suppress(OSError):
            staging.unlink()
    cfg_dst = (
        config_target
        if config_target is not None
        else _runner_config_path_for_identity(id_tmp)
    )
    if config_payload:
        config = save_runner_config_overrides(
            config_payload, path=cfg_dst, merge=False
        )
    else:
        config = load_runner_config_overrides(cfg_dst)
    return {
        "runner_id": ident.runner_id,
        "public_key": ident.public_key_hex,
        "config": config,
    }
