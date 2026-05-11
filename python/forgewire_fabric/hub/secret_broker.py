"""M2.5.5: hub-side sealed secret broker.

Stores caller-supplied secrets (think ``GITHUB_TOKEN``, ``OPENAI_API_KEY``)
encrypted at rest with a hub-local master key, and decrypts them on demand
at task-claim time so the runner can inject them into its task subprocess
environment.

Wire diagram::

    dispatcher --(secrets_needed: ["GITHUB_TOKEN"])--> hub /tasks
                                                       |
                                          create_task()|
                                                       v
                                          tasks.secrets_needed = '["GITHUB_TOKEN"]'
                                                       |
                                          /tasks/claim-v2
                                                       v
        hub:  broker.resolve(["GITHUB_TOKEN"])  -> {"GITHUB_TOKEN": "<plaintext>"}
              audit("claim", secrets_dispatched=["GITHUB_TOKEN"])     <-- names only
              response: { task: {..., secrets: {GITHUB_TOKEN: "..."}} }
                                                       v
        runner: subprocess env injection (process-scoped, never written to disk)

Encryption: AES-GCM 256-bit (``cryptography.hazmat.primitives.ciphers.aead``)
with a 12-byte random nonce per row. The stored ``ciphertext`` blob is
laid out as ``nonce(12) || ct_plus_tag``. The secret ``name`` is bound as
GCM associated data so a row cannot be renamed inside the database
without detection.

Master-key resolution order (M2.5.5a default + M2.5.5c keychain hook):

1. ``FORGEWIRE_SECRETS_KEY_HEX`` environment variable, if set and 64 hex
   chars long. Useful for unit tests and ephemeral hub deployments.
2. OS keychain entry (service ``forgewire-fabric``, account
   ``secrets-master-key``) when ``FORGEWIRE_SECRETS_BACKEND=keychain``
   and the optional ``keyring`` dep is installed (M2.5.5c).
3. File-backed key at ``<db_path>.secrets.key`` (auto-generated 32 random
   bytes, chmod 0600 on POSIX). This is the M2.5.5a default.

The broker never logs or returns the master key. Plaintext secret values
are returned only through :meth:`SecretBroker.resolve` and only to the
claim handler, which immediately scopes them to the JSON response.

Mocking policy: there is none. Tests use real AES-GCM against
``FileKeyProvider`` rooted in ``tmp_path``.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import secrets as _stdlib_secrets
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


LOGGER = logging.getLogger(__name__)

# Service/account names used for the optional OS keychain backend.
KEYRING_SERVICE = "forgewire-fabric"
KEYRING_ACCOUNT = "secrets-master-key"

# Sentinel placeholder substituted into redacted log payloads.
REDACTION_MARKER = "***SECRET:{name}***"


class KeyProvider(Protocol):
    """Returns the 32-byte AES-GCM master key for the secret broker."""

    def load(self) -> bytes: ...


@dataclass(slots=True)
class EnvKeyProvider:
    """Read the master key from ``$FORGEWIRE_SECRETS_KEY_HEX`` (64 hex chars).

    Returns ``b""`` if the env var is unset, so the caller can chain to
    the next provider.
    """

    env_var: str = "FORGEWIRE_SECRETS_KEY_HEX"

    def load(self) -> bytes:
        raw = os.environ.get(self.env_var)
        if not raw:
            return b""
        raw = raw.strip()
        try:
            key = bytes.fromhex(raw)
        except ValueError as exc:
            raise ValueError(
                f"{self.env_var} must be 64-char hex (got {len(raw)} chars)"
            ) from exc
        if len(key) != 32:
            raise ValueError(
                f"{self.env_var} must decode to 32 raw bytes (got {len(key)})"
            )
        return key


@dataclass(slots=True)
class FileKeyProvider:
    """File-backed master key. Auto-generates on first read.

    Sets POSIX 0600 perms on creation; on Windows the inherited NTFS ACL
    of the parent directory is left untouched (typical hub deployment
    puts the key alongside the SQLite DB which already lives in a
    user-scoped directory).
    """

    path: Path

    def load(self) -> bytes:
        if self.path.exists():
            data = self.path.read_bytes()
            if len(data) != 32:
                raise ValueError(
                    f"master key at {self.path} is {len(data)} bytes, expected 32"
                )
            return data
        self.path.parent.mkdir(parents=True, exist_ok=True)
        key = AESGCM.generate_key(bit_length=256)
        # Write+chmod tightly so a partial read can't observe a 0-byte
        # key. We write to a temp path then rename atomically.
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_bytes(key)
        if sys.platform != "win32":
            try:
                os.chmod(tmp, 0o600)
            except OSError as exc:  # pragma: no cover - filesystem rarity
                LOGGER.warning("chmod 0600 failed for %s: %s", tmp, exc)
        os.replace(tmp, self.path)
        LOGGER.info("generated new secrets master key at %s", self.path)
        return key


@dataclass(slots=True)
class KeyringKeyProvider:
    """M2.5.5c: OS keychain backend (DPAPI / libsecret / Keychain).

    Lazily imports the optional ``keyring`` package. Generates a new
    32-byte key on first read and stores it under
    ``(KEYRING_SERVICE, KEYRING_ACCOUNT)``.
    """

    service: str = KEYRING_SERVICE
    account: str = KEYRING_ACCOUNT

    def load(self) -> bytes:
        try:
            import keyring  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError(
                "FORGEWIRE_SECRETS_BACKEND=keychain requires the 'keyring' "
                "package; install with `pip install forgewire-fabric[keychain]`"
            ) from exc
        stored = keyring.get_password(self.service, self.account)
        if stored:
            try:
                key = bytes.fromhex(stored)
            except ValueError as exc:
                raise RuntimeError(
                    f"keyring entry {self.service}/{self.account} is not hex"
                ) from exc
            if len(key) != 32:
                raise RuntimeError(
                    f"keyring entry {self.service}/{self.account} is "
                    f"{len(key)} bytes, expected 32"
                )
            return key
        key = AESGCM.generate_key(bit_length=256)
        keyring.set_password(self.service, self.account, key.hex())
        LOGGER.info(
            "generated new secrets master key in OS keychain %s/%s",
            self.service, self.account,
        )
        return key


def default_key_provider(
    *,
    db_path: Path,
    backend: str | None = None,
) -> KeyProvider:
    """Build the production key provider chain.

    Resolution order:

    * env var (always tried first; lets ops override anything else)
    * keychain (if ``backend == 'keychain'``)
    * file at ``<db_path>.secrets.key`` (always the final fallback)
    """
    backend = (backend or os.environ.get("FORGEWIRE_SECRETS_BACKEND") or "file").lower()
    fallback = FileKeyProvider(path=db_path.with_suffix(db_path.suffix + ".secrets.key"))
    if backend == "keychain":
        return _ChainedKeyProvider([EnvKeyProvider(), KeyringKeyProvider(), fallback])
    return _ChainedKeyProvider([EnvKeyProvider(), fallback])


@dataclass(slots=True)
class _ChainedKeyProvider:
    providers: list[KeyProvider]

    def load(self) -> bytes:
        for p in self.providers:
            data = p.load()
            if data:
                return data
        raise RuntimeError("no key provider returned a master key")


# ---------------------------------------------------------------------------
# Broker
# ---------------------------------------------------------------------------


def _coerce_ciphertext(stored: Any) -> bytes:
    """Normalise a ciphertext column value back to raw bytes.

    Ciphertext is written as base64-encoded TEXT so it round-trips over
    the rqlite JSON wire (which cannot carry Python ``bytes`` params).
    SQLite stores the value with TEXT affinity in that case. Older or
    direct-SQLite rows that were written as a ``BLOB`` come back as
    ``bytes``/``memoryview``; accept those too for forward compatibility.
    """
    if isinstance(stored, (bytes, bytearray, memoryview)):
        return bytes(stored)
    if isinstance(stored, str):
        return base64.b64decode(stored.encode("ascii"))
    raise TypeError(f"unexpected ciphertext column type: {type(stored).__name__}")


class SecretBroker:
    """AES-GCM seal/open layer on top of a ``secrets`` SQL table.

    The broker holds the master key in memory after first read. A
    plaintext-value cache (``_value_cache``) is maintained for the
    redactor; it is repopulated lazily and invalidated on every write.
    """

    def __init__(self, key_provider: KeyProvider) -> None:
        self._key_provider = key_provider
        self._key: bytes | None = None
        self._aesgcm: AESGCM | None = None
        self._value_cache: dict[str, str] | None = None

    # ----- key + cipher state -----

    def _cipher(self) -> AESGCM:
        if self._aesgcm is None:
            self._key = self._key_provider.load()
            self._aesgcm = AESGCM(self._key)
        return self._aesgcm

    def _encrypt(self, name: str, plaintext: str) -> bytes:
        aes = self._cipher()
        nonce = _stdlib_secrets.token_bytes(12)
        ct = aes.encrypt(nonce, plaintext.encode("utf-8"), name.encode("utf-8"))
        return nonce + ct

    def _decrypt(self, name: str, blob: bytes) -> str:
        aes = self._cipher()
        nonce, ct = blob[:12], blob[12:]
        try:
            pt = aes.decrypt(nonce, ct, name.encode("utf-8"))
        except InvalidTag as exc:
            raise PermissionError(
                f"secret {name!r} failed authentication (wrong key or tampered row)"
            ) from exc
        return pt.decode("utf-8")

    # ----- crud (operates on caller-provided sqlite3 connection) -----

    @staticmethod
    def init_schema(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS secrets (
                name             TEXT PRIMARY KEY,
                ciphertext       BLOB NOT NULL,
                version          INTEGER NOT NULL DEFAULT 1,
                created_at       TEXT NOT NULL,
                updated_at       TEXT NOT NULL,
                last_rotated_at  TEXT
            )
            """
        )

    def put(
        self,
        conn: sqlite3.Connection,
        *,
        name: str,
        value: str,
        now_iso: str,
    ) -> dict[str, Any]:
        if not name or not name.replace("_", "").replace("-", "").isalnum():
            raise ValueError(
                f"secret name {name!r} must be alphanumeric (plus _ -)"
            )
        if not value:
            raise ValueError("secret value must be non-empty")
        blob = self._encrypt(name, value)
        b64 = base64.b64encode(blob).decode("ascii")
        existing = conn.execute(
            "SELECT version FROM secrets WHERE name = ?", (name,)
        ).fetchone()
        if existing is None:
            conn.execute(
                """
                INSERT INTO secrets (name, ciphertext, version, created_at, updated_at)
                VALUES (?, ?, 1, ?, ?)
                """,
                (name, b64, now_iso, now_iso),
            )
            version = 1
        else:
            version = int(existing["version"]) + 1
            conn.execute(
                """
                UPDATE secrets
                   SET ciphertext = ?, version = ?, updated_at = ?
                 WHERE name = ?
                """,
                (b64, version, now_iso, name),
            )
        self._value_cache = None
        return {"name": name, "version": version, "updated_at": now_iso}

    def rotate(
        self,
        conn: sqlite3.Connection,
        *,
        name: str,
        value: str,
        now_iso: str,
    ) -> dict[str, Any]:
        existing = conn.execute(
            "SELECT version FROM secrets WHERE name = ?", (name,)
        ).fetchone()
        if existing is None:
            raise KeyError(name)
        version = int(existing["version"]) + 1
        blob = self._encrypt(name, value)
        b64 = base64.b64encode(blob).decode("ascii")
        conn.execute(
            """
            UPDATE secrets
               SET ciphertext = ?, version = ?, updated_at = ?, last_rotated_at = ?
             WHERE name = ?
            """,
            (b64, version, now_iso, now_iso, name),
        )
        self._value_cache = None
        return {"name": name, "version": version, "last_rotated_at": now_iso}

    def delete(self, conn: sqlite3.Connection, *, name: str) -> bool:
        cur = conn.execute("DELETE FROM secrets WHERE name = ?", (name,))
        deleted = (cur.rowcount or 0) > 0
        if deleted:
            self._value_cache = None
        return deleted

    @staticmethod
    def list_metadata(conn: sqlite3.Connection) -> list[dict[str, Any]]:
        rows = conn.execute(
            "SELECT name, version, created_at, updated_at, last_rotated_at "
            "FROM secrets ORDER BY name ASC"
        ).fetchall()
        return [dict(r) for r in rows]

    def resolve(
        self,
        conn: sqlite3.Connection,
        *,
        names: list[str],
    ) -> dict[str, str]:
        """Decrypt the named secrets. Missing names are silently skipped.

        Returns ``{name: plaintext}`` in input order, omitting any name
        that does not exist in the store.
        """
        if not names:
            return {}
        out: dict[str, str] = {}
        placeholders = ",".join("?" * len(names))
        rows = conn.execute(
            f"SELECT name, ciphertext FROM secrets WHERE name IN ({placeholders})",
            tuple(names),
        ).fetchall()
        by_name = {r["name"]: r["ciphertext"] for r in rows}
        for name in names:
            stored = by_name.get(name)
            if stored is None:
                continue
            out[name] = self._decrypt(name, _coerce_ciphertext(stored))
        return out

    # ----- redaction -----

    def _load_value_cache(
        self, conn_factory: Callable[[], sqlite3.Connection]
    ) -> dict[str, str]:
        if self._value_cache is not None:
            return self._value_cache
        with conn_factory() as conn:
            rows = conn.execute("SELECT name, ciphertext FROM secrets").fetchall()
        cache: dict[str, str] = {}
        for row in rows:
            name = row["name"]
            try:
                cache[name] = self._decrypt(name, _coerce_ciphertext(row["ciphertext"]))
            except PermissionError as exc:  # corrupt row; skip with warning
                LOGGER.warning("secret %r failed decrypt during cache load: %s", name, exc)
        self._value_cache = cache
        return cache

    def redact(
        self,
        text: str | None,
        *,
        conn_factory: Callable[[], sqlite3.Connection],
    ) -> str | None:
        """Replace any stored secret value substring with ``***SECRET:NAME***``.

        Pass-through for ``None`` and the empty string.
        """
        if not text:
            return text
        cache = self._load_value_cache(conn_factory)
        if not cache:
            return text
        out = text
        # Replace longer values first so a secret that is a prefix of
        # another doesn't get partially redacted.
        for name, value in sorted(cache.items(), key=lambda kv: -len(kv[1])):
            if value and value in out:
                out = out.replace(value, REDACTION_MARKER.format(name=name))
        return out

    def invalidate_cache(self) -> None:
        """Drop the plaintext value cache. Called on external rotations."""
        self._value_cache = None


__all__ = [
    "EnvKeyProvider",
    "FileKeyProvider",
    "KeyProvider",
    "KeyringKeyProvider",
    "REDACTION_MARKER",
    "SecretBroker",
    "default_key_provider",
]
