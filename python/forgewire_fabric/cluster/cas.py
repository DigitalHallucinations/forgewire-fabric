"""Filesystem content-addressed store (CAS) for the blob fabric.

This is the storage substrate consumed by both the small-blob path
(:mod:`forgewire_fabric.cluster.blobs`) and the chunked path
(:mod:`forgewire_fabric.cluster.blobs_chunked`). It is transport-agnostic —
no bus, no async, just bytes-in/bytes-out keyed by SHA-256.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

CAS_SCHEMA_VERSION = 1
DEFAULT_CAS_NAMESPACE = "default"
DEFAULT_CAS_CAPACITY_BYTES = 50 * 1024 * 1024 * 1024  # 50 GB


class BlobFabricError(RuntimeError):
    """Base error for blob-fabric operations."""


class BlobNotAllowed(BlobFabricError):
    """Raised when a digest is rejected by the allowlist."""


class BlobUnavailable(BlobFabricError):
    """Raised when no peer can serve a requested digest in time."""


class BlobIntegrityError(BlobFabricError):
    """Raised when a transferred blob fails its SHA-256 check."""


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True, slots=True)
class BlobMetadata:
    """Metadata sidecar persisted alongside each blob body."""

    digest: str
    size: int
    namespace: str = DEFAULT_CAS_NAMESPACE
    created_at: float = field(default_factory=time.time)
    last_accessed_at: float = field(default_factory=time.time)
    schema_version: int = CAS_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "digest": self.digest,
            "size": self.size,
            "namespace": self.namespace,
            "created_at": self.created_at,
            "last_accessed_at": self.last_accessed_at,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BlobMetadata":
        return cls(
            schema_version=int(data.get("schema_version") or CAS_SCHEMA_VERSION),
            digest=str(data["digest"]),
            size=int(data["size"]),
            namespace=str(data.get("namespace") or DEFAULT_CAS_NAMESPACE),
            created_at=float(data.get("created_at") or time.time()),
            last_accessed_at=float(data.get("last_accessed_at") or time.time()),
        )


class ContentAddressedStore:
    """Filesystem-backed CAS at ``<root>/<digest[:2]>/<digest>``.

    Each entry has a body file and a ``<digest>.meta.json`` sidecar storing
    namespace, sizes, and access timestamps used by LRU eviction.
    """

    def __init__(
        self,
        root: Path | str,
        *,
        capacity_bytes: int = DEFAULT_CAS_CAPACITY_BYTES,
        allowlist: Iterable[str] | None = None,
    ):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.capacity_bytes = int(capacity_bytes)
        self._allowlist: set[str] | None = (
            {digest.lower() for digest in allowlist} if allowlist is not None else None
        )

    def _path_for(self, digest: str) -> Path:
        digest = digest.lower()
        return self.root / digest[:2] / digest

    def _meta_path_for(self, digest: str) -> Path:
        return self._path_for(digest).with_suffix(".meta.json")

    def is_allowed(self, digest: str) -> bool:
        if self._allowlist is None:
            return True
        return digest.lower() in self._allowlist

    def set_allowlist(self, allowlist: Iterable[str] | None) -> None:
        self._allowlist = (
            {digest.lower() for digest in allowlist} if allowlist is not None else None
        )

    def has(self, digest: str) -> bool:
        return self._path_for(digest).is_file()

    def put_bytes(self, data: bytes, *, namespace: str = DEFAULT_CAS_NAMESPACE) -> str:
        digest = sha256_hex(data)
        if not self.is_allowed(digest):
            raise BlobNotAllowed(f"digest {digest} not in allowlist")
        body_path = self._path_for(digest)
        body_path.parent.mkdir(parents=True, exist_ok=True)
        if not body_path.exists():
            tmp_path = body_path.with_suffix(".tmp")
            tmp_path.write_bytes(data)
            os.replace(tmp_path, body_path)
        meta = BlobMetadata(digest=digest, size=len(data), namespace=namespace)
        self._write_meta(meta)
        self.enforce_capacity()
        return digest

    def get_bytes(self, digest: str) -> bytes | None:
        body_path = self._path_for(digest)
        if not body_path.is_file():
            return None
        data = body_path.read_bytes()
        self._touch(digest)
        return data

    def delete(self, digest: str) -> bool:
        body_path = self._path_for(digest)
        meta_path = self._meta_path_for(digest)
        removed = False
        if body_path.exists():
            body_path.unlink()
            removed = True
        if meta_path.exists():
            meta_path.unlink()
        return removed

    def metadata(self, digest: str) -> BlobMetadata | None:
        meta_path = self._meta_path_for(digest)
        if not meta_path.is_file():
            return None
        return BlobMetadata.from_dict(json.loads(meta_path.read_text()))

    def total_size(self) -> int:
        return sum(meta.size for meta in self._iter_metadata())

    def list_digests(self) -> tuple[str, ...]:
        return tuple(meta.digest for meta in self._iter_metadata())

    def evict_lru(self, *, target_bytes: int) -> int:
        """Evict least-recently-accessed blobs until total <= target_bytes."""

        if target_bytes < 0:
            raise ValueError("target_bytes must be >= 0")
        metas = sorted(self._iter_metadata(), key=lambda m: m.last_accessed_at)
        total = sum(m.size for m in metas)
        removed = 0
        for meta in metas:
            if total <= target_bytes:
                break
            self.delete(meta.digest)
            total -= meta.size
            removed += 1
        return removed

    def enforce_capacity(self) -> int:
        if self.total_size() <= self.capacity_bytes:
            return 0
        return self.evict_lru(target_bytes=self.capacity_bytes)

    def _touch(self, digest: str) -> None:
        meta = self.metadata(digest)
        if meta is None:
            return
        updated = BlobMetadata(
            digest=meta.digest,
            size=meta.size,
            namespace=meta.namespace,
            created_at=meta.created_at,
            last_accessed_at=time.time(),
        )
        self._write_meta(updated)

    def _write_meta(self, meta: BlobMetadata) -> None:
        meta_path = self._meta_path_for(meta.digest)
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        meta_path.write_text(json.dumps(meta.to_dict()))

    def _iter_metadata(self) -> Iterable[BlobMetadata]:
        for meta_path in self.root.rglob("*.meta.json"):
            try:
                yield BlobMetadata.from_dict(json.loads(meta_path.read_text()))
            except (OSError, ValueError, KeyError):
                continue


__all__ = [
    "CAS_SCHEMA_VERSION",
    "DEFAULT_CAS_NAMESPACE",
    "DEFAULT_CAS_CAPACITY_BYTES",
    "BlobFabricError",
    "BlobNotAllowed",
    "BlobUnavailable",
    "BlobIntegrityError",
    "BlobMetadata",
    "ContentAddressedStore",
    "sha256_hex",
]
