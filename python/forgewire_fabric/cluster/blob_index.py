"""Durable per-node CAS metadata index in SQLite.

Backs the chunked blob fabric (:mod:`forgewire_fabric.cluster.blobs_chunked`)
with persistent records of digest → size/namespace/access-time. Independent
from the on-disk body files in :class:`forgewire_fabric.cluster.cas.ContentAddressedStore`
so eviction policy can be centralised in SQL.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from .cas import DEFAULT_CAS_NAMESPACE

CHUNKED_CAS_SCHEMA_VERSION = 1


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS blobs (
    digest TEXT PRIMARY KEY,
    size INTEGER NOT NULL,
    namespace TEXT NOT NULL,
    created_at REAL NOT NULL,
    last_accessed_at REAL NOT NULL,
    schema_version INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_blobs_namespace ON blobs(namespace);
CREATE INDEX IF NOT EXISTS idx_blobs_last_accessed ON blobs(last_accessed_at);
"""


class SqliteBlobIndex:
    """Durable per-node CAS metadata index in SQLite (``cas.sqlite3``)."""

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            str(self.db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.executescript(_SCHEMA_SQL)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def upsert(
        self,
        *,
        digest: str,
        size: int,
        namespace: str = DEFAULT_CAS_NAMESPACE,
        created_at: float | None = None,
        last_accessed_at: float | None = None,
    ) -> None:
        now = time.time()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO blobs(digest, size, namespace, created_at, last_accessed_at, schema_version)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(digest) DO UPDATE SET
                    size = excluded.size,
                    namespace = excluded.namespace,
                    last_accessed_at = excluded.last_accessed_at;
                """,
                (
                    digest.lower(),
                    int(size),
                    namespace,
                    float(created_at if created_at is not None else now),
                    float(last_accessed_at if last_accessed_at is not None else now),
                    CHUNKED_CAS_SCHEMA_VERSION,
                ),
            )

    def touch(self, digest: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE blobs SET last_accessed_at = ? WHERE digest = ?",
                (time.time(), digest.lower()),
            )

    def remove(self, digest: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM blobs WHERE digest = ?", (digest.lower(),))

    def get(self, digest: str) -> Mapping[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT digest, size, namespace, created_at, last_accessed_at, schema_version"
                " FROM blobs WHERE digest = ?",
                (digest.lower(),),
            ).fetchone()
        if row is None:
            return None
        return {
            "digest": row[0],
            "size": int(row[1]),
            "namespace": row[2],
            "created_at": float(row[3]),
            "last_accessed_at": float(row[4]),
            "schema_version": int(row[5]),
        }

    def list_digests(self) -> tuple[str, ...]:
        with self._lock:
            rows = self._conn.execute("SELECT digest FROM blobs").fetchall()
        return tuple(r[0] for r in rows)

    def total_size(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COALESCE(SUM(size), 0) FROM blobs").fetchone()
        return int(row[0])

    def least_recently_accessed(self) -> Iterable[tuple[str, int]]:
        """Yield ``(digest, size)`` tuples in ascending access-time order."""

        with self._lock:
            rows = self._conn.execute(
                "SELECT digest, size FROM blobs ORDER BY last_accessed_at ASC"
            ).fetchall()
        for row in rows:
            yield row[0], int(row[1])


__all__ = ["CHUNKED_CAS_SCHEMA_VERSION", "SqliteBlobIndex"]
