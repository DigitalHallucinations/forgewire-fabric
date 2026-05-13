"""sqlite3-compatible adapter for rqlite over HTTP.

Provides ``connect()`` returning a ``Connection`` whose API is a drop-in
subset of :mod:`sqlite3` sufficient for ``forgewire_fabric.hub.server``:

* ``conn.execute(sql, params=())`` -> :class:`Cursor`
* ``conn.executemany(sql, seq_of_params)`` -> :class:`Cursor`
* ``conn.executescript(sql_script)``
* ``conn.commit()`` / ``conn.rollback()`` / ``conn.close()``
* ``with conn: ...`` context manager (closes on exit)
* ``cursor.fetchone() / fetchall()`` returning :class:`Row`
* ``Row`` supports ``row["col"]``, ``row[0]``, ``row.keys()``, iteration

Transaction model
-----------------
rqlite executes each HTTP request as one transaction; cross-request
transactions are not supported. We honour the existing
``BEGIN IMMEDIATE`` / ``COMMIT`` markers by buffering all subsequent
write statements and flushing them as a single batch on ``COMMIT``
(POST ``/db/execute?transaction=true``).

A SELECT issued while a transaction batch is open raises
:class:`UnsupportedTransactionError`. Call sites that need to read
their own pending writes must either (a) refactor to a single
``UPDATE ... RETURNING`` style statement or (b) commit the batch first.

This intentionally mirrors the constraint that rqlite's HTTP API
imposes; it would be unsafe to silently drop the BEGIN / COMMIT
markers because the caller relies on the all-or-nothing semantics
across multiple statements.
"""
from __future__ import annotations

import logging
import re
import threading
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any

import httpx

LOGGER = logging.getLogger("forgewire_fabric.hub._rqlite_db")

# ---------------------------------------------------------------------------
# Public exceptions: surface the same names sqlite3 raises so callers can
# ``except sqlite3.DatabaseError`` against either backend.
# ---------------------------------------------------------------------------


class Error(Exception):
    """Base class for rqlite adapter errors (mirrors :class:`sqlite3.Error`)."""


class DatabaseError(Error):
    """Server-side error reported by rqlite."""


class OperationalError(DatabaseError):
    """Transport / availability error (cluster unreachable, leader election)."""


class IntegrityError(DatabaseError):
    """Constraint / FK violation reported by rqlite."""


class UnsupportedTransactionError(Error):
    """Raised when a SELECT is issued inside a buffered BEGIN/COMMIT block.

    rqlite's HTTP API has no cross-request transactions. Refactor the
    call site to a single ``UPDATE ... RETURNING`` statement, or close
    the transaction before reading.
    """


# ---------------------------------------------------------------------------
# Row: supports row["col"] AND row[0]
# ---------------------------------------------------------------------------


class Row(Sequence[Any]):
    """A read-only row supporting numeric and column-name access.

    Matches the behaviour of :class:`sqlite3.Row` closely enough for the
    hub server: ``row["col"]``, ``row[0]``, ``len(row)``, iteration,
    and ``row.keys()`` all work as expected.
    """

    __slots__ = ("_columns", "_values", "_index")

    def __init__(self, columns: Sequence[str], values: Sequence[Any]) -> None:
        if len(columns) != len(values):
            raise DatabaseError(
                f"row column/value length mismatch: {len(columns)} vs {len(values)}"
            )
        self._columns = tuple(columns)
        self._values = tuple(values)
        # Build name->index lazily; tiny rows so a tuple scan is fine, but
        # caching keeps repeated key lookups O(1) which matters in hot loops.
        self._index = {c: i for i, c in enumerate(self._columns)}

    def __getitem__(self, key: Any) -> Any:  # type: ignore[override]
        if isinstance(key, str):
            try:
                return self._values[self._index[key]]
            except KeyError as exc:
                raise IndexError(f"no such column: {key!r}") from exc
        if isinstance(key, slice):
            return self._values[key]
        return self._values[int(key)]

    def __len__(self) -> int:
        return len(self._values)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._values)

    def __contains__(self, item: Any) -> bool:  # type: ignore[override]
        return item in self._values

    def keys(self) -> tuple[str, ...]:
        return self._columns

    def __repr__(self) -> str:
        pairs = ", ".join(f"{c}={v!r}" for c, v in zip(self._columns, self._values, strict=True))
        return f"<Row {pairs}>"


# ---------------------------------------------------------------------------
# Cursor
# ---------------------------------------------------------------------------


class Cursor:
    """Minimal DB-API cursor exposing fetch* and lastrowid."""

    def __init__(self, connection: "Connection") -> None:
        self._connection = connection
        self._rows: list[Row] = []
        self._idx = 0
        self.lastrowid: int | None = None
        self.rowcount: int = -1
        self.description: tuple[tuple[str, None, None, None, None, None, None], ...] | None = None

    # -- internal helpers ---------------------------------------------------

    def _set_rows(self, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> None:
        self._rows = [Row(columns, r) for r in rows]
        self._idx = 0
        self.description = tuple(
            (c, None, None, None, None, None, None) for c in columns
        )
        self.rowcount = len(self._rows)

    def _set_writeresult(self, last_insert_id: int | None, rows_affected: int | None) -> None:
        self._rows = []
        self._idx = 0
        self.description = None
        self.lastrowid = last_insert_id
        self.rowcount = rows_affected if rows_affected is not None else -1

    # -- public API ---------------------------------------------------------

    def fetchone(self) -> Row | None:
        if self._idx >= len(self._rows):
            return None
        row = self._rows[self._idx]
        self._idx += 1
        return row

    def fetchall(self) -> list[Row]:
        out = list(self._rows[self._idx :])
        self._idx = len(self._rows)
        return out

    def fetchmany(self, size: int = 1) -> list[Row]:
        end = min(self._idx + size, len(self._rows))
        out = self._rows[self._idx : end]
        self._idx = end
        return out

    def __iter__(self) -> Iterator[Row]:
        while True:
            r = self.fetchone()
            if r is None:
                return
            yield r

    def close(self) -> None:
        self._rows = []
        self._idx = 0


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


_SELECT_RE = re.compile(r"^\s*(SELECT|WITH|PRAGMA\s+TABLE_INFO|PRAGMA\s+INDEX_LIST|PRAGMA\s+INDEX_INFO)", re.IGNORECASE)
_BEGIN_RE = re.compile(r"^\s*BEGIN\b", re.IGNORECASE)
_COMMIT_RE = re.compile(r"^\s*COMMIT\b", re.IGNORECASE)
_ROLLBACK_RE = re.compile(r"^\s*ROLLBACK\b", re.IGNORECASE)
_PRAGMA_FK_RE = re.compile(r"^\s*PRAGMA\s+foreign_keys", re.IGNORECASE)
# PRAGMAs that rqlite either rejects or that target a single SQLite file
# (rqlite manages its own journal/sync). We strip these from
# ``executescript`` batches to keep the rest of the schema in one tx.
_UNSUPPORTED_PRAGMA_RE = re.compile(
    r"^\s*PRAGMA\s+(journal_mode|synchronous|foreign_keys|locking_mode|"
    r"temp_store|cache_size|page_size|mmap_size|wal_autocheckpoint)\b",
    re.IGNORECASE,
)
# Word-boundary "RETURNING" detection. We use \bRETURNING\b so that
# string literals containing the word elsewhere don't cause false
# positives in the very rare cases they exist; the hub's SQL doesn't.
_RETURNING_RE = re.compile(r"\bRETURNING\b", re.IGNORECASE)


class Connection:
    """rqlite Connection with a sqlite3-shaped surface.

    Thread-safe at the request boundary: each HTTP call uses its own
    httpx.Client transaction, but the buffered-transaction state is
    not shared across threads -- pair one Connection with one logical
    operation, mirroring how the existing hub uses ``_connect()``.
    """

    # Attributes the existing code sets/reads; honoured as no-ops where
    # rqlite semantics differ.
    isolation_level: str | None = None
    row_factory: Any = None  # ignored; we always return Row

    def __init__(
        self,
        host: str,
        port: int,
        *,
        timeout: float = 30.0,
        scheme: str = "http",
        consistency: str = "strong",
        client: "httpx.Client | None" = None,
    ) -> None:
        self._base = f"{scheme}://{host}:{port}"
        self._timeout = timeout
        # rqlite read consistency level for SELECTs ("none" | "weak" | "strong" | "linearizable").
        # "strong" issues a Raft round-trip per read; safe default for hub state.
        self._consistency = consistency
        # httpx follows redirects by default; rqlite redirects writes from
        # followers to the leader (HTTP 301), which is exactly what we want.
        # If a shared client is supplied, reuse it (pool-of-one across the
        # whole hub process) and do not close it on Connection.close(). This
        # avoids per-request TCP setup and connection-pool churn under load.
        if client is not None:
            self._client = client
            self._owns_client = False
        else:
            self._client = httpx.Client(
                base_url=self._base,
                timeout=timeout,
                follow_redirects=True,
                limits=httpx.Limits(
                    max_connections=200,
                    max_keepalive_connections=100,
                ),
            )
            self._owns_client = True
        self._lock = threading.Lock()
        # Buffered transaction state. None -> autocommit; list -> open tx.
        self._tx: list[tuple[str, tuple[Any, ...]]] | None = None
        self._closed = False

    # -- context manager (matches sqlite3.connect(...) used as ctx mgr) ----

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        # sqlite3.Connection's __exit__ commits on success / rolls back on
        # error and does NOT close. We mirror that behaviour because the
        # existing code relies on it (see the ``with sqlite3.connect(...)``
        # blocks in the snapshot endpoint).
        if exc is None:
            try:
                self.commit()
            except Exception:
                self.close()
                raise
        else:
            try:
                self.rollback()
            finally:
                pass

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if not self._owns_client:
            return
        try:
            self._client.close()
        except Exception:  # pragma: no cover - best effort
            LOGGER.debug("rqlite client close failed", exc_info=True)

    # -- core HTTP helpers --------------------------------------------------

    def _post(self, path: str, payload: Any) -> dict[str, Any]:
        try:
            resp = self._client.post(path, json=payload)
        except httpx.HTTPError as exc:
            raise OperationalError(f"rqlite request failed: {exc}") from exc
        if resp.status_code >= 500:
            raise OperationalError(
                f"rqlite returned {resp.status_code}: {resp.text[:200]}"
            )
        if resp.status_code >= 400:
            raise DatabaseError(
                f"rqlite returned {resp.status_code}: {resp.text[:200]}"
            )
        try:
            return resp.json()
        except ValueError as exc:
            raise OperationalError(
                f"rqlite returned non-JSON: {resp.text[:200]}"
            ) from exc

    @staticmethod
    def _check_results(body: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        results = body.get("results") or []
        for r in results:
            err = r.get("error") if isinstance(r, Mapping) else None
            if err:
                low = str(err).lower()
                if "unique" in low or "constraint" in low or "foreign key" in low:
                    raise IntegrityError(err)
                raise DatabaseError(err)
        return list(results)

    @staticmethod
    def _params_to_list(params: Iterable[Any] | None) -> list[Any]:
        if params is None:
            return []
        if isinstance(params, Mapping):
            # Named params: rqlite supports :name binding via {"name": value}
            return [dict(params)]
        return list(params)

    @staticmethod
    def _stmt_array(sql: str, params: Iterable[Any] | None) -> list[Any]:
        plist = Connection._params_to_list(params)
        if plist and isinstance(plist[0], dict):
            return [sql, plist[0]]
        return [sql, *plist]

    # -- statement execution ------------------------------------------------

    def execute(self, sql: str, params: Iterable[Any] | None = None) -> Cursor:
        if self._closed:
            raise OperationalError("connection is closed")

        # Special-case the txn markers: BEGIN [IMMEDIATE], COMMIT, ROLLBACK.
        if _BEGIN_RE.match(sql):
            with self._lock:
                if self._tx is not None:
                    raise OperationalError(
                        "nested BEGIN: rqlite does not support nested transactions"
                    )
                self._tx = []
            return Cursor(self)
        if _COMMIT_RE.match(sql):
            self.commit()
            return Cursor(self)
        if _ROLLBACK_RE.match(sql):
            self.rollback()
            return Cursor(self)

        # PRAGMA foreign_keys: rqlite has FKs on by default; no-op the SET form.
        if _PRAGMA_FK_RE.match(sql):
            cur = Cursor(self)
            cur._set_writeresult(None, 0)
            return cur

        is_read = bool(_SELECT_RE.match(sql))

        with self._lock:
            in_tx = self._tx is not None

        if is_read:
            if in_tx:
                raise UnsupportedTransactionError(
                    "SELECT inside BEGIN/COMMIT is not supported by rqlite. "
                    "Refactor to UPDATE ... RETURNING or commit the batch "
                    "before reading."
                )
            return self._do_query(sql, params)

        # Write statement.
        has_returning = bool(_RETURNING_RE.search(sql))
        if in_tx:
            if has_returning:
                # RETURNING needs to surface row results to the caller
                # immediately; we can't buffer it. The cleanest contract
                # is to refuse RETURNING-inside-tx and require the caller
                # to either commit first or use autocommit.
                raise UnsupportedTransactionError(
                    "RETURNING inside BEGIN/COMMIT is not supported by the "
                    "rqlite adapter. Run the statement in autocommit mode."
                )
            with self._lock:
                assert self._tx is not None
                self._tx.append((sql, tuple(self._params_to_list(params))))
            cur = Cursor(self)
            # We can't know last_insert_id until commit; leave as None.
            return cur
        if has_returning:
            return self._do_request(
                [(sql, tuple(self._params_to_list(params)))]
            )
        return self._do_execute([(sql, tuple(self._params_to_list(params)))])

    def executemany(
        self,
        sql: str,
        seq_of_params: Iterable[Iterable[Any]],
    ) -> Cursor:
        if self._closed:
            raise OperationalError("connection is closed")
        batch = [(sql, tuple(p)) for p in seq_of_params]
        if not batch:
            cur = Cursor(self)
            cur._set_writeresult(None, 0)
            return cur

        with self._lock:
            in_tx = self._tx is not None
            if in_tx:
                assert self._tx is not None
                self._tx.extend(batch)
                cur = Cursor(self)
                cur._set_writeresult(None, len(batch))
                return cur

        return self._do_execute(batch, transaction=True)

    def executescript(self, script: str) -> Cursor:
        """Run a multi-statement SQL script as a single transaction.

        Mirrors :meth:`sqlite3.Connection.executescript`. Splits on ``;``
        outside string literals and posts the whole batch to
        ``/db/execute?transaction=true``.

        rqlite manages its own journaling/sync, so local SQLite tuning
        PRAGMAs (``journal_mode``, ``synchronous``, ``foreign_keys``)
        are silently dropped from the batch -- they would otherwise fail
        the whole transaction.
        """
        if self._closed:
            raise OperationalError("connection is closed")
        raw = [s.strip() for s in _split_sql_statements(script) if s.strip()]
        statements = [s for s in raw if not _UNSUPPORTED_PRAGMA_RE.match(s)]
        if not statements:
            cur = Cursor(self)
            cur._set_writeresult(None, 0)
            return cur
        return self._do_execute([(s, ()) for s in statements], transaction=True)

    # -- transaction control -----------------------------------------------

    def commit(self) -> None:
        if self._closed:
            return
        with self._lock:
            pending = self._tx
            self._tx = None
        if pending:
            self._do_execute(pending, transaction=True)

    def rollback(self) -> None:
        with self._lock:
            self._tx = None

    # -- low-level dispatch -------------------------------------------------

    def _do_query(self, sql: str, params: Iterable[Any] | None) -> Cursor:
        body = self._post(
            f"/db/query?level={self._consistency}",
            [self._stmt_array(sql, params)],
        )
        results = self._check_results(body)
        cur = Cursor(self)
        if not results:
            cur._set_rows((), [])
            return cur
        r0 = results[0]
        columns = r0.get("columns") or ()
        values = r0.get("values") or []
        cur._set_rows(columns, values)
        return cur

    def _do_execute(
        self,
        statements: Sequence[tuple[str, Sequence[Any]]],
        *,
        transaction: bool = False,
    ) -> Cursor:
        path = "/db/execute"
        if transaction and len(statements) > 1:
            path += "?transaction=true"
        payload = [self._stmt_array(s, p) for s, p in statements]
        body = self._post(path, payload)
        results = self._check_results(body)
        cur = Cursor(self)
        if not results:
            cur._set_writeresult(None, 0)
            return cur
        last = results[-1]
        cur._set_writeresult(
            last.get("last_insert_id"),
            sum(int(r.get("rows_affected") or 0) for r in results),
        )
        return cur

    def _do_request(
        self,
        statements: Sequence[tuple[str, Sequence[Any]]],
    ) -> Cursor:
        """Send via ``/db/request`` for statements that mix writes + reads.

        Used when a statement contains a ``RETURNING`` clause: rqlite's
        ``/db/execute`` discards the returned rows, but ``/db/request``
        surfaces them with the same payload shape as ``/db/query``
        (``columns`` + ``values``) plus the write metadata.
        """
        path = f"/db/request?level={self._consistency}"
        payload = [self._stmt_array(s, p) for s, p in statements]
        body = self._post(path, payload)
        results = self._check_results(body)
        cur = Cursor(self)
        if not results:
            cur._set_rows((), [])
            return cur
        # Use the LAST result for both rows and write metadata: this
        # matches the calling convention of the existing code, which
        # always issues one statement at a time when it expects rows.
        last = results[-1]
        columns = last.get("columns") or ()
        values = last.get("values") or []
        cur._set_rows(columns, values)
        # Preserve write metadata in addition to row data so callers
        # that only care about ``rowcount`` / ``lastrowid`` still work.
        cur.lastrowid = last.get("last_insert_id")
        if values:
            cur.rowcount = len(values)
        elif "rows_affected" in last:
            cur.rowcount = int(last.get("rows_affected") or 0)
        return cur


# ---------------------------------------------------------------------------
# Connect entry point
# ---------------------------------------------------------------------------


def connect(
    host: str,
    port: int,
    *,
    timeout: float = 30.0,
    scheme: str = "http",
    consistency: str = "strong",
    client: "httpx.Client | None" = None,
) -> Connection:
    """Open a Connection to an rqlite cluster member.

    ``host`` may be any cluster node; writes are auto-redirected to the
    leader by rqlite (HTTP 301), and httpx follows the redirect. For
    best latency on read-heavy paths point at the closest node.

    ``consistency`` selects the rqlite read level:

    * ``"none"``: stale reads from local follower (fastest)
    * ``"weak"``: leader local read (default in rqlite)
    * ``"strong"``: Raft round-trip per read (default here; safest)
    * ``"linearizable"``: rqlite v8.34+; equivalent to strong with extra checks
    """
    return Connection(
        host,
        port,
        timeout=timeout,
        scheme=scheme,
        consistency=consistency,
        client=client,
    )


# ---------------------------------------------------------------------------
# Internal: simple SQL splitter for executescript()
# ---------------------------------------------------------------------------


def _split_sql_statements(script: str) -> list[str]:
    """Split a SQL script on ``;`` outside of string literals and comments.

    Handles single quotes (with `''` doubled-up escape), `--` line comments,
    and `/* ... */` block comments. Sufficient for the hub's schema.sql,
    which contains no exotic SQL constructs (e.g. no PL/pgSQL DOLLAR-quotes).
    """
    out: list[str] = []
    buf: list[str] = []
    i = 0
    n = len(script)
    in_str = False
    in_line_comment = False
    in_block_comment = False
    while i < n:
        ch = script[i]
        nxt = script[i + 1] if i + 1 < n else ""
        if in_line_comment:
            buf.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue
        if in_block_comment:
            buf.append(ch)
            if ch == "*" and nxt == "/":
                buf.append(nxt)
                i += 2
                in_block_comment = False
                continue
            i += 1
            continue
        if in_str:
            buf.append(ch)
            if ch == "'":
                if nxt == "'":  # escaped single-quote
                    buf.append(nxt)
                    i += 2
                    continue
                in_str = False
            i += 1
            continue
        if ch == "-" and nxt == "-":
            buf.append(ch)
            buf.append(nxt)
            i += 2
            in_line_comment = True
            continue
        if ch == "/" and nxt == "*":
            buf.append(ch)
            buf.append(nxt)
            i += 2
            in_block_comment = True
            continue
        if ch == "'":
            buf.append(ch)
            in_str = True
            i += 1
            continue
        if ch == ";":
            out.append("".join(buf))
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        out.append(tail)
    return out


__all__ = [
    "Connection",
    "Cursor",
    "Row",
    "Error",
    "DatabaseError",
    "OperationalError",
    "IntegrityError",
    "UnsupportedTransactionError",
    "connect",
]
