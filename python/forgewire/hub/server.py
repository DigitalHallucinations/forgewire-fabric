"""PhrenForge remote-subagent blackboard HTTP/SSE service.

Runs on the always-on hub host (currently the OptiPlex 7050). Exposes a small
REST + SSE API used by:

* the dispatcher MCP server (driver host, drives the queue from the main
  agent), reaching the hub over the LAN, and
* the runner MCP server (colocated on the hub), reaching the hub on
  localhost.

Auth: bearer token from ``BLACKBOARD_TOKEN`` env (or ``--token-file`` path).
Storage: SQLite WAL at ``BLACKBOARD_DB_PATH`` (default
``~/.phrenforge/remote_subagent.sqlite3``).

Run::

    python -m forgewire.hub.server --host 0.0.0.0 --port 8765

Hardening notes:
* Default bind is 127.0.0.1 (safe for colocation-only setups). The hub
  launcher (``scripts/remote/start_hub.ps1``) overrides this with 0.0.0.0
  so dispatchers on the LAN can reach it.
* Bearer required on every endpoint except ``/healthz``.
* SQLite is opened per-request via a context manager; WAL handles concurrency.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import secrets
import sqlite3
import time
from collections.abc import AsyncIterator, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from forgewire.hub._crypto import HAS_RUST as _HUB_CRYPTO_HAS_RUST
from forgewire.hub._crypto import verify_signature
from forgewire.hub._router import HAS_RUST as _HUB_ROUTER_HAS_RUST
from forgewire.hub._router import pick_task as _router_pick_task
from forgewire.hub._streams import HAS_RUST as _HUB_STREAMS_HAS_RUST
from forgewire.hub._streams import make_counter as _make_stream_counter

LOGGER = logging.getLogger("phrenforge.remote.blackboard")

DEFAULT_DB = Path.home() / ".phrenforge" / "remote_subagent.sqlite3"
SCHEMA_PATH = Path(__file__).with_name("schema.sql")
PROGRESS_POLL_SECONDS = 1.0
DEFAULT_PORT = 8765

# Protocol/handshake version. The dispatcher and runner both ship this value
# in /runners/register; the hub rejects any peer whose major version differs.
PROTOCOL_VERSION = 2
MIN_COMPATIBLE_PROTOCOL_VERSION = 2

# Heartbeat / state machine thresholds.
HEARTBEAT_DEGRADED_SECONDS = 45
HEARTBEAT_OFFLINE_SECONDS = 120
SIGNATURE_MAX_SKEW_SECONDS = 300

# Resource gate defaults (tasks may override via metadata).
DEFAULT_MIN_RAM_FREE_MB = 512
DEFAULT_MIN_BATTERY_PCT = 20

# Minimum runner version the hub will accept. Override via
# ``BLACKBOARD_MIN_RUNNER_VERSION`` env or ``--min-runner-version`` CLI flag.
DEFAULT_MIN_RUNNER_VERSION = "0.0.0"


def _parse_version(value: str) -> tuple[int, int, int]:
    """Parse a semver-ish ``major.minor.patch`` string.

    Trailing pre-release / build suffixes after ``-`` or ``+`` are ignored.
    Missing components default to ``0``. Non-numeric components also map to
    ``0`` so misconfigured runners sort below any numeric floor.
    """

    if not value:
        return (0, 0, 0)
    head = value.split("-", 1)[0].split("+", 1)[0]
    parts = head.split(".")
    out: list[int] = []
    for part in parts[:3]:
        try:
            out.append(int(part))
        except ValueError:
            out.append(0)
    while len(out) < 3:
        out.append(0)
    return (out[0], out[1], out[2])


# ---------------------------------------------------------------------------
# Storage layer
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class BlackboardConfig:
    db_path: Path
    token: str
    host: str
    port: int
    min_runner_version: str = DEFAULT_MIN_RUNNER_VERSION


class Blackboard:
    """Thin wrapper over the SQLite blackboard schema.

    All public methods take/return plain Python types. The class is intentionally
    procedural -- this module is the boundary, no business logic should leak in.
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()
        # Stage C.3: in-memory per-task stream-seq counter. Resets on hub
        # restart and re-primes lazily from MAX(seq) in SQLite, so kill -9
        # is safe.
        self._stream_counter = _make_stream_counter()

    # ------------------------------------------------------------------ infra

    @contextlib.contextmanager
    def _connect(self) -> Iterable[sqlite3.Connection]:
        conn = sqlite3.connect(
            self._db_path,
            isolation_level=None,  # autocommit; we use BEGIN IMMEDIATE explicitly
            timeout=30.0,
        )
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA foreign_keys = ON")
            yield conn
        finally:
            conn.close()

    def _init_schema(self) -> None:
        sql = SCHEMA_PATH.read_text(encoding="utf-8")
        with self._connect() as conn:
            conn.executescript(sql)
            self._migrate_v2_columns(conn)

    @staticmethod
    def _migrate_v2_columns(conn: sqlite3.Connection) -> None:
        """Idempotently add v2 columns to the legacy ``tasks`` table.

        SQLite < 3.35 has no ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS``,
        so we introspect the schema and add only what's missing. This is
        safe to run on every startup.
        """
        existing = {
            r["name"]
            for r in conn.execute("PRAGMA table_info(tasks)").fetchall()
        }
        additions = [
            ("required_tools", "TEXT NOT NULL DEFAULT '[]'"),
            ("required_tags", "TEXT NOT NULL DEFAULT '[]'"),
            ("tenant", "TEXT"),
            ("workspace_root", "TEXT"),
            ("require_base_commit", "INTEGER NOT NULL DEFAULT 0"),
        ]
        for col, decl in additions:
            if col not in existing:
                conn.execute(f"ALTER TABLE tasks ADD COLUMN {col} {decl}")

    # ----------------------------------------------------------------- tasks

    def create_task(
        self,
        *,
        title: str,
        prompt: str,
        scope_globs: list[str],
        base_commit: str,
        branch: str,
        todo_id: str | None,
        timeout_minutes: int,
        priority: int,
        metadata: dict[str, Any] | None,
        required_tools: list[str] | None = None,
        required_tags: list[str] | None = None,
        tenant: str | None = None,
        workspace_root: str | None = None,
        require_base_commit: bool = False,
    ) -> dict[str, Any]:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                """
                INSERT INTO tasks (
                    todo_id, title, prompt, scope_globs, base_commit, branch,
                    timeout_minutes, priority, metadata,
                    required_tools, required_tags, tenant, workspace_root,
                    require_base_commit
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    todo_id,
                    title,
                    prompt,
                    json.dumps(scope_globs),
                    base_commit,
                    branch,
                    timeout_minutes,
                    priority,
                    json.dumps(metadata or {}),
                    json.dumps(required_tools or []),
                    json.dumps(required_tags or []),
                    tenant,
                    workspace_root,
                    1 if require_base_commit else 0,
                ),
            )
            task_id = cur.lastrowid
            conn.execute("COMMIT")
        return self.get_task(task_id)

    def get_task(self, task_id: int) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if row is None:
                raise KeyError(task_id)
            result_row = conn.execute(
                "SELECT * FROM results WHERE task_id = ?", (task_id,)
            ).fetchone()
        record = _task_row_to_dict(row)
        if result_row is not None:
            record["result"] = _result_row_to_dict(result_row)
        return record

    def list_tasks(
        self,
        *,
        status_filter: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM tasks"
        params: tuple[Any, ...] = ()
        if status_filter:
            query += " WHERE status = ?"
            params = (status_filter,)
        query += " ORDER BY priority DESC, id ASC LIMIT ?"
        params = params + (limit,)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [_task_row_to_dict(r) for r in rows]

    def claim_next_task(
        self,
        *,
        worker_id: str,
        hostname: str | None,
        capabilities: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Atomically transition the highest-priority queued task to claimed."""
        now_iso = _now_iso()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT id FROM tasks
                WHERE status = 'queued' AND cancel_requested = 0
                ORDER BY priority DESC, id ASC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO workers (worker_id, hostname, capabilities, last_seen)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(worker_id) DO UPDATE SET
                        hostname = excluded.hostname,
                        capabilities = excluded.capabilities,
                        last_seen = excluded.last_seen
                    """,
                    (
                        worker_id,
                        hostname,
                        json.dumps(capabilities or {}),
                        now_iso,
                    ),
                )
                conn.execute("COMMIT")
                return None
            task_id = row["id"]
            conn.execute(
                """
                UPDATE tasks
                SET status = 'claimed',
                    worker_id = ?,
                    claimed_at = ?
                WHERE id = ?
                """,
                (worker_id, now_iso, task_id),
            )
            conn.execute(
                """
                INSERT INTO workers (worker_id, hostname, capabilities, last_seen, current_task_id)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET
                    hostname = excluded.hostname,
                    capabilities = excluded.capabilities,
                    last_seen = excluded.last_seen,
                    current_task_id = excluded.current_task_id
                """,
                (
                    worker_id,
                    hostname,
                    json.dumps(capabilities or {}),
                    now_iso,
                    task_id,
                ),
            )
            conn.execute("COMMIT")
        return self.get_task(task_id)

    def mark_running(self, task_id: int) -> dict[str, Any]:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE tasks
                SET status = 'running', started_at = COALESCE(started_at, ?)
                WHERE id = ? AND status IN ('claimed', 'running')
                """,
                (_now_iso(), task_id),
            )
        return self.get_task(task_id)

    def cancel_task(self, task_id: int) -> dict[str, Any]:
        with self._connect() as conn:
            conn.execute(
                "UPDATE tasks SET cancel_requested = 1 WHERE id = ?",
                (task_id,),
            )
            # If still queued, terminate immediately.
            conn.execute(
                """
                UPDATE tasks
                SET status = 'cancelled', completed_at = ?
                WHERE id = ? AND status = 'queued'
                """,
                (_now_iso(), task_id),
            )
        return self.get_task(task_id)

    def submit_result(
        self,
        *,
        task_id: int,
        worker_id: str,
        status_value: str,
        head_commit: str | None,
        commits: list[str],
        files_touched: list[str],
        test_summary: str | None,
        log_tail: str | None,
        error: str | None,
    ) -> dict[str, Any]:
        if status_value not in {"done", "failed", "cancelled", "timed_out"}:
            raise ValueError(f"invalid terminal status: {status_value}")
        now = _now_iso()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT worker_id, status FROM tasks WHERE id = ?",
                (task_id,),
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise KeyError(task_id)
            if row["worker_id"] != worker_id:
                conn.execute("ROLLBACK")
                raise PermissionError(
                    f"worker {worker_id!r} cannot report result for task "
                    f"owned by {row['worker_id']!r}"
                )
            conn.execute(
                """
                INSERT OR REPLACE INTO results (
                    task_id, status, branch, head_commit, commits_json,
                    files_touched, test_summary, log_tail, error, reported_at
                )
                SELECT ?, ?, branch, ?, ?, ?, ?, ?, ?, ?
                FROM tasks WHERE id = ?
                """,
                (
                    task_id,
                    status_value,
                    head_commit,
                    json.dumps(commits),
                    json.dumps(files_touched),
                    test_summary,
                    log_tail,
                    error,
                    now,
                    task_id,
                ),
            )
            conn.execute(
                """
                UPDATE tasks
                SET status = ?, completed_at = ?
                WHERE id = ?
                """,
                (status_value, now, task_id),
            )
            conn.execute(
                "UPDATE workers SET current_task_id = NULL, last_seen = ? WHERE worker_id = ?",
                (now, worker_id),
            )
            conn.execute("COMMIT")
        return self.get_task(task_id)

    # -------------------------------------------------------------- progress

    def append_progress(
        self,
        *,
        task_id: int,
        worker_id: str,
        message: str,
        files_touched: list[str] | None,
    ) -> dict[str, Any]:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT worker_id FROM tasks WHERE id = ?",
                (task_id,),
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise KeyError(task_id)
            if row["worker_id"] != worker_id:
                conn.execute("ROLLBACK")
                raise PermissionError("worker mismatch on progress")
            seq_row = conn.execute(
                "SELECT COALESCE(MAX(seq), 0) AS s FROM progress WHERE task_id = ?",
                (task_id,),
            ).fetchone()
            next_seq = int(seq_row["s"]) + 1
            cur = conn.execute(
                """
                INSERT INTO progress (task_id, seq, message, files_touched)
                VALUES (?, ?, ?, ?)
                """,
                (
                    task_id,
                    next_seq,
                    message,
                    json.dumps(files_touched or []),
                ),
            )
            entry_id = cur.lastrowid
            conn.execute(
                "UPDATE workers SET last_seen = ? WHERE worker_id = ?",
                (_now_iso(), worker_id),
            )
            conn.execute("COMMIT")
        return {
            "id": entry_id,
            "task_id": task_id,
            "seq": next_seq,
            "message": message,
            "files_touched": files_touched or [],
        }

    def progress_since(
        self, *, task_id: int, after_seq: int
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, task_id, seq, message, files_touched, created_at
                FROM progress
                WHERE task_id = ? AND seq > ?
                ORDER BY seq ASC
                """,
                (task_id, after_seq),
            ).fetchall()
        return [
            {
                "id": r["id"],
                "task_id": r["task_id"],
                "seq": r["seq"],
                "message": r["message"],
                "files_touched": json.loads(r["files_touched"]),
                "created_at": r["created_at"],
            }
            for r in rows
        ]

    # ----------------------------------------------------------------- notes

    def post_note(
        self, *, task_id: int, author: str, body: str
    ) -> dict[str, Any]:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT 1 FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise KeyError(task_id)
            cur = conn.execute(
                "INSERT INTO notes (task_id, author, body) VALUES (?, ?, ?)",
                (task_id, author, body),
            )
            note_id = cur.lastrowid
            conn.execute("COMMIT")
        return {"id": note_id, "task_id": task_id, "author": author, "body": body}

    def read_notes(
        self, *, task_id: int, after_id: int = 0
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, task_id, author, body, created_at
                FROM notes
                WHERE task_id = ? AND id > ?
                ORDER BY id ASC
                """,
                (task_id, after_id),
            ).fetchall()
        return [dict(r) for r in rows]

    # ---------------------------------------------------------------- streams

    def append_stream(
        self,
        *,
        task_id: int,
        worker_id: str,
        channel: str,
        line: str,
    ) -> dict[str, Any]:
        if channel not in {"stdout", "stderr", "info"}:
            raise ValueError(f"invalid stream channel: {channel}")
        with self._connect() as conn:
            # Worker-ownership check is read-only; no BEGIN IMMEDIATE needed.
            row = conn.execute(
                "SELECT worker_id FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if row is None:
                raise KeyError(task_id)
            if row["worker_id"] != worker_id:
                raise PermissionError("worker mismatch on stream append")
            # Lazy-prime the in-memory seq counter from SQLite. Idempotent:
            # the counter only accepts a higher floor, so concurrent racers
            # can't push it backwards.
            if not self._stream_counter.is_primed(task_id):
                seq_row = conn.execute(
                    "SELECT COALESCE(MAX(seq), 0) AS s FROM task_streams WHERE task_id = ?",
                    (task_id,),
                ).fetchone()
                self._stream_counter.prime(task_id, int(seq_row["s"]))
            next_seq = self._stream_counter.next_seq(task_id)
            cur = conn.execute(
                """
                INSERT INTO task_streams (task_id, seq, channel, line)
                VALUES (?, ?, ?, ?)
                """,
                (task_id, next_seq, channel, line),
            )
            entry_id = cur.lastrowid
        return {
            "id": entry_id,
            "task_id": task_id,
            "seq": next_seq,
            "channel": channel,
            "line": line,
        }

    def append_stream_bulk(
        self,
        *,
        task_id: int,
        worker_id: str,
        entries: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        """Append many stream entries in a single transaction.

        ``entries`` is a sequence of ``{"channel": ..., "line": ...}``
        mappings. ``worker_id`` is checked once against the task's owner.
        All inserts share one ``BEGIN IMMEDIATE`` / ``COMMIT`` so the WAL
        fsync cost amortises across the whole batch — this is the
        throughput payoff for the in-memory ``StreamCounter`` (todo 113
        Stage C.3 follow-up).

        Returns the count of inserted rows and the first/last seq numbers
        assigned. Empty batches are a no-op (returns ``count=0``).
        """
        if not entries:
            return {"task_id": task_id, "count": 0, "first_seq": None, "last_seq": None}
        for idx, entry in enumerate(entries):
            channel = entry.get("channel")
            if channel not in {"stdout", "stderr", "info"}:
                raise ValueError(
                    f"invalid stream channel at index {idx}: {channel!r}"
                )
            if not isinstance(entry.get("line"), str):
                raise ValueError(
                    f"missing or non-string 'line' at index {idx}"
                )

        with self._connect() as conn:
            row = conn.execute(
                "SELECT worker_id FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if row is None:
                raise KeyError(task_id)
            if row["worker_id"] != worker_id:
                raise PermissionError("worker mismatch on stream bulk append")
            if not self._stream_counter.is_primed(task_id):
                seq_row = conn.execute(
                    "SELECT COALESCE(MAX(seq), 0) AS s FROM task_streams WHERE task_id = ?",
                    (task_id,),
                ).fetchone()
                self._stream_counter.prime(task_id, int(seq_row["s"]))

            assigned: list[tuple[int, str, int, str]] = []
            for entry in entries:
                seq = self._stream_counter.next_seq(task_id)
                assigned.append(
                    (task_id, seq, str(entry["channel"]), str(entry["line"]))
                )

            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.executemany(
                    """
                    INSERT INTO task_streams (task_id, seq, channel, line)
                    VALUES (?, ?, ?, ?)
                    """,
                    assigned,
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

        return {
            "task_id": task_id,
            "count": len(assigned),
            "first_seq": assigned[0][1],
            "last_seq": assigned[-1][1],
        }

    def streams_since(
        self, *, task_id: int, after_seq: int, limit: int = 500
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, task_id, seq, channel, line, created_at
                FROM task_streams
                WHERE task_id = ? AND seq > ?
                ORDER BY seq ASC
                LIMIT ?
                """,
                (task_id, after_seq, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    # ---------------------------------------------------------------- runners

    def upsert_runner(self, record: dict[str, Any]) -> dict[str, Any]:
        """Insert or update a runner registration row.

        Caller must have already verified the signature and protocol version.
        """
        now = _now_iso()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT public_key, first_seen FROM runners WHERE runner_id = ?",
                (record["runner_id"],),
            ).fetchone()
            if existing is not None and existing["public_key"] != record["public_key"]:
                conn.execute("ROLLBACK")
                raise PermissionError(
                    "runner_id is already bound to a different public_key"
                )
            first_seen = existing["first_seen"] if existing else now
            conn.execute(
                """
                INSERT INTO runners (
                    runner_id, public_key, hostname, os, arch, cpu_model,
                    cpu_count, ram_mb, gpu, tools, tags, scope_prefixes,
                    tenant, workspace_root, runner_version, protocol_version,
                    max_concurrent, state, drain_requested, metadata,
                    first_seen, last_heartbeat
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(runner_id) DO UPDATE SET
                    public_key       = excluded.public_key,
                    hostname         = excluded.hostname,
                    os               = excluded.os,
                    arch             = excluded.arch,
                    cpu_model        = excluded.cpu_model,
                    cpu_count        = excluded.cpu_count,
                    ram_mb           = excluded.ram_mb,
                    gpu              = excluded.gpu,
                    tools            = excluded.tools,
                    tags             = excluded.tags,
                    scope_prefixes   = excluded.scope_prefixes,
                    tenant           = excluded.tenant,
                    workspace_root   = excluded.workspace_root,
                    runner_version   = excluded.runner_version,
                    protocol_version = excluded.protocol_version,
                    max_concurrent   = excluded.max_concurrent,
                    state            = 'online',
                    drain_requested  = 0,
                    metadata         = excluded.metadata,
                    last_heartbeat   = excluded.last_heartbeat
                """,
                (
                    record["runner_id"],
                    record["public_key"],
                    record["hostname"],
                    record["os"],
                    record["arch"],
                    record.get("cpu_model"),
                    record.get("cpu_count"),
                    record.get("ram_mb"),
                    record.get("gpu"),
                    json.dumps(record.get("tools", [])),
                    json.dumps(record.get("tags", [])),
                    json.dumps(record.get("scope_prefixes", [])),
                    record.get("tenant"),
                    record.get("workspace_root"),
                    record["runner_version"],
                    int(record["protocol_version"]),
                    int(record.get("max_concurrent", 1)),
                    "online",
                    0,
                    json.dumps(record.get("metadata", {})),
                    first_seen,
                    now,
                ),
            )
            conn.execute("COMMIT")
        return self.get_runner(record["runner_id"])

    def get_runner(self, runner_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM runners WHERE runner_id = ?", (runner_id,)
            ).fetchone()
        if row is None:
            raise KeyError(runner_id)
        return _runner_row_to_dict(row)

    def list_runners(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM runners ORDER BY hostname, runner_id"
            ).fetchall()
        out = []
        for row in rows:
            record = _runner_row_to_dict(row)
            record["state"] = self._derive_state(record)
            record["current_load"] = self._current_load(row["runner_id"])
            out.append(record)
        return out

    def heartbeat_runner(
        self,
        *,
        runner_id: str,
        cpu_load_pct: float | None,
        ram_free_mb: int | None,
        battery_pct: int | None,
        on_battery: bool,
        last_known_commit: str | None,
        nonce: str,
    ) -> dict[str, Any]:
        now = _now_iso()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT runner_id, last_nonce FROM runners WHERE runner_id = ?",
                (runner_id,),
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise KeyError(runner_id)
            if row["last_nonce"] is not None and row["last_nonce"] == nonce:
                conn.execute("ROLLBACK")
                raise PermissionError("nonce replay rejected")
            conn.execute(
                """
                UPDATE runners
                SET last_heartbeat = ?,
                    cpu_load_pct   = ?,
                    ram_free_mb    = ?,
                    battery_pct    = ?,
                    on_battery     = ?,
                    last_known_commit = COALESCE(?, last_known_commit),
                    last_nonce     = ?,
                    state          = CASE
                                       WHEN drain_requested = 1 THEN 'draining'
                                       ELSE 'online'
                                     END
                WHERE runner_id = ?
                """,
                (
                    now,
                    cpu_load_pct,
                    ram_free_mb,
                    battery_pct,
                    1 if on_battery else 0,
                    last_known_commit,
                    nonce,
                    runner_id,
                ),
            )
            conn.execute("COMMIT")
        return self.get_runner(runner_id)

    def request_drain(self, runner_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE runners
                SET drain_requested = 1,
                    state           = 'draining'
                WHERE runner_id = ?
                """,
                (runner_id,),
            )
            if cur.rowcount == 0:
                raise KeyError(runner_id)
        return self.get_runner(runner_id)

    def runner_public_key(self, runner_id: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT public_key FROM runners WHERE runner_id = ?", (runner_id,)
            ).fetchone()
        return row["public_key"] if row else None

    @staticmethod
    def _derive_state(runner: dict[str, Any]) -> str:
        if runner.get("drain_requested"):
            return "draining"
        try:
            last = time.strptime(runner["last_heartbeat"], "%Y-%m-%dT%H:%M:%SZ")
            age = time.time() - time.mktime(last) + time.timezone
        except Exception:
            return runner.get("state") or "online"
        if age >= HEARTBEAT_OFFLINE_SECONDS:
            return "offline"
        if age >= HEARTBEAT_DEGRADED_SECONDS:
            return "degraded"
        return "online"

    def _current_load(self, runner_id: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS n FROM tasks
                WHERE worker_id = ? AND status IN ('claimed', 'running', 'reporting')
                """,
                (runner_id,),
            ).fetchone()
        return int(row["n"]) if row else 0

    def claim_next_task_v2(
        self,
        *,
        runner_id: str,
        scope_prefixes: list[str],
        tools: list[str],
        tags: list[str],
        tenant: str | None,
        workspace_root: str | None,
        last_known_commit: str | None,
        cpu_load_pct: float | None,
        ram_free_mb: int | None,
        battery_pct: int | None,
        on_battery: bool,
    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        """Capability-aware task claim.

        Returns ``(task_or_none, info)`` where ``info`` is a structured
        no-match diagnostic when no task is handed out, including the
        refusal reason ('queue_empty', 'no_eligible_runner', 'drain',
        'concurrency_cap', 'resource_gate', 'base_commit_mismatch').
        """
        info: dict[str, Any] = {"reason": "queue_empty", "candidates_seen": 0}
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            runner_row = conn.execute(
                "SELECT * FROM runners WHERE runner_id = ?", (runner_id,)
            ).fetchone()
            if runner_row is None:
                conn.execute("ROLLBACK")
                raise KeyError(runner_id)
            if runner_row["drain_requested"]:
                conn.execute("ROLLBACK")
                info["reason"] = "drain"
                return None, info
            current_load = conn.execute(
                """
                SELECT COUNT(*) AS n FROM tasks
                WHERE worker_id = ? AND status IN ('claimed', 'running', 'reporting')
                """,
                (runner_id,),
            ).fetchone()["n"]
            if current_load >= int(runner_row["max_concurrent"]):
                conn.execute("ROLLBACK")
                info["reason"] = "concurrency_cap"
                info["current_load"] = current_load
                info["max_concurrent"] = int(runner_row["max_concurrent"])
                return None, info
            # Resource gates.
            if ram_free_mb is not None and ram_free_mb < DEFAULT_MIN_RAM_FREE_MB:
                conn.execute("ROLLBACK")
                info["reason"] = "resource_gate"
                info["detail"] = f"ram_free_mb {ram_free_mb} < {DEFAULT_MIN_RAM_FREE_MB}"
                return None, info
            if on_battery and battery_pct is not None and battery_pct < DEFAULT_MIN_BATTERY_PCT:
                conn.execute("ROLLBACK")
                info["reason"] = "resource_gate"
                info["detail"] = f"on battery {battery_pct}% < {DEFAULT_MIN_BATTERY_PCT}"
                return None, info
            rows = conn.execute(
                """
                SELECT * FROM tasks
                WHERE status = 'queued' AND cancel_requested = 0
                ORDER BY priority DESC, id ASC
                LIMIT 50
                """,
            ).fetchall()
            if not rows:
                conn.execute("ROLLBACK")
                return None, info
            # Build candidate dicts the router facade can consume.
            candidates: list[dict[str, Any]] = []
            for row in rows:
                candidates.append(
                    {
                        "scope_globs": json.loads(row["scope_globs"]),
                        "required_tools": json.loads(row["required_tools"] or "[]"),
                        "required_tags": json.loads(row["required_tags"] or "[]"),
                        "tenant": row["tenant"],
                        "workspace_root": row["workspace_root"],
                        "require_base_commit": bool(row["require_base_commit"]),
                        "base_commit": row["base_commit"] or "",
                    }
                )
            runner_view = {
                "scope_prefixes": scope_prefixes,
                "tools": tools,
                "tags": tags,
                "tenant": tenant,
                "workspace_root": workspace_root,
                "last_known_commit": last_known_commit,
            }
            picked_idx, candidates_seen = _router_pick_task(candidates, runner_view)
            info["candidates_seen"] = candidates_seen
            chosen: sqlite3.Row | None = (
                rows[picked_idx] if picked_idx is not None else None
            )
            if chosen is None:
                conn.execute("ROLLBACK")
                info["reason"] = "no_eligible_runner"
                return None, info
            task_id = chosen["id"]
            now = _now_iso()
            conn.execute(
                """
                UPDATE tasks
                SET status = 'claimed', worker_id = ?, claimed_at = ?
                WHERE id = ?
                """,
                (runner_id, now, task_id),
            )
            # Maintain legacy workers row for backcompat consumers.
            conn.execute(
                """
                INSERT INTO workers (worker_id, hostname, capabilities, last_seen, current_task_id)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET
                    hostname        = excluded.hostname,
                    capabilities    = excluded.capabilities,
                    last_seen       = excluded.last_seen,
                    current_task_id = excluded.current_task_id
                """,
                (
                    runner_id,
                    runner_row["hostname"],
                    json.dumps(
                        {
                            "tools": tools,
                            "tags": tags,
                            "scope_prefixes": scope_prefixes,
                        }
                    ),
                    now,
                    task_id,
                ),
            )
            conn.execute("COMMIT")
        return self.get_task(task_id), {"reason": "claimed"}


def _task_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    record = dict(row)
    record["scope_globs"] = json.loads(record["scope_globs"])
    record["metadata"] = json.loads(record["metadata"])
    record["cancel_requested"] = bool(record["cancel_requested"])
    if "required_tools" in record and isinstance(record["required_tools"], str):
        record["required_tools"] = json.loads(record["required_tools"])
    if "required_tags" in record and isinstance(record["required_tags"], str):
        record["required_tags"] = json.loads(record["required_tags"])
    if "require_base_commit" in record:
        record["require_base_commit"] = bool(record["require_base_commit"])
    return record


def _result_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    record = dict(row)
    record["commits"] = json.loads(record.pop("commits_json"))
    record["files_touched"] = json.loads(record["files_touched"])
    return record


def _runner_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    record = dict(row)
    record["tools"] = json.loads(record["tools"])
    record["tags"] = json.loads(record["tags"])
    record["scope_prefixes"] = json.loads(record["scope_prefixes"])
    record["metadata"] = json.loads(record["metadata"])
    record["drain_requested"] = bool(record["drain_requested"])
    record["on_battery"] = bool(record["on_battery"])
    # Never leak internal nonce.
    record.pop("last_nonce", None)
    return record


def _glob_static_prefix(glob: str) -> str:
    """Return the leading static (wildcard-free) prefix of a glob.

    e.g. ``modules/jobs/**`` -> ``modules/jobs/``,
         ``tests/**/test_x.py`` -> ``tests/``.
    """
    norm = glob.replace("\\", "/")
    cut = len(norm)
    for ch in ("*", "?", "["):
        idx = norm.find(ch)
        if idx != -1 and idx < cut:
            cut = idx
    head = norm[:cut]
    if "/" in head:
        head = head.rsplit("/", 1)[0] + "/"
    return head


def _scopes_within(task_globs: list[str], runner_prefixes: list[str]) -> bool:
    """True iff every task glob's static prefix is contained in some runner prefix."""
    for glob in task_globs:
        head = _glob_static_prefix(glob)
        if not any(head.startswith(p) or p.startswith(head) for p in runner_prefixes):
            return False
    return True


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# ---------------------------------------------------------------------------
# Pydantic request/response schemas
# ---------------------------------------------------------------------------


class DispatchTaskRequest(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)
    prompt: str = Field(..., min_length=1)
    scope_globs: list[str] = Field(..., min_length=1)
    base_commit: str = Field(..., min_length=7, max_length=64)
    branch: str = Field(..., min_length=1, max_length=200)
    todo_id: str | None = None
    timeout_minutes: int = Field(default=60, ge=1, le=720)
    priority: int = Field(default=100, ge=0, le=10_000)
    metadata: dict[str, Any] | None = None
    required_tools: list[str] | None = None
    required_tags: list[str] | None = None
    tenant: str | None = None
    workspace_root: str | None = None
    require_base_commit: bool = False


class ClaimRequest(BaseModel):
    worker_id: str = Field(..., min_length=1, max_length=120)
    hostname: str | None = None
    capabilities: dict[str, Any] | None = None


class ClaimV2Request(BaseModel):
    runner_id: str = Field(..., min_length=8, max_length=120)
    timestamp: int
    nonce: str = Field(..., min_length=8, max_length=80)
    signature: str
    scope_prefixes: list[str] = Field(default_factory=list)
    tools: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    tenant: str | None = None
    workspace_root: str | None = None
    last_known_commit: str | None = None
    cpu_load_pct: float | None = None
    ram_free_mb: int | None = None
    battery_pct: int | None = None
    on_battery: bool = False


class RegisterRequest(BaseModel):
    runner_id: str = Field(..., min_length=8, max_length=120)
    public_key: str = Field(..., min_length=64, max_length=64)
    protocol_version: int
    runner_version: str = Field(..., min_length=1, max_length=80)
    hostname: str = Field(..., min_length=1, max_length=200)
    os: str = Field(..., min_length=1, max_length=200)
    arch: str = Field(..., min_length=1, max_length=64)
    cpu_model: str | None = None
    cpu_count: int | None = None
    ram_mb: int | None = None
    gpu: str | None = None
    tools: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    scope_prefixes: list[str] = Field(default_factory=list)
    tenant: str | None = None
    workspace_root: str | None = None
    max_concurrent: int = Field(default=1, ge=1, le=64)
    metadata: dict[str, Any] | None = None
    timestamp: int
    nonce: str = Field(..., min_length=8, max_length=80)
    signature: str


class HeartbeatRequest(BaseModel):
    runner_id: str
    timestamp: int
    nonce: str = Field(..., min_length=8, max_length=80)
    signature: str
    cpu_load_pct: float | None = None
    ram_free_mb: int | None = None
    battery_pct: int | None = None
    on_battery: bool = False
    last_known_commit: str | None = None


class DrainRequest(BaseModel):
    runner_id: str
    timestamp: int
    nonce: str = Field(..., min_length=8, max_length=80)
    signature: str


class StreamRequest(BaseModel):
    worker_id: str
    channel: str = Field(..., pattern="^(stdout|stderr|info)$")
    line: str


class StreamBulkEntry(BaseModel):
    channel: str = Field(..., pattern="^(stdout|stderr|info)$")
    line: str


class StreamBulkRequest(BaseModel):
    worker_id: str
    entries: list[StreamBulkEntry] = Field(default_factory=list)


class ProgressRequest(BaseModel):
    worker_id: str
    message: str = Field(..., min_length=1)
    files_touched: list[str] | None = None


class ResultRequest(BaseModel):
    worker_id: str
    status: str
    head_commit: str | None = None
    commits: list[str] = Field(default_factory=list)
    files_touched: list[str] = Field(default_factory=list)
    test_summary: str | None = None
    log_tail: str | None = None
    error: str | None = None


class NoteRequest(BaseModel):
    author: str = Field(..., min_length=1, max_length=80)
    body: str = Field(..., min_length=1)


# ---------------------------------------------------------------------------
# FastAPI app factory
# ---------------------------------------------------------------------------


def create_app(config: BlackboardConfig) -> FastAPI:
    app = FastAPI(
        title="PhrenForge Remote Subagent Blackboard",
        version="0.1.0",
    )
    blackboard = Blackboard(config.db_path)
    app.state.blackboard = blackboard
    app.state.token = config.token

    async def require_auth(request: Request) -> None:
        header = request.headers.get("authorization", "")
        if not header.lower().startswith("bearer "):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="missing bearer token",
            )
        presented = header.split(" ", 1)[1].strip()
        if not secrets.compare_digest(presented, app.state.token):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="invalid bearer token",
            )

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {
            "status": "ok",
            "version": app.version,
            "protocol_version": PROTOCOL_VERSION,
            "rust_crypto": _HUB_CRYPTO_HAS_RUST,
            "rust_router": _HUB_ROUTER_HAS_RUST,
            "rust_streams": _HUB_STREAMS_HAS_RUST,
        }

    @app.post("/tasks", dependencies=[Depends(require_auth)])
    async def dispatch_task(payload: DispatchTaskRequest) -> dict[str, Any]:
        task = blackboard.create_task(
            title=payload.title,
            prompt=payload.prompt,
            scope_globs=payload.scope_globs,
            base_commit=payload.base_commit,
            branch=payload.branch,
            todo_id=payload.todo_id,
            timeout_minutes=payload.timeout_minutes,
            priority=payload.priority,
            metadata=payload.metadata,
            required_tools=payload.required_tools,
            required_tags=payload.required_tags,
            tenant=payload.tenant,
            workspace_root=payload.workspace_root,
            require_base_commit=payload.require_base_commit,
        )
        return task

    @app.get("/tasks", dependencies=[Depends(require_auth)])
    async def list_tasks(
        status: str | None = None, limit: int = 100
    ) -> dict[str, Any]:
        return {"tasks": blackboard.list_tasks(status_filter=status, limit=limit)}

    @app.get("/tasks/{task_id}", dependencies=[Depends(require_auth)])
    async def get_task(task_id: int) -> dict[str, Any]:
        try:
            return blackboard.get_task(task_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="task not found") from exc

    @app.post("/tasks/claim", dependencies=[Depends(require_auth)])
    async def claim_task(payload: ClaimRequest) -> JSONResponse:
        task = blackboard.claim_next_task(
            worker_id=payload.worker_id,
            hostname=payload.hostname,
            capabilities=payload.capabilities,
        )
        # Always 200 with body. 204 cannot carry a body, and a missing body is
        # harder for clients to parse than the explicit {"task": null} envelope.
        return JSONResponse(content={"task": task})

    # ----- v2 runner registry / handshake / heartbeat / drain --------------

    def _signed_payload(payload: dict[str, Any]) -> bytes:
        # Canonical JSON over the signed fields. Order is fixed so the runner
        # and hub produce byte-identical signing inputs.
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )

    def _check_skew(timestamp: int) -> None:
        now = int(time.time())
        if abs(now - int(timestamp)) > SIGNATURE_MAX_SKEW_SECONDS:
            raise HTTPException(status_code=401, detail="timestamp out of skew window")

    @app.post("/runners/register", dependencies=[Depends(require_auth)])
    async def register_runner(payload: RegisterRequest) -> dict[str, Any]:
        if payload.protocol_version != PROTOCOL_VERSION:
            if payload.protocol_version < MIN_COMPATIBLE_PROTOCOL_VERSION:
                raise HTTPException(
                    status_code=426,
                    detail=(
                        f"runner protocol_version={payload.protocol_version} "
                        f"is older than the hub's minimum "
                        f"{MIN_COMPATIBLE_PROTOCOL_VERSION}"
                    ),
                )
            if payload.protocol_version > PROTOCOL_VERSION:
                raise HTTPException(
                    status_code=426,
                    detail=(
                        f"runner protocol_version={payload.protocol_version} "
                        f"is newer than the hub's {PROTOCOL_VERSION}"
                    ),
                )
        if _parse_version(payload.runner_version) < _parse_version(
            config.min_runner_version
        ):
            raise HTTPException(
                status_code=426,
                detail=(
                    f"runner_version={payload.runner_version} is below the "
                    f"hub's minimum {config.min_runner_version}"
                ),
            )
        _check_skew(payload.timestamp)
        signed = _signed_payload(
            {
                "op": "register",
                "runner_id": payload.runner_id,
                "public_key": payload.public_key,
                "protocol_version": payload.protocol_version,
                "timestamp": payload.timestamp,
                "nonce": payload.nonce,
            }
        )
        if not verify_signature(payload.public_key, signed, payload.signature):
            raise HTTPException(status_code=403, detail="invalid registration signature")
        try:
            record = blackboard.upsert_runner(
                {
                    "runner_id": payload.runner_id,
                    "public_key": payload.public_key,
                    "hostname": payload.hostname,
                    "os": payload.os,
                    "arch": payload.arch,
                    "cpu_model": payload.cpu_model,
                    "cpu_count": payload.cpu_count,
                    "ram_mb": payload.ram_mb,
                    "gpu": payload.gpu,
                    "tools": payload.tools,
                    "tags": payload.tags,
                    "scope_prefixes": payload.scope_prefixes,
                    "tenant": payload.tenant,
                    "workspace_root": payload.workspace_root,
                    "runner_version": payload.runner_version,
                    "protocol_version": payload.protocol_version,
                    "max_concurrent": payload.max_concurrent,
                    "metadata": payload.metadata or {},
                }
            )
        except PermissionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {
            "hub_protocol_version": PROTOCOL_VERSION,
            "runner": record,
        }

    def _verify_runner_signature(
        *,
        op: str,
        runner_id: str,
        timestamp: int,
        nonce: str,
        signature: str,
        extra: dict[str, Any] | None = None,
    ) -> None:
        _check_skew(timestamp)
        public_key = blackboard.runner_public_key(runner_id)
        if public_key is None:
            raise HTTPException(status_code=404, detail="runner not registered")
        body = {
            "op": op,
            "runner_id": runner_id,
            "timestamp": timestamp,
            "nonce": nonce,
        }
        if extra:
            body.update(extra)
        if not verify_signature(public_key, _signed_payload(body), signature):
            raise HTTPException(status_code=403, detail="invalid runner signature")

    @app.get("/runners", dependencies=[Depends(require_auth)])
    async def list_runners() -> dict[str, Any]:
        return {
            "hub_protocol_version": PROTOCOL_VERSION,
            "runners": blackboard.list_runners(),
        }

    @app.post("/runners/{runner_id}/heartbeat", dependencies=[Depends(require_auth)])
    async def heartbeat_runner(runner_id: str, payload: HeartbeatRequest) -> dict[str, Any]:
        if runner_id != payload.runner_id:
            raise HTTPException(status_code=400, detail="runner_id mismatch")
        _verify_runner_signature(
            op="heartbeat",
            runner_id=payload.runner_id,
            timestamp=payload.timestamp,
            nonce=payload.nonce,
            signature=payload.signature,
        )
        try:
            record = blackboard.heartbeat_runner(
                runner_id=payload.runner_id,
                cpu_load_pct=payload.cpu_load_pct,
                ram_free_mb=payload.ram_free_mb,
                battery_pct=payload.battery_pct,
                on_battery=payload.on_battery,
                last_known_commit=payload.last_known_commit,
                nonce=payload.nonce,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="runner not registered") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return record

    @app.post("/runners/{runner_id}/drain", dependencies=[Depends(require_auth)])
    async def drain_runner(runner_id: str, payload: DrainRequest) -> dict[str, Any]:
        if runner_id != payload.runner_id:
            raise HTTPException(status_code=400, detail="runner_id mismatch")
        _verify_runner_signature(
            op="drain",
            runner_id=payload.runner_id,
            timestamp=payload.timestamp,
            nonce=payload.nonce,
            signature=payload.signature,
        )
        try:
            return blackboard.request_drain(payload.runner_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="runner not registered") from exc

    @app.post("/runners/{runner_id}/drain-by-dispatcher", dependencies=[Depends(require_auth)])
    async def drain_runner_by_dispatcher(runner_id: str) -> dict[str, Any]:
        """Dispatcher-initiated drain. Bearer-only (no runner signature)."""
        try:
            return blackboard.request_drain(runner_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="runner not registered") from exc

    @app.post("/tasks/claim-v2", dependencies=[Depends(require_auth)])
    async def claim_task_v2(payload: ClaimV2Request) -> JSONResponse:
        _verify_runner_signature(
            op="claim",
            runner_id=payload.runner_id,
            timestamp=payload.timestamp,
            nonce=payload.nonce,
            signature=payload.signature,
        )
        try:
            task, info = blackboard.claim_next_task_v2(
                runner_id=payload.runner_id,
                scope_prefixes=payload.scope_prefixes,
                tools=payload.tools,
                tags=payload.tags,
                tenant=payload.tenant,
                workspace_root=payload.workspace_root,
                last_known_commit=payload.last_known_commit,
                cpu_load_pct=payload.cpu_load_pct,
                ram_free_mb=payload.ram_free_mb,
                battery_pct=payload.battery_pct,
                on_battery=payload.on_battery,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="runner not registered") from exc
        return JSONResponse(content={"task": task, "info": info})

    @app.post("/tasks/{task_id}/start", dependencies=[Depends(require_auth)])
    async def mark_running(task_id: int) -> dict[str, Any]:
        try:
            return blackboard.mark_running(task_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="task not found") from exc

    @app.post("/tasks/{task_id}/cancel", dependencies=[Depends(require_auth)])
    async def cancel_task(task_id: int) -> dict[str, Any]:
        try:
            return blackboard.cancel_task(task_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="task not found") from exc

    @app.post("/tasks/{task_id}/progress", dependencies=[Depends(require_auth)])
    async def append_progress(
        task_id: int, payload: ProgressRequest
    ) -> dict[str, Any]:
        try:
            return blackboard.append_progress(
                task_id=task_id,
                worker_id=payload.worker_id,
                message=payload.message,
                files_touched=payload.files_touched,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="task not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc

    @app.post("/tasks/{task_id}/stream", dependencies=[Depends(require_auth)])
    async def append_stream(
        task_id: int, payload: StreamRequest
    ) -> dict[str, Any]:
        try:
            return blackboard.append_stream(
                task_id=task_id,
                worker_id=payload.worker_id,
                channel=payload.channel,
                line=payload.line,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="task not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/tasks/{task_id}/stream/bulk", dependencies=[Depends(require_auth)]
    )
    async def append_stream_bulk(
        task_id: int, payload: StreamBulkRequest
    ) -> dict[str, Any]:
        try:
            return blackboard.append_stream_bulk(
                task_id=task_id,
                worker_id=payload.worker_id,
                entries=[e.model_dump() for e in payload.entries],
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="task not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/tasks/{task_id}/stream", dependencies=[Depends(require_auth)])
    async def read_stream(
        task_id: int, after_seq: int = 0, limit: int = 500
    ) -> dict[str, Any]:
        return {
            "lines": blackboard.streams_since(
                task_id=task_id, after_seq=after_seq, limit=limit
            )
        }

    @app.post("/tasks/{task_id}/result", dependencies=[Depends(require_auth)])
    async def submit_result(
        task_id: int, payload: ResultRequest
    ) -> dict[str, Any]:
        try:
            return blackboard.submit_result(
                task_id=task_id,
                worker_id=payload.worker_id,
                status_value=payload.status,
                head_commit=payload.head_commit,
                commits=payload.commits,
                files_touched=payload.files_touched,
                test_summary=payload.test_summary,
                log_tail=payload.log_tail,
                error=payload.error,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="task not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/tasks/{task_id}/notes", dependencies=[Depends(require_auth)])
    async def post_note(task_id: int, payload: NoteRequest) -> dict[str, Any]:
        try:
            return blackboard.post_note(
                task_id=task_id, author=payload.author, body=payload.body
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="task not found") from exc

    @app.get("/tasks/{task_id}/notes", dependencies=[Depends(require_auth)])
    async def read_notes(task_id: int, after_id: int = 0) -> dict[str, Any]:
        return {"notes": blackboard.read_notes(task_id=task_id, after_id=after_id)}

    @app.get("/tasks/{task_id}/events", dependencies=[Depends(require_auth)])
    async def task_events(task_id: int, request: Request) -> EventSourceResponse:
        async def stream() -> AsyncIterator[dict[str, Any]]:
            last_seq = 0
            terminal = {"done", "failed", "cancelled", "timed_out"}
            while True:
                if await request.is_disconnected():
                    return
                try:
                    task = blackboard.get_task(task_id)
                except KeyError:
                    yield {"event": "error", "data": json.dumps({"error": "not_found"})}
                    return
                progress = blackboard.progress_since(
                    task_id=task_id, after_seq=last_seq
                )
                for entry in progress:
                    last_seq = entry["seq"]
                    yield {"event": "progress", "data": json.dumps(entry)}
                yield {"event": "task", "data": json.dumps(task)}
                if task["status"] in terminal:
                    return
                await asyncio.sleep(PROGRESS_POLL_SECONDS)

        return EventSourceResponse(stream())

    return app


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _load_token(args: argparse.Namespace) -> str:
    if args.token_file:
        token = Path(args.token_file).read_text(encoding="utf-8").strip()
    else:
        token = (
            os.environ.get("FORGEWIRE_HUB_TOKEN", "").strip()
            or os.environ.get("BLACKBOARD_TOKEN", "").strip()
        )
    if not token:
        raise SystemExit(
            "FORGEWIRE_HUB_TOKEN env var or --token-file is required (no anon access)"
        )
    if len(token) < 16:
        raise SystemExit("hub token must be >= 16 characters")
    return token


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="forgewire-hub",
        description="ForgeWire hub server (signed dispatch / claim / streams)",
    )
    parser.add_argument("--host", default=os.environ.get("FORGEWIRE_HUB_HOST", "127.0.0.1"))
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("FORGEWIRE_HUB_PORT", str(DEFAULT_PORT))),
    )
    parser.add_argument(
        "--db-path",
        default=str(
            os.environ.get("FORGEWIRE_HUB_DB_PATH")
            or os.environ.get("BLACKBOARD_DB_PATH")
            or DEFAULT_DB
        ),
    )
    parser.add_argument("--token-file", default=None)
    parser.add_argument(
        "--min-runner-version",
        default=os.environ.get(
            "FORGEWIRE_HUB_MIN_RUNNER_VERSION",
            os.environ.get("BLACKBOARD_MIN_RUNNER_VERSION", DEFAULT_MIN_RUNNER_VERSION),
        ),
        help="Reject /runners/register from runners reporting a lower version.",
    )
    parser.add_argument("--log-level", default="info")
    parser.add_argument(
        "--mdns",
        action="store_true",
        default=(
            os.environ.get("FORGEWIRE_HUB_MDNS", "")
            or os.environ.get("BLACKBOARD_MDNS", "")
        ).lower()
        in {"1", "true", "yes", "on"},
        help="Advertise the hub on the local LAN via mDNS (_forgewire-hub._tcp).",
    )
    return parser.parse_args(argv)



def main(argv: list[str] | None = None) -> None:
    import uvicorn

    args = _parse_args(argv)
    config = BlackboardConfig(
        db_path=Path(args.db_path).expanduser(),
        token=_load_token(args),
        host=args.host,
        port=args.port,
        min_runner_version=args.min_runner_version,
    )
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    app = create_app(config)
    advertisement = None
    if args.mdns:
        from forgewire.hub.discovery import advertise_hub

        advertisement = advertise_hub(
            port=config.port,
            protocol_version=PROTOCOL_VERSION,
            token_preview=config.token[-8:] if len(config.token) >= 8 else "",
        )
    try:
        uvicorn.run(app, host=config.host, port=config.port, log_level=args.log_level)
    finally:
        if advertisement is not None:
            advertisement.close()


if __name__ == "__main__":
    main()
