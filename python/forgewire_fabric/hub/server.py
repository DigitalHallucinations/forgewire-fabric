"""ForgeWire hub HTTP/SSE service.

Runs on the always-on hub host. Exposes a small REST + SSE API used by:

* the dispatcher MCP server (driver host, drives the queue from the main
  agent), reaching the hub over the LAN, and
* the runner MCP server (colocated on the hub), reaching the hub on
  localhost.

Auth: bearer token from ``FORGEWIRE_HUB_TOKEN`` env (or ``--token-file`` path).
Legacy alias ``BLACKBOARD_TOKEN`` is also honoured.
Storage: SQLite WAL at ``FORGEWIRE_HUB_DB_PATH`` (default
``~/.forgewire/hub.sqlite3``). On first start, an existing
``~/.phrenforge/remote_subagent.sqlite3`` is auto-copied for one-shot upgrade.

Run::

    python -m forgewire_fabric.hub.server --host 0.0.0.0 --port 8765

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
import hashlib
import json
import logging
import os
import secrets
import sqlite3
import sys
import time
import uuid
import calendar
from collections.abc import AsyncIterator, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from forgewire_fabric.hub._crypto import HAS_RUST as _HUB_CRYPTO_HAS_RUST
from forgewire_fabric.hub._crypto import verify_signature
from forgewire_fabric.hub._router import HAS_RUST as _HUB_ROUTER_HAS_RUST
from forgewire_fabric.hub._router import pick_task as _router_pick_task
from forgewire_fabric.hub._streams import HAS_RUST as _HUB_STREAMS_HAS_RUST
from forgewire_fabric.hub._streams import make_counter as _make_stream_counter
from forgewire_fabric.hub import _rqlite_db

LOGGER = logging.getLogger("forgewire_fabric.hub")

# Default DB lives under ~/.forgewire/ on a fresh install. The legacy
# ~/.phrenforge/remote_subagent.sqlite3 path is auto-migrated on first
# start so existing PhrenForge installs upgrade in place; once moved,
# the legacy file is left behind for operator visibility.
DEFAULT_DB = Path.home() / ".forgewire" / "hub.sqlite3"
_LEGACY_DEFAULT_DB = Path.home() / ".phrenforge" / "remote_subagent.sqlite3"
SCHEMA_PATH = Path(__file__).with_name("schema.sql")
PROGRESS_POLL_SECONDS = 1.0
DEFAULT_PORT = 8765

# Protocol/handshake version. The dispatcher and runner both ship this value
# in /runners/register; the hub rejects any peer whose major version differs.
#
# v0.4 (atomic bump): wire moves to v3 alongside the additive observability
# fields. ``MIN_COMPATIBLE_PROTOCOL_VERSION`` stays at 2 so a hub restart
# that lands before its runners doesn't lock the fleet out during a rolling
# redeploy. Tighten to 3 once every runner is confirmed on v0.4+.
PROTOCOL_VERSION = 3
MIN_COMPATIBLE_PROTOCOL_VERSION = 2

# Heartbeat / state machine thresholds.
HEARTBEAT_DEGRADED_SECONDS = 45
HEARTBEAT_OFFLINE_SECONDS = 120
# v0.4: when a runner reports this many consecutive claim failures via
# heartbeat, /runners marks it as 'degraded' even though heartbeats are
# fresh. This catches the "claim loop wedged on 404" failure mode that
# was previously silent in both the API and the UI.
CLAIM_FAILURE_DEGRADED_THRESHOLD = 3
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
    require_signed_dispatch: bool = False
    # M2.5.1 / M2.5.2: optional path to a ``policy.yaml`` consumed by
    # :class:`forgewire_fabric.policy.HubDispatchGate`. ``None`` means the
    # gate operates with an empty policy + zero budget, which is
    # equivalent to permit-all but still emits structured
    # :class:`PolicyDecision` records on every dispatch/completion.
    policy_path: Path | None = None
    # Phase 2 (rqlite migration): "sqlite" keeps the legacy single-node
    # WAL backend (default for backward compat); "rqlite" routes all
    # statements to the rqlite cluster over HTTP. The two backends share
    # the same Blackboard call surface; the only divergence is in the
    # state-snapshot endpoint, which uses VACUUM INTO under sqlite and
    # rqlite's /db/backup under rqlite. Under "rqlite" the /state/snapshot
    # and /state/import endpoints are PARITY-ONLY exit hatches -- routine
    # DR is handled by the cluster itself (see
    # docs/operations/dr-rqlite-backups.md and
    # docs/operations/state-endpoints-parity.md).
    backend: str = "sqlite"
    rqlite_host: str = "127.0.0.1"
    rqlite_port: int = 4001
    rqlite_consistency: str = "strong"
    # M2.5.1: optional outbound webhook fired when a dispatch is held for
    # human approval (REQUIRE_APPROVAL). The hub POSTs a JSON body
    # ``{event: "approval.created", approval_id, decision, task_label,
    # branch, scope_globs}`` to this URL with a 5s timeout. Failures are
    # logged but never block the dispatch path.
    approval_webhook_url: str | None = None


class Blackboard:
    """Thin wrapper over the SQLite blackboard schema.

    All public methods take/return plain Python types. The class is intentionally
    procedural -- this module is the boundary, no business logic should leak in.
    """

    def __init__(
        self,
        db_path: Path,
        *,
        backend: str = "sqlite",
        rqlite_host: str = "127.0.0.1",
        rqlite_port: int = 4001,
        rqlite_consistency: str = "strong",
    ) -> None:
        if backend not in ("sqlite", "rqlite"):
            raise ValueError(f"unknown backend {backend!r}")
        self._backend = backend
        self._rqlite_host = rqlite_host
        self._rqlite_port = rqlite_port
        self._rqlite_consistency = rqlite_consistency
        self._db_path = db_path
        # Shared httpx.Client for the rqlite backend. One process-wide HTTP
        # client with a generous keepalive pool means that the per-request
        # `_connect()` context manager only allocates a thin wrapper around
        # the already-warm TCP/keepalive sockets to rqlite, instead of
        # paying TCP setup + a fresh connection-pool per call. Without
        # this, every blackboard call under threadpool concurrency burns a
        # new socket and starves the FastAPI threadpool waiting on Raft.
        self._rqlite_client: httpx.Client | None = None
        if backend == "rqlite":
            self._rqlite_client = httpx.Client(
                base_url=f"http://{rqlite_host}:{rqlite_port}",
                timeout=30.0,
                follow_redirects=True,
                limits=httpx.Limits(
                    max_connections=200,
                    max_keepalive_connections=100,
                ),
            )
        if backend == "sqlite":
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            # One-shot legacy migration: if the operator hasn't pointed
            # FORGEWIRE_HUB_DB_PATH anywhere and the canonical path doesn't
            # exist yet, but a PhrenForge-era ~/.phrenforge/remote_subagent.sqlite3
            # does, copy it across so existing fleets keep their task history.
            if (
                db_path == DEFAULT_DB
                and not db_path.exists()
                and _LEGACY_DEFAULT_DB.exists()
            ):
                try:
                    import shutil

                    shutil.copy2(_LEGACY_DEFAULT_DB, db_path)
                    LOGGER.info(
                        "Migrated legacy hub DB %s -> %s",
                        _LEGACY_DEFAULT_DB,
                        db_path,
                    )
                except OSError as exc:  # pragma: no cover - migration is advisory
                    LOGGER.warning(
                        "Legacy hub DB migration failed (%s -> %s): %s",
                        _LEGACY_DEFAULT_DB,
                        db_path,
                        exc,
                    )
        else:
            LOGGER.info(
                "Blackboard backend=rqlite host=%s port=%s",
                rqlite_host,
                rqlite_port,
            )
        self._init_schema()
        # Stage C.3: in-memory per-task stream-seq counter. Resets on hub
        # restart and re-primes lazily from MAX(seq) in SQLite, so kill -9
        # is safe.
        self._stream_counter = _make_stream_counter()

    # ------------------------------------------------------------------ infra

    @property
    def backend(self) -> str:
        """Active backend: ``"sqlite"`` or ``"rqlite"``."""
        return self._backend

    @contextlib.contextmanager
    def _connect(self) -> Iterable[Any]:
        if self._backend == "rqlite":
            conn = _rqlite_db.connect(
                self._rqlite_host,
                self._rqlite_port,
                timeout=30.0,
                consistency=self._rqlite_consistency,
                client=self._rqlite_client,
            )
            try:
                yield conn
            finally:
                conn.close()
            return
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
            # M2.4: signed-dispatch column. Nullable so legacy bearer-only
            # dispatches keep working when require_signed_dispatch=False.
            ("dispatcher_id", "TEXT"),
        ]
        for col, decl in additions:
            if col not in existing:
                conn.execute(f"ALTER TABLE tasks ADD COLUMN {col} {decl}")

        # v0.4: runner self-reported reliability counters. Surfaced on
        # /runners so a stuck claim loop is visible in the UI.
        runner_cols = {
            r["name"]
            for r in conn.execute("PRAGMA table_info(runners)").fetchall()
        }
        runner_additions = [
            ("claim_failures_total", "INTEGER NOT NULL DEFAULT 0"),
            ("claim_failures_consecutive", "INTEGER NOT NULL DEFAULT 0"),
            ("last_claim_error", "TEXT"),
            ("last_claim_error_at", "TEXT"),
            ("heartbeat_failures_total", "INTEGER NOT NULL DEFAULT 0"),
        ]
        for col, decl in runner_additions:
            if col not in runner_cols:
                conn.execute(f"ALTER TABLE runners ADD COLUMN {col} {decl}")

        # M2.4: dispatcher registry. Mirror of ``runners`` but for the
        # other end of the protocol.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dispatchers (
                dispatcher_id  TEXT PRIMARY KEY,
                public_key     TEXT NOT NULL,
                label          TEXT NOT NULL,
                hostname       TEXT,
                metadata       TEXT NOT NULL DEFAULT '{}',
                first_seen     TEXT NOT NULL DEFAULT (datetime('now')),
                last_seen      TEXT NOT NULL DEFAULT (datetime('now')),
                last_nonce     TEXT
            )
            """
        )

        # Fabric-wide cosmetic labels: hub display name + per-runner aliases.
        # These are scoped to the hub (one row per logical key) and propagate
        # to every connected client. No effect on identity, auth, or routing.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS labels (
                key         TEXT PRIMARY KEY,
                value       TEXT NOT NULL,
                updated_by  TEXT,
                updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )

        # M2.5.1: human-approval queue for REQUIRE_APPROVAL dispatch
        # decisions. The gate computes a stable envelope_hash over the
        # policy-relevant fields (sorted scope_globs, target branch, task
        # label) and either reuses the matching pending row or creates a
        # new one. Operators clear the queue with the
        # ``forgewire-fabric approvals`` CLI; the dispatcher then re-POSTs
        # the same brief with ``approval_id`` set, which the gate consumes.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS approvals (
                approval_id      TEXT PRIMARY KEY,
                envelope_hash    TEXT NOT NULL,
                decision_json    TEXT NOT NULL,
                task_label       TEXT NOT NULL,
                branch           TEXT,
                scope_globs_json TEXT NOT NULL,
                dispatcher_id    TEXT,
                status           TEXT NOT NULL DEFAULT 'pending',
                approver         TEXT,
                reason           TEXT,
                created_at       TEXT NOT NULL DEFAULT (datetime('now')),
                resolved_at      TEXT,
                consumed_at      TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_approvals_status ON approvals(status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_approvals_envelope ON approvals(envelope_hash, status)"
        )

    # ----------------------------------------------------------------- labels

    def get_labels(self) -> dict[str, Any]:
        """Return the fabric-wide label payload: hub_name + runner_aliases."""
        with self._connect() as conn:
            rows = conn.execute("SELECT key, value FROM labels").fetchall()
        hub_name = ""
        aliases: dict[str, str] = {}
        for r in rows:
            k = r["key"]
            v = r["value"]
            if k == "hub_name":
                hub_name = v
            elif k.startswith("runner_alias:"):
                aliases[k[len("runner_alias:") :]] = v
        return {"hub_name": hub_name, "runner_aliases": aliases}

    def set_hub_name(self, name: str, *, updated_by: str | None = None) -> None:
        self._upsert_label("hub_name", name, updated_by)

    def set_runner_alias(
        self,
        runner_id: str,
        alias: str,
        *,
        updated_by: str | None = None,
    ) -> None:
        self._upsert_label(f"runner_alias:{runner_id}", alias, updated_by)

    def _upsert_label(self, key: str, value: str, updated_by: str | None) -> None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if value == "":
                conn.execute("DELETE FROM labels WHERE key = ?", (key,))
            else:
                conn.execute(
                    """
                    INSERT INTO labels (key, value, updated_by, updated_at)
                    VALUES (?, ?, ?, datetime('now'))
                    ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value,
                        updated_by = excluded.updated_by,
                        updated_at = excluded.updated_at
                    """,
                    (key, value, updated_by),
                )
            conn.commit()

    # ------------------------------------------------------------ approvals

    @staticmethod
    def envelope_hash(
        *,
        scope_globs: list[str],
        branch: str | None,
        task_label: str,
    ) -> str:
        """Stable hash over the policy-relevant slice of a dispatch.

        Operators approve an *intent* — "let this brief touch this scope on
        this branch", not "let this exact prompt run". We therefore hash the
        sorted scope_globs, the target branch, and the human task label
        (todo_id when set, else title). A re-dispatch of the same intent
        reuses the existing pending approval row instead of spawning a new
        one, which keeps the queue bounded under retry storms.
        """
        canonical = json.dumps(
            {
                "scope_globs": sorted(str(s) for s in scope_globs),
                "branch": branch or "",
                "task_label": str(task_label),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    def create_or_get_pending_approval(
        self,
        *,
        envelope_hash: str,
        decision: dict[str, Any],
        task_label: str,
        branch: str | None,
        scope_globs: list[str],
        dispatcher_id: str | None,
    ) -> tuple[str, bool]:
        """Insert or reuse a pending approval row. Returns ``(approval_id, created)``.

        ``created`` is True when a new row was inserted; False when an existing
        pending row matched on ``envelope_hash``. The hub fires the approval
        webhook only on creation.
        """
        with self._connect() as conn:
            row = conn.execute(
                "SELECT approval_id FROM approvals "
                "WHERE envelope_hash = ? AND status = 'pending' LIMIT 1",
                (envelope_hash,),
            ).fetchone()
            if row is not None:
                return row["approval_id"], False
            approval_id = uuid.uuid4().hex
            conn.execute(
                """
                INSERT INTO approvals (
                    approval_id, envelope_hash, decision_json, task_label,
                    branch, scope_globs_json, dispatcher_id, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                (
                    approval_id,
                    envelope_hash,
                    json.dumps(decision, sort_keys=True),
                    task_label,
                    branch,
                    json.dumps(list(scope_globs)),
                    dispatcher_id,
                ),
            )
            conn.commit()
            return approval_id, True

    def get_approval(self, approval_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM approvals WHERE approval_id = ?",
                (approval_id,),
            ).fetchone()
        return dict(row) if row is not None else None

    def list_approvals(
        self,
        *,
        status: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM approvals"
        params: tuple[Any, ...] = ()
        if status is not None:
            sql += " WHERE status = ?"
            params = (status,)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params = params + (int(limit),)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def resolve_approval(
        self,
        *,
        approval_id: str,
        status: str,
        approver: str | None,
        reason: str | None,
    ) -> dict[str, Any]:
        if status not in ("approved", "denied"):
            raise ValueError("status must be 'approved' or 'denied'")
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE approvals
                   SET status = ?, approver = ?, reason = ?,
                       resolved_at = datetime('now')
                 WHERE approval_id = ? AND status = 'pending'
                """,
                (status, approver, reason, approval_id),
            )
            if cur.rowcount == 0:
                # Either unknown or already resolved.
                row = conn.execute(
                    "SELECT * FROM approvals WHERE approval_id = ?",
                    (approval_id,),
                ).fetchone()
                if row is None:
                    raise KeyError(approval_id)
                raise PermissionError(
                    f"approval already resolved: status={row['status']}"
                )
            conn.commit()
            row = conn.execute(
                "SELECT * FROM approvals WHERE approval_id = ?",
                (approval_id,),
            ).fetchone()
        return dict(row)

    def consume_approval(self, approval_id: str, envelope_hash: str) -> bool:
        """Atomically consume an approved row matching ``envelope_hash``.

        Returns True if the row was consumed (CAS succeeded), False otherwise
        (unknown id, wrong envelope, denied, already consumed). Callers treat
        False as "approval is not valid for this dispatch" and re-raise the
        original 428.
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE approvals
                   SET status = 'consumed', consumed_at = datetime('now')
                 WHERE approval_id = ?
                   AND envelope_hash = ?
                   AND status = 'approved'
                """,
                (approval_id, envelope_hash),
            )
            conn.commit()
            return cur.rowcount > 0

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
        dispatcher_id: str | None = None,
    ) -> dict[str, Any]:
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO tasks (
                    todo_id, title, prompt, scope_globs, base_commit, branch,
                    timeout_minutes, priority, metadata,
                    required_tools, required_tags, tenant, workspace_root,
                    require_base_commit, dispatcher_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
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
                    dispatcher_id,
                ),
            )
            row = cur.fetchone()
            task_id = int(row["id"]) if row is not None else cur.lastrowid
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

    def count_tasks(self) -> int:
        """Return total task count. Used by /state/import safety check."""
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS n FROM tasks").fetchone()
        if row is None:
            return 0
        return int(row["n"])

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
        """Atomically transition the highest-priority queued task to claimed.

        Implementation note: previously this used ``BEGIN IMMEDIATE`` +
        ``SELECT id ... LIMIT 1`` + ``UPDATE``. That cross-statement
        transaction does not survive on rqlite (HTTP request boundary
        is the transaction boundary). We now use a single
        ``UPDATE ... WHERE id = (SELECT ... LIMIT 1) RETURNING id``
        which is atomic on both stdlib :mod:`sqlite3` and rqlite (each
        rqlite write goes through Raft consensus so concurrent claims
        are serialized).
        """
        now_iso = _now_iso()
        with self._connect() as conn:
            claim = conn.execute(
                """
                UPDATE tasks
                SET status = 'claimed', worker_id = ?, claimed_at = ?
                WHERE id = (
                    SELECT id FROM tasks
                    WHERE status = 'queued' AND cancel_requested = 0
                    ORDER BY priority DESC, id ASC
                    LIMIT 1
                )
                RETURNING id
                """,
                (worker_id, now_iso),
            ).fetchone()
            if claim is None:
                # No queued task. Still record the worker heartbeat.
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
                return None
            task_id = claim["id"]
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
            # Ownership-CAS via UPDATE...RETURNING. If no row matches the
            # ``id = ? AND worker_id = ?`` precondition we then disambiguate
            # KeyError vs PermissionError with a single follow-up SELECT.
            # Previously a BEGIN IMMEDIATE wrapped the whole block, which is
            # not portable to rqlite (no cross-statement transactions over
            # HTTP).
            claimed = conn.execute(
                """
                UPDATE tasks
                SET status = ?, completed_at = ?
                WHERE id = ? AND worker_id = ?
                RETURNING id
                """,
                (status_value, now, task_id, worker_id),
            ).fetchone()
            if claimed is None:
                # Disambiguate: did the task not exist, or did it exist but
                # belong to someone else?
                existing = conn.execute(
                    "SELECT worker_id FROM tasks WHERE id = ?", (task_id,)
                ).fetchone()
                if existing is None:
                    raise KeyError(task_id)
                raise PermissionError(
                    f"worker {worker_id!r} cannot report result for task "
                    f"owned by {existing['worker_id']!r}"
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
                "UPDATE workers SET current_task_id = NULL, last_seen = ? WHERE worker_id = ?",
                (now, worker_id),
            )
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
        """Append one progress entry under an ownership guard.

        Single-statement INSERT...SELECT computes ``next_seq`` from
        ``MAX(seq)`` *and* enforces the worker-ownership precondition
        in one round-trip. ``RETURNING`` surfaces the assigned ``id``
        and ``seq`` so the caller never needs a follow-up read.
        """
        now = _now_iso()
        files_json = json.dumps(files_touched or [])
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO progress (task_id, seq, message, files_touched)
                SELECT
                    t.id,
                    COALESCE(
                        (SELECT MAX(seq) FROM progress WHERE task_id = t.id),
                        0
                    ) + 1,
                    ?,
                    ?
                FROM tasks t
                WHERE t.id = ? AND t.worker_id = ?
                RETURNING id, seq
                """,
                (message, files_json, task_id, worker_id),
            )
            row = cur.fetchone()
            if row is None:
                # Disambiguate KeyError vs PermissionError.
                existing = conn.execute(
                    "SELECT worker_id FROM tasks WHERE id = ?", (task_id,)
                ).fetchone()
                if existing is None:
                    raise KeyError(task_id)
                raise PermissionError("worker mismatch on progress")
            entry_id = row["id"]
            next_seq = row["seq"]
            conn.execute(
                "UPDATE workers SET last_seen = ? WHERE worker_id = ?",
                (now, worker_id),
            )
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
        """Post a note against a task; raises KeyError if no such task."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO notes (task_id, author, body)
                SELECT t.id, ?, ?
                FROM tasks t WHERE t.id = ?
                RETURNING id
                """,
                (author, body, task_id),
            )
            row = cur.fetchone()
            if row is None:
                raise KeyError(task_id)
            note_id = row["id"]
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

        Key-binding rule: an existing ``runner_id`` may not be re-bound to a
        new ``public_key``. We enforce this in a single statement using
        ``ON CONFLICT(runner_id) DO UPDATE ... WHERE
        runners.public_key = excluded.public_key`` -- a mismatch leaves the
        row untouched (rows_affected == 0) and we then raise
        ``PermissionError``.
        """
        now = _now_iso()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO runners (
                    runner_id, public_key, hostname, os, arch, cpu_model,
                    cpu_count, ram_mb, gpu, tools, tags, scope_prefixes,
                    tenant, workspace_root, runner_version, protocol_version,
                    max_concurrent, state, drain_requested, metadata,
                    first_seen, last_heartbeat
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(runner_id) DO UPDATE SET
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
                    last_heartbeat   = excluded.last_heartbeat,
                    -- v0.4: a fresh registration means the runner believes
                    -- it just (re)attached to the hub. Reset reliability
                    -- counters so /runners doesn't show stale failure
                    -- numbers from the previous incarnation.
                    claim_failures_consecutive = 0,
                    last_claim_error           = NULL
                WHERE runners.public_key = excluded.public_key
                RETURNING runner_id
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
                    now,  # first_seen (only used on INSERT path)
                    now,
                ),
            )
            if cur.fetchone() is None:
                # Either no row was inserted/updated. The only reason that
                # can happen here is the conflict-WHERE filter: an existing
                # runner_id with a different public_key.
                raise PermissionError(
                    "runner_id is already bound to a different public_key"
                )
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
        claim_failures_total: int | None = None,
        claim_failures_consecutive: int | None = None,
        last_claim_error: str | None = None,
        heartbeat_failures_total: int | None = None,
    ) -> dict[str, Any]:
        now = _now_iso()
        # When the runner reports a current claim error, stamp _at; when it
        # reports an empty error (recovered), keep the historical _at so
        # operators can still see when the last incident was.
        last_claim_error_at_clause = (
            "last_claim_error_at = CASE WHEN ? IS NOT NULL AND ? != '' "
            "THEN ? ELSE last_claim_error_at END"
        )
        with self._connect() as conn:
            cur = conn.execute(
                f"""
                UPDATE runners
                SET last_heartbeat = ?,
                    cpu_load_pct   = ?,
                    ram_free_mb    = ?,
                    battery_pct    = ?,
                    on_battery     = ?,
                    last_known_commit = COALESCE(?, last_known_commit),
                    last_nonce     = ?,
                    claim_failures_total       = COALESCE(?, claim_failures_total),
                    claim_failures_consecutive = COALESCE(?, claim_failures_consecutive),
                    last_claim_error           = ?,
                    {last_claim_error_at_clause},
                    heartbeat_failures_total   = COALESCE(?, heartbeat_failures_total),
                    state          = CASE
                                       WHEN drain_requested = 1 THEN 'draining'
                                       ELSE 'online'
                                     END
                WHERE runner_id = ?
                  AND (last_nonce IS NULL OR last_nonce != ?)
                RETURNING runner_id
                """,
                (
                    now,
                    cpu_load_pct,
                    ram_free_mb,
                    battery_pct,
                    1 if on_battery else 0,
                    last_known_commit,
                    nonce,
                    claim_failures_total,
                    claim_failures_consecutive,
                    last_claim_error,
                    last_claim_error,
                    last_claim_error,
                    now,
                    heartbeat_failures_total,
                    runner_id,
                    nonce,
                ),
            )
            if cur.fetchone() is None:
                # Either the runner doesn't exist or the nonce was replayed.
                exists = conn.execute(
                    "SELECT 1 FROM runners WHERE runner_id = ?", (runner_id,)
                ).fetchone()
                if exists is None:
                    raise KeyError(runner_id)
                raise PermissionError("nonce replay rejected")
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

    def request_undrain(self, runner_id: str) -> dict[str, Any]:
        """Reverse a drain request. Restores state to 'online' so the
        runner accepts new tasks again on its next heartbeat."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE runners
                SET drain_requested = 0,
                    state           = CASE
                        WHEN state = 'draining' THEN 'online'
                        ELSE state
                    END
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

    # ------------------------------------------------------------ dispatchers

    def upsert_dispatcher(
        self,
        *,
        dispatcher_id: str,
        public_key: str,
        label: str,
        hostname: str | None,
        metadata: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Insert or update a dispatcher row.

        Caller must have already verified the self-attestation signature.
        Re-binding ``dispatcher_id`` to a different ``public_key`` is
        rejected; rotate by issuing a new ``dispatcher_id``.
        """
        now = _now_iso()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO dispatchers (
                    dispatcher_id, public_key, label, hostname, metadata,
                    first_seen, last_seen
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(dispatcher_id) DO UPDATE SET
                    label     = excluded.label,
                    hostname  = excluded.hostname,
                    metadata  = excluded.metadata,
                    last_seen = excluded.last_seen
                WHERE dispatchers.public_key = excluded.public_key
                RETURNING dispatcher_id
                """,
                (
                    dispatcher_id,
                    public_key,
                    label,
                    hostname,
                    json.dumps(metadata or {}),
                    now,  # first_seen (only used on INSERT path)
                    now,
                ),
            )
            if cur.fetchone() is None:
                # Conflict-WHERE filtered the UPDATE: existing dispatcher_id
                # bound to a different public_key.
                raise PermissionError(
                    "dispatcher_id is already bound to a different public_key"
                )
        return self.get_dispatcher(dispatcher_id)

    def get_dispatcher(self, dispatcher_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM dispatchers WHERE dispatcher_id = ?",
                (dispatcher_id,),
            ).fetchone()
        if row is None:
            raise KeyError(dispatcher_id)
        record = dict(row)
        try:
            record["metadata"] = json.loads(record.get("metadata") or "{}")
        except (TypeError, ValueError):
            record["metadata"] = {}
        return record

    def list_dispatchers(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM dispatchers ORDER BY label, dispatcher_id"
            ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            record = dict(row)
            try:
                record["metadata"] = json.loads(record.get("metadata") or "{}")
            except (TypeError, ValueError):
                record["metadata"] = {}
            out.append(record)
        return out

    def dispatcher_public_key(self, dispatcher_id: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT public_key FROM dispatchers WHERE dispatcher_id = ?",
                (dispatcher_id,),
            ).fetchone()
        return row["public_key"] if row else None

    def consume_dispatcher_nonce(self, dispatcher_id: str, nonce: str) -> None:
        """Atomically check-and-set ``last_nonce`` on a dispatcher.

        Raises ``KeyError`` if the dispatcher is unknown and
        ``PermissionError`` on replay. The check is the strict "reject if
        last_nonce == nonce" form used for runners; combined with the 5
        minute skew window this gives basic replay protection.
        """
        now = _now_iso()
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE dispatchers
                SET last_nonce = ?, last_seen = ?
                WHERE dispatcher_id = ?
                  AND (last_nonce IS NULL OR last_nonce != ?)
                RETURNING dispatcher_id
                """,
                (nonce, now, dispatcher_id, nonce),
            )
            if cur.fetchone() is None:
                exists = conn.execute(
                    "SELECT 1 FROM dispatchers WHERE dispatcher_id = ?",
                    (dispatcher_id,),
                ).fetchone()
                if exists is None:
                    raise KeyError(dispatcher_id)
                raise PermissionError("nonce replay rejected")

    @staticmethod
    def _derive_state(runner: dict[str, Any]) -> str:
        if runner.get("drain_requested"):
            return "draining"
        try:
            last = time.strptime(runner["last_heartbeat"], "%Y-%m-%dT%H:%M:%SZ")
            # last_heartbeat is UTC (trailing 'Z'); convert via calendar.timegm
            # so we don't mix mktime's DST-aware offset with time.timezone.
            age = time.time() - calendar.timegm(last)
        except Exception:
            return runner.get("state") or "online"
        if age >= HEARTBEAT_OFFLINE_SECONDS:
            return "offline"
        if age >= HEARTBEAT_DEGRADED_SECONDS:
            return "degraded"
        # v0.4: a runner whose claim loop is stuck (e.g. signature/identity
        # mismatch yielding repeated 404s) is heartbeating fine but unable
        # to take work. Surface that as 'degraded' so /runners and the UI
        # don't silently mislabel it as 'online'.
        try:
            consecutive = int(runner.get("claim_failures_consecutive") or 0)
        except (TypeError, ValueError):
            consecutive = 0
        if consecutive >= CLAIM_FAILURE_DEGRADED_THRESHOLD:
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
            # Reads (autocommit). With rqlite there is no cross-statement
            # transaction; each request is its own Raft round-trip.
            # Concurrency-safety for the final claim still holds because
            # the UPDATE-CAS at the bottom checks the precondition
            # ``status='queued' AND cancel_requested=0`` and rqlite
            # serializes writes through Raft.
            runner_row = conn.execute(
                "SELECT * FROM runners WHERE runner_id = ?", (runner_id,)
            ).fetchone()
            if runner_row is None:
                raise KeyError(runner_id)
            if runner_row["drain_requested"]:
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
                info["reason"] = "concurrency_cap"
                info["current_load"] = current_load
                info["max_concurrent"] = int(runner_row["max_concurrent"])
                return None, info
            # Resource gates.
            if ram_free_mb is not None and ram_free_mb < DEFAULT_MIN_RAM_FREE_MB:
                info["reason"] = "resource_gate"
                info["detail"] = f"ram_free_mb {ram_free_mb} < {DEFAULT_MIN_RAM_FREE_MB}"
                return None, info
            if on_battery and battery_pct is not None and battery_pct < DEFAULT_MIN_BATTERY_PCT:
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
            chosen = rows[picked_idx] if picked_idx is not None else None
            if chosen is None:
                info["reason"] = "no_eligible_runner"
                return None, info
            task_id = int(chosen["id"])
            now = _now_iso()
            # CAS claim: only succeed if the task is still queued.
            # If two runners pick the same task concurrently (against
            # different cluster nodes), Raft serializes and exactly one
            # wins. The loser sees ``rowcount == 0`` and falls through
            # to a "no_eligible_runner" diagnostic so the caller retries.
            claimed = conn.execute(
                """
                UPDATE tasks
                SET status = 'claimed', worker_id = ?, claimed_at = ?
                WHERE id = ?
                  AND status = 'queued'
                  AND cancel_requested = 0
                RETURNING id
                """,
                (runner_id, now, task_id),
            ).fetchone()
            if claimed is None:
                # Lost the race or task was cancelled between candidate
                # SELECT and CAS. Surface as "no_eligible_runner" so the
                # caller treats it like any other no-match outcome.
                info["reason"] = "no_eligible_runner"
                info["detail"] = "lost_claim_race"
                return None, info
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
    # M2.5.1: when a previous attempt at the same envelope returned 428
    # REQUIRE_APPROVAL, the dispatcher re-POSTs with the approval_id from
    # the issued queue row. The hub validates + consumes it on a match
    # against the canonical envelope hash and bypasses the gate. Excluded
    # from the v2 canonical signed payload (out-of-band, bearer-gated).
    approval_id: str | None = None


class ApprovalDecisionRequest(BaseModel):
    """Body for ``POST /approvals/{id}/approve`` and ``/deny``."""

    approver: str | None = Field(default=None, max_length=200)
    reason: str | None = Field(default=None, max_length=2000)


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
    # v0.4: additive runner self-reported reliability counters.
    # Older runners simply omit these and the hub stores zeros.
    claim_failures_total: int | None = None
    claim_failures_consecutive: int | None = None
    last_claim_error: str | None = None
    heartbeat_failures_total: int | None = None


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


# ---- M2.4: dispatcher signing ---------------------------------------------


class RegisterDispatcherRequest(BaseModel):
    """Self-attesting registration of a dispatcher's ed25519 public key.

    Signed payload (canonical JSON, sort_keys, no whitespace) is::

        {"op": "register-dispatcher",
         "dispatcher_id": ...,
         "public_key":    ...,
         "timestamp":     ...,
         "nonce":         ...}
    """

    dispatcher_id: str = Field(..., min_length=8, max_length=120)
    public_key: str = Field(..., min_length=64, max_length=64)
    label: str = Field(..., min_length=1, max_length=200)
    hostname: str | None = Field(default=None, max_length=200)
    metadata: dict[str, Any] | None = None
    timestamp: int
    nonce: str = Field(..., min_length=8, max_length=80)
    signature: str


class DispatchTaskSignedRequest(DispatchTaskRequest):
    """Signed-dispatch envelope.

    Identical to :class:`DispatchTaskRequest` plus the four signing fields.
    Signed payload (canonical JSON) is::

        {"op": "dispatch",
         "dispatcher_id": ...,
         "title": ...,
         "prompt": ...,
         "scope_globs": [...],
         "base_commit": ...,
         "branch": ...,
         "timestamp": ...,
         "nonce": ...}

    The signature covers only the immutable fields above. Optional fields
    (``todo_id``, ``timeout_minutes``, ``priority``, ``metadata``,
    ``required_tools``, ``required_tags``, ``tenant``, ``workspace_root``,
    ``require_base_commit``) are *not* in the signed payload -- they are
    routing hints that the bearer token already authenticates.
    """

    dispatcher_id: str = Field(..., min_length=8, max_length=120)
    timestamp: int
    nonce: str = Field(..., min_length=8, max_length=80)
    signature: str


# ---------------------------------------------------------------------------
# FastAPI app factory
# ---------------------------------------------------------------------------


def create_app(config: BlackboardConfig) -> FastAPI:
    from forgewire_fabric import __version__ as _pkg_version

    app = FastAPI(
        title="ForgeWire Fabric Hub",
        version=_pkg_version,
    )
    blackboard = Blackboard(
        config.db_path,
        backend=config.backend,
        rqlite_host=config.rqlite_host,
        rqlite_port=config.rqlite_port,
        rqlite_consistency=config.rqlite_consistency,
    )
    app.state.blackboard = blackboard
    app.state.token = config.token
    app.state.started_at = time.time()
    app.state.config = config

    # ---- M2.5.1 + M2.5.2: hub-side policy + budget gate ----------------
    from forgewire_fabric.policy import (
        BudgetEnforcer,
        BudgetPolicy,
        CostLedger,
        FabricPolicy,
        FabricPolicyEngine,
        HubDispatchGate,
        load_policy_yaml,
    )

    if config.policy_path is not None and Path(config.policy_path).exists():
        fabric_policy = load_policy_yaml(str(config.policy_path))
    else:
        fabric_policy = FabricPolicy()
    app.state.cost_ledger = CostLedger()
    app.state.gate = HubDispatchGate(
        policy_engine=FabricPolicyEngine(fabric_policy),
        budget_enforcer=BudgetEnforcer(
            ledger=app.state.cost_ledger,
            policy=BudgetPolicy(),
        ),
    )

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
    def healthz() -> dict[str, Any]:
        return {
            "status": "ok",
            "version": app.version,
            "protocol_version": PROTOCOL_VERSION,
            "rust_crypto": _HUB_CRYPTO_HAS_RUST,
            "rust_router": _HUB_ROUTER_HAS_RUST,
            "rust_streams": _HUB_STREAMS_HAS_RUST,
            "started_at": app.state.started_at,
            "uptime_seconds": time.time() - app.state.started_at,
            "host": config.host,
            "port": config.port,
        }

    @app.get("/state/snapshot", dependencies=[Depends(require_auth)])
    def state_snapshot(request: Request) -> JSONResponse:
        """PARITY-ONLY: atomic state snapshot for failover replication.

        .. deprecated::
            Under ``--backend rqlite`` (the production default), the
            Raft cluster IS the durability tier. Routine DR backups go
            through ``scripts/dr/backup_rqlite.ps1`` pulling
            ``/db/backup?redirect=true`` directly from the cluster.
            See ``docs/operations/dr-rqlite-backups.md``.

            This endpoint is kept ONLY as a parity path for:
              * legacy ``--backend sqlite`` single-node deployments;
              * one-shot exit-hatch dumps when migrating off rqlite;
              * authenticated snapshot fetches that don't have direct
                network access to the rqlite voters.

            New automation MUST NOT depend on this endpoint.

        Backend-aware:

        * ``sqlite``: ``VACUUM INTO`` over a freshly-opened read-only
          handle. Returns the raw SQLite file as
          ``application/x-sqlite3``.
        * ``rqlite``: proxies the call to the cluster's
          ``/db/backup`` endpoint and returns the byte-identical blob.
        """
        from fastapi.responses import Response as _FResp

        if config.backend == "rqlite":
            # Stream the rqlite-native backup. ``/db/backup`` returns a
            # consistent SQLite file produced by VACUUM INTO inside the
            # cluster, so the body is byte-for-byte equivalent to the
            # sqlite-mode response.
            try:
                with httpx.Client(
                    base_url=f"http://{config.rqlite_host}:{config.rqlite_port}",
                    timeout=60.0,
                    follow_redirects=True,
                ) as client:
                    resp = client.get("/db/backup")
                    if resp.status_code != 200:
                        raise HTTPException(
                            status_code=502,
                            detail=(
                                f"rqlite /db/backup failed: "
                                f"{resp.status_code} {resp.text[:200]}"
                            ),
                        )
                    data = resp.content
            except httpx.HTTPError as exc:
                raise HTTPException(
                    status_code=502, detail=f"rqlite unreachable: {exc}"
                )
            return _FResp(
                content=data,
                media_type="application/x-sqlite3",
                headers={
                    "X-Snapshot-Generated-At": str(time.time()),
                    "X-Hub-Started-At": str(app.state.started_at),
                    "X-Snapshot-Source": "rqlite",
                },
            )

        snap_dir = config.db_path.parent / "snapshots"
        snap_dir.mkdir(parents=True, exist_ok=True)
        # We always overwrite the same file -- callers checksum/timestamp the
        # response themselves.
        snap_path = snap_dir / f".snapshot-{os.getpid()}.sqlite3"
        if snap_path.exists():
            snap_path.unlink()
        with sqlite3.connect(config.db_path) as src:
            src.execute(f"VACUUM INTO '{snap_path.as_posix()}'")
        data = snap_path.read_bytes()
        try:
            snap_path.unlink()
        except OSError:
            pass
        return _FResp(
            content=data,
            media_type="application/x-sqlite3",
            headers={
                "X-Snapshot-Generated-At": str(time.time()),
                "X-Hub-Started-At": str(app.state.started_at),
                "X-Snapshot-Source": "sqlite",
            },
        )

    @app.post("/state/import", dependencies=[Depends(require_auth)])
    async def state_import(request: Request) -> dict[str, Any]:
        """PARITY-ONLY: import a snapshot to bootstrap a fresh hub.

        .. deprecated::
            Under ``--backend rqlite``, hub failover is handled by Raft
            consensus -- there is no "promote" step and no need to
            replay a snapshot to recover. Routine restores go through
            rqlite's native ``/db/load`` (see
            ``docs/operations/dr-rqlite-backups.md``).

            This endpoint is kept ONLY as a parity path for:
              * bootstrapping an empty rqlite cluster from a DR backup
                (the rqlite branch below proxies straight to ``/db/load``);
              * legacy ``--backend sqlite`` single-node restores;
              * authenticated bulk restores that don't have direct
                network access to the rqlite voters.

            New automation MUST NOT depend on this endpoint.

        Refuses if any tasks have been claimed *after* the hub started --
        this protects against accidentally stomping a live hub. Use
        ``X-Force: 1`` to override (operator must explicitly opt in).
        """
        body = await request.body()
        if not body:
            raise HTTPException(status_code=400, detail="empty body")
        force = request.headers.get("x-force", "").strip() == "1"
        # Safety: refuse if this hub has activity since start.
        if not force:
            count = blackboard.count_tasks()
            if count > 0:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"refusing to import over a non-empty hub "
                        f"({count} tasks); send X-Force: 1 to override"
                    ),
                )

        if config.backend == "rqlite":
            # rqlite-native bulk load. ``/db/load`` accepts a SQLite file
            # body (application/octet-stream) and atomically replaces the
            # cluster state via Raft. Returns 200 on success.
            try:
                with httpx.Client(
                    base_url=f"http://{config.rqlite_host}:{config.rqlite_port}",
                    timeout=120.0,
                    follow_redirects=True,
                ) as client:
                    resp = client.post(
                        "/db/load",
                        content=body,
                        headers={"Content-Type": "application/octet-stream"},
                    )
                    if resp.status_code != 200:
                        raise HTTPException(
                            status_code=502,
                            detail=(
                                f"rqlite /db/load failed: "
                                f"{resp.status_code} {resp.text[:200]}"
                            ),
                        )
            except httpx.HTTPError as exc:
                raise HTTPException(
                    status_code=502, detail=f"rqlite unreachable: {exc}"
                )
            return {"status": "imported", "bytes": len(body), "backend": "rqlite"}

        # Atomic replace: write to .new, fsync, rename.
        new_path = config.db_path.with_suffix(config.db_path.suffix + ".new")
        new_path.write_bytes(body)
        # Best-effort: verify the bytes are a real SQLite db before swap.
        try:
            with sqlite3.connect(new_path) as test:
                test.execute("SELECT COUNT(*) FROM tasks").fetchone()
        except sqlite3.DatabaseError as exc:
            new_path.unlink(missing_ok=True)
            raise HTTPException(status_code=400, detail=f"invalid sqlite blob: {exc}")
        # Replace under our feet -- existing readers using WAL will reopen.
        os.replace(new_path, config.db_path)
        return {
            "status": "imported",
            "bytes": len(body),
            "backend": "sqlite",
        }

    @app.post("/tasks", dependencies=[Depends(require_auth)])
    def dispatch_task(payload: DispatchTaskRequest) -> dict[str, Any]:
        # M2.4: when require_signed_dispatch is set, the legacy bearer-only
        # path is closed. Clients must POST /tasks/v2 with a signed envelope.
        if config.require_signed_dispatch:
            raise HTTPException(
                status_code=426,
                detail=(
                    "this hub requires signed dispatch envelopes; "
                    "POST /tasks/v2 with a registered dispatcher key"
                ),
            )
        _enforce_dispatch_gate(
            task_id=(payload.todo_id or payload.title),
            scope_globs=payload.scope_globs,
            branch=payload.branch,
            approval_id=payload.approval_id,
        )
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
    def list_tasks(
        status: str | None = None, limit: int = 100
    ) -> dict[str, Any]:
        return {"tasks": blackboard.list_tasks(status_filter=status, limit=limit)}

    @app.get("/tasks/{task_id}", dependencies=[Depends(require_auth)])
    def get_task(task_id: int) -> dict[str, Any]:
        try:
            return blackboard.get_task(task_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="task not found") from exc

    @app.post("/tasks/claim", dependencies=[Depends(require_auth)])
    def claim_task(payload: ClaimRequest) -> JSONResponse:
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

    # ----- M2.5.1 + M2.5.2: dispatch / completion policy enforcement -----

    def _fire_approval_webhook(payload: dict[str, Any]) -> None:
        url = config.approval_webhook_url
        if not url:
            return
        try:
            with httpx.Client(timeout=5.0) as client:
                client.post(url, json=payload)
        except Exception as exc:  # noqa: BLE001 - best-effort notify
            logging.getLogger(__name__).warning(
                "approval webhook to %s failed: %s", url, exc
            )

    def _enforce_dispatch_gate(
        *,
        task_id: str,
        scope_globs: list[str],
        branch: str | None,
        dispatcher_id: str | None = None,
        approval_id: str | None = None,
    ) -> None:
        from forgewire_fabric.policy import DispatchRequest

        decision = app.state.gate.evaluate_dispatch(
            DispatchRequest(
                task_id=str(task_id),
                scope_globs=list(scope_globs),
                target_branch=branch,
                dispatcher_id=dispatcher_id,
            )
        )
        if decision.allowed:
            return
        if decision.denied:
            # Hard deny is non-bypassable — even with an approval token.
            raise HTTPException(status_code=403, detail=decision.to_dict())
        # REQUIRE_APPROVAL path. Compute envelope hash so we can either
        # consume a matching prior approval or seed a new pending row.
        env_hash = blackboard.envelope_hash(
            scope_globs=list(scope_globs),
            branch=branch,
            task_label=str(task_id),
        )
        if approval_id and blackboard.consume_approval(approval_id, env_hash):
            return
        approval_id_new, created = blackboard.create_or_get_pending_approval(
            envelope_hash=env_hash,
            decision=decision.to_dict(),
            task_label=str(task_id),
            branch=branch,
            scope_globs=list(scope_globs),
            dispatcher_id=dispatcher_id,
        )
        detail = decision.to_dict()
        detail["approval_id"] = approval_id_new
        detail["envelope_hash"] = env_hash
        detail["hint"] = (
            "re-POST the same brief with approval_id=<id> after an operator "
            "runs `forgewire-fabric approvals approve "
            f"{approval_id_new}`"
        )
        if created:
            _fire_approval_webhook(
                {
                    "event": "approval.created",
                    "approval_id": approval_id_new,
                    "task_label": str(task_id),
                    "branch": branch,
                    "scope_globs": list(scope_globs),
                    "decision": decision.to_dict(),
                }
            )
        raise HTTPException(status_code=428, detail=detail)

    def _enforce_completion_gate(
        *,
        task_id: str,
        changed_paths: list[str],
    ) -> None:
        from forgewire_fabric.policy import CompletionRequest

        decision = app.state.gate.evaluate_completion(
            CompletionRequest(
                task_id=str(task_id),
                changed_paths=list(changed_paths or ()),
                diff_lines=0,
            )
        )
        if decision.allowed or decision.needs_approval:
            # On completion, REQUIRE_APPROVAL (e.g. protected branch) still
            # allows the result envelope to land — the work happened. Hard
            # DENY is the only path that refuses to record the result.
            return
        raise HTTPException(status_code=403, detail=decision.to_dict())

    # ----- M2.5.1: approval queue HTTP surface -----------------------------

    @app.get("/approvals", dependencies=[Depends(require_auth)])
    def list_approvals(status: str | None = None, limit: int = 200) -> dict[str, Any]:
        if status is not None and status not in (
            "pending",
            "approved",
            "denied",
            "consumed",
        ):
            raise HTTPException(
                status_code=400,
                detail="status must be one of pending|approved|denied|consumed",
            )
        return {
            "approvals": blackboard.list_approvals(status=status, limit=limit),
        }

    @app.get("/approvals/{approval_id}", dependencies=[Depends(require_auth)])
    def get_approval(approval_id: str) -> dict[str, Any]:
        row = blackboard.get_approval(approval_id)
        if row is None:
            raise HTTPException(status_code=404, detail="approval not found")
        return row

    @app.post(
        "/approvals/{approval_id}/approve", dependencies=[Depends(require_auth)]
    )
    def approve_approval(
        approval_id: str, payload: ApprovalDecisionRequest
    ) -> dict[str, Any]:
        try:
            return blackboard.resolve_approval(
                approval_id=approval_id,
                status="approved",
                approver=payload.approver,
                reason=payload.reason,
            )
        except KeyError:
            raise HTTPException(status_code=404, detail="approval not found")
        except PermissionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post(
        "/approvals/{approval_id}/deny", dependencies=[Depends(require_auth)]
    )
    def deny_approval(
        approval_id: str, payload: ApprovalDecisionRequest
    ) -> dict[str, Any]:
        try:
            return blackboard.resolve_approval(
                approval_id=approval_id,
                status="denied",
                approver=payload.approver,
                reason=payload.reason,
            )
        except KeyError:
            raise HTTPException(status_code=404, detail="approval not found")
        except PermissionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    # ----- M2.4: dispatcher registry / signed dispatch ---------------------

    @app.post("/dispatchers/register", dependencies=[Depends(require_auth)])
    def register_dispatcher(payload: RegisterDispatcherRequest) -> dict[str, Any]:
        _check_skew(payload.timestamp)
        signed = _signed_payload(
            {
                "op": "register-dispatcher",
                "dispatcher_id": payload.dispatcher_id,
                "public_key": payload.public_key,
                "timestamp": payload.timestamp,
                "nonce": payload.nonce,
            }
        )
        if not verify_signature(payload.public_key, signed, payload.signature):
            raise HTTPException(
                status_code=403, detail="invalid dispatcher self-attestation"
            )
        try:
            record = blackboard.upsert_dispatcher(
                dispatcher_id=payload.dispatcher_id,
                public_key=payload.public_key,
                label=payload.label,
                hostname=payload.hostname,
                metadata=payload.metadata,
            )
        except PermissionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {
            "hub_protocol_version": PROTOCOL_VERSION,
            "dispatcher": record,
        }

    @app.get("/dispatchers", dependencies=[Depends(require_auth)])
    def list_dispatchers() -> dict[str, Any]:
        return {
            "hub_protocol_version": PROTOCOL_VERSION,
            "dispatchers": blackboard.list_dispatchers(),
        }

    @app.post("/tasks/v2", dependencies=[Depends(require_auth)])
    def dispatch_task_signed(
        payload: DispatchTaskSignedRequest,
    ) -> dict[str, Any]:
        _check_skew(payload.timestamp)
        public_key = blackboard.dispatcher_public_key(payload.dispatcher_id)
        if public_key is None:
            raise HTTPException(
                status_code=404, detail="dispatcher not registered"
            )
        signed = _signed_payload(
            {
                "op": "dispatch",
                "dispatcher_id": payload.dispatcher_id,
                "title": payload.title,
                "prompt": payload.prompt,
                "scope_globs": list(payload.scope_globs),
                "base_commit": payload.base_commit,
                "branch": payload.branch,
                "timestamp": payload.timestamp,
                "nonce": payload.nonce,
            }
        )
        if not verify_signature(public_key, signed, payload.signature):
            raise HTTPException(status_code=403, detail="invalid dispatch signature")
        try:
            blackboard.consume_dispatcher_nonce(
                payload.dispatcher_id, payload.nonce
            )
        except KeyError:
            raise HTTPException(
                status_code=404, detail="dispatcher not registered"
            )
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        _enforce_dispatch_gate(
            task_id=(payload.todo_id or payload.title),
            scope_globs=payload.scope_globs,
            branch=payload.branch,
            dispatcher_id=payload.dispatcher_id,
            approval_id=payload.approval_id,
        )
        return blackboard.create_task(
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
            dispatcher_id=payload.dispatcher_id,
        )

    @app.post("/runners/register", dependencies=[Depends(require_auth)])
    def register_runner(payload: RegisterRequest) -> dict[str, Any]:
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
    def list_runners() -> dict[str, Any]:
        return {
            "hub_protocol_version": PROTOCOL_VERSION,
            "runners": blackboard.list_runners(),
        }

    # -- labels (cosmetic, fabric-wide) ----------------------------------
    @app.get("/labels", dependencies=[Depends(require_auth)])
    def get_labels() -> dict[str, Any]:
        return blackboard.get_labels()

    @app.put("/labels/hub", dependencies=[Depends(require_auth)])
    def set_hub_label(payload: dict[str, Any]) -> dict[str, Any]:
        name = str(payload.get("name", "")).strip()
        if len(name) > 80:
            raise HTTPException(status_code=400, detail="hub name max 80 chars")
        updated_by = str(payload.get("updated_by", "") or "")[:80] or None
        blackboard.set_hub_name(name, updated_by=updated_by)
        return blackboard.get_labels()

    @app.put("/labels/runners/{runner_id}", dependencies=[Depends(require_auth)])
    def set_runner_label(runner_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        alias = str(payload.get("alias", "")).strip()
        if len(alias) > 80:
            raise HTTPException(status_code=400, detail="runner alias max 80 chars")
        updated_by = str(payload.get("updated_by", "") or "")[:80] or None
        blackboard.set_runner_alias(runner_id, alias, updated_by=updated_by)
        return blackboard.get_labels()

    @app.post("/runners/{runner_id}/heartbeat", dependencies=[Depends(require_auth)])
    def heartbeat_runner(runner_id: str, payload: HeartbeatRequest) -> dict[str, Any]:
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
                claim_failures_total=payload.claim_failures_total,
                claim_failures_consecutive=payload.claim_failures_consecutive,
                last_claim_error=payload.last_claim_error,
                heartbeat_failures_total=payload.heartbeat_failures_total,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="runner not registered") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return record

    @app.post("/runners/{runner_id}/drain", dependencies=[Depends(require_auth)])
    def drain_runner(runner_id: str, payload: DrainRequest) -> dict[str, Any]:
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
    def drain_runner_by_dispatcher(runner_id: str) -> dict[str, Any]:
        """Dispatcher-initiated drain. Bearer-only (no runner signature)."""
        try:
            return blackboard.request_drain(runner_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="runner not registered") from exc

    @app.post("/runners/{runner_id}/undrain-by-dispatcher", dependencies=[Depends(require_auth)])
    def undrain_runner_by_dispatcher(runner_id: str) -> dict[str, Any]:
        """Dispatcher-initiated un-drain (resume). Bearer-only."""
        try:
            return blackboard.request_undrain(runner_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="runner not registered") from exc

    @app.post("/tasks/claim-v2", dependencies=[Depends(require_auth)])
    def claim_task_v2(payload: ClaimV2Request) -> JSONResponse:
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
    def mark_running(task_id: int) -> dict[str, Any]:
        try:
            return blackboard.mark_running(task_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="task not found") from exc

    @app.post("/tasks/{task_id}/cancel", dependencies=[Depends(require_auth)])
    def cancel_task(task_id: int) -> dict[str, Any]:
        try:
            return blackboard.cancel_task(task_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="task not found") from exc

    @app.post("/tasks/{task_id}/progress", dependencies=[Depends(require_auth)])
    def append_progress(
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
    def append_stream(
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
    def append_stream_bulk(
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
    def read_stream(
        task_id: int, after_seq: int = 0, limit: int = 500
    ) -> dict[str, Any]:
        return {
            "lines": blackboard.streams_since(
                task_id=task_id, after_seq=after_seq, limit=limit
            )
        }

    @app.post("/tasks/{task_id}/result", dependencies=[Depends(require_auth)])
    def submit_result(
        task_id: int, payload: ResultRequest
    ) -> dict[str, Any]:
        _enforce_completion_gate(
            task_id=str(task_id),
            changed_paths=payload.files_touched,
        )
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
    def post_note(task_id: int, payload: NoteRequest) -> dict[str, Any]:
        try:
            return blackboard.post_note(
                task_id=task_id, author=payload.author, body=payload.body
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="task not found") from exc

    @app.get("/tasks/{task_id}/notes", dependencies=[Depends(require_auth)])
    def read_notes(task_id: int, after_id: int = 0) -> dict[str, Any]:
        return {"notes": blackboard.read_notes(task_id=task_id, after_id=after_id)}

    @app.get("/tasks/{task_id}/events", dependencies=[Depends(require_auth)])
    def task_events(task_id: int, request: Request) -> EventSourceResponse:
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

    # -- Resilience: fast-exit on fatal socket / accept-loop failures so that
    #    the service supervisor (NSSM / systemd / launchd) restarts us. On
    #    Windows IOCP we have observed `OSError: [WinError 64]` propagating
    #    out of `accept_coro` and silently killing the listening socket while
    #    the process keeps running. That state is invisible to the supervisor
    #    and produces "service running, hub unreachable". Convert any such
    #    fatal asyncio exception into a hard process exit; the supervisor
    #    will bring us back within seconds.
    @app.on_event("startup")
    async def _bump_threadpool() -> None:  # pragma: no cover - runtime
        # FastAPI runs sync `def` route handlers on the anyio threadpool.
        # The default limiter is 40, which is undersized for a hub serving
        # tens of runners polling at >=1 Hz against an rqlite backend
        # whose Raft-backed writes take 10-30 ms each. Bumping to 200 lets
        # heartbeats and claims overlap freely instead of queueing behind
        # /healthz.
        try:
            import anyio.to_thread

            limiter = anyio.to_thread.current_default_thread_limiter()
            limiter.total_tokens = 200
            logging.getLogger("forgewire_fabric.hub").info(
                "anyio threadpool sized to %d tokens", limiter.total_tokens
            )
        except Exception:  # pragma: no cover - best effort
            logging.getLogger("forgewire_fabric.hub").exception(
                "failed to resize anyio threadpool"
            )

    @app.on_event("startup")
    async def _install_loop_watchdog() -> None:  # pragma: no cover - runtime
        loop = asyncio.get_running_loop()
        log = logging.getLogger("forgewire_fabric.hub.watchdog")
        prev = loop.get_exception_handler()

        def _fatal(message: str, exc: BaseException | None) -> bool:
            text = (message or "").lower()
            if "accept failed" in text or "accept_coro" in text:
                return True
            if isinstance(exc, OSError):
                # WinError 64 / 121 / 1236 — listening socket has been torn
                # down by the OS; we cannot recover without re-binding.
                return getattr(exc, "winerror", None) in {64, 121, 1236}
            return False

        def _handler(_loop: asyncio.AbstractEventLoop, ctx: dict) -> None:
            msg = str(ctx.get("message", ""))
            exc = ctx.get("exception")
            if _fatal(msg, exc if isinstance(exc, BaseException) else None):
                log.critical(
                    "fatal asyncio failure, exiting for supervisor restart: "
                    "msg=%r exc=%r",
                    msg, exc,
                )
                # Flush stdio before bailing.
                try:
                    sys.stdout.flush()
                    sys.stderr.flush()
                except Exception:
                    pass
                os._exit(75)  # EX_TEMPFAIL
            if prev is not None:
                prev(_loop, ctx)
            else:
                _loop.default_exception_handler(ctx)

        loop.set_exception_handler(_handler)
        log.info("loop watchdog installed (fatal-exit on accept failures)")

    return app


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _load_token(args: argparse.Namespace) -> str:
    token_file = args.token_file or os.environ.get(
        "FORGEWIRE_HUB_TOKEN_FILE"
    ) or os.environ.get("BLACKBOARD_TOKEN_FILE")
    if token_file:
        token = Path(token_file).read_text(encoding="utf-8").strip()
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
        description="ForgeWire Fabric hub server (signed dispatch / claim / streams)",
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
    parser.add_argument(
        "--require-signed-dispatch",
        action="store_true",
        default=os.environ.get(
            "FORGEWIRE_HUB_REQUIRE_SIGNED_DISPATCH", ""
        ).lower()
        in {"1", "true", "yes", "on"},
        help=(
            "Reject the legacy bearer-only POST /tasks. Clients must POST "
            "/tasks/v2 with a registered dispatcher signature."
        ),
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
    parser.add_argument(
        "--backend",
        choices=("sqlite", "rqlite"),
        default=os.environ.get("FORGEWIRE_HUB_BACKEND", "sqlite"),
        help=(
            "State backend. 'sqlite' = legacy single-node WAL (default). "
            "'rqlite' = Raft-replicated cluster via HTTP. Schema is identical."
        ),
    )
    parser.add_argument(
        "--rqlite-host",
        default=os.environ.get("FORGEWIRE_HUB_RQLITE_HOST", "127.0.0.1"),
        help="rqlite cluster member host (any node; writes auto-redirect to leader).",
    )
    parser.add_argument(
        "--rqlite-port",
        type=int,
        default=int(os.environ.get("FORGEWIRE_HUB_RQLITE_PORT", "4001")),
        help="rqlite HTTP API port (default 4001).",
    )
    parser.add_argument(
        "--rqlite-consistency",
        default=os.environ.get("FORGEWIRE_HUB_RQLITE_CONSISTENCY", "strong"),
        choices=("none", "weak", "strong", "linearizable"),
        help="rqlite read consistency level for SELECTs.",
    )
    parser.add_argument(
        "--policy-file",
        default=os.environ.get("FORGEWIRE_HUB_POLICY_FILE"),
        help=(
            "Path to a policy.yaml consumed by HubDispatchGate (M2.5.1/M2.5.2). "
            "When omitted the hub runs with an empty (permissive) policy and "
            "still emits structured PolicyDecision records on dispatch/completion."
        ),
    )
    parser.add_argument(
        "--approval-webhook",
        default=os.environ.get("FORGEWIRE_HUB_APPROVAL_WEBHOOK"),
        help=(
            "Optional URL the hub POSTs to when a new approval row is created. "
            "Receives JSON {event:'approval.created', approval_id, task_label, "
            "branch, scope_globs, decision}. Failures are logged, never blocking."
        ),
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
        require_signed_dispatch=args.require_signed_dispatch,
        backend=args.backend,
        rqlite_host=args.rqlite_host,
        rqlite_port=args.rqlite_port,
        rqlite_consistency=args.rqlite_consistency,
        policy_path=Path(args.policy_file).expanduser() if args.policy_file else None,
        approval_webhook_url=args.approval_webhook,
    )
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    app = create_app(config)
    advertisement = None
    if args.mdns:
        from forgewire_fabric.hub.discovery import advertise_hub

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
