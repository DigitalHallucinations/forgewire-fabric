"""Live-cluster smoke tests for :mod:`forgewire_fabric.hub._rqlite_db`.

Runs against the real 3-voter rqlite cluster bootstrapped in Phase 1.
Requires ``RQLITE_HOST`` (default ``10.120.81.95``) and ``RQLITE_PORT``
(default ``4001``) reachable on the LAN. Tests are skipped automatically
when the cluster is unreachable so this file is safe to keep in the
default test pass.

Each test uses an isolated table name to avoid cross-test interference
and tears it down on teardown.
"""
from __future__ import annotations

import contextlib
import os
import socket
import uuid

import httpx
import pytest

from forgewire_fabric.hub import _rqlite_db as rdb

RQLITE_HOST = os.environ.get("RQLITE_HOST", "10.120.81.95")
RQLITE_PORT = int(os.environ.get("RQLITE_PORT", "4001"))


def _cluster_reachable() -> bool:
    try:
        with socket.create_connection((RQLITE_HOST, RQLITE_PORT), timeout=1.0):
            pass
    except OSError:
        return False
    try:
        with httpx.Client(
            base_url=f"http://{RQLITE_HOST}:{RQLITE_PORT}", timeout=2.0
        ) as c:
            r = c.get("/status")
            return r.status_code == 200
    except httpx.HTTPError:
        return False


pytestmark = pytest.mark.skipif(
    not _cluster_reachable(),
    reason=f"rqlite cluster {RQLITE_HOST}:{RQLITE_PORT} not reachable",
)


@pytest.fixture
def conn():
    c = rdb.connect(RQLITE_HOST, RQLITE_PORT, timeout=10.0)
    try:
        yield c
    finally:
        c.close()


@pytest.fixture
def temp_table(conn):
    name = f"t_{uuid.uuid4().hex[:8]}"
    conn.execute(f"CREATE TABLE {name} (id INTEGER PRIMARY KEY, v TEXT)")
    try:
        yield name
    finally:
        with contextlib.suppress(rdb.Error):
            conn.execute(f"DROP TABLE IF EXISTS {name}")


def test_insert_and_select(conn, temp_table):
    conn.execute(f"INSERT INTO {temp_table}(v) VALUES (?)", ("alpha",))
    conn.execute(f"INSERT INTO {temp_table}(v) VALUES (?)", ("beta",))
    rows = conn.execute(
        f"SELECT id, v FROM {temp_table} ORDER BY id"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]["v"] == "alpha"
    assert rows[1]["v"] == "beta"
    # Numeric and name access agree.
    assert rows[0][1] == rows[0]["v"]
    assert rows[0].keys() == ("id", "v")


def test_executemany(conn, temp_table):
    conn.executemany(
        f"INSERT INTO {temp_table}(v) VALUES (?)",
        [("a",), ("b",), ("c",), ("d",)],
    )
    n = conn.execute(f"SELECT COUNT(*) AS n FROM {temp_table}").fetchone()
    assert n["n"] == 4


def test_transaction_commit(conn, temp_table):
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(f"INSERT INTO {temp_table}(v) VALUES (?)", ("tx-row",))
    conn.execute(f"INSERT INTO {temp_table}(v) VALUES (?)", ("tx-row-2",))
    conn.execute("COMMIT")
    rows = conn.execute(f"SELECT v FROM {temp_table} ORDER BY id").fetchall()
    assert [r["v"] for r in rows] == ["tx-row", "tx-row-2"]


def test_transaction_rollback(conn, temp_table):
    conn.execute(f"INSERT INTO {temp_table}(v) VALUES (?)", ("seed",))
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(f"INSERT INTO {temp_table}(v) VALUES (?)", ("doomed",))
    conn.execute("ROLLBACK")
    rows = conn.execute(f"SELECT v FROM {temp_table} ORDER BY id").fetchall()
    assert [r["v"] for r in rows] == ["seed"]


def test_select_inside_tx_raises(conn, temp_table):
    conn.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(rdb.UnsupportedTransactionError):
            conn.execute(f"SELECT 1 FROM {temp_table}")
    finally:
        conn.execute("ROLLBACK")


def test_executescript_runs_multistatement(conn):
    name = f"t_{uuid.uuid4().hex[:8]}"
    try:
        conn.executescript(
            f"""
            CREATE TABLE {name} (id INTEGER PRIMARY KEY, v TEXT);
            INSERT INTO {name}(v) VALUES ('one');
            INSERT INTO {name}(v) VALUES ('two');
            """
        )
        rows = conn.execute(f"SELECT v FROM {name} ORDER BY id").fetchall()
        assert [r["v"] for r in rows] == ["one", "two"]
    finally:
        conn.execute(f"DROP TABLE IF EXISTS {name}")


def test_pragma_table_info_works(conn, temp_table):
    rows = conn.execute(f"PRAGMA table_info({temp_table})").fetchall()
    names = {r["name"] for r in rows}
    assert {"id", "v"} <= names


def test_pragma_foreign_keys_is_noop(conn):
    # Should not raise; rqlite has FKs on by default.
    conn.execute("PRAGMA foreign_keys = ON")


def test_integrity_error_on_unique_violation(conn):
    name = f"t_{uuid.uuid4().hex[:8]}"
    try:
        conn.execute(
            f"CREATE TABLE {name} (id INTEGER PRIMARY KEY, k TEXT UNIQUE)"
        )
        conn.execute(f"INSERT INTO {name}(k) VALUES (?)", ("dup",))
        with pytest.raises(rdb.IntegrityError):
            conn.execute(f"INSERT INTO {name}(k) VALUES (?)", ("dup",))
    finally:
        conn.execute(f"DROP TABLE IF EXISTS {name}")


def test_returning_clause_replaces_select_then_update(conn, temp_table):
    """Verify the refactor target: UPDATE ... RETURNING in one statement.

    This is the pattern Phase 2B will use to replace SELECT-inside-tx
    sequences. It must work via the adapter end-to-end.
    """
    conn.execute(f"INSERT INTO {temp_table}(v) VALUES (?)", ("before",))
    cur = conn.execute(
        f"UPDATE {temp_table} SET v = ? WHERE v = ? RETURNING id, v",
        ("after", "before"),
    )
    # rqlite returns the RETURNING rows as a result set; fetch as rows.
    rows = cur.fetchall()
    assert len(rows) == 1
    assert rows[0]["v"] == "after"


def test_context_manager_commits_on_success(conn, temp_table):
    with conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(f"INSERT INTO {temp_table}(v) VALUES (?)", ("ctx-ok",))
        conn.execute("COMMIT")
    rows = conn.execute(f"SELECT v FROM {temp_table}").fetchall()
    assert any(r["v"] == "ctx-ok" for r in rows)


def test_replication_visible_on_follower(temp_table):
    """Write through one node, read strong from another. Sanity-checks the
    Raft replication path end-to-end through the adapter.
    """
    leader = rdb.connect(RQLITE_HOST, RQLITE_PORT, timeout=10.0)
    follower_host = os.environ.get("RQLITE_FOLLOWER_HOST", "10.120.81.56")
    follower_port = int(os.environ.get("RQLITE_FOLLOWER_PORT", "4001"))
    try:
        with httpx.Client(
            base_url=f"http://{follower_host}:{follower_port}", timeout=2.0
        ) as probe:
            try:
                probe.get("/status")
            except httpx.HTTPError:
                pytest.skip(f"follower {follower_host}:{follower_port} unreachable")

        marker = f"repl-{uuid.uuid4().hex[:8]}"
        leader.execute(
            f"INSERT INTO {temp_table}(v) VALUES (?)", (marker,)
        )
        follower = rdb.connect(follower_host, follower_port, timeout=10.0)
        try:
            # strong-consistency read forces a Raft round-trip via the leader.
            row = follower.execute(
                f"SELECT v FROM {temp_table} WHERE v = ?", (marker,)
            ).fetchone()
            assert row is not None
            assert row["v"] == marker
        finally:
            follower.close()
    finally:
        leader.close()
