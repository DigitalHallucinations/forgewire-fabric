"""Parity + correctness tests for the ForgeWire stream-seq counter.

Stage C.3 of PhrenForge todo 113. The Rust counter and Python fallback must
have identical observable behavior under sequential and concurrent load,
and they must hand SQLite strictly-monotonic, gap-free seqs per task even
when many threads call ``append_stream`` simultaneously.
"""

from __future__ import annotations

import importlib
import os
import sqlite3
import threading
from pathlib import Path

import pytest

from forgewire.hub import _streams as streams_module
from forgewire.hub._streams import _PyStreamCounter

try:
    import forgewire_runtime as _rust  # type: ignore[import-not-found]

    _RUST_AVAILABLE = bool(getattr(_rust, "HAS_RUST", False)) and hasattr(
        _rust, "StreamCounter"
    )
except ImportError:
    _RUST_AVAILABLE = False


def _both_counters() -> list[tuple[str, object]]:
    out: list[tuple[str, object]] = [("python", _PyStreamCounter())]
    if _RUST_AVAILABLE:
        out.append(("rust", _rust.StreamCounter()))
    return out


@pytest.mark.parametrize("name,counter", _both_counters())
def test_next_seq_requires_prime(name: str, counter: object) -> None:
    with pytest.raises(LookupError):
        counter.next_seq(1)  # type: ignore[attr-defined]


@pytest.mark.parametrize("name,counter", _both_counters())
def test_basic_increment(name: str, counter: object) -> None:
    counter.prime(1, 0)  # type: ignore[attr-defined]
    assert counter.next_seq(1) == 1  # type: ignore[attr-defined]
    assert counter.next_seq(1) == 2  # type: ignore[attr-defined]
    assert counter.next_seq(1) == 3  # type: ignore[attr-defined]


@pytest.mark.parametrize("name,counter", _both_counters())
def test_prime_resumes_after_existing_rows(name: str, counter: object) -> None:
    counter.prime(42, 17)  # type: ignore[attr-defined]
    assert counter.next_seq(42) == 18  # type: ignore[attr-defined]


@pytest.mark.parametrize("name,counter", _both_counters())
def test_prime_idempotent_no_regression(name: str, counter: object) -> None:
    counter.prime(7, 100)  # type: ignore[attr-defined]
    counter.next_seq(7)  # type: ignore[attr-defined]
    counter.prime(7, 50)  # stale prime  # type: ignore[attr-defined]
    assert counter.next_seq(7) == 102  # type: ignore[attr-defined]


@pytest.mark.parametrize("name,counter", _both_counters())
def test_distinct_tasks_independent(name: str, counter: object) -> None:
    counter.prime(1, 0)  # type: ignore[attr-defined]
    counter.prime(2, 0)  # type: ignore[attr-defined]
    assert counter.next_seq(1) == 1  # type: ignore[attr-defined]
    assert counter.next_seq(2) == 1  # type: ignore[attr-defined]
    assert counter.next_seq(1) == 2  # type: ignore[attr-defined]


@pytest.mark.parametrize("name,counter", _both_counters())
def test_forget_resets(name: str, counter: object) -> None:
    counter.prime(1, 5)  # type: ignore[attr-defined]
    assert counter.next_seq(1) == 6  # type: ignore[attr-defined]
    counter.forget(1)  # type: ignore[attr-defined]
    with pytest.raises(LookupError):
        counter.next_seq(1)  # type: ignore[attr-defined]


@pytest.mark.parametrize("name,counter", _both_counters())
def test_concurrent_increments_unique_and_dense(name: str, counter: object) -> None:
    counter.prime(1, 0)  # type: ignore[attr-defined]
    n_threads = 8
    per_thread = 500
    out: list[int] = []
    out_lock = threading.Lock()

    def worker() -> None:
        local = [counter.next_seq(1) for _ in range(per_thread)]  # type: ignore[attr-defined]
        with out_lock:
            out.extend(local)

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    out.sort()
    total = n_threads * per_thread
    assert out[0] == 1
    assert out[-1] == total
    assert out == list(range(1, total + 1))


def test_facade_force_python(monkeypatch: pytest.MonkeyPatch) -> None:
    """Operators can pin the Python path with FORGEWIRE_FORCE_PYTHON=1."""
    monkeypatch.setenv("FORGEWIRE_FORCE_PYTHON", "1")
    reloaded = importlib.reload(streams_module)
    try:
        assert reloaded.HAS_RUST is False
        c = reloaded.make_counter()
        c.prime(1, 0)
        assert c.next_seq(1) == 1
    finally:
        monkeypatch.delenv("FORGEWIRE_FORCE_PYTHON", raising=False)
        importlib.reload(streams_module)


def test_blackboard_append_stream_uses_counter(tmp_path: Path) -> None:
    """End-to-end: Blackboard.append_stream emits dense, monotonic seqs."""
    from forgewire.hub.server import Blackboard

    db_path = tmp_path / "bb.sqlite3"
    bb = Blackboard(db_path)

    # Seed a task + claimed worker via direct SQL (bypass policy layer; this
    # is a unit-level test of the streaming path, not the orchestrator).
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO tasks
                (id, title, prompt, scope_globs, base_commit, branch,
                 status, worker_id)
            VALUES (1, 't', 'p', '[]', 'abc', 'agent/x/1',
                    'running', 'w')
            """
        )
        conn.commit()

    seqs: list[int] = []
    for i in range(50):
        result = bb.append_stream(
            task_id=1, worker_id="w", channel="stdout", line=f"line {i}"
        )
        seqs.append(result["seq"])

    assert seqs == list(range(1, 51))


def test_blackboard_reprimes_after_restart(tmp_path: Path) -> None:
    """A fresh Blackboard re-primes from MAX(seq) — kill -9 is safe."""
    from forgewire.hub.server import Blackboard

    db_path = tmp_path / "bb.sqlite3"
    bb = Blackboard(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO tasks
                (id, title, prompt, scope_globs, base_commit, branch,
                 status, worker_id)
            VALUES (1, 't', 'p', '[]', 'abc', 'agent/x/1',
                    'running', 'w')
            """
        )
        conn.commit()

    for i in range(5):
        bb.append_stream(task_id=1, worker_id="w", channel="stdout", line=str(i))

    # Simulate hub restart
    bb2 = Blackboard(db_path)
    result = bb2.append_stream(
        task_id=1, worker_id="w", channel="stdout", line="post-restart"
    )
    assert result["seq"] == 6


def test_blackboard_append_stream_bulk_assigns_dense_seqs(tmp_path: Path) -> None:
    """append_stream_bulk wraps N inserts in one BEGIN/COMMIT.

    Verifies (a) all entries land, (b) seqs are dense and monotonic, and
    (c) the response reports first/last seq accurately. The throughput
    payoff (one fsync per batch instead of N) isn't asserted here — that
    lives in the bench — but the wire protocol is validated end-to-end.
    """
    from forgewire.hub.server import Blackboard

    db_path = tmp_path / "bb.sqlite3"
    bb = Blackboard(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO tasks
                (id, title, prompt, scope_globs, base_commit, branch,
                 status, worker_id)
            VALUES (1, 't', 'p', '[]', 'abc', 'agent/x/1',
                    'running', 'w')
            """
        )
        conn.commit()

    entries = [
        {"channel": "stdout", "line": f"bulk-{i}"} for i in range(100)
    ]
    response = bb.append_stream_bulk(task_id=1, worker_id="w", entries=entries)

    assert response["count"] == 100
    assert response["first_seq"] == 1
    assert response["last_seq"] == 100

    # Subsequent single-line append picks up at 101.
    follow = bb.append_stream(
        task_id=1, worker_id="w", channel="info", line="after"
    )
    assert follow["seq"] == 101

    # All 101 rows are persisted in order.
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT seq, channel, line FROM task_streams ORDER BY seq"
        ).fetchall()
    assert len(rows) == 101
    assert [r[0] for r in rows] == list(range(1, 102))
    assert rows[-1] == (101, "info", "after")


def test_blackboard_append_stream_bulk_rejects_bad_channel(tmp_path: Path) -> None:
    from forgewire.hub.server import Blackboard

    db_path = tmp_path / "bb.sqlite3"
    bb = Blackboard(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO tasks
                (id, title, prompt, scope_globs, base_commit, branch,
                 status, worker_id)
            VALUES (1, 't', 'p', '[]', 'abc', 'agent/x/1',
                    'running', 'w')
            """
        )
        conn.commit()

    with pytest.raises(ValueError):
        bb.append_stream_bulk(
            task_id=1,
            worker_id="w",
            entries=[
                {"channel": "stdout", "line": "ok"},
                {"channel": "bogus", "line": "no"},
            ],
        )

    # Validation runs before any insert: nothing was written.
    with sqlite3.connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM task_streams").fetchone()[0]
    assert count == 0


def test_blackboard_append_stream_bulk_empty_is_noop(tmp_path: Path) -> None:
    from forgewire.hub.server import Blackboard

    db_path = tmp_path / "bb.sqlite3"
    bb = Blackboard(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO tasks
                (id, title, prompt, scope_globs, base_commit, branch,
                 status, worker_id)
            VALUES (1, 't', 'p', '[]', 'abc', 'agent/x/1',
                    'running', 'w')
            """
        )
        conn.commit()

    response = bb.append_stream_bulk(task_id=1, worker_id="w", entries=[])
    assert response == {"task_id": 1, "count": 0, "first_seq": None, "last_seq": None}
