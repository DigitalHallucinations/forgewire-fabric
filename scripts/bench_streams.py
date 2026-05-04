"""Bench `Blackboard.append_stream` before/after the in-memory seq counter.

The naive bench is "lines per second per task". Real runners stream output
via per-line HTTP POST, so the counter's job is to remove SQLite write-lock
contention from the hot path, not to enable batching.

Run:
    python scripts/remote/bench_streams.py

Set ``FORGEWIRE_FORCE_PYTHON=1`` to compare paths.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
import time
from pathlib import Path

from scripts.remote.hub._streams import HAS_RUST


def _seed_task(db_path: Path) -> None:
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


def bench(n_lines: int = 5_000) -> None:
    from scripts.remote.hub.server import Blackboard

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
        db_path = Path(td) / "bb.sqlite3"
        bb = Blackboard(db_path)
        _seed_task(db_path)

        # Warm up
        for _ in range(50):
            bb.append_stream(
                task_id=1, worker_id="w", channel="stdout", line="warm"
            )

        start = time.perf_counter()
        for i in range(n_lines):
            bb.append_stream(
                task_id=1, worker_id="w", channel="stdout", line=f"line {i}"
            )
        elapsed = time.perf_counter() - start

    rate = n_lines / elapsed
    backend = (
        "rust"
        if HAS_RUST and not os.environ.get("FORGEWIRE_FORCE_PYTHON")
        else "python"
    )
    print(f"[{backend}] {n_lines} lines in {elapsed:.3f}s = {rate:,.0f} lines/sec")


if __name__ == "__main__":
    bench()
