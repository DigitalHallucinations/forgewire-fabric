"""ForgeWire stream-counter facade.

Resolves at import time to the Rust-accelerated
`forgewire_runtime.StreamCounter` when available, otherwise falls back to a
pure-Python equivalent that mirrors the contract one-for-one.

Operators can force the Python path with ``FORGEWIRE_FORCE_PYTHON=1``.

Stage C.3 of PhrenForge todo 113 (forgewire-runtime).

The counter assigns strictly-increasing per-task sequence numbers in-memory.
SQLite still owns durability (the hub still INSERTs every line); the counter
just eliminates the per-call ``BEGIN IMMEDIATE`` + ``SELECT MAX(seq)``
round-trip.
"""

from __future__ import annotations

import os
import threading
from typing import Protocol

__all__ = ["HAS_RUST", "StreamCounter", "make_counter"]


def _force_python() -> bool:
    return os.environ.get("FORGEWIRE_FORCE_PYTHON", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


_use_rust = False
if not _force_python():
    try:
        import forgewire_runtime as _rust  # type: ignore[import-not-found]

        _use_rust = bool(getattr(_rust, "HAS_RUST", False)) and hasattr(
            _rust, "StreamCounter"
        )
    except ImportError:
        _use_rust = False


class StreamCounter(Protocol):
    def prime(self, task_id: int, current_max: int) -> None: ...
    def is_primed(self, task_id: int) -> bool: ...
    def next_seq(self, task_id: int) -> int: ...
    def forget(self, task_id: int) -> None: ...
    def task_count(self) -> int: ...


class _PyStreamCounter:
    """Pure-Python reference. Locked dict; behavior matches the Rust impl."""

    __slots__ = ("_lock", "_seqs")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seqs: dict[int, int] = {}

    def prime(self, task_id: int, current_max: int) -> None:
        with self._lock:
            current = self._seqs.get(task_id, 0)
            # First prime always installs the entry (even at 0); subsequent
            # primes only ratchet the floor upward, never backward.
            if task_id not in self._seqs or current_max > current:
                self._seqs[task_id] = max(current_max, current)

    def is_primed(self, task_id: int) -> bool:
        with self._lock:
            return task_id in self._seqs

    def next_seq(self, task_id: int) -> int:
        with self._lock:
            if task_id not in self._seqs:
                raise LookupError(f"stream counter for task {task_id} not primed")
            self._seqs[task_id] += 1
            return self._seqs[task_id]

    def forget(self, task_id: int) -> None:
        with self._lock:
            self._seqs.pop(task_id, None)

    def task_count(self) -> int:
        with self._lock:
            return len(self._seqs)


if _use_rust:
    HAS_RUST = True

    def make_counter() -> StreamCounter:
        return _rust.StreamCounter()  # type: ignore[no-any-return]

else:
    HAS_RUST = False

    def make_counter() -> StreamCounter:
        return _PyStreamCounter()
