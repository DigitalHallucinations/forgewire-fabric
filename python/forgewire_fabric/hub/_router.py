"""ForgeWire claim-router facade.

Resolves at import time to the Rust-accelerated `forgewire_runtime.pick_task`
when available, otherwise falls back to a pure-Python implementation that
mirrors `Blackboard.claim_next_task_v2`'s candidate match loop.

Operators can force the Python path with ``FORGEWIRE_FORCE_PYTHON=1``.

Lineage: Stage C.2 of the forgewire-runtime extraction (formerly PhrenForge todo 113).
"""

from __future__ import annotations

import os
from typing import Any, Mapping, Sequence

__all__ = ["HAS_RUST", "pick_task", "glob_static_prefix", "scopes_within"]


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

        _use_rust = bool(getattr(_rust, "HAS_RUST", False)) and hasattr(_rust, "pick_task")
    except ImportError:
        _use_rust = False


# ---------------------------------------------------------------------------
# Pure-Python reference implementation (also used as the fallback path).
# Kept byte-identical to the loop in scripts/remote/hub/server.py.
# ---------------------------------------------------------------------------


def glob_static_prefix(glob: str) -> str:
    """Return the leading wildcard-free directory prefix of a glob."""
    norm = glob.replace("\\", "/")
    cut = len(norm)
    for ch in ("*", "?", "["):
        idx = norm.find(ch)
        if idx != -1 and idx < cut:
            cut = idx
    head = norm[:cut]
    if "/" in head:
        head = head.rsplit("/", 1)[0] + "/"
    else:
        head = ""
    return head


def scopes_within(task_globs: Sequence[str], runner_prefixes: Sequence[str]) -> bool:
    """True iff every task glob's static prefix overlaps some runner prefix."""
    if not runner_prefixes:
        return True
    for glob in task_globs:
        head = glob_static_prefix(glob)
        if not any(head.startswith(p) or p.startswith(head) for p in runner_prefixes):
            return False
    return True


def _py_pick_task(
    tasks: Sequence[Mapping[str, Any]],
    runner: Mapping[str, Any],
) -> tuple[int | None, int]:
    scope_prefixes = [
        p.replace("\\", "/").rstrip("/") + "/"
        for p in (runner.get("scope_prefixes") or [])
        if p
    ]
    tool_set = {t.lower() for t in (runner.get("tools") or [])}
    tag_set = {t.lower() for t in (runner.get("tags") or [])}
    runner_tenant = runner.get("tenant")
    runner_ws = runner.get("workspace_root")
    last_known = runner.get("last_known_commit")

    seen = 0
    for idx, task in enumerate(tasks):
        seen += 1
        t_tenant = task.get("tenant")
        if t_tenant and t_tenant != runner_tenant:
            continue
        t_ws = task.get("workspace_root")
        if t_ws and runner_ws and t_ws != runner_ws:
            continue
        if scope_prefixes and not scopes_within(task.get("scope_globs") or [], scope_prefixes):
            continue
        req_tools = task.get("required_tools") or []
        if any(t.lower() not in tool_set for t in req_tools):
            continue
        req_tags = task.get("required_tags") or []
        if any(t.lower() not in tag_set for t in req_tags):
            continue
        if task.get("require_base_commit"):
            if not last_known:
                continue
            if task.get("base_commit") != last_known:
                continue
        return idx, seen
    return None, seen


if _use_rust:
    HAS_RUST = True

    def pick_task(
        tasks: Sequence[Mapping[str, Any]],
        runner: Mapping[str, Any],
    ) -> tuple[int | None, int]:
        # Rust binding requires concrete list/dict types.
        return _rust.pick_task(list(tasks), dict(runner))

else:
    HAS_RUST = False

    def pick_task(
        tasks: Sequence[Mapping[str, Any]],
        runner: Mapping[str, Any],
    ) -> tuple[int | None, int]:
        return _py_pick_task(tasks, runner)
