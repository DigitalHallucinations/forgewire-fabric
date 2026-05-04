"""Microbench: forgewire_runtime.pick_task (Rust) vs Python reference.

Run from the repo root with the venv active:

    python scripts/remote/bench_claim_router.py

Locked numbers are recorded in ``forgewire-runtime/PERFORMANCE.md``.
"""

from __future__ import annotations

import random
import time
from statistics import median

from forgewire.hub._router import _py_pick_task

try:
    import forgewire_runtime as _rust
    HAS_RUST = bool(getattr(_rust, "HAS_RUST", False)) and hasattr(_rust, "pick_task")
except ImportError:
    _rust = None
    HAS_RUST = False


SCOPES = [
    "modules/jobs/**", "modules/orchestration/**", "core/services/**",
    "docs/research/**", "docs/operations/**", "tests/remote/**",
    "shell/http/**", "shell/cli/**", "**/*.md", "scripts/**",
]
PREFIXES = ["modules/", "core/", "docs/", "tests/", "shell/", "scripts/",
            "modules/jobs/", "docs/research/"]
TOOLS = ["git", "pytest", "rg", "rust", "mypy", "ruff"]
TAGS = ["persona:forge", "persona:researcher", "tier:1", "tier:2"]


def _make_corpus(n_tasks: int, seed: int = 42) -> tuple[list, dict]:
    rng = random.Random(seed)
    tasks = []
    for _ in range(n_tasks):
        tasks.append(
            {
                "scope_globs": rng.sample(SCOPES, rng.randint(1, 3)),
                "required_tools": rng.sample(TOOLS, rng.randint(0, 2)),
                "required_tags": rng.sample(TAGS, rng.randint(0, 2)),
                "tenant": rng.choice([None, "alpha", "beta"]),
                "workspace_root": None,
                "require_base_commit": False,
                "base_commit": "",
            }
        )
    runner = {
        "scope_prefixes": ["modules/", "docs/", "tests/"],
        "tools": TOOLS,
        "tags": TAGS,
        "tenant": "alpha",
        "workspace_root": None,
        "last_known_commit": None,
    }
    return tasks, runner


def _bench(label: str, fn, iterations: int) -> float:
    fn()
    samples: list[float] = []
    for _ in range(5):
        t0 = time.perf_counter()
        for _ in range(iterations):
            fn()
        samples.append((time.perf_counter() - t0) / iterations)
    m = median(samples)
    print(f"  {label:<24s} {m * 1e6:9.2f} µs/op  ({iterations:,} iters x 5 runs)")
    return m


def main() -> None:
    print(f"forgewire_runtime HAS_RUST = {HAS_RUST}\n")
    print("=== Match in middle (typical case) ===")
    for n in (5, 25, 50):
        tasks, runner = _make_corpus(n)
        print(f"\nN_tasks = {n}")
        py_t = _bench("python", lambda: _py_pick_task(tasks, runner), 20_000)
        if HAS_RUST:
            rust_t = _bench("rust ", lambda: _rust.pick_task(tasks, runner), 20_000)
            print(f"  speedup            = {py_t / rust_t:6.2f}x")

    print("\n=== Worst case: full scan, no match (tenant mismatch on every task) ===")
    for n in (5, 25, 50):
        tasks, runner = _make_corpus(n)
        # Force every candidate to be filtered — runner tenant doesn't match any.
        runner_no = dict(runner, tenant="zzz_unknown")
        # And every task pins a tenant.
        for t in tasks:
            t["tenant"] = "alpha"
        print(f"\nN_tasks = {n}")
        py_t = _bench("python", lambda: _py_pick_task(tasks, runner_no), 20_000)
        if HAS_RUST:
            rust_t = _bench("rust ", lambda: _rust.pick_task(tasks, runner_no), 20_000)
            print(f"  speedup            = {py_t / rust_t:6.2f}x")


if __name__ == "__main__":
    main()
