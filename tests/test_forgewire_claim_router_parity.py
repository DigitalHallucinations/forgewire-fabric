"""Parity tests for the ForgeWire claim router.

Asserts the Rust `forgewire_runtime.pick_task` selects the same candidate as
the Python reference implementation across a wide-coverage fuzz corpus.

Stage C.2 of PhrenForge todo 113.
"""

from __future__ import annotations

import importlib
import random
from typing import Any

import pytest

# Skip the whole module if the Rust extension isn't built.
forgewire_runtime = pytest.importorskip("forgewire_runtime")
if not getattr(forgewire_runtime, "HAS_RUST", False) or not hasattr(
    forgewire_runtime, "pick_task"
):
    pytest.skip("forgewire_runtime.pick_task not available", allow_module_level=True)

from forgewire.hub import _router as router_mod
from forgewire.hub._router import _py_pick_task


def _rust_pick(tasks: list[dict[str, Any]], runner: dict[str, Any]) -> tuple[int | None, int]:
    return forgewire_runtime.pick_task(list(tasks), dict(runner))


# --- Fixed cases --------------------------------------------------------------


def test_simple_match() -> None:
    tasks = [
        {
            "scope_globs": ["modules/jobs/**"],
            "required_tools": [],
            "required_tags": [],
            "tenant": None,
            "workspace_root": None,
            "require_base_commit": False,
            "base_commit": "",
        }
    ]
    runner = {
        "scope_prefixes": ["modules/"],
        "tools": [],
        "tags": [],
        "tenant": None,
        "workspace_root": None,
        "last_known_commit": None,
    }
    assert _rust_pick(tasks, runner) == _py_pick_task(tasks, runner) == (0, 1)


def test_skips_to_second_when_first_disjoint() -> None:
    tasks = [
        {
            "scope_globs": ["docs/foo.md"],
            "required_tools": [],
            "required_tags": [],
            "tenant": None,
            "workspace_root": None,
            "require_base_commit": False,
            "base_commit": "",
        },
        {
            "scope_globs": ["modules/jobs/x.py"],
            "required_tools": [],
            "required_tags": [],
            "tenant": None,
            "workspace_root": None,
            "require_base_commit": False,
            "base_commit": "",
        },
    ]
    runner = {
        "scope_prefixes": ["modules/"],
        "tools": [],
        "tags": [],
        "tenant": None,
        "workspace_root": None,
        "last_known_commit": None,
    }
    assert _rust_pick(tasks, runner) == _py_pick_task(tasks, runner) == (1, 2)


def test_no_match_returns_none() -> None:
    tasks = [
        {
            "scope_globs": ["docs/foo.md"],
            "required_tools": [],
            "required_tags": [],
            "tenant": None,
            "workspace_root": None,
            "require_base_commit": False,
            "base_commit": "",
        },
    ]
    runner = {
        "scope_prefixes": ["modules/"],
        "tools": [],
        "tags": [],
        "tenant": None,
        "workspace_root": None,
        "last_known_commit": None,
    }
    assert _rust_pick(tasks, runner) == _py_pick_task(tasks, runner) == (None, 1)


def test_required_tools_case_insensitive() -> None:
    tasks = [
        {
            "scope_globs": ["docs/foo.md"],
            "required_tools": ["Git", "PYTEST"],
            "required_tags": [],
            "tenant": None,
            "workspace_root": None,
            "require_base_commit": False,
            "base_commit": "",
        }
    ]
    runner = {
        "scope_prefixes": ["docs/"],
        "tools": ["git", "pytest", "rg"],
        "tags": [],
        "tenant": None,
        "workspace_root": None,
        "last_known_commit": None,
    }
    assert _rust_pick(tasks, runner)[0] == _py_pick_task(tasks, runner)[0] == 0


def test_tenant_must_match_when_pinned() -> None:
    tasks = [
        {
            "scope_globs": ["docs/foo.md"],
            "required_tools": [],
            "required_tags": [],
            "tenant": "alpha",
            "workspace_root": None,
            "require_base_commit": False,
            "base_commit": "",
        }
    ]
    r_match = {
        "scope_prefixes": ["docs/"],
        "tools": [],
        "tags": [],
        "tenant": "alpha",
        "workspace_root": None,
        "last_known_commit": None,
    }
    r_miss = dict(r_match, tenant="beta")
    assert _rust_pick(tasks, r_match)[0] == _py_pick_task(tasks, r_match)[0] == 0
    assert _rust_pick(tasks, r_miss)[0] == _py_pick_task(tasks, r_miss)[0] is None


def test_base_commit_precondition() -> None:
    tasks = [
        {
            "scope_globs": ["docs/foo.md"],
            "required_tools": [],
            "required_tags": [],
            "tenant": None,
            "workspace_root": None,
            "require_base_commit": True,
            "base_commit": "abc1234",
        }
    ]
    runner_no_commit = {
        "scope_prefixes": ["docs/"],
        "tools": [],
        "tags": [],
        "tenant": None,
        "workspace_root": None,
        "last_known_commit": None,
    }
    runner_match = dict(runner_no_commit, last_known_commit="abc1234")
    runner_mismatch = dict(runner_no_commit, last_known_commit="zzz9999")
    assert _rust_pick(tasks, runner_no_commit)[0] == _py_pick_task(tasks, runner_no_commit)[0] is None
    assert _rust_pick(tasks, runner_match)[0] == _py_pick_task(tasks, runner_match)[0] == 0
    assert _rust_pick(tasks, runner_mismatch)[0] == _py_pick_task(tasks, runner_mismatch)[0] is None


def test_empty_runner_prefixes_accept_any_scope() -> None:
    tasks = [
        {
            "scope_globs": ["modules/jobs/**"],
            "required_tools": [],
            "required_tags": [],
            "tenant": None,
            "workspace_root": None,
            "require_base_commit": False,
            "base_commit": "",
        }
    ]
    runner = {
        "scope_prefixes": [],
        "tools": [],
        "tags": [],
        "tenant": None,
        "workspace_root": None,
        "last_known_commit": None,
    }
    assert _rust_pick(tasks, runner)[0] == _py_pick_task(tasks, runner)[0] == 0


# --- Fuzz ---------------------------------------------------------------------


SCOPES = [
    "modules/jobs/**",
    "modules/orchestration/**",
    "core/services/**",
    "docs/research/**",
    "docs/operations/**",
    "tests/remote/**",
    "shell/http/**",
    "shell/cli/**",
    "**/*.md",
    "scripts/**",
]
PREFIXES = [
    "modules/",
    "core/",
    "docs/",
    "tests/",
    "shell/",
    "scripts/",
    "modules/jobs/",
    "docs/research/",
]
TOOLS = ["git", "pytest", "rg", "rust", "mypy", "ruff"]
TAGS = ["persona:forge", "persona:researcher", "tier:1", "tier:2"]
TENANTS = [None, "alpha", "beta"]
WORKSPACES = [None, "/repo/a", "/repo/b"]
COMMITS = ["abc1234", "def5678", "fedcba9", "0000000"]


def _rand_task(rng: random.Random) -> dict[str, Any]:
    n_globs = rng.randint(1, 3)
    return {
        "scope_globs": rng.sample(SCOPES, n_globs),
        "required_tools": rng.sample(TOOLS, rng.randint(0, 2)),
        "required_tags": rng.sample(TAGS, rng.randint(0, 2)),
        "tenant": rng.choice(TENANTS),
        "workspace_root": rng.choice(WORKSPACES),
        "require_base_commit": rng.random() < 0.3,
        "base_commit": rng.choice(COMMITS),
    }


def _rand_runner(rng: random.Random) -> dict[str, Any]:
    return {
        "scope_prefixes": rng.sample(PREFIXES, rng.randint(0, 4)),
        "tools": rng.sample(TOOLS, rng.randint(0, len(TOOLS))),
        "tags": rng.sample(TAGS, rng.randint(0, len(TAGS))),
        "tenant": rng.choice(TENANTS),
        "workspace_root": rng.choice(WORKSPACES),
        "last_known_commit": rng.choice([None, *COMMITS]),
    }


@pytest.mark.parametrize("seed", list(range(200)))
def test_fuzz_parity_short(seed: int) -> None:
    rng = random.Random(seed)
    n_tasks = rng.randint(0, 20)
    tasks = [_rand_task(rng) for _ in range(n_tasks)]
    runner = _rand_runner(rng)
    rust_idx, rust_seen = _rust_pick(tasks, runner)
    py_idx, py_seen = _py_pick_task(tasks, runner)
    assert rust_idx == py_idx, (seed, tasks, runner)
    assert rust_seen == py_seen, (seed, tasks, runner)


def test_fuzz_parity_long() -> None:
    """10,000 random cases. Any divergence is a parity bug."""
    rng = random.Random(20260421)
    for _ in range(10_000):
        n_tasks = rng.randint(0, 50)
        tasks = [_rand_task(rng) for _ in range(n_tasks)]
        runner = _rand_runner(rng)
        rust = _rust_pick(tasks, runner)
        py = _py_pick_task(tasks, runner)
        assert rust == py, (rust, py, tasks, runner)


# --- Facade toggle ------------------------------------------------------------


def test_facade_force_python(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FORGEWIRE_FORCE_PYTHON", "1")
    reloaded = importlib.reload(router_mod)
    try:
        assert reloaded.HAS_RUST is False
        tasks = [
            {
                "scope_globs": ["modules/jobs/**"],
                "required_tools": [],
                "required_tags": [],
                "tenant": None,
                "workspace_root": None,
                "require_base_commit": False,
                "base_commit": "",
            }
        ]
        runner = {
            "scope_prefixes": ["modules/"],
            "tools": [],
            "tags": [],
            "tenant": None,
            "workspace_root": None,
            "last_known_commit": None,
        }
        assert reloaded.pick_task(tasks, runner) == (0, 1)
    finally:
        monkeypatch.delenv("FORGEWIRE_FORCE_PYTHON", raising=False)
        importlib.reload(router_mod)
