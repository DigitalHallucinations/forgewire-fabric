"""Tests for the runner's startup-validation and capability-probing logic.

Both behaviors exist to keep the hub from routing work to a runner that
*looks* healthy but cannot actually execute tasks:

* ``RunnerConfig.from_env`` must reject a missing ``workspace_root`` so the
  service fails to start instead of registering and then failing every
  single shell spawn with ``WinError 267``.
* ``detect_tools`` must probe each candidate binary with its ``--version``
  argv before reporting it. The Windows ``py.exe`` launcher is on PATH for
  every account, but under accounts that have no installed Python (e.g.
  LocalSystem) it exits 103/112 with "No installed Python found!" -- the
  runner advertised ``py`` and the hub routed every Python task to a host
  that could not run them.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from forgewire_fabric.runner.agent import RunnerConfig
from forgewire_fabric.runner import runner_capabilities


def test_from_env_rejects_missing_workspace_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    missing = tmp_path / "nope" / "does-not-exist"
    monkeypatch.setenv("FORGEWIRE_RUNNER_WORKSPACE_ROOT", str(missing))
    with pytest.raises(RuntimeError) as excinfo:
        RunnerConfig.from_env()
    msg = str(excinfo.value)
    assert "FORGEWIRE_RUNNER_WORKSPACE_ROOT" in msg
    assert "does-not-exist" in msg


def test_from_env_accepts_existing_workspace_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FORGEWIRE_RUNNER_WORKSPACE_ROOT", str(tmp_path))
    cfg = RunnerConfig.from_env()
    assert cfg.workspace_root == str(tmp_path)


def test_from_env_falls_back_to_cwd(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FORGEWIRE_RUNNER_WORKSPACE_ROOT", raising=False)
    cfg = RunnerConfig.from_env()
    # ``os.getcwd()`` is by definition an existing directory, so this must
    # succeed without raising.
    assert os.path.isdir(cfg.workspace_root)


def test_detect_tools_drops_present_but_broken_binary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If ``shutil.which`` finds a binary but its probe fails, the tool
    must be omitted from the advertised list."""

    def fake_which(name: str) -> str | None:
        # Pretend git, python, and py are all on PATH.
        if name in {"git", "python", "py"}:
            return f"/fake/{name}"
        return None

    def fake_works(tool: str) -> bool:
        # py is the broken one (LocalSystem case).
        return tool != "py"

    monkeypatch.setattr(runner_capabilities.shutil, "which", fake_which)
    monkeypatch.setattr(runner_capabilities, "_tool_works", fake_works)

    tools = runner_capabilities.detect_tools()
    assert "git" in tools
    assert "python" in tools
    assert "py" not in tools


def test_detect_tools_includes_all_when_probe_passes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runner_capabilities.shutil,
        "which",
        lambda name: f"/fake/{name}" if name in {"git", "node"} else None,
    )
    monkeypatch.setattr(runner_capabilities, "_tool_works", lambda tool: True)
    assert set(runner_capabilities.detect_tools()) == {"git", "node"}
