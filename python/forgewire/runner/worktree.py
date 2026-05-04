"""Worktree and scope-guard helpers for the remote runner.

Two CLI entry points:

* ``python -m forgewire.runner.worktree prepare --task-file <json>``
    Creates a git worktree at the configured location, checks out a fresh
    branch from ``base_commit``, writes ``.git/phrenforge_task.json`` inside the
    worktree (used by the pre-commit hook), and prints the worktree path.

* ``python -m forgewire.runner.worktree check-scope <files>...``
    Reads ``.git/phrenforge_task.json`` from the current repo and rejects any
    of the given paths that don't match a scope glob. Used as the pre-commit
    hook: ``git diff --cached --name-only | xargs python -m ... check-scope``.

The task file is small JSON written by ``prepare``::

    {
      "task_id": 42,
      "todo_id": "109-jobs",
      "branch": "agent/optiplex/109-jobs",
      "base_commit": "abc123...",
      "scope_globs": ["modules/jobs/**", "tests/jobs/**"]
    }
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

TASK_FILE_NAME = "phrenforge_task.json"


@dataclass(slots=True)
class TaskRecord:
    task_id: int
    branch: str
    base_commit: str
    scope_globs: list[str]
    todo_id: str | None = None

    def to_json(self) -> str:
        return json.dumps(
            {
                "task_id": self.task_id,
                "todo_id": self.todo_id,
                "branch": self.branch,
                "base_commit": self.base_commit,
                "scope_globs": self.scope_globs,
            },
            indent=2,
        )

    @classmethod
    def from_dict(cls, data: dict) -> "TaskRecord":
        return cls(
            task_id=int(data["task_id"]),
            branch=str(data["branch"]),
            base_commit=str(data["base_commit"]),
            scope_globs=list(data["scope_globs"]),
            todo_id=data.get("todo_id"),
        )


# ---------------------------------------------------------------------------
# Scope matching
# ---------------------------------------------------------------------------


def _normalise(path: str) -> str:
    return path.replace("\\", "/").lstrip("./")


def matches_any(path: str, globs: Sequence[str]) -> bool:
    """Return True if ``path`` matches at least one glob.

    Globs use POSIX-style separators. ``**`` matches zero or more path
    components; bare ``*`` matches a single component. Implemented via
    fnmatch on a normalised string -- adequate for our deny-by-default check.
    """
    norm = _normalise(path)
    for pattern in globs:
        normalised_pattern = pattern.replace("\\", "/")
        if fnmatch.fnmatchcase(norm, normalised_pattern):
            return True
        # Fallback: '**' wildcard fnmatch quirk -- treat '**' as '*' when needed.
        if "**" in normalised_pattern:
            collapsed = normalised_pattern.replace("**", "*")
            if fnmatch.fnmatchcase(norm, collapsed):
                return True
    return False


def find_violations(
    paths: Iterable[str], globs: Sequence[str]
) -> list[str]:
    return [p for p in paths if not matches_any(p, globs)]


# ---------------------------------------------------------------------------
# git helpers
# ---------------------------------------------------------------------------


def _run_git(args: Sequence[str], *, cwd: Path | None = None) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=str(cwd) if cwd else None,
        check=True,
        text=True,
        capture_output=True,
    )
    return proc.stdout.strip()


def _repo_root(cwd: Path | None = None) -> Path:
    return Path(_run_git(["rev-parse", "--show-toplevel"], cwd=cwd))


def _git_dir(cwd: Path | None = None) -> Path:
    return Path(_run_git(["rev-parse", "--git-dir"], cwd=cwd))


# ---------------------------------------------------------------------------
# prepare-worktree
# ---------------------------------------------------------------------------


def prepare_worktree(
    *,
    task: TaskRecord,
    repo: Path,
    worktree_root: Path,
) -> Path:
    """Create a fresh worktree at ``worktree_root / task-<id>``.

    The branch is created from ``task.base_commit``. If the worktree path
    already exists it is reused (useful for retries) but we still ensure the
    correct branch is checked out.
    """
    repo = repo.resolve()
    worktree_root.mkdir(parents=True, exist_ok=True)
    target = worktree_root / f"task-{task.task_id}"
    if not target.exists():
        # Make sure the base commit exists locally first.
        _run_git(["fetch", "origin"], cwd=repo)
        _run_git(
            [
                "worktree",
                "add",
                "-b",
                task.branch,
                str(target),
                task.base_commit,
            ],
            cwd=repo,
        )
    else:
        # Existing worktree: ensure branch is checked out.
        try:
            _run_git(["checkout", task.branch], cwd=target)
        except subprocess.CalledProcessError:
            _run_git(["checkout", "-b", task.branch, task.base_commit], cwd=target)

    _install_task_file(task, worktree=target)
    _install_pre_commit_hook(worktree=target)
    return target


def _install_task_file(task: TaskRecord, *, worktree: Path) -> None:
    git_dir = _git_dir(cwd=worktree)
    if not git_dir.is_absolute():
        git_dir = (worktree / git_dir).resolve()
    task_path = git_dir / TASK_FILE_NAME
    task_path.write_text(task.to_json(), encoding="utf-8")


def _install_pre_commit_hook(*, worktree: Path) -> None:
    git_dir = _git_dir(cwd=worktree)
    if not git_dir.is_absolute():
        git_dir = (worktree / git_dir).resolve()
    hooks_dir = git_dir / "hooks"
    hooks_dir.mkdir(parents=True, exist_ok=True)
    hook_path = hooks_dir / "pre-commit"
    repo_root = _repo_root(cwd=worktree)
    # Use the main repo's python; falls back to system python on PATH.
    contents = (
        "#!/bin/sh\n"
        "# Auto-generated by phrenforge remote-runner. Rejects commits whose\n"
        "# staged paths fall outside the task's scope_globs.\n"
        "python_exe=${PHRENFORGE_PYTHON:-python}\n"
        f"repo_root='{repo_root.as_posix()}'\n"
        'staged="$(git diff --cached --name-only)"\n'
        'if [ -z "$staged" ]; then\n'
        "  exit 0\n"
        "fi\n"
        'echo "$staged" | "$python_exe" -m forgewire.runner.worktree '
        'check-scope --repo "$repo_root" --stdin\n'
    )
    hook_path.write_text(contents, encoding="utf-8")
    try:
        os.chmod(hook_path, 0o755)
    except OSError:
        # Windows: chmod is a no-op effectively; git for windows runs hooks via sh.
        pass


# ---------------------------------------------------------------------------
# check-scope
# ---------------------------------------------------------------------------


def load_task_file(*, worktree: Path) -> TaskRecord:
    git_dir = _git_dir(cwd=worktree)
    if not git_dir.is_absolute():
        git_dir = (worktree / git_dir).resolve()
    task_path = git_dir / TASK_FILE_NAME
    if not task_path.exists():
        raise FileNotFoundError(
            f"missing {TASK_FILE_NAME} -- worktree was not prepared by phrenforge"
        )
    return TaskRecord.from_dict(json.loads(task_path.read_text(encoding="utf-8")))


def check_scope(
    *, files: Sequence[str], worktree: Path
) -> tuple[bool, list[str]]:
    task = load_task_file(worktree=worktree)
    violations = find_violations(files, task.scope_globs)
    return (not violations, violations)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cmd_prepare(args: argparse.Namespace) -> int:
    payload = json.loads(Path(args.task_file).read_text(encoding="utf-8"))
    task = TaskRecord.from_dict(payload)
    worktree = prepare_worktree(
        task=task,
        repo=Path(args.repo).resolve(),
        worktree_root=Path(args.worktree_root).resolve(),
    )
    print(str(worktree))
    return 0


def _cmd_check_scope(args: argparse.Namespace) -> int:
    if args.stdin:
        files = [line.strip() for line in sys.stdin.read().splitlines() if line.strip()]
    else:
        files = list(args.files)
    if not files:
        return 0
    worktree = Path(args.repo or os.getcwd()).resolve()
    ok, violations = check_scope(files=files, worktree=worktree)
    if ok:
        return 0
    print(
        "phrenforge scope guard: rejecting commit -- the following paths are "
        "outside the task's writable scope:",
        file=sys.stderr,
    )
    for v in violations:
        print(f"  {v}", file=sys.stderr)
    task = load_task_file(worktree=worktree)
    print(
        f"  scope_globs = {task.scope_globs}",
        file=sys.stderr,
    )
    return 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="phrenforge-runner-worktree",
        description="Worktree + scope-guard helpers for the remote runner.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_prepare = sub.add_parser("prepare", help="Create or reuse a task worktree.")
    p_prepare.add_argument("--task-file", required=True)
    p_prepare.add_argument("--repo", required=True)
    p_prepare.add_argument(
        "--worktree-root",
        default=str(Path.home() / ".phrenforge" / "worktrees"),
    )
    p_prepare.set_defaults(func=_cmd_prepare)

    p_check = sub.add_parser(
        "check-scope",
        help="Verify staged paths fall within the task's scope_globs.",
    )
    p_check.add_argument("files", nargs="*", help="Paths relative to the worktree.")
    p_check.add_argument("--stdin", action="store_true", help="Read paths from stdin.")
    p_check.add_argument("--repo", default=None, help="Worktree path (default cwd).")
    p_check.set_defaults(func=_cmd_check_scope)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
