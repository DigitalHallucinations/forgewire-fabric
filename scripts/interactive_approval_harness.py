"""Interactive multi-test harness for the approval gate + task round trip.

Unlike :mod:`scripts.live_smoke_approvals` (which auto-approves to validate
the wire), this harness intentionally **leaves both approvals pending in the
hub** so the operator can click Approve in the VSIX Approvals pane. Once
both are approved (or denied), the harness resubmits each signed brief with
the matching ``approval_id`` and drives the round trip to a terminal state.

What it exercises per iteration:

  command leg
      signed POST /tasks/v2 on protected branch ``main`` -> 428 +
      approval_id. Operator approves in the pane. Harness re-POSTs with
      ``approval_id``. The online ``kind:command`` runner claims the task,
      executes the prompt as a shell command, reports done.

  agent leg
      signed POST /tasks/v2 on protected branch
      ``release/<slice>`` -> 428 + approval_id. Operator approves.
      Harness re-POSTs with ``approval_id``. The online ``kind:agent``
      runner (forgewire_fabric.runner.agent_kind) claims, writes the
      marker file into its sandbox, reports done.

Origin trail: every dispatch records ``metadata.origin`` with hostname,
user, dispatcher_id, harness version, iteration index, slice id, and
local wall clock. The hub also stores ``dispatcher_id`` on the approval
row natively; the metadata is the human-readable mirror that shows up in
``GET /tasks/{id}`` and the VSIX history view.

Usage:

  python scripts\\interactive_approval_harness.py
  python scripts\\interactive_approval_harness.py --iterations 3 \\
      --approval-timeout 600
  python scripts\\interactive_approval_harness.py --kinds command \\
      --no-cleanup
"""

from __future__ import annotations

import argparse
import asyncio
import getpass
import json
import os
import secrets
import socket
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

import httpx  # noqa: E402

from forgewire_fabric.dispatcher.identity import (  # noqa: E402
    load_or_create as load_or_create_dispatcher,
)

HARNESS_VERSION = "0.1.0"
DEFAULT_HUB_URL = "http://10.120.81.95:8765"
DEFAULT_TOKEN_PATH = Path(r"C:\Users\jerem\.forgewire\hub.token")
TERMINAL_STATUSES = {"done", "failed", "cancelled", "timed_out"}
APPROVAL_RESOLVED_STATUSES = {"approved", "denied", "expired", "consumed"}

COMMAND_PROBES: dict[str, tuple[str, str]] = {
    "py": ('py -c "print(\'approval-roundtrip-marker:{marker}\')"', "Python"),
    "python": ('python -c "print(\'approval-roundtrip-marker:{marker}\')"', "Python"),
    "node": ('node -e "console.log(\'approval-roundtrip-marker:{marker}\')"', "v"),
}


def _canonical(body: dict[str, Any]) -> bytes:
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _ts() -> int:
    return int(time.time())


def _nonce() -> str:
    return secrets.token_hex(16)


def _short() -> str:
    return uuid.uuid4().hex[:10]


def _auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _origin_block(*, dispatcher: Any, iteration: int, slice_id: str) -> dict[str, Any]:
    """Human-readable origin trail stamped onto every dispatch."""
    try:
        user = getpass.getuser()
    except Exception:  # pragma: no cover - defensive
        user = "unknown"
    return {
        "harness": "interactive_approval_harness",
        "harness_version": HARNESS_VERSION,
        "iteration": iteration,
        "slice_id": slice_id,
        "dispatcher_id": dispatcher.dispatcher_id,
        "dispatcher_label": dispatcher.label,
        "hostname": socket.gethostname(),
        "user": user,
        "pid": os.getpid(),
        "dispatched_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }


def _signed_dispatch_payload(
    ident: Any,
    *,
    title: str,
    prompt: str,
    scope_globs: list[str],
    branch: str,
    base_commit: str,
    todo_id: str,
    kind: str,
    timeout_minutes: int,
    priority: int,
    metadata: dict[str, Any],
    approval_id: str | None = None,
    required_tools: list[str] | None = None,
    required_tags: list[str] | None = None,
) -> dict[str, Any]:
    ts = _ts()
    nonce = _nonce()
    signed = {
        "op": "dispatch",
        "dispatcher_id": ident.dispatcher_id,
        "title": title,
        "prompt": prompt,
        "scope_globs": scope_globs,
        "base_commit": base_commit,
        "branch": branch,
        "timestamp": ts,
        "nonce": nonce,
    }
    return {
        "title": title,
        "prompt": prompt,
        "scope_globs": scope_globs,
        "base_commit": base_commit,
        "branch": branch,
        "dispatcher_id": ident.dispatcher_id,
        "timestamp": ts,
        "nonce": nonce,
        "signature": ident.sign(_canonical(signed)),
        "todo_id": todo_id,
        "timeout_minutes": timeout_minutes,
        "priority": priority,
        "metadata": metadata,
        "required_tools": required_tools or [],
        "required_tags": required_tags or [],
        "tenant": None,
        "workspace_root": None,
        "require_base_commit": False,
        "required_capabilities": [],
        "secrets_needed": [],
        "network_egress": None,
        "kind": kind,
        "approval_id": approval_id,
    }


def _dispatcher_register_payload(ident: Any) -> dict[str, Any]:
    signed = {
        "op": "register-dispatcher",
        "dispatcher_id": ident.dispatcher_id,
        "public_key": ident.public_key_hex,
        "timestamp": _ts(),
        "nonce": _nonce(),
    }
    return {
        "dispatcher_id": ident.dispatcher_id,
        "public_key": ident.public_key_hex,
        "label": ident.label,
        "hostname": socket.gethostname(),
        "timestamp": signed["timestamp"],
        "nonce": signed["nonce"],
        "signature": ident.sign(_canonical(signed)),
    }


# ---------------------------------------------------------------------------
# Hub queries
# ---------------------------------------------------------------------------


async def _resolve_base_commit(client: httpx.AsyncClient) -> str:
    """Find a real base commit the runner can route against.

    Tries the dispatcher repo first (origin/main of forgewire-fabric); falls
    back to a deterministic synthetic value if git is not available. The
    runner does not enforce the base_commit on harness briefs (the
    ``require_base_commit`` flag stays false), so any 40-hex sha is fine
    -- but a real one makes the dispatch row useful in tracing.
    """
    import subprocess

    for cwd in (ROOT, Path.cwd()):
        try:
            out = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=str(cwd),
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (subprocess.SubprocessError, FileNotFoundError):
            continue
        sha = out.stdout.strip()
        if len(sha) == 40 and all(c in "0123456789abcdef" for c in sha):
            return sha
    return "deadbeef" * 5


async def _pick_command_probe(client: httpx.AsyncClient) -> tuple[str, str, list[str]]:
    resp = await client.get("/runners")
    resp.raise_for_status()
    runners = resp.json()["runners"]
    online_command = [
        r for r in runners if r.get("state") == "online" and "kind:command" in (r.get("tags") or [])
    ]
    if not online_command:
        raise RuntimeError("no online kind:command runner; start the command runner first")
    tools_union: set[str] = set()
    for r in online_command:
        tools_union.update(r.get("tools") or [])
    for key, (cmd, _marker) in COMMAND_PROBES.items():
        if key in tools_union:
            return key, cmd, [key]
    raise RuntimeError(
        f"no usable command probe; runners advertise tools={sorted(tools_union)}"
    )


async def _check_agent_runner_online(client: httpx.AsyncClient) -> None:
    resp = await client.get("/runners")
    resp.raise_for_status()
    runners = resp.json()["runners"]
    online_agent = [
        r for r in runners if r.get("state") == "online" and "kind:agent" in (r.get("tags") or [])
    ]
    if not online_agent:
        raise RuntimeError(
            "no online kind:agent runner; start one with "
            "scripts\\run_agent_runner.ps1 before running the agent leg"
        )


# ---------------------------------------------------------------------------
# Round-trip helpers
# ---------------------------------------------------------------------------


@dataclass
class LegPlan:
    kind: str
    title: str
    prompt: str
    scope_globs: list[str]
    branch: str
    todo_id: str
    timeout_minutes: int
    priority: int
    metadata: dict[str, Any]
    required_tools: list[str] = field(default_factory=list)
    approval_id: str | None = None
    envelope_hash: str | None = None
    task_id: int | None = None
    final_status: str | None = None


async def _expect_approval(
    client: httpx.AsyncClient,
    dispatcher: Any,
    *,
    plan: LegPlan,
    base_commit: str,
) -> None:
    payload = _signed_dispatch_payload(
        dispatcher,
        title=plan.title,
        prompt=plan.prompt,
        scope_globs=plan.scope_globs,
        branch=plan.branch,
        base_commit=base_commit,
        todo_id=plan.todo_id,
        kind=plan.kind,
        timeout_minutes=plan.timeout_minutes,
        priority=plan.priority,
        metadata=plan.metadata,
        required_tools=plan.required_tools,
    )
    resp = await client.post("/tasks/v2", json=payload)
    if resp.status_code != 428:
        raise RuntimeError(
            f"[{plan.kind}] expected approval 428, got {resp.status_code}: {resp.text}"
        )
    detail = resp.json()["detail"]
    plan.approval_id = detail["approval_id"]
    plan.envelope_hash = detail["envelope_hash"]


async def _wait_for_resolution(
    client: httpx.AsyncClient,
    *,
    plans: list[LegPlan],
    timeout_seconds: int,
    poll_seconds: float,
) -> dict[str, str]:
    """Block until every plan's approval is resolved or timeout.

    Returns a mapping ``approval_id -> resolved_status``. Prints a
    countdown each poll so the operator sees the harness is alive.
    """
    deadline = time.monotonic() + timeout_seconds
    pending = {p.approval_id: p for p in plans if p.approval_id}
    resolved: dict[str, str] = {}
    while pending and time.monotonic() < deadline:
        for approval_id in list(pending):
            r = await client.get(f"/approvals/{approval_id}")
            if r.status_code == 404:
                resolved[approval_id] = "missing"
                pending.pop(approval_id, None)
                continue
            r.raise_for_status()
            status = r.json().get("status", "pending")
            if status in APPROVAL_RESOLVED_STATUSES:
                resolved[approval_id] = status
                pending.pop(approval_id, None)
        if not pending:
            break
        remaining = int(deadline - time.monotonic())
        ids = ", ".join(f"{p.kind}={p.approval_id[:8]}" for p in pending.values())
        print(
            f"  waiting for operator approval ({remaining:>4d}s left): {ids}",
            flush=True,
        )
        await asyncio.sleep(poll_seconds)
    for approval_id in pending:
        resolved[approval_id] = "timeout"
    return resolved


async def _resubmit_approved(
    client: httpx.AsyncClient,
    dispatcher: Any,
    *,
    plan: LegPlan,
    base_commit: str,
) -> None:
    payload = _signed_dispatch_payload(
        dispatcher,
        title=plan.title,
        prompt=plan.prompt,
        scope_globs=plan.scope_globs,
        branch=plan.branch,
        base_commit=base_commit,
        todo_id=plan.todo_id,
        kind=plan.kind,
        timeout_minutes=plan.timeout_minutes,
        priority=plan.priority,
        metadata=plan.metadata,
        approval_id=plan.approval_id,
        required_tools=plan.required_tools,
    )
    resp = await client.post("/tasks/v2", json=payload)
    if resp.status_code != 200:
        raise RuntimeError(
            f"[{plan.kind}] approved resubmit failed {resp.status_code}: {resp.text}"
        )
    task = resp.json()
    plan.task_id = int(task["id"])


async def _wait_for_task(
    client: httpx.AsyncClient,
    *,
    plan: LegPlan,
    timeout_seconds: int,
    poll_seconds: float,
) -> dict[str, Any]:
    if plan.task_id is None:
        raise RuntimeError(f"[{plan.kind}] cannot wait: task was never queued")
    deadline = time.monotonic() + timeout_seconds
    last_status = ""
    while time.monotonic() < deadline:
        r = await client.get(f"/tasks/{plan.task_id}")
        r.raise_for_status()
        task = r.json()
        status = task.get("status", "")
        if status != last_status:
            print(
                f"  task {plan.task_id} ({plan.kind}) -> {status}"
                + (f" worker={task.get('worker_id','')}" if task.get("worker_id") else ""),
                flush=True,
            )
            last_status = status
        if status in TERMINAL_STATUSES:
            plan.final_status = status
            return task
        await asyncio.sleep(poll_seconds)
    raise RuntimeError(f"[{plan.kind}] task {plan.task_id} did not reach terminal in time")


# ---------------------------------------------------------------------------
# Plans
# ---------------------------------------------------------------------------


def _build_plans(
    *,
    iteration: int,
    dispatcher: Any,
    kinds: list[str],
    command_probe: tuple[str, str, list[str]] | None,
) -> list[LegPlan]:
    slice_id = _short()
    origin = _origin_block(dispatcher=dispatcher, iteration=iteration, slice_id=slice_id)
    plans: list[LegPlan] = []
    if "command" in kinds:
        assert command_probe is not None
        probe_key, probe_cmd_tpl, probe_tools = command_probe
        marker = f"cmd-{slice_id}"
        prompt = probe_cmd_tpl.format(marker=marker)
        plans.append(
            LegPlan(
                kind="command",
                title=f"Approval roundtrip - command leg {slice_id}",
                prompt=prompt,
                scope_globs=[f"docs/_audit/approval-test-cmd-{slice_id}.md"],
                branch="main",
                todo_id=f"approval-harness-cmd-{slice_id}",
                timeout_minutes=5,
                priority=100,
                required_tools=probe_tools,
                metadata={
                    "origin": origin,
                    "leg": "command",
                    "probe_key": probe_key,
                    "marker": marker,
                },
            )
        )
    if "agent" in kinds:
        marker = f"agent-{slice_id}"
        rel = f"docs/_audit/approval-test-agent-{slice_id}.md"
        plans.append(
            LegPlan(
                kind="agent",
                title=f"Approval roundtrip - agent leg {slice_id}",
                prompt=(
                    f"Write file {rel} containing exactly one line "
                    f"'approval-roundtrip-marker: {marker}' and report done. "
                    "The kind:agent harness runner satisfies this from "
                    "scope_globs[0] + the marker line in this prompt."
                ),
                scope_globs=[rel],
                branch=f"release/approval-test-{slice_id}",
                todo_id=f"approval-harness-agent-{slice_id}",
                timeout_minutes=10,
                priority=100,
                metadata={
                    "origin": origin,
                    "leg": "agent",
                    "marker": marker,
                },
            )
        )
    return plans


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


async def _cleanup(
    client: httpx.AsyncClient,
    *,
    plans: list[LegPlan],
    resolved: dict[str, str],
) -> None:
    for plan in plans:
        if plan.task_id is not None and plan.final_status not in TERMINAL_STATUSES:
            try:
                await client.post(f"/tasks/{plan.task_id}/cancel", json={})
                print(f"  cleanup: cancelled task {plan.task_id}")
            except httpx.HTTPError as exc:
                print(f"  cleanup: cancel task {plan.task_id} failed: {exc}")
        if plan.approval_id and resolved.get(plan.approval_id) == "timeout":
            try:
                await client.post(
                    f"/approvals/{plan.approval_id}/deny",
                    json={"approver": "harness", "reason": "harness timeout"},
                )
                print(f"  cleanup: denied stale approval {plan.approval_id}")
            except httpx.HTTPError as exc:
                print(f"  cleanup: deny {plan.approval_id} failed: {exc}")


# ---------------------------------------------------------------------------
# Iteration driver
# ---------------------------------------------------------------------------


async def _run_iteration(
    client: httpx.AsyncClient,
    dispatcher: Any,
    *,
    iteration: int,
    kinds: list[str],
    approval_timeout: int,
    task_timeout: int,
    no_cleanup: bool,
) -> bool:
    print(f"\n=== iteration {iteration} ({', '.join(kinds)}) ===")
    base_commit = await _resolve_base_commit(client)
    command_probe: tuple[str, str, list[str]] | None = None
    if "command" in kinds:
        command_probe = await _pick_command_probe(client)
        print(f"  command probe: {command_probe[0]} ({command_probe[1]})")
    if "agent" in kinds:
        await _check_agent_runner_online(client)
        print("  kind:agent runner is online")
    plans = _build_plans(
        iteration=iteration,
        dispatcher=dispatcher,
        kinds=kinds,
        command_probe=command_probe,
    )

    resolved: dict[str, str] = {}
    try:
        for plan in plans:
            await _expect_approval(client, dispatcher, plan=plan, base_commit=base_commit)
            print(
                f"  [{plan.kind}] approval requested id={plan.approval_id} "
                f"branch={plan.branch}\n"
                f"      envelope={plan.envelope_hash[:16]}... open the "
                f"Approvals pane and click Approve"
            )
        resolved = await _wait_for_resolution(
            client,
            plans=plans,
            timeout_seconds=approval_timeout,
            poll_seconds=3.0,
        )
        for plan in plans:
            state = resolved.get(plan.approval_id or "", "missing")
            if state != "approved":
                print(f"  [{plan.kind}] approval ended {state}; skipping resubmit")
                continue
            await _resubmit_approved(client, dispatcher, plan=plan, base_commit=base_commit)
            print(f"  [{plan.kind}] resubmitted with approval_id; task_id={plan.task_id}")
        for plan in plans:
            if plan.task_id is None:
                continue
            task = await _wait_for_task(
                client,
                plan=plan,
                timeout_seconds=task_timeout,
                poll_seconds=2.0,
            )
            print(
                f"  [{plan.kind}] final status={task['status']} "
                f"completed_at={task.get('completed_at')}"
            )
        ok = all(p.final_status == "done" for p in plans)
        return ok
    finally:
        if not no_cleanup:
            await _cleanup(client, plans=plans, resolved=resolved)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def _run(args: argparse.Namespace) -> int:
    token = args.token_path.read_text(encoding="utf-8").strip()
    dispatcher = load_or_create_dispatcher()
    async with httpx.AsyncClient(
        base_url=args.hub_url,
        headers=_auth_headers(token),
        timeout=30.0,
    ) as client:
        try:
            await client.post(
                "/dispatchers/register",
                json=_dispatcher_register_payload(dispatcher),
            )
        except httpx.HTTPError as exc:
            print(f"warning: dispatcher register failed: {exc}", file=sys.stderr)
        all_ok = True
        for i in range(1, args.iterations + 1):
            ok = await _run_iteration(
                client,
                dispatcher,
                iteration=i,
                kinds=args.kinds,
                approval_timeout=args.approval_timeout,
                task_timeout=args.task_timeout,
                no_cleanup=args.no_cleanup,
            )
            all_ok = all_ok and ok
        print(f"\nharness done; ok={all_ok}")
        return 0 if all_ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--hub-url", default=DEFAULT_HUB_URL)
    ap.add_argument("--token-path", type=Path, default=DEFAULT_TOKEN_PATH)
    ap.add_argument("--iterations", type=int, default=1)
    ap.add_argument(
        "--kinds",
        nargs="+",
        default=["command", "agent"],
        choices=["command", "agent"],
        help="Which legs to run each iteration.",
    )
    ap.add_argument(
        "--approval-timeout",
        type=int,
        default=600,
        help="Seconds to wait for operator approval per iteration.",
    )
    ap.add_argument(
        "--task-timeout",
        type=int,
        default=300,
        help="Seconds to wait for each approved task to reach terminal.",
    )
    ap.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Leave queued/non-terminal tasks and pending approvals in place.",
    )
    args = ap.parse_args()
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
