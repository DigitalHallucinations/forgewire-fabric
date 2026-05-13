"""Live approval + task round-trip smoke against a running ForgeWire hub.

This exercises the operator approval gate all the way back into task
execution for both task substrates:

  1. signed POST /tasks/v2 on protected branch -> 428 + approval_id
  2. approve the pending row
  3. signed re-dispatch with approval_id -> queued task
  4. agent task: a temporary signed kind:agent runner claims and reports done
  5. command task: an online kind:command runner executes a tiny shell command

The smoke uses unique todo IDs and denies/cancels leftovers on failure so the
approval pane does not accumulate stale probe rows.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import secrets
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

import httpx  # noqa: E402
from forgewire_fabric.dispatcher.identity import (  # noqa: E402
    load_or_create as load_or_create_dispatcher,
)
from forgewire_fabric.runner.identity import (  # noqa: E402
    load_or_create as load_or_create_runner,
)
from forgewire_fabric.runner.runner_capabilities import sign_payload  # noqa: E402


DEFAULT_HUB_URL = "http://10.120.81.95:8765"
DEFAULT_TOKEN_PATH = Path(r"C:\Users\jerem\.forgewire\hub.token")
BASE_COMMIT = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
TERMINAL_STATUSES = {"done", "failed", "cancelled", "timed_out"}

COMMAND_PROBES: dict[str, tuple[str, str]] = {
    "py": ("py --version", "Python"),
    "python": ("python --version", "Python"),
    "node": ("node --version", "v"),
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
        "hostname": "live-approval-smoke",
        "timestamp": signed["timestamp"],
        "nonce": signed["nonce"],
        "signature": ident.sign(_canonical(signed)),
    }


def _signed_dispatch_payload(
    ident: Any,
    *,
    title: str,
    prompt: str,
    scope_globs: list[str],
    branch: str,
    todo_id: str,
    kind: str,
    required_tools: list[str] | None = None,
    required_tags: list[str] | None = None,
    approval_id: str | None = None,
) -> dict[str, Any]:
    ts = _ts()
    nonce = _nonce()
    signed = {
        "op": "dispatch",
        "dispatcher_id": ident.dispatcher_id,
        "title": title,
        "prompt": prompt,
        "scope_globs": scope_globs,
        "base_commit": BASE_COMMIT,
        "branch": branch,
        "timestamp": ts,
        "nonce": nonce,
    }
    return {
        "title": title,
        "prompt": prompt,
        "scope_globs": scope_globs,
        "base_commit": BASE_COMMIT,
        "branch": branch,
        "dispatcher_id": ident.dispatcher_id,
        "timestamp": ts,
        "nonce": nonce,
        "signature": ident.sign(_canonical(signed)),
        "todo_id": todo_id,
        "timeout_minutes": 2,
        "priority": 10_000,
        "metadata": {"live_smoke": "approval-roundtrip", "kind": kind},
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


def _runner_register_payload(
    ident: Any,
    *,
    runner_version: str,
    tags: list[str],
    tools: list[str],
) -> dict[str, Any]:
    ts = _ts()
    nonce = _nonce()
    signed = {
        "op": "register",
        "runner_id": ident.runner_id,
        "public_key": ident.public_key_hex,
        "protocol_version": 3,
        "timestamp": ts,
        "nonce": nonce,
    }
    return {
        "runner_id": ident.runner_id,
        "public_key": ident.public_key_hex,
        "protocol_version": 3,
        "runner_version": runner_version,
        "hostname": "live-agent-approval-smoke",
        "os": sys.platform,
        "arch": "smoke",
        "cpu_model": "smoke",
        "cpu_count": 1,
        "ram_mb": 1024,
        "gpu": None,
        "tools": tools,
        "tags": tags,
        "scope_prefixes": [],
        "tenant": None,
        "workspace_root": str(ROOT),
        "max_concurrent": 1,
        "metadata": {"flavor": "live-approval-smoke"},
        "capabilities": {"services": ["approval-smoke"], "toolchains": {}},
        "timestamp": ts,
        "nonce": nonce,
        "signature": sign_payload(ident, signed),
    }


def _runner_claim_payload(
    ident: Any,
    *,
    tags: list[str],
    tools: list[str],
) -> dict[str, Any]:
    ts = _ts()
    nonce = _nonce()
    signed = {"op": "claim", "runner_id": ident.runner_id, "timestamp": ts, "nonce": nonce}
    return {
        "runner_id": ident.runner_id,
        "timestamp": ts,
        "nonce": nonce,
        "signature": sign_payload(ident, signed),
        "scope_prefixes": [],
        "tools": tools,
        "tags": tags,
        "tenant": None,
        "workspace_root": str(ROOT),
        "last_known_commit": None,
        "cpu_load_pct": 1.0,
        "ram_free_mb": 1024,
        "battery_pct": None,
        "on_battery": False,
    }


def _load_token(path: Path) -> str:
    env_token = os.environ.get("FORGEWIRE_HUB_TOKEN", "").strip()
    if env_token:
        return env_token
    return path.read_text(encoding="utf-8").strip()


async def _expect_approval(
    client: httpx.AsyncClient,
    dispatcher: Any,
    *,
    title: str,
    prompt: str,
    scope_globs: list[str],
    todo_id: str,
    kind: str,
    pending_approvals: set[str],
    required_tools: list[str] | None = None,
    required_tags: list[str] | None = None,
) -> tuple[str, str]:
    payload = _signed_dispatch_payload(
        dispatcher,
        title=title,
        prompt=prompt,
        scope_globs=scope_globs,
        branch="main",
        todo_id=todo_id,
        kind=kind,
        required_tools=required_tools,
        required_tags=required_tags,
    )
    response = await client.post("/tasks/v2", json=payload)
    assert response.status_code == 428, (
        f"expected approval 428 for {kind}, got {response.status_code}: {response.text}"
    )
    detail = response.json()["detail"]
    approval_id = detail["approval_id"]
    envelope_hash = detail["envelope_hash"]
    pending_approvals.add(approval_id)

    listed = await client.get("/approvals", params={"status": "pending", "limit": 200})
    listed.raise_for_status()
    rows = listed.json()["approvals"]
    assert any(row["approval_id"] == approval_id for row in rows), rows
    print(
        f"[{kind}] approval requested id={approval_id} envelope={envelope_hash[:12]} "
        f"pending_total={len(rows)}"
    )
    return approval_id, envelope_hash


async def _approve_and_dispatch(
    client: httpx.AsyncClient,
    dispatcher: Any,
    *,
    approval_id: str,
    title: str,
    prompt: str,
    scope_globs: list[str],
    todo_id: str,
    kind: str,
    pending_approvals: set[str],
    task_ids: set[int],
    required_tools: list[str] | None = None,
    required_tags: list[str] | None = None,
) -> dict[str, Any]:
    approved = await client.post(
        f"/approvals/{approval_id}/approve",
        json={"approver": "live-smoke", "reason": f"approval roundtrip {kind}"},
    )
    approved.raise_for_status()
    assert approved.json()["status"] == "approved", approved.text

    payload = _signed_dispatch_payload(
        dispatcher,
        title=title,
        prompt=prompt,
        scope_globs=scope_globs,
        branch="main",
        todo_id=todo_id,
        kind=kind,
        required_tools=required_tools,
        required_tags=required_tags,
        approval_id=approval_id,
    )
    response = await client.post("/tasks/v2", json=payload)
    assert response.status_code == 200, (
        f"expected approved dispatch 200 for {kind}, got {response.status_code}: {response.text}"
    )
    task = response.json()
    task_id = int(task["id"])
    task_ids.add(task_id)
    pending_approvals.discard(approval_id)
    assert task.get("kind") == kind, task
    print(f"[{kind}] approved dispatch created task_id={task_id}")
    return task


async def _complete_agent_task(
    client: httpx.AsyncClient,
    runner_ident: Any,
    *,
    task_id: int,
    scope_path: str,
    marker: str,
    tags: list[str],
    tools: list[str],
    artifact_dir: Path,
) -> dict[str, Any]:
    claim = await client.post(
        "/tasks/claim-v2",
        json=_runner_claim_payload(runner_ident, tags=tags, tools=tools),
    )
    claim.raise_for_status()
    claim_body = claim.json()
    claimed = claim_body.get("task")
    assert claimed is not None, claim_body
    assert int(claimed["id"]) == task_id, claim_body
    assert claimed.get("kind") == "agent", claimed

    started = await client.post(f"/tasks/{task_id}/start")
    started.raise_for_status()

    artifact = artifact_dir / f"{marker}.txt"
    artifact.write_text(f"agent approval roundtrip marker={marker}\n", encoding="utf-8")

    progress = await client.post(
        f"/tasks/{task_id}/progress",
        json={
            "worker_id": runner_ident.runner_id,
            "message": f"agent runner completed real smoke artifact {marker}",
            "files_touched": [scope_path],
        },
    )
    progress.raise_for_status()
    stream = await client.post(
        f"/tasks/{task_id}/stream",
        json={"worker_id": runner_ident.runner_id, "channel": "stdout", "line": marker},
    )
    stream.raise_for_status()
    result = await client.post(
        f"/tasks/{task_id}/result",
        json={
            "worker_id": runner_ident.runner_id,
            "status": "done",
            "head_commit": BASE_COMMIT,
            "commits": [],
            "files_touched": [scope_path],
            "test_summary": f"agent approval roundtrip marker {marker}",
            "log_tail": f"agent approval roundtrip marker {marker}",
            "error": None,
        },
    )
    result.raise_for_status()
    final = result.json()
    assert final["status"] == "done", final
    assert final["worker_id"] == runner_ident.runner_id, final
    print(f"[agent] completed task_id={task_id} worker_id={runner_ident.runner_id}")
    return final


async def _wait_for_command_task(
    client: httpx.AsyncClient,
    *,
    task_id: int,
    marker: str,
    deadline_seconds: float,
) -> dict[str, Any]:
    deadline = time.time() + deadline_seconds
    final: dict[str, Any] | None = None
    while time.time() < deadline:
        response = await client.get(f"/tasks/{task_id}")
        response.raise_for_status()
        current = response.json()
        if current.get("status") in TERMINAL_STATUSES:
            final = current
            break
        await asyncio.sleep(1.0)
    assert final is not None, f"command task {task_id} did not finish before deadline"
    assert final.get("status") == "done", final

    stream = await client.get(f"/tasks/{task_id}/stream")
    stream.raise_for_status()
    lines = stream.json().get("lines", [])
    stream_text = "\n".join(str(line.get("line", "")) for line in lines)
    result = final.get("result") if isinstance(final.get("result"), dict) else {}
    log_tail = str(result.get("log_tail") or "")
    assert marker in stream_text or marker in log_tail, {
        "task": final,
        "stream_tail": lines[-10:],
    }
    print(f"[command] completed task_id={task_id} worker_id={final.get('worker_id')}")
    return final


def _pick_command_probe(runners: list[dict[str, Any]]) -> tuple[str, str]:
    online = [runner for runner in runners if runner.get("state") == "online"]
    command_runners = [
        runner
        for runner in online
        if "kind:command" in {str(tag).lower() for tag in runner.get("tags", [])}
    ]
    for runner in command_runners:
        tools = {str(tool) for tool in runner.get("tools", [])}
        for tool, probe in COMMAND_PROBES.items():
            if tool in tools:
                print(
                    f"[command] selected runner={runner.get('runner_id')} "
                    f"host={runner.get('hostname')} tool={tool}"
                )
                return tool, probe[0]
    raise AssertionError(
        "no online kind:command runner advertises one of "
        f"{', '.join(COMMAND_PROBES)}"
    )


async def _cleanup(
    client: httpx.AsyncClient,
    *,
    pending_approvals: set[str],
    task_ids: set[int],
    ephemeral_runner_ids: set[str] | None = None,
    ephemeral_dispatcher_ids: set[str] | None = None,
) -> None:
    for approval_id in sorted(pending_approvals):
        try:
            current = await client.get(f"/approvals/{approval_id}")
            if current.status_code == 200 and current.json().get("status") == "pending":
                await client.post(
                    f"/approvals/{approval_id}/deny",
                    json={"approver": "live-smoke", "reason": "cleanup after failed smoke"},
                )
                print(f"[cleanup] denied pending approval {approval_id}")
        except httpx.HTTPError as exc:
            print(f"[cleanup] failed approval cleanup {approval_id}: {exc}")

    for task_id in sorted(task_ids):
        try:
            current = await client.get(f"/tasks/{task_id}")
            if current.status_code != 200:
                continue
            task = current.json()
            if task.get("status") not in TERMINAL_STATUSES:
                await client.post(f"/tasks/{task_id}/cancel")
                print(f"[cleanup] cancel requested for nonterminal task {task_id}")
        except httpx.HTTPError as exc:
            print(f"[cleanup] failed task cleanup {task_id}: {exc}")

    # Deregister ephemeral runner/dispatcher identities so they do not
    # accumulate as ghost rows in the /hosts pane. Idempotent: 404 is
    # treated as already-gone.
    for runner_id in sorted(ephemeral_runner_ids or set()):
        try:
            r = await client.delete(f"/runners/{runner_id}")
            if r.status_code in (200, 404):
                print(f"[cleanup] deregistered runner {runner_id} ({r.status_code})")
            else:
                print(f"[cleanup] runner deregister {runner_id} -> {r.status_code} {r.text}")
        except httpx.HTTPError as exc:
            print(f"[cleanup] failed runner deregister {runner_id}: {exc}")
    for dispatcher_id in sorted(ephemeral_dispatcher_ids or set()):
        try:
            r = await client.delete(f"/dispatchers/{dispatcher_id}")
            if r.status_code in (200, 404):
                print(f"[cleanup] deregistered dispatcher {dispatcher_id} ({r.status_code})")
            else:
                print(
                    f"[cleanup] dispatcher deregister {dispatcher_id} -> {r.status_code} {r.text}"
                )
        except httpx.HTTPError as exc:
            print(f"[cleanup] failed dispatcher deregister {dispatcher_id}: {exc}")


async def run(args: argparse.Namespace) -> int:
    token = _load_token(Path(args.token_path))
    headers = _auth_headers(token)
    pending_approvals: set[str] = set()
    task_ids: set[int] = set()

    with tempfile.TemporaryDirectory(prefix="fw-approval-smoke-") as temp_dir:
        temp_path = Path(temp_dir)
        dispatcher = load_or_create_dispatcher(
            temp_path / "dispatcher_identity.json",
            label=f"approval-smoke-{_short()}",
        )
        agent_runner = load_or_create_runner(temp_path / "agent_runner_identity.json")
        agent_tag = f"approval-smoke:{_short()}"
        agent_tags = [agent_tag, "kind:agent"]
        agent_tools = ["approval-smoke-agent"]

        async with httpx.AsyncClient(
            base_url=args.hub_url,
            headers=headers,
            timeout=10.0,
        ) as client:
            try:
                health = await client.get("/healthz")
                health.raise_for_status()
                health_body = health.json()
                hub_version = str(health_body.get("version") or "0.11.6")
                print(
                    f"hub status={health_body.get('status')} version={hub_version} "
                    f"protocol={health_body.get('protocol')}"
                )

                registration = await client.post(
                    "/dispatchers/register",
                    json=_dispatcher_register_payload(dispatcher),
                )
                registration.raise_for_status()
                print(f"registered dispatcher={dispatcher.dispatcher_id}")

                runner_registration = await client.post(
                    "/runners/register",
                    json=_runner_register_payload(
                        agent_runner,
                        runner_version=hub_version,
                        tags=agent_tags,
                        tools=agent_tools,
                    ),
                )
                runner_registration.raise_for_status()
                print(f"registered temporary agent runner={agent_runner.runner_id}")

                runners_response = await client.get("/runners")
                runners_response.raise_for_status()
                runners = runners_response.json().get("runners", [])
                command_tool, command_prompt = _pick_command_probe(runners)
                command_marker = COMMAND_PROBES[command_tool][1]

                agent_suffix = _short()
                agent_scope = f"docs/_audit/approval-roundtrip-agent-{agent_suffix}.md"
                agent_approval, _ = await _expect_approval(
                    client,
                    dispatcher,
                    title=f"live-approval-roundtrip-agent-{agent_suffix}",
                    prompt=f"create approval roundtrip marker {agent_suffix}",
                    scope_globs=[agent_scope],
                    todo_id=f"approval-roundtrip-agent-{agent_suffix}",
                    kind="agent",
                    required_tags=[agent_tag],
                    pending_approvals=pending_approvals,
                )
                agent_task = await _approve_and_dispatch(
                    client,
                    dispatcher,
                    approval_id=agent_approval,
                    title=f"live-approval-roundtrip-agent-{agent_suffix}",
                    prompt=f"create approval roundtrip marker {agent_suffix}",
                    scope_globs=[agent_scope],
                    todo_id=f"approval-roundtrip-agent-{agent_suffix}",
                    kind="agent",
                    required_tags=[agent_tag],
                    pending_approvals=pending_approvals,
                    task_ids=task_ids,
                )
                await _complete_agent_task(
                    client,
                    agent_runner,
                    task_id=int(agent_task["id"]),
                    scope_path=agent_scope,
                    marker=f"agent-{agent_suffix}",
                    tags=agent_tags,
                    tools=agent_tools,
                    artifact_dir=temp_path,
                )

                command_suffix = _short()
                command_scope = f"docs/_audit/approval-roundtrip-command-{command_suffix}.md"
                command_approval, _ = await _expect_approval(
                    client,
                    dispatcher,
                    title=f"live-approval-roundtrip-command-{command_suffix}",
                    prompt=command_prompt,
                    scope_globs=[command_scope],
                    todo_id=f"approval-roundtrip-command-{command_suffix}",
                    kind="command",
                    required_tools=[command_tool],
                    pending_approvals=pending_approvals,
                )
                command_task = await _approve_and_dispatch(
                    client,
                    dispatcher,
                    approval_id=command_approval,
                    title=f"live-approval-roundtrip-command-{command_suffix}",
                    prompt=command_prompt,
                    scope_globs=[command_scope],
                    todo_id=f"approval-roundtrip-command-{command_suffix}",
                    kind="command",
                    required_tools=[command_tool],
                    pending_approvals=pending_approvals,
                    task_ids=task_ids,
                )
                await _wait_for_command_task(
                    client,
                    task_id=int(command_task["id"]),
                    marker=command_marker,
                    deadline_seconds=args.deadline,
                )

                print("PASS approval roundtrip: agent and command tasks completed")
                return 0
            finally:
                await _cleanup(
                    client,
                    pending_approvals=pending_approvals,
                    task_ids=task_ids,
                    ephemeral_runner_ids={agent_runner.runner_id},
                    ephemeral_dispatcher_ids={dispatcher.dispatcher_id},
                )


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--hub-url",
        default=os.environ.get("FORGEWIRE_HUB_URL", DEFAULT_HUB_URL),
        help="ForgeWire hub URL",
    )
    parser.add_argument(
        "--token-path",
        default=str(DEFAULT_TOKEN_PATH),
        help="Hub token file; FORGEWIRE_HUB_TOKEN overrides this",
    )
    parser.add_argument(
        "--deadline",
        type=float,
        default=90.0,
        help="Seconds to wait for the live command runner to finish",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(run(parse_args(argv or sys.argv[1:])))


if __name__ == "__main__":
    raise SystemExit(main())