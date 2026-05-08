"""Live capability-routing probe against the running hub.

This probe ONLY validates routing: "did the right runner claim the task?".
It does NOT validate task execution. The dispatched tasks intentionally
use a fake base_commit and an unreachable scope, so the runner WILL fail
the task at the git-checkout step almost immediately. That terminal
``status=failed`` is *expected* and is not what the verdict measures --
the verdict measures which worker_id picked up each task before it died.

The probe also cancels each task as a cleanup step before exit so the
queue stays empty, and to avoid runners spinning on the doomed work.

Usage:
    set FORGEWIRE_HUB_TOKEN=...
    python scripts\\dev\\probe_capability_routing.py [--hub-url ...]
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

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "python"))

import httpx  # noqa: E402
from forgewire_fabric.dispatcher.identity import load_or_create  # noqa: E402


def _canonical(body: dict) -> bytes:
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _ts() -> int:
    return int(time.time())


def _nonce() -> str:
    return secrets.token_hex(16)


def _sign_register(ident, label: str, hostname: str) -> dict:
    body = {
        "op": "register-dispatcher",
        "dispatcher_id": ident.dispatcher_id,
        "public_key": ident.public_key_hex,
        "timestamp": _ts(),
        "nonce": _nonce(),
    }
    return {
        "dispatcher_id": ident.dispatcher_id,
        "public_key": ident.public_key_hex,
        "label": label,
        "hostname": hostname,
        "timestamp": body["timestamp"],
        "nonce": body["nonce"],
        "signature": ident.sign(_canonical(body)),
    }


def _sign_dispatch(ident, *, title: str, required_tools: list[str]) -> dict:
    ts = _ts()
    nonce = _nonce()
    branch = f"agent/probe/{uuid.uuid4().hex[:8]}"
    sig_body = {
        "op": "dispatch",
        "dispatcher_id": ident.dispatcher_id,
        "title": title,
        "prompt": "capability-probe (auto-cancelled)",
        "scope_globs": ["docs/probe/**"],
        "base_commit": "deadbeef",
        "branch": branch,
        "timestamp": ts,
        "nonce": nonce,
    }
    return {
        "title": title,
        "prompt": sig_body["prompt"],
        "scope_globs": sig_body["scope_globs"],
        "base_commit": sig_body["base_commit"],
        "branch": branch,
        "dispatcher_id": ident.dispatcher_id,
        "timestamp": ts,
        "nonce": nonce,
        "signature": ident.sign(_canonical(sig_body)),
        "todo_id": None,
        "timeout_minutes": 5,
        "priority": 1000,
        "metadata": {"probe": True},
        "required_tools": required_tools,
        "required_tags": [],
        "tenant": None,
        "workspace_root": None,
        "require_base_commit": False,
    }


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hub-url", default=os.environ.get("FORGEWIRE_HUB_URL", "http://127.0.0.1:8765"))
    ap.add_argument("--deadline", type=float, default=30.0)
    args = ap.parse_args()

    token = os.environ.get("FORGEWIRE_HUB_TOKEN", "").strip()
    if not token:
        print("FORGEWIRE_HUB_TOKEN not set", file=sys.stderr)
        return 2
    headers = {"Authorization": f"Bearer {token}"}

    with tempfile.TemporaryDirectory() as td:
        ident = load_or_create(Path(td) / "dispatcher_identity.json", label=f"cap-probe-{uuid.uuid4().hex[:6]}")

        async with httpx.AsyncClient(base_url=args.hub_url, headers=headers, timeout=10.0) as c:
            r = await c.get("/runners")
            r.raise_for_status()
            runners = r.json().get("runners", [])
            print(f"--- {len(runners)} runners online ---")
            tools_by_host = {ru["hostname"]: list(ru.get("tools", [])) for ru in runners}
            for h, t in tools_by_host.items():
                print(f"  {h}: tools={t}")

            unique_per_host: dict[str, str] = {}
            for h, t in tools_by_host.items():
                others: set[str] = set()
                for k, v in tools_by_host.items():
                    if k != h:
                        others |= set(v)
                uniq = sorted(set(t) - others)
                if uniq:
                    unique_per_host[h] = uniq[0]
            print("--- discriminating tools ---")
            for h, t in unique_per_host.items():
                print(f"  {h}: {t}")
            if len(unique_per_host) < 2:
                print("Need >=2 disjoint capabilities; abort.")
                return 1

            r = await c.post("/dispatchers/register", json=_sign_register(ident, ident.label, "capability-probe"))
            r.raise_for_status()
            print(f"--- registered dispatcher {ident.dispatcher_id} ({ident.label}) ---")

            task_records: list[dict] = []
            try:
                for host, tool in unique_per_host.items():
                    r = await c.post("/tasks/v2", json=_sign_dispatch(ident, title=f"probe-{tool}", required_tools=[tool]))
                    r.raise_for_status()
                    rec = r.json()
                    rec["_target_host"] = host
                    rec["_target_tool"] = tool
                    task_records.append(rec)
                    print(f"  dispatched task {rec['id']} required_tools=[{tool}] -> expect host {host}")

                print(f"--- waiting up to {args.deadline}s for claims ---")
                deadline = time.time() + args.deadline
                pending = {rec["id"]: rec for rec in task_records}
                while pending and time.time() < deadline:
                    for tid in list(pending.keys()):
                        rr = await c.get(f"/tasks/{tid}")
                        rr.raise_for_status()
                        cur = rr.json()
                        if cur.get("status") in ("claimed", "running", "completed", "failed"):
                            pending[tid].update(cur)
                            print(f"  task {tid}: status={cur.get('status')} worker_id={cur.get('worker_id')}")
                            del pending[tid]
                    if pending:
                        await asyncio.sleep(1.0)

                print("--- routing verdict (capability match only; task exec is expected to fail) ---")
                ok = True
                for rec in task_records:
                    target = rec["_target_host"]
                    tool = rec["_target_tool"]
                    wid = rec.get("worker_id")
                    final_status = rec.get("status")
                    claimer = next((ru for ru in runners if ru["runner_id"] == wid), None)
                    if claimer is None:
                        print(f"  ROUTING-FAIL task {rec['id']} ({target}/{tool}): never claimed (status={final_status})")
                        ok = False
                        continue
                    if claimer["hostname"] == target and tool in claimer.get("tools", []):
                        print(
                            f"  ROUTING-OK task {rec['id']}: required {tool} -> {claimer['hostname']} "
                            f"(tools={claimer.get('tools')}); task exec status={final_status} (expected failed/running, see docstring)"
                        )
                    else:
                        print(
                            f"  ROUTING-FAIL task {rec['id']} ({target}/{tool}): claimed by "
                            f"{claimer['hostname']} tools={claimer.get('tools')} status={final_status}"
                        )
                        ok = False
                return 0 if ok else 1
            finally:
                for rec in task_records:
                    try:
                        await c.post(f"/tasks/{rec['id']}/cancel")
                    except Exception as exc:
                        print(f"  cancel({rec['id']}) ignored: {exc}", file=sys.stderr)
                print("--- cleanup done ---")


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
