"""Live smoke against both runners simultaneously.

Dispatches N parallel tasks (each `python --version`) so the queue
spreads across all idle runners. Verifies each task ran to status=done
and prints which runner claimed which task.
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


def _sign_dispatch(ident, *, title: str, prompt: str, required_tools: list[str]) -> dict:
    ts = _ts()
    nonce = _nonce()
    branch = f"agent/smoke/{uuid.uuid4().hex[:8]}"
    sig_body = {
        "op": "dispatch",
        "dispatcher_id": ident.dispatcher_id,
        "title": title,
        "prompt": prompt,
        "scope_globs": ["docs/smoke/**"],
        "base_commit": "deadbeef",
        "branch": branch,
        "timestamp": ts,
        "nonce": nonce,
    }
    return {
        "title": title,
        "prompt": prompt,
        "scope_globs": sig_body["scope_globs"],
        "base_commit": sig_body["base_commit"],
        "branch": branch,
        "dispatcher_id": ident.dispatcher_id,
        "timestamp": ts,
        "nonce": nonce,
        "signature": ident.sign(_canonical(sig_body)),
        "todo_id": None,
        "timeout_minutes": 2,
        "priority": 1000,
        "metadata": {"smoke": True},
        "required_tools": required_tools,
        "required_tags": [],
        "tenant": None,
        "workspace_root": None,
        "require_base_commit": False,
    }


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hub-url", default=os.environ.get("FORGEWIRE_HUB_URL", "http://127.0.0.1:8765"))
    ap.add_argument("--deadline", type=float, default=60.0)
    ap.add_argument("--prompt", default="py --version")
    ap.add_argument("--tool", default="py")
    args = ap.parse_args()

    token = os.environ.get("FORGEWIRE_HUB_TOKEN", "").strip()
    if not token:
        print("FORGEWIRE_HUB_TOKEN not set", file=sys.stderr)
        return 2
    headers = {"Authorization": f"Bearer {token}"}

    with tempfile.TemporaryDirectory() as td:
        ident = load_or_create(Path(td) / "dispatcher_identity.json", label=f"live-smoke-fanout-{uuid.uuid4().hex[:6]}")

        async with httpx.AsyncClient(base_url=args.hub_url, headers=headers, timeout=10.0) as c:
            r = await c.get("/runners")
            r.raise_for_status()
            runners = [ru for ru in r.json().get("runners", []) if ru.get("state") == "online"]
            print(f"--- {len(runners)} runners online ---")
            for ru in runners:
                print(f"  {ru['hostname']}: tools={ru.get('tools')}")
            n = len(runners)
            if n == 0:
                return 1

            r = await c.post("/dispatchers/register", json=_sign_register(ident, ident.label, "live-smoke"))
            r.raise_for_status()
            print(f"--- registered dispatcher {ident.dispatcher_id} ---")

            # Dispatch one task per runner. Hub queue + max_concurrent=1 should spread them.
            tasks = []
            for i in range(n):
                r = await c.post(
                    "/tasks/v2",
                    json=_sign_dispatch(
                        ident,
                        title=f"live-smoke-fanout-{i}",
                        prompt=args.prompt,
                        required_tools=[args.tool],
                    ),
                )
                r.raise_for_status()
                rec = r.json()
                tasks.append(rec)
                print(f"  dispatched task {rec['id']} prompt={args.prompt!r}")

            print(f"--- waiting up to {args.deadline}s ---")
            deadline = time.time() + args.deadline
            pending = {rec["id"]: rec for rec in tasks}
            terminal = {"done", "failed", "cancelled", "timed_out"}
            results: dict[int, dict] = {}
            while pending and time.time() < deadline:
                for tid in list(pending.keys()):
                    rr = await c.get(f"/tasks/{tid}")
                    rr.raise_for_status()
                    cur = rr.json()
                    if cur.get("status") in terminal:
                        results[tid] = cur
                        pending.pop(tid)
                        worker = cur.get("worker_id")
                        print(f"  task {tid}: status={cur.get('status')} worker_id={worker}")
                if pending:
                    await asyncio.sleep(1.0)

            print("--- verdict ---")
            ok = True
            host_by_runner = {ru["runner_id"]: ru["hostname"] for ru in runners}
            workers_seen: set[str] = set()
            for tid, cur in results.items():
                worker = cur.get("worker_id") or ""
                workers_seen.add(worker)
                host = host_by_runner.get(worker, worker)
                stream = (await c.get(f"/tasks/{tid}/stream")).json()
                tail = "".join(ev.get("payload", {}).get("data", "") for ev in stream.get("events", []) if ev.get("kind") == "stdout")
                status_ok = cur.get("status") == "done"
                ok = ok and status_ok
                tag = "PASS" if status_ok else "FAIL"
                print(f"  {tag} task {tid} host={host} status={cur.get('status')} stdout-tail={tail.strip()[:120]!r}")
            for tid in pending:
                ok = False
                print(f"  TIMEOUT task {tid}")
            print(f"--- runners that claimed work: {len(workers_seen)} of {n} ---")
            return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
