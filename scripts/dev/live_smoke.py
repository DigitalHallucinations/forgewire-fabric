"""Live end-to-end smoke test against the running hub fleet.

Dispatches one real shell task per discriminating tool (so each task
naturally pins to one host), waits for terminal status, and asserts
status=done with stdout matching expectation. Unlike
``probe_capability_routing.py`` -- which only validates routing and
expects failure -- this script validates the full path:

    dispatch -> claim -> mark_running -> shell_executor -> stream ->
    submit_result -> terminal status=done

Usage:
    set FORGEWIRE_HUB_TOKEN=...
    python scripts\\dev\\live_smoke.py [--hub-url http://...] [--deadline 60]
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


# Real shell prompts keyed by the discriminating tool. Each command must:
#   - exit 0 on success
#   - print a known marker to stdout so we can grep the result
#   - take < 5 seconds end-to-end
#
# Note: prompts are passed verbatim to ``cmd /c <prompt>`` on Windows.
# Avoid double-quotes inside the prompt -- cmd's quote handling will eat
# them. Either redirect a here-doc to a temp file, or use commands that
# need no inline string args.
PROMPTS = {
    "node": (
        "node --version",
        "v",  # node prints e.g. "v24.15.0"
    ),
    "py": (
        "py --version",
        "Python",  # py.exe launcher prints e.g. "Python 3.11.9"
    ),
}


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
    args = ap.parse_args()

    token = os.environ.get("FORGEWIRE_HUB_TOKEN", "").strip()
    if not token:
        print("FORGEWIRE_HUB_TOKEN not set", file=sys.stderr)
        return 2
    headers = {"Authorization": f"Bearer {token}"}

    with tempfile.TemporaryDirectory() as td:
        ident = load_or_create(Path(td) / "dispatcher_identity.json", label=f"live-smoke-{uuid.uuid4().hex[:6]}")

        async with httpx.AsyncClient(base_url=args.hub_url, headers=headers, timeout=10.0) as c:
            r = await c.get("/runners")
            r.raise_for_status()
            runners = r.json().get("runners", [])
            print(f"--- {len(runners)} runners online ---")
            tools_by_host = {ru["hostname"]: list(ru.get("tools", [])) for ru in runners}
            for h, t in tools_by_host.items():
                print(f"  {h}: tools={t}")

            # Find one discriminating tool per host that we have a real prompt for.
            target_tools: dict[str, str] = {}  # host -> tool
            for h, t in tools_by_host.items():
                others: set[str] = set()
                for k, v in tools_by_host.items():
                    if k != h:
                        others |= set(v)
                uniq = sorted(set(t) - others)
                for u in uniq:
                    if u in PROMPTS:
                        target_tools[h] = u
                        break
            if not target_tools:
                print("No host has a discriminating tool with a registered live-smoke prompt; abort.")
                return 1
            print("--- live-smoke targets ---")
            for h, u in target_tools.items():
                print(f"  {h}: tool={u} prompt={PROMPTS[u][0]}")

            r = await c.post("/dispatchers/register", json=_sign_register(ident, ident.label, "live-smoke"))
            r.raise_for_status()
            print(f"--- registered dispatcher {ident.dispatcher_id} ---")

            task_records: list[dict] = []
            for host, tool in target_tools.items():
                prompt, marker = PROMPTS[tool]
                r = await c.post(
                    "/tasks/v2",
                    json=_sign_dispatch(
                        ident,
                        title=f"live-smoke-{tool}",
                        prompt=prompt,
                        required_tools=[tool],
                    ),
                )
                r.raise_for_status()
                rec = r.json()
                rec["_target_host"] = host
                rec["_target_tool"] = tool
                rec["_marker"] = marker
                task_records.append(rec)
                print(f"  dispatched task {rec['id']} required_tools=[{tool}] -> expect host {host}")

            print(f"--- waiting up to {args.deadline}s for terminal status ---")
            deadline = time.time() + args.deadline
            pending = {rec["id"]: rec for rec in task_records}
            terminal = {"done", "failed", "cancelled", "timed_out"}
            while pending and time.time() < deadline:
                for tid in list(pending.keys()):
                    rr = await c.get(f"/tasks/{tid}")
                    rr.raise_for_status()
                    cur = rr.json()
                    status = cur.get("status")
                    if status in terminal:
                        pending[tid].update(cur)
                        print(f"  task {tid}: status={status} worker_id={cur.get('worker_id')}")
                        del pending[tid]
                if pending:
                    await asyncio.sleep(1.0)

            print("--- end-to-end verdict ---")
            ok = True
            for rec in task_records:
                tid = rec["id"]
                target = rec["_target_host"]
                tool = rec["_target_tool"]
                marker = rec["_marker"]
                wid = rec.get("worker_id")
                claimer = next((ru for ru in runners if ru["runner_id"] == wid), None)
                status = rec.get("status")

                # Pull result + stream tail.
                try:
                    rr = await c.get(f"/tasks/{tid}")
                    full = rr.json()
                except httpx.HTTPError as exc:
                    full = {}
                    print(f"  ! task {tid}: failed to refetch: {exc}")
                log_tail = ""
                try:
                    sr = await c.get(f"/tasks/{tid}/stream")
                    sr.raise_for_status()
                    lines = sr.json().get("lines", [])
                    log_tail = "\n".join(f"  | [{ln.get('channel')}] {ln.get('line')}" for ln in lines[-20:])
                except httpx.HTTPError:
                    pass

                claimer_host = claimer["hostname"] if claimer else "<none>"
                routing_ok = claimer is not None and claimer.get("hostname") == target
                exec_ok = status == "done"
                marker_ok = marker in log_tail

                if routing_ok and exec_ok and marker_ok:
                    verdict = "PASS"
                else:
                    verdict = "FAIL"
                    ok = False
                print(
                    f"  {verdict} task {tid} ({tool}): routed={routing_ok} (claimer={claimer_host}) "
                    f"status={status} marker_in_stdout={marker_ok}"
                )
                if log_tail:
                    print(log_tail)
                err = (full.get("result") or {}).get("error") if isinstance(full.get("result"), dict) else None
                if not exec_ok and err:
                    print(f"  | result.error: {err}")

            return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
