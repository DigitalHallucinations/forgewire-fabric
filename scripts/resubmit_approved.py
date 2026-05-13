"""One-shot helper: re-dispatch already-approved briefs.

The MCP dispatch_task tool does not expose `approval_id`. After an operator
approves a pending approval in the VSIX pane (or via the CLI), this script
re-POSTs the same brief to /tasks/v2 with `approval_id` set, using the local
dispatcher identity for signing.

Usage:
    python scripts/resubmit_approved.py \
        --approval-id <hex> --kind agent|command \
        --title "..." --prompt "..." --branch <branch> \
        --base-commit <sha> --scope-glob "<glob>" \
        [--scope-glob "<glob>" ...] --todo-id <id> \
        [--required-tag <tag>] [--required-tool <tool>] \
        [--timeout-minutes 30] [--priority 100]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import secrets
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

import httpx  # noqa: E402

from forgewire_fabric.dispatcher.identity import (  # noqa: E402
    load_or_create as load_or_create_dispatcher,
)


def _canonical(body: dict[str, Any]) -> bytes:
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")


async def _post(
    *,
    hub_url: str,
    token: str,
    approval_id: str,
    kind: str,
    title: str,
    prompt: str,
    branch: str,
    base_commit: str,
    scope_globs: list[str],
    todo_id: str,
    required_tags: list[str],
    required_tools: list[str],
    timeout_minutes: int,
    priority: int,
) -> dict[str, Any]:
    dispatcher = load_or_create_dispatcher()
    ts = int(time.time())
    nonce = secrets.token_hex(16)
    signed = {
        "op": "dispatch",
        "dispatcher_id": dispatcher.dispatcher_id,
        "title": title,
        "prompt": prompt,
        "scope_globs": scope_globs,
        "base_commit": base_commit,
        "branch": branch,
        "timestamp": ts,
        "nonce": nonce,
    }
    payload = {
        "title": title,
        "prompt": prompt,
        "scope_globs": scope_globs,
        "base_commit": base_commit,
        "branch": branch,
        "dispatcher_id": dispatcher.dispatcher_id,
        "timestamp": ts,
        "nonce": nonce,
        "signature": dispatcher.sign(_canonical(signed)),
        "todo_id": todo_id,
        "timeout_minutes": timeout_minutes,
        "priority": priority,
        "metadata": {"resubmit": "approval-pane-roundtrip", "kind": kind},
        "required_tools": required_tools,
        "required_tags": required_tags,
        "tenant": None,
        "workspace_root": None,
        "require_base_commit": False,
        "required_capabilities": [],
        "secrets_needed": [],
        "network_egress": None,
        "kind": kind,
        "approval_id": approval_id,
    }
    async with httpx.AsyncClient(
        base_url=hub_url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=30.0,
    ) as client:
        resp = await client.post("/tasks/v2", json=payload)
        if resp.status_code != 200:
            print(f"[resubmit] HTTP {resp.status_code}: {resp.text}", file=sys.stderr)
            resp.raise_for_status()
        return resp.json()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hub-url", default="http://10.120.81.95:8765")
    ap.add_argument(
        "--token-path", type=Path, default=Path(r"C:\Users\jerem\.forgewire\hub.token")
    )
    ap.add_argument("--approval-id", required=True)
    ap.add_argument("--kind", required=True, choices=["agent", "command"])
    ap.add_argument("--title", required=True)
    ap.add_argument("--prompt", required=True)
    ap.add_argument("--branch", required=True)
    ap.add_argument("--base-commit", required=True)
    ap.add_argument("--scope-glob", action="append", required=True, dest="scope_globs")
    ap.add_argument("--todo-id", required=True)
    ap.add_argument("--required-tag", action="append", default=[], dest="required_tags")
    ap.add_argument(
        "--required-tool", action="append", default=[], dest="required_tools"
    )
    ap.add_argument("--timeout-minutes", type=int, default=30)
    ap.add_argument("--priority", type=int, default=100)
    args = ap.parse_args()
    token = args.token_path.read_text(encoding="utf-8").strip()
    task = asyncio.run(
        _post(
            hub_url=args.hub_url,
            token=token,
            approval_id=args.approval_id,
            kind=args.kind,
            title=args.title,
            prompt=args.prompt,
            branch=args.branch,
            base_commit=args.base_commit,
            scope_globs=args.scope_globs,
            todo_id=args.todo_id,
            required_tags=args.required_tags,
            required_tools=args.required_tools,
            timeout_minutes=args.timeout_minutes,
            priority=args.priority,
        )
    )
    print(json.dumps(task, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
