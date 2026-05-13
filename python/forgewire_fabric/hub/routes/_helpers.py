"""Shared route helpers for the ForgeWire Fabric hub."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any

import httpx
from fastapi import HTTPException

from forgewire_fabric.hub._crypto import verify_signature
from forgewire_fabric.hub.server import Blackboard, SIGNATURE_MAX_SKEW_SECONDS

from ._deps import HubContext


def signed_payload(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def check_skew(timestamp: int) -> None:
    now = int(time.time())
    if abs(now - int(timestamp)) > SIGNATURE_MAX_SKEW_SECONDS:
        raise HTTPException(status_code=401, detail="timestamp out of skew window")


def fire_approval_webhook(ctx: HubContext, payload: dict[str, Any]) -> None:
    url = ctx.config.approval_webhook_url
    if not url:
        return
    try:
        with httpx.Client(timeout=5.0) as client:
            client.post(url, json=payload)
    except Exception as exc:  # noqa: BLE001 - best-effort notify
        logging.getLogger(__name__).warning("approval webhook to %s failed: %s", url, exc)


def enforce_dispatch_gate(
    ctx: HubContext,
    *,
    task_id: str,
    scope_globs: list[str],
    branch: str | None,
    dispatcher_id: str | None = None,
    approval_id: str | None = None,
) -> None:
    from forgewire_fabric.policy import DispatchRequest

    blackboard = ctx.blackboard
    decision = ctx.gate.evaluate_dispatch(
        DispatchRequest(
            task_id=str(task_id),
            scope_globs=list(scope_globs),
            target_branch=branch,
            dispatcher_id=dispatcher_id,
        )
    )
    if decision.allowed:
        return
    if decision.denied:
        raise HTTPException(status_code=403, detail=decision.to_dict())

    env_hash = blackboard.envelope_hash(
        scope_globs=list(scope_globs),
        branch=branch,
        task_label=str(task_id),
    )
    if approval_id and blackboard.consume_approval(approval_id, env_hash):
        return
    approval_id_new, created = blackboard.create_or_get_pending_approval(
        envelope_hash=env_hash,
        decision=decision.to_dict(),
        task_label=str(task_id),
        branch=branch,
        scope_globs=list(scope_globs),
        dispatcher_id=dispatcher_id,
    )
    detail = decision.to_dict()
    detail["approval_id"] = approval_id_new
    detail["envelope_hash"] = env_hash
    detail["hint"] = (
        "re-POST the same brief with approval_id=<id> after an operator "
        f"runs `forgewire-fabric approvals approve {approval_id_new}`"
    )
    if created:
        fire_approval_webhook(
            ctx,
            {
                "event": "approval.created",
                "approval_id": approval_id_new,
                "task_label": str(task_id),
                "branch": branch,
                "scope_globs": list(scope_globs),
                "decision": decision.to_dict(),
            },
        )
    raise HTTPException(status_code=428, detail=detail)


def enforce_completion_gate(
    ctx: HubContext,
    *,
    task_id: str,
    changed_paths: list[str],
) -> None:
    from forgewire_fabric.policy import CompletionRequest

    decision = ctx.gate.evaluate_completion(
        CompletionRequest(
            task_id=str(task_id),
            changed_paths=list(changed_paths or ()),
            diff_lines=0,
        )
    )
    if decision.allowed or decision.needs_approval:
        return
    raise HTTPException(status_code=403, detail=decision.to_dict())


def sealed_brief_hash(
    *,
    title: str,
    prompt: str,
    scope_globs: list[str],
    base_commit: str,
    branch: str,
) -> str:
    return hashlib.sha256(
        json.dumps(
            {
                "title": title,
                "prompt": prompt,
                "scope_globs": sorted(scope_globs),
                "base_commit": base_commit,
                "branch": branch,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def audit_dispatch(
    ctx: HubContext,
    task: dict[str, Any],
    *,
    signed: bool,
    dispatcher_id: str | None,
    approval_id: str | None,
) -> None:
    try:
        sealed = sealed_brief_hash(
            title=task.get("title", ""),
            prompt=task.get("prompt", ""),
            scope_globs=list(task.get("scope_globs") or []),
            base_commit=task.get("base_commit", ""),
            branch=task.get("branch", ""),
        )
        ctx.blackboard.append_audit_event(
            kind="dispatch",
            task_id=int(task["id"]),
            payload={
                "task_id": int(task["id"]),
                "todo_id": task.get("todo_id"),
                "title": task.get("title"),
                "branch": task.get("branch"),
                "base_commit": task.get("base_commit"),
                "scope_globs": list(task.get("scope_globs") or []),
                "sealed_brief_hash": sealed,
                "dispatcher_id": dispatcher_id,
                "signed": signed,
                "approval_id": approval_id,
                "tenant": task.get("tenant"),
                "workspace_root": task.get("workspace_root"),
                "required_tools": list(task.get("required_tools") or []),
                "required_tags": list(task.get("required_tags") or []),
                "secrets_needed": list(task.get("secrets_needed") or []),
                "network_egress": task.get("network_egress"),
                "timeout_minutes": task.get("timeout_minutes"),
                "priority": task.get("priority"),
            },
        )
    except Exception as exc:  # noqa: BLE001 - audit must never block dispatch
        logging.getLogger(__name__).warning(
            "audit append failed for dispatch task=%s: %s", task.get("id"), exc
        )


def audit_claim(
    ctx: HubContext,
    task: dict[str, Any] | None,
    *,
    worker_id: str,
    secrets_dispatched: list[str] | None = None,
) -> None:
    if task is None:
        return
    try:
        ctx.blackboard.append_audit_event(
            kind="claim",
            task_id=int(task["id"]),
            payload={
                "task_id": int(task["id"]),
                "worker_id": worker_id,
                "claimed_at": task.get("claimed_at"),
                "secrets_dispatched": list(secrets_dispatched or []),
            },
        )
    except Exception as exc:  # noqa: BLE001
        logging.getLogger(__name__).warning(
            "audit append failed for claim task=%s: %s", task.get("id"), exc
        )


def audit_result(ctx: HubContext, task: dict[str, Any], *, worker_id: str) -> None:
    try:
        result = task.get("result") or {}
        ctx.blackboard.append_audit_event(
            kind="result",
            task_id=int(task["id"]),
            payload={
                "task_id": int(task["id"]),
                "worker_id": worker_id,
                "status": result.get("status"),
                "head_commit": result.get("head_commit"),
                "commits": list(result.get("commits") or []),
                "files_touched": list(result.get("files_touched") or []),
                "test_summary": result.get("test_summary"),
                "error": result.get("error"),
                "reported_at": result.get("reported_at"),
            },
        )
    except Exception as exc:  # noqa: BLE001
        logging.getLogger(__name__).warning(
            "audit append failed for result task=%s: %s", task.get("id"), exc
        )


def verify_runner_signature(
    ctx: HubContext,
    *,
    op: str,
    runner_id: str,
    timestamp: int,
    nonce: str,
    signature: str,
    extra: dict[str, Any] | None = None,
) -> None:
    check_skew(timestamp)
    public_key = ctx.blackboard.runner_public_key(runner_id)
    if public_key is None:
        raise HTTPException(status_code=404, detail="runner not registered")
    body = {
        "op": op,
        "runner_id": runner_id,
        "timestamp": timestamp,
        "nonce": nonce,
    }
    if extra:
        body.update(extra)
    if not verify_signature(public_key, signed_payload(body), signature):
        raise HTTPException(status_code=403, detail="invalid runner signature")


def verify_audit_events(events: list[dict[str, Any]]) -> tuple[bool, str | None]:
    return Blackboard.verify_audit_chain(events)
