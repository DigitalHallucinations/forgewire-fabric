"""Task dispatch, listing, and claim routes."""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from forgewire_fabric.hub.capability_matcher import match as _capability_match
from forgewire_fabric.hub.server import ClaimRequest, DispatchTaskRequest

from ._deps import get_context, require_auth
from ._helpers import (
    audit_claim,
    audit_dispatch,
    enforce_dispatch_gate,
)

router = APIRouter()


@router.post("/tasks", dependencies=[Depends(require_auth)])
def dispatch_task(request: Request, payload: DispatchTaskRequest) -> dict[str, Any]:
    ctx = get_context(request)
    config = ctx.config
    blackboard = ctx.blackboard
    if config.require_signed_dispatch:
        raise HTTPException(
            status_code=426,
            detail=(
                "this hub requires signed dispatch envelopes; "
                "POST /tasks/v2 with a registered dispatcher key"
            ),
        )
    enforce_dispatch_gate(
        ctx,
        task_id=(payload.todo_id or payload.title),
        scope_globs=payload.scope_globs,
        branch=payload.branch,
        approval_id=payload.approval_id,
    )
    try:
        task = blackboard.create_task(
            title=payload.title,
            prompt=payload.prompt,
            scope_globs=payload.scope_globs,
            base_commit=payload.base_commit,
            branch=payload.branch,
            todo_id=payload.todo_id,
            timeout_minutes=payload.timeout_minutes,
            priority=payload.priority,
            metadata=payload.metadata,
            required_tools=payload.required_tools,
            required_tags=payload.required_tags,
            tenant=payload.tenant,
            workspace_root=payload.workspace_root,
            require_base_commit=payload.require_base_commit,
            required_capabilities=payload.required_capabilities,
            secrets_needed=payload.secrets_needed,
            network_egress=payload.network_egress,
            kind=payload.kind,
        )
        audit_dispatch(ctx, task, signed=False, dispatcher_id=None, approval_id=payload.approval_id)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"rqlite unreachable: {exc}") from exc
    return task


@router.get("/tasks", dependencies=[Depends(require_auth)])
def list_tasks(request: Request, status: str | None = None, limit: int = 100) -> dict[str, Any]:
    blackboard = get_context(request).blackboard
    return {"tasks": blackboard.list_tasks(status_filter=status, limit=limit)}


@router.get("/tasks/waiting", dependencies=[Depends(require_auth)])
def list_waiting_tasks(request: Request) -> dict[str, Any]:
    blackboard = get_context(request).blackboard
    runners = blackboard.list_runners()
    online = [
        runner
        for runner in runners
        if runner.get("state") in ("online", "degraded") and not runner.get("drain_requested")
    ]
    out: list[dict[str, Any]] = []
    for task in blackboard.list_tasks(status_filter="queued", limit=200):
        reqs = task.get("required_capabilities") or []
        if not reqs:
            continue
        satisfied_by: list[str] = []
        misses: dict[str, list[str]] = {}
        for runner in online:
            caps = runner.get("capabilities") or {}
            ok, missing = _capability_match(reqs, caps)
            if ok:
                satisfied_by.append(runner["runner_id"])
            else:
                misses[runner["runner_id"]] = missing
        if satisfied_by:
            continue
        out.append(
            {
                "task_id": task["id"],
                "title": task.get("title"),
                "branch": task.get("branch"),
                "required_capabilities": reqs,
                "missing_per_runner": misses,
            }
        )
    return {"tasks": out, "online_runners": [runner["runner_id"] for runner in online]}


@router.get("/tasks/{task_id}", dependencies=[Depends(require_auth)])
def get_task(request: Request, task_id: int) -> dict[str, Any]:
    try:
        return get_context(request).blackboard.get_task(task_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="task not found") from exc


@router.post("/tasks/claim", dependencies=[Depends(require_auth)])
def claim_task(request: Request, payload: ClaimRequest) -> JSONResponse:
    ctx = get_context(request)
    task = ctx.blackboard.claim_next_task(
        worker_id=payload.worker_id,
        hostname=payload.hostname,
        capabilities=payload.capabilities,
    )
    audit_claim(ctx, task, worker_id=payload.worker_id)
    return JSONResponse(content={"task": task})
