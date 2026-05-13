"""Audit log routes."""

from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from ._deps import get_context, require_auth
from ._helpers import verify_audit_events

router = APIRouter()


@router.get("/audit/tasks/{task_id}", dependencies=[Depends(require_auth)])
def audit_for_task(request: Request, task_id: int) -> dict[str, Any]:
    events = get_context(request).blackboard.audit_iter_task(task_id)
    ok, err = verify_audit_events(events)
    return {"events": events, "verified": ok, "error": err}


@router.get("/audit/day/{day}", dependencies=[Depends(require_auth)])
def audit_for_day(request: Request, day: str) -> dict[str, Any]:
    try:
        time.strptime(day, "%Y-%m-%d")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="day must be YYYY-MM-DD") from exc
    events = get_context(request).blackboard.audit_iter_day(day)
    ok, err = verify_audit_events(events)
    return {"day": day, "events": events, "verified": ok, "error": err}


@router.get("/audit/tail", dependencies=[Depends(require_auth)])
def audit_tail(request: Request) -> dict[str, Any]:
    return {"chain_tail": get_context(request).blackboard.audit_chain_tail()}
