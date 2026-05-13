"""Approval queue routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from forgewire_fabric.hub.server import ApprovalDecisionRequest

from ._deps import get_context, require_auth

router = APIRouter()


@router.get("/approvals", dependencies=[Depends(require_auth)])
def list_approvals(
    request: Request, status: str | None = None, limit: int = 200
) -> dict[str, Any]:
    if status is not None and status not in (
        "pending",
        "approved",
        "denied",
        "consumed",
    ):
        raise HTTPException(
            status_code=400,
            detail="status must be one of pending|approved|denied|consumed",
        )
    return {
        "approvals": get_context(request).blackboard.list_approvals(status=status, limit=limit),
    }


@router.get("/approvals/{approval_id}", dependencies=[Depends(require_auth)])
def get_approval(request: Request, approval_id: str) -> dict[str, Any]:
    row = get_context(request).blackboard.get_approval(approval_id)
    if row is None:
        raise HTTPException(status_code=404, detail="approval not found")
    return row


@router.post("/approvals/{approval_id}/approve", dependencies=[Depends(require_auth)])
def approve_approval(
    request: Request, approval_id: str, payload: ApprovalDecisionRequest
) -> dict[str, Any]:
    try:
        return get_context(request).blackboard.resolve_approval(
            approval_id=approval_id,
            status="approved",
            approver=payload.approver,
            reason=payload.reason,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="approval not found") from exc
    except PermissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/approvals/{approval_id}/deny", dependencies=[Depends(require_auth)])
def deny_approval(
    request: Request, approval_id: str, payload: ApprovalDecisionRequest
) -> dict[str, Any]:
    try:
        return get_context(request).blackboard.resolve_approval(
            approval_id=approval_id,
            status="denied",
            approver=payload.approver,
            reason=payload.reason,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="approval not found") from exc
    except PermissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
