"""Secret broker routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from forgewire_fabric.hub.server import SecretPutRequest

from ._deps import get_context, require_auth

router = APIRouter()


@router.post("/secrets", dependencies=[Depends(require_auth)])
def put_or_rotate_secret(request: Request, payload: SecretPutRequest) -> dict[str, Any]:
    blackboard = get_context(request).blackboard
    existed = any(row.get("name") == payload.name for row in blackboard.list_secrets())
    try:
        if existed:
            meta = blackboard.rotate_secret(name=payload.name, value=payload.value)
        else:
            meta = blackboard.put_secret(name=payload.name, value=payload.value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"secret": meta, "rotated": existed}


@router.get("/secrets", dependencies=[Depends(require_auth)])
def list_secrets(request: Request) -> dict[str, Any]:
    return {"secrets": get_context(request).blackboard.list_secrets()}


@router.delete("/secrets/{name}", dependencies=[Depends(require_auth)])
def delete_secret(request: Request, name: str) -> dict[str, Any]:
    if not name:
        raise HTTPException(status_code=400, detail="name required")
    deleted = get_context(request).blackboard.delete_secret(name=name)
    if not deleted:
        raise HTTPException(status_code=404, detail="secret not found")
    return {"deleted": True, "name": name}
