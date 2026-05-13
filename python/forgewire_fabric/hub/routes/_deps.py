"""Shared FastAPI dependencies for hub route modules."""

from __future__ import annotations

import secrets as secrets_lib
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException, Request, status


@dataclass(slots=True)
class HubContext:
    config: Any
    blackboard: Any
    gate: Any


def get_context(request: Request) -> HubContext:
    return request.app.state.hub_context


async def require_auth(request: Request) -> None:
    header = request.headers.get("authorization", "")
    if not header.lower().startswith("bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing bearer token",
        )
    presented = header.split(" ", 1)[1].strip()
    if not secrets_lib.compare_digest(presented, request.app.state.token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="invalid bearer token",
        )
