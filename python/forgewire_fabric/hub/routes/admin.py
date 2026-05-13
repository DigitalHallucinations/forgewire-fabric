"""Administrative snapshot/import routes."""

from __future__ import annotations

import contextlib
import os
import sqlite3
import time
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response as FastAPIResponse

from ._deps import get_context, require_auth

router = APIRouter()


@router.get("/state/snapshot", dependencies=[Depends(require_auth)])
def state_snapshot(request: Request) -> FastAPIResponse:
    ctx = get_context(request)
    config = ctx.config

    if config.backend == "rqlite":
        try:
            with httpx.Client(
                base_url=f"http://{config.rqlite_host}:{config.rqlite_port}",
                timeout=60.0,
                follow_redirects=True,
            ) as client:
                resp = client.get("/db/backup")
                if resp.status_code != 200:
                    raise HTTPException(
                        status_code=502,
                        detail=(
                            f"rqlite /db/backup failed: "
                            f"{resp.status_code} {resp.text[:200]}"
                        ),
                    )
                data = resp.content
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail=f"rqlite unreachable: {exc}") from exc
        return FastAPIResponse(
            content=data,
            media_type="application/x-sqlite3",
            headers={
                "X-Snapshot-Generated-At": str(time.time()),
                "X-Hub-Started-At": str(request.app.state.started_at),
                "X-Snapshot-Source": "rqlite",
            },
        )

    snap_dir = config.db_path.parent / "snapshots"
    snap_dir.mkdir(parents=True, exist_ok=True)
    snap_path = snap_dir / f".snapshot-{os.getpid()}.sqlite3"
    if snap_path.exists():
        snap_path.unlink()
    with sqlite3.connect(config.db_path) as src:
        src.execute(f"VACUUM INTO '{snap_path.as_posix()}'")
    data = snap_path.read_bytes()
    with contextlib.suppress(OSError):
        snap_path.unlink()
    return FastAPIResponse(
        content=data,
        media_type="application/x-sqlite3",
        headers={
            "X-Snapshot-Generated-At": str(time.time()),
            "X-Hub-Started-At": str(request.app.state.started_at),
            "X-Snapshot-Source": "sqlite",
        },
    )


@router.post("/state/import", dependencies=[Depends(require_auth)])
async def state_import(request: Request) -> dict[str, Any]:
    ctx = get_context(request)
    config = ctx.config
    blackboard = ctx.blackboard
    body = await request.body()
    if not body:
        raise HTTPException(status_code=400, detail="empty body")
    force = request.headers.get("x-force", "").strip() == "1"
    if not force:
        count = blackboard.count_tasks()
        if count > 0:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"refusing to import over a non-empty hub "
                    f"({count} tasks); send X-Force: 1 to override"
                ),
            )

    if config.backend == "rqlite":
        try:
            with httpx.Client(
                base_url=f"http://{config.rqlite_host}:{config.rqlite_port}",
                timeout=120.0,
                follow_redirects=True,
            ) as client:
                resp = client.post(
                    "/db/load",
                    content=body,
                    headers={"Content-Type": "application/octet-stream"},
                )
                if resp.status_code != 200:
                    raise HTTPException(
                        status_code=502,
                        detail=(
                            f"rqlite /db/load failed: "
                            f"{resp.status_code} {resp.text[:200]}"
                        ),
                    )
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail=f"rqlite unreachable: {exc}") from exc
        return {"status": "imported", "bytes": len(body), "backend": "rqlite"}

    new_path = config.db_path.with_suffix(config.db_path.suffix + ".new")
    new_path.write_bytes(body)
    try:
        with sqlite3.connect(new_path) as test:
            test.execute("SELECT COUNT(*) FROM tasks").fetchone()
    except sqlite3.DatabaseError as exc:
        new_path.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=f"invalid sqlite blob: {exc}") from exc
    os.replace(new_path, config.db_path)
    return {
        "status": "imported",
        "bytes": len(body),
        "backend": "sqlite",
    }
