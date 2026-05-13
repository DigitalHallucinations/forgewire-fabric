"""Task progress, stream, result, note, and SSE routes."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from sse_starlette.sse import EventSourceResponse

from forgewire_fabric.hub.server import (
    PROGRESS_POLL_SECONDS,
    NoteRequest,
    ProgressRequest,
    ResultRequest,
    StreamBulkRequest,
    StreamRequest,
)

from ._deps import get_context, require_auth
from ._helpers import audit_result, enforce_completion_gate

router = APIRouter()


@router.post("/tasks/{task_id}/start", dependencies=[Depends(require_auth)])
def mark_running(request: Request, task_id: int) -> dict[str, Any]:
    try:
        return get_context(request).blackboard.mark_running(task_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="task not found") from exc


@router.post("/tasks/{task_id}/cancel", dependencies=[Depends(require_auth)])
def cancel_task(request: Request, task_id: int) -> dict[str, Any]:
    try:
        return get_context(request).blackboard.cancel_task(task_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="task not found") from exc


@router.post("/tasks/{task_id}/progress", dependencies=[Depends(require_auth)])
def append_progress(request: Request, task_id: int, payload: ProgressRequest) -> dict[str, Any]:
    blackboard = get_context(request).blackboard
    try:
        return blackboard.append_progress(
            task_id=task_id,
            worker_id=payload.worker_id,
            message=blackboard.redact_text(payload.message) or "",
            files_touched=payload.files_touched,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="task not found") from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@router.post("/tasks/{task_id}/stream", dependencies=[Depends(require_auth)])
def append_stream(request: Request, task_id: int, payload: StreamRequest) -> dict[str, Any]:
    blackboard = get_context(request).blackboard
    try:
        return blackboard.append_stream(
            task_id=task_id,
            worker_id=payload.worker_id,
            channel=payload.channel,
            line=blackboard.redact_text(payload.line) or "",
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="task not found") from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/tasks/{task_id}/stream/bulk", dependencies=[Depends(require_auth)])
def append_stream_bulk(
    request: Request, task_id: int, payload: StreamBulkRequest
) -> dict[str, Any]:
    blackboard = get_context(request).blackboard
    try:
        return blackboard.append_stream_bulk(
            task_id=task_id,
            worker_id=payload.worker_id,
            entries=[
                {"channel": entry.channel, "line": blackboard.redact_text(entry.line) or ""}
                for entry in payload.entries
            ],
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="task not found") from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/tasks/{task_id}/stream", dependencies=[Depends(require_auth)])
def read_stream(
    request: Request, task_id: int, after_seq: int = 0, limit: int = 500
) -> dict[str, Any]:
    return {
        "lines": get_context(request).blackboard.streams_since(
            task_id=task_id, after_seq=after_seq, limit=limit
        )
    }


@router.post("/tasks/{task_id}/result", dependencies=[Depends(require_auth)])
def submit_result(request: Request, task_id: int, payload: ResultRequest) -> dict[str, Any]:
    ctx = get_context(request)
    blackboard = ctx.blackboard
    enforce_completion_gate(ctx, task_id=str(task_id), changed_paths=payload.files_touched)
    try:
        task = blackboard.submit_result(
            task_id=task_id,
            worker_id=payload.worker_id,
            status_value=payload.status,
            head_commit=payload.head_commit,
            commits=payload.commits,
            files_touched=payload.files_touched,
            test_summary=payload.test_summary,
            log_tail=blackboard.redact_text(payload.log_tail),
            error=blackboard.redact_text(payload.error),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="task not found") from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    audit_result(ctx, task, worker_id=payload.worker_id)
    return task


@router.post("/tasks/{task_id}/notes", dependencies=[Depends(require_auth)])
def post_note(request: Request, task_id: int, payload: NoteRequest) -> dict[str, Any]:
    try:
        return get_context(request).blackboard.post_note(
            task_id=task_id, author=payload.author, body=payload.body
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="task not found") from exc


@router.get("/tasks/{task_id}/notes", dependencies=[Depends(require_auth)])
def read_notes(request: Request, task_id: int, after_id: int = 0) -> dict[str, Any]:
    return {"notes": get_context(request).blackboard.read_notes(task_id=task_id, after_id=after_id)}


@router.get("/tasks/{task_id}/events", dependencies=[Depends(require_auth)])
def task_events(request: Request, task_id: int) -> EventSourceResponse:
    blackboard = get_context(request).blackboard

    async def stream() -> AsyncIterator[dict[str, Any]]:
        last_seq = 0
        terminal = {"done", "failed", "cancelled", "timed_out"}
        while True:
            if await request.is_disconnected():
                return
            try:
                task = blackboard.get_task(task_id)
            except KeyError:
                yield {"event": "error", "data": json.dumps({"error": "not_found"})}
                return
            progress = blackboard.progress_since(task_id=task_id, after_seq=last_seq)
            for entry in progress:
                last_seq = entry["seq"]
                yield {"event": "progress", "data": json.dumps(entry)}
            yield {"event": "task", "data": json.dumps(task)}
            if task["status"] in terminal:
                return
            await asyncio.sleep(PROGRESS_POLL_SECONDS)

    return EventSourceResponse(stream())
