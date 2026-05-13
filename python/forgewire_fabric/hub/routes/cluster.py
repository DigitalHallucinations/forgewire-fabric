"""Cluster, health, and host-summary routes."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, Request

from forgewire_fabric.hub._crypto import HAS_RUST as _HUB_CRYPTO_HAS_RUST
from forgewire_fabric.hub._router import HAS_RUST as _HUB_ROUTER_HAS_RUST
from forgewire_fabric.hub._streams import HAS_RUST as _HUB_STREAMS_HAS_RUST
from forgewire_fabric.hub.server import (
    PROTOCOL_VERSION,
)

from ._deps import get_context, require_auth

router = APIRouter()


@router.get("/healthz")
def healthz(request: Request) -> dict[str, Any]:
    ctx = get_context(request)
    return {
        "status": "ok",
        "version": request.app.version,
        # Explicit alias so clients (VSIX, dispatcher) don't have to guess
        # whether "version" is the package, the API surface, or the protocol.
        "package_version": request.app.version,
        "protocol_version": PROTOCOL_VERSION,
        "rust_crypto": _HUB_CRYPTO_HAS_RUST,
        "rust_router": _HUB_ROUTER_HAS_RUST,
        "rust_streams": _HUB_STREAMS_HAS_RUST,
        "started_at": request.app.state.started_at,
        "uptime_seconds": time.time() - request.app.state.started_at,
        "host": ctx.config.host,
        "port": ctx.config.port,
    }


@router.get("/cluster/health", dependencies=[Depends(require_auth)])
def cluster_health(request: Request) -> dict[str, Any]:
    ctx = get_context(request)
    report = getattr(request.app.state, "labels_snapshot_report", None) or {
        "status": "unknown",
        "applied": 0,
        "path": None,
    }
    sidecar: dict[str, Any] = {
        "status": report.get("status"),
        "applied": report.get("applied", 0),
        "path": report.get("path"),
        "exists": False,
        "size_bytes": None,
        "mtime": None,
    }
    sp = report.get("path")
    if sp:
        try:
            path = Path(sp)
            if path.exists():
                stat = path.stat()
                sidecar["exists"] = True
                sidecar["size_bytes"] = stat.st_size
                sidecar["mtime"] = stat.st_mtime
        except OSError:
            pass
    rqlite: dict[str, Any] | None = None
    if ctx.config.backend == "rqlite":
        rqlite = {
            "host": ctx.config.rqlite_host,
            "port": ctx.config.rqlite_port,
            "consistency": ctx.config.rqlite_consistency,
        }
    return {
        "backend": ctx.config.backend,
        "rqlite": rqlite,
        "labels_snapshot": sidecar,
    }
