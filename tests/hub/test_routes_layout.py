from __future__ import annotations

import importlib
from pathlib import Path

from fastapi import APIRouter

from forgewire_fabric.hub.server import BlackboardConfig, create_app

ROUTES_DIR = Path(__file__).parents[2] / "python" / "forgewire_fabric" / "hub" / "routes"
ROUTE_MODULES = sorted(
    path.stem
    for path in ROUTES_DIR.glob("*.py")
    if not path.name.startswith("_") and path.name != "__init__.py"
)

EXPECTED_ROUTES = {
    ("/openapi.json", ("GET",)),
    ("/docs", ("GET",)),
    ("/docs/oauth2-redirect", ("GET",)),
    ("/redoc", ("GET",)),
    ("/healthz", ("GET",)),
    ("/cluster/health", ("GET",)),
    ("/state/snapshot", ("GET",)),
    ("/state/import", ("POST",)),
    ("/tasks", ("GET",)),
    ("/tasks", ("POST",)),
    ("/tasks/waiting", ("GET",)),
    ("/tasks/{task_id}", ("GET",)),
    ("/tasks/claim", ("POST",)),
    ("/tasks/v2", ("POST",)),
    ("/approvals", ("GET",)),
    ("/approvals/{approval_id}", ("GET",)),
    ("/approvals/{approval_id}/approve", ("POST",)),
    ("/approvals/{approval_id}/deny", ("POST",)),
    ("/audit/tasks/{task_id}", ("GET",)),
    ("/audit/day/{day}", ("GET",)),
    ("/audit/tail", ("GET",)),
    ("/secrets", ("GET",)),
    ("/secrets", ("POST",)),
    ("/secrets/{name}", ("DELETE",)),
    ("/hosts/roles", ("POST",)),
    ("/hosts", ("GET",)),
    ("/dispatchers/register", ("POST",)),
    ("/dispatchers", ("GET",)),
    ("/runners/register", ("POST",)),
    ("/runners", ("GET",)),
    ("/labels", ("GET",)),
    ("/labels/hub", ("PUT",)),
    ("/labels/runners/{runner_id}", ("PUT",)),
    ("/labels/hosts/{hostname}", ("PUT",)),
    ("/runners/{runner_id}/heartbeat", ("POST",)),
    ("/runners/{runner_id}/drain", ("POST",)),
    ("/runners/{runner_id}/drain-by-dispatcher", ("POST",)),
    ("/runners/{runner_id}/undrain-by-dispatcher", ("POST",)),
    ("/tasks/claim-v2", ("POST",)),
    ("/tasks/{task_id}/start", ("POST",)),
    ("/tasks/{task_id}/cancel", ("POST",)),
    ("/tasks/{task_id}/progress", ("POST",)),
    ("/tasks/{task_id}/stream", ("GET",)),
    ("/tasks/{task_id}/stream", ("POST",)),
    ("/tasks/{task_id}/stream/bulk", ("POST",)),
    ("/tasks/{task_id}/result", ("POST",)),
    ("/tasks/{task_id}/notes", ("GET",)),
    ("/tasks/{task_id}/notes", ("POST",)),
    ("/tasks/{task_id}/events", ("GET",)),
}


def test_route_modules_export_exactly_one_router() -> None:
    assert ROUTE_MODULES == [
        "admin",
        "approvals",
        "audit",
        "auth",
        "cluster",
        "runners",
        "secrets",
        "streams",
        "tasks",
    ]
    for module_name in ROUTE_MODULES:
        module = importlib.import_module(f"forgewire_fabric.hub.routes.{module_name}")
        routers = [value for value in vars(module).values() if isinstance(value, APIRouter)]
        assert routers == [module.router]


def test_create_app_route_surface_matches_pre_split_snapshot(tmp_path: Path) -> None:
    cfg = BlackboardConfig(
        db_path=tmp_path / "hub.sqlite3",
        token="x" * 16,
        host="127.0.0.1",
        port=8765,
        labels_snapshot_path=tmp_path / "labels.snapshot.json",
    )
    app = create_app(cfg)
    actual = {
        (
            getattr(route, "path", ""),
            tuple(
                sorted(
                    method
                    for method in getattr(route, "methods", set())
                    if method not in {"HEAD", "OPTIONS"}
                )
            ),
        )
        for route in app.routes
        if getattr(route, "methods", None)
    }
    assert actual == EXPECTED_ROUTES


def test_create_app_public_import_surface() -> None:
    from forgewire_fabric.hub.server import create_app as imported

    assert imported is create_app
