"""Async HTTP client for the blackboard REST API.

Used by both MCP servers (dispatcher and runner). Single source of truth for
endpoints, auth, and error mapping.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

import httpx


class BlackboardError(RuntimeError):
    """Raised when the blackboard returns a non-success status."""

    def __init__(self, status_code: int, body: str) -> None:
        super().__init__(f"blackboard HTTP {status_code}: {body}")
        self.status_code = status_code
        self.body = body


class BlackboardClient:
    """Thin async wrapper around the blackboard HTTP API."""

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float = 30.0,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self._base,
            headers={"authorization": f"Bearer {token}"},
            timeout=timeout,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "BlackboardClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # ------------------------------------------------------------------ low

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        # Network-resilience: retry transport-level errors (DNS flaps, dropped
        # routes, hub restart). HTTP-level 4xx/5xx are *not* retried because
        # they are application failures.
        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                response = await self._client.request(
                    method, path, json=json, params=params
                )
                break
            except (httpx.ConnectError, httpx.ReadError, httpx.RemoteProtocolError) as exc:
                last_exc = exc
                if attempt == 2:
                    raise BlackboardError(
                        status_code=0,
                        body=f"transport error after 3 attempts: {exc}",
                    ) from exc
                await asyncio.sleep(0.5 * (2 ** attempt))
        else:
            assert last_exc is not None
            raise BlackboardError(0, f"transport error: {last_exc}")
        if response.status_code == 204:
            return None
        if response.status_code >= 400:
            raise BlackboardError(response.status_code, response.text)
        return response.json()

    # --------------------------------------------------------------- public

    async def healthz(self) -> dict[str, Any]:
        result = await self._request("GET", "/healthz")
        assert result is not None
        return result

    async def dispatch_task(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await self._request("POST", "/tasks", json=payload)
        assert result is not None
        return result

    async def list_tasks(
        self, *, status: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit}
        if status:
            params["status"] = status
        result = await self._request("GET", "/tasks", params=params)
        assert result is not None
        return result["tasks"]

    async def get_task(self, task_id: int) -> dict[str, Any]:
        result = await self._request("GET", f"/tasks/{task_id}")
        assert result is not None
        return result

    async def claim_task(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        result = await self._request("POST", "/tasks/claim", json=payload)
        if result is None:
            return None
        return result.get("task")

    async def mark_running(self, task_id: int) -> dict[str, Any]:
        result = await self._request("POST", f"/tasks/{task_id}/start")
        assert result is not None
        return result

    async def cancel_task(self, task_id: int) -> dict[str, Any]:
        result = await self._request("POST", f"/tasks/{task_id}/cancel")
        assert result is not None
        return result

    async def append_progress(
        self, task_id: int, payload: dict[str, Any]
    ) -> dict[str, Any]:
        result = await self._request(
            "POST", f"/tasks/{task_id}/progress", json=payload
        )
        assert result is not None
        return result

    async def submit_result(
        self, task_id: int, payload: dict[str, Any]
    ) -> dict[str, Any]:
        result = await self._request(
            "POST", f"/tasks/{task_id}/result", json=payload
        )

    # --------------------------------------------------------------- v2 API

    async def register_runner(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await self._request("POST", "/runners/register", json=payload)
        assert result is not None
        return result

    async def heartbeat(
        self, runner_id: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        result = await self._request(
            "POST", f"/runners/{runner_id}/heartbeat", json=payload
        )
        assert result is not None
        return result

    async def list_runners(self) -> dict[str, Any]:
        result = await self._request("GET", "/runners")
        assert result is not None
        return result

    async def drain_runner_signed(
        self, runner_id: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        result = await self._request(
            "POST", f"/runners/{runner_id}/drain", json=payload
        )
        assert result is not None
        return result

    async def drain_runner_by_dispatcher(self, runner_id: str) -> dict[str, Any]:
        result = await self._request(
            "POST", f"/runners/{runner_id}/drain-by-dispatcher"
        )
        assert result is not None
        return result

    async def claim_task_v2(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Capability-aware claim. Returns ``{task: <task|null>, info: {...}}``."""
        result = await self._request("POST", "/tasks/claim-v2", json=payload)
        assert result is not None
        return result

    async def append_stream(
        self, task_id: int, payload: dict[str, Any]
    ) -> dict[str, Any]:
        result = await self._request(
            "POST", f"/tasks/{task_id}/stream", json=payload
        )
        assert result is not None
        return result

    async def append_stream_bulk(
        self, task_id: int, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """POST a batch of stream entries in a single transaction.

        ``payload`` is ``{"worker_id": str, "entries": [{"channel": ...,
        "line": ...}, ...]}``. Server-side, all entries share one
        ``BEGIN IMMEDIATE``/``COMMIT`` so the WAL fsync amortises across
        the batch. Use this when a worker is producing high-frequency
        stdout/stderr lines (todo 113 Stage C.3 follow-up).
        """
        result = await self._request(
            "POST", f"/tasks/{task_id}/stream/bulk", json=payload
        )
        assert result is not None
        return result

    async def read_stream(
        self, task_id: int, *, after_seq: int = 0, limit: int = 500
    ) -> list[dict[str, Any]]:
        result = await self._request(
            "GET", f"/tasks/{task_id}/stream",
            params={"after_seq": after_seq, "limit": limit},
        )
        assert result is not None
        return result["lines"]
        assert result is not None
        return result

    async def post_note(
        self, task_id: int, payload: dict[str, Any]
    ) -> dict[str, Any]:
        result = await self._request(
            "POST", f"/tasks/{task_id}/notes", json=payload
        )
        assert result is not None
        return result

    async def read_notes(
        self, task_id: int, *, after_id: int = 0
    ) -> list[dict[str, Any]]:
        result = await self._request(
            "GET", f"/tasks/{task_id}/notes", params={"after_id": after_id}
        )
        assert result is not None
        return result["notes"]

    async def stream_events(self, task_id: int):
        """Yield SSE events as ``(event, data)`` tuples until terminal status.

        ``httpx`` is fine for short SSE streams; for very long streams switch
        to ``httpx-sse``. The blackboard sends a final ``task`` event when
        terminal so the consumer can break out cleanly.
        """
        async with self._client.stream(
            "GET", f"/tasks/{task_id}/events"
        ) as response:
            response.raise_for_status()
            event = "message"
            data_buffer: list[str] = []
            async for line in response.aiter_lines():
                if line == "":
                    if data_buffer:
                        yield event, "\n".join(data_buffer)
                    event = "message"
                    data_buffer = []
                elif line.startswith("event:"):
                    event = line[len("event:"):].strip()
                elif line.startswith("data:"):
                    data_buffer.append(line[len("data:"):].strip())


# ---------------------------------------------------------------------------
# Convenience: load (base_url, token) from env or token file
# ---------------------------------------------------------------------------


def load_client_from_env() -> BlackboardClient:
    """Construct a client from ``BLACKBOARD_URL`` and ``BLACKBOARD_TOKEN``.

    ``BLACKBOARD_TOKEN_FILE`` is honored as a fallback.

    If ``BLACKBOARD_URL`` is unset and ``BLACKBOARD_DISCOVER=1`` is set,
    attempt an mDNS browse for ``_phrenforge-hub._tcp.local.`` and pick the
    first responder. Falls back to ``http://127.0.0.1:8765`` otherwise.
    """
    base = os.environ.get("BLACKBOARD_URL", "").strip()
    if not base and os.environ.get("BLACKBOARD_DISCOVER", "").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        try:
            from scripts.remote.hub.discovery import discover_hubs

            hits = discover_hubs(timeout=3.0)
        except Exception:
            hits = []
        if hits:
            top = hits[0]
            base = f"http://{top['host']}:{top['port']}"
    if not base:
        base = "http://127.0.0.1:8765"
    token = os.environ.get("BLACKBOARD_TOKEN", "").strip()
    if not token:
        token_file = os.environ.get("BLACKBOARD_TOKEN_FILE")
        if token_file:
            token = Path(token_file).read_text(encoding="utf-8").strip()
    if not token:
        raise SystemExit(
            "BLACKBOARD_TOKEN (or BLACKBOARD_TOKEN_FILE) must be set for MCP clients"
        )
    return BlackboardClient(base, token)
