"""In-memory :class:`ClusterTransport` for tests + single-node dev."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from forgewire_fabric.cluster.protocol import (
    DEFAULT_IDEMPOTENCY_TTL_SECONDS,
    FabricEnvelope,
)
from forgewire_fabric.cluster.transport import (
    ClusterTransport,
    EnvelopeFilter,
    EnvelopeHandler,
    Subscription,
)

LOGGER = logging.getLogger("forgewire_fabric.cluster.inmemory")


@dataclass
class _ChannelConfig:
    idempotency_key_field: str | None = None
    idempotency_ttl_seconds: float = DEFAULT_IDEMPOTENCY_TTL_SECONDS
    dead_letter_channel: str | None = None
    seen: dict[str, float] = field(default_factory=dict)

    def admit(self, envelope: FabricEnvelope) -> bool:
        """Return True if the envelope is new w.r.t. the dedup key."""
        if not self.idempotency_key_field:
            return True
        key = envelope.payload.get(self.idempotency_key_field)
        if key is None:
            return True
        now = time.monotonic()
        # Sweep expired entries lazily so the dict cannot grow unbounded.
        cutoff = now - self.idempotency_ttl_seconds
        if self.seen and len(self.seen) > 64:
            stale = [k for k, t in self.seen.items() if t < cutoff]
            for k in stale:
                self.seen.pop(k, None)
        prior = self.seen.get(str(key))
        if prior is not None and prior >= cutoff:
            return False
        self.seen[str(key)] = now
        return True


@dataclass
class _Subscriber:
    handler: EnvelopeHandler
    filter_fn: EnvelopeFilter | None
    handler_name: str
    sub_id: str


class _InMemorySubscription(Subscription):
    def __init__(self, transport: "InMemoryClusterTransport", channel: str, sub_id: str) -> None:
        self._transport = transport
        self._channel = channel
        self._sub_id = sub_id
        self._cancelled = False

    async def cancel(self) -> None:
        if self._cancelled:
            return
        self._cancelled = True
        await self._transport._cancel(self._channel, self._sub_id)


class InMemoryClusterTransport(ClusterTransport):
    """Asyncio-only in-process transport.

    Publish dispatches sequentially to each subscriber on the same loop.
    Handler exceptions are logged and routed to the configured DLQ if one
    is set; they do NOT propagate back into ``publish``.
    """

    def __init__(self) -> None:
        self._channels: dict[str, _ChannelConfig] = {}
        self._subscribers: dict[str, dict[str, _Subscriber]] = {}
        self._lock = asyncio.Lock()
        self._started = False

    async def start(self) -> None:
        self._started = True

    async def stop(self) -> None:
        self._started = False
        async with self._lock:
            self._subscribers.clear()
            self._channels.clear()

    async def configure_channel(
        self,
        channel: str,
        *,
        idempotency_key_field: str | None = None,
        idempotency_ttl_seconds: float = DEFAULT_IDEMPOTENCY_TTL_SECONDS,
        dead_letter_channel: str | None = None,
    ) -> None:
        cfg = self._channels.setdefault(channel, _ChannelConfig())
        if idempotency_key_field is not None:
            cfg.idempotency_key_field = idempotency_key_field
            cfg.idempotency_ttl_seconds = idempotency_ttl_seconds
        if dead_letter_channel is not None:
            cfg.dead_letter_channel = dead_letter_channel

    async def publish(self, envelope: FabricEnvelope) -> str:
        if not self._started:
            raise RuntimeError("InMemoryClusterTransport not started")

        cfg = self._channels.setdefault(envelope.channel, _ChannelConfig())
        if not cfg.admit(envelope):
            return envelope.trace_id

        msg_id = uuid.uuid4().hex
        # Snapshot subscribers so cancellations during dispatch don't mutate iteration.
        subs = list(self._subscribers.get(envelope.channel, {}).values())
        for sub in subs:
            try:
                if sub.filter_fn is not None and not sub.filter_fn(envelope):
                    continue
                await sub.handler(envelope)
            except Exception:  # noqa: BLE001 - logged + DLQ'd
                LOGGER.exception(
                    "in-memory handler raised on %s (handler=%s)",
                    envelope.channel,
                    sub.handler_name,
                )
                if cfg.dead_letter_channel:
                    dlq = FabricEnvelope(
                        channel=cfg.dead_letter_channel,
                        payload={
                            "original_channel": envelope.channel,
                            "original_payload": dict(envelope.payload),
                            "handler": sub.handler_name,
                        },
                        priority=envelope.priority,
                        trace_id=envelope.trace_id,
                    )
                    # Best-effort; recursion is bounded by user-configured DLQ topology.
                    try:
                        await self.publish(dlq)
                    except Exception:  # noqa: BLE001
                        LOGGER.exception("DLQ republish failed for %s", cfg.dead_letter_channel)
        return msg_id

    async def subscribe(
        self,
        channel: str,
        handler: EnvelopeHandler,
        *,
        handler_name: str | None = None,
        filter_fn: EnvelopeFilter | None = None,
    ) -> Subscription:
        if not self._started:
            raise RuntimeError("InMemoryClusterTransport not started")

        sub_id = uuid.uuid4().hex
        name = handler_name or f"handler_{sub_id[:8]}"
        async with self._lock:
            self._subscribers.setdefault(channel, {})[sub_id] = _Subscriber(
                handler=handler,
                filter_fn=filter_fn,
                handler_name=name,
                sub_id=sub_id,
            )
        return _InMemorySubscription(self, channel, sub_id)

    async def _cancel(self, channel: str, sub_id: str) -> None:
        async with self._lock:
            channel_subs = self._subscribers.get(channel)
            if channel_subs is not None:
                channel_subs.pop(sub_id, None)
                if not channel_subs:
                    self._subscribers.pop(channel, None)

    # ----- Test introspection helpers (NOT public API) -----

    def _channel_config(self, channel: str) -> _ChannelConfig | None:
        return self._channels.get(channel)

    def _subscriber_count(self, channel: str) -> int:
        return len(self._subscribers.get(channel, {}))

    def _snapshot(self) -> dict[str, Any]:
        return {
            "channels": {k: dict(v.__dict__) for k, v in self._channels.items()},
            "subscribers": {k: list(v.keys()) for k, v in self._subscribers.items()},
        }
