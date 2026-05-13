"""Transport abstraction for the fabric cluster substrate.

Cluster modules depend on :class:`ClusterTransport`, never on a concrete
bus implementation. Two adapters exist:

* :class:`~forgewire_fabric.cluster._inmemory.InMemoryClusterTransport` —
  in-process pub/sub for tests and single-node local dev.
* External ``BusTransport`` lives in the embedding application
  (ForgeWire-internal wires :class:`ClusterTransport` over its in-process
  AgentBus / NCB substrate; nothing in this repo imports it).

The contract is intentionally narrow — publish, subscribe with optional
filter, and configure a channel's idempotency / DLQ semantics. Anything
richer (request/reply correlation, ack/nack, persistent retry) must be
built on top of these primitives.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

from forgewire_fabric.cluster.protocol import (
    DEFAULT_IDEMPOTENCY_TTL_SECONDS,
    FabricEnvelope,
)

EnvelopeHandler = Callable[[FabricEnvelope], Awaitable[None]]
EnvelopeFilter = Callable[[FabricEnvelope], bool]


class Subscription(ABC):
    """Handle to an active subscription. Cancellation is idempotent."""

    @abstractmethod
    async def cancel(self) -> None:
        ...


class ClusterTransport(ABC):
    """Minimal pub/sub contract that cluster modules depend on."""

    @abstractmethod
    async def start(self) -> None:
        ...

    @abstractmethod
    async def stop(self) -> None:
        ...

    @abstractmethod
    async def configure_channel(
        self,
        channel: str,
        *,
        idempotency_key_field: str | None = None,
        idempotency_ttl_seconds: float = DEFAULT_IDEMPOTENCY_TTL_SECONDS,
        dead_letter_channel: str | None = None,
    ) -> None:
        """Configure idempotency + DLQ semantics for ``channel``.

        Calling this multiple times with the same arguments must be a
        no-op. Calling with different arguments updates the configuration
        in place.
        """

    @abstractmethod
    async def publish(self, envelope: FabricEnvelope) -> str:
        """Publish ``envelope`` and return the broker-assigned message id.

        Implementations must enforce the configured idempotency key
        deduplication when one has been set on the channel.
        """

    @abstractmethod
    async def subscribe(
        self,
        channel: str,
        handler: EnvelopeHandler,
        *,
        handler_name: str | None = None,
        filter_fn: EnvelopeFilter | None = None,
    ) -> Subscription:
        """Subscribe ``handler`` to ``channel``.

        ``filter_fn``, when provided, receives the deserialized envelope
        and must return ``True`` to admit it to ``handler``. ``filter_fn``
        runs synchronously and must be cheap.
        """
