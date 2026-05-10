"""ForgeWire Fabric — LAN cluster substrate.

Transport-agnostic primitives for membership, claim, blob fabric, streaming,
and operator control plane. Two adapters satisfy :class:`ClusterTransport`:

* :class:`InMemoryClusterTransport` — in-process pub/sub for tests and
  single-node local dev. Lives in :mod:`forgewire_fabric.cluster._inmemory`.
* External `BusTransport` (lives in the embedding application, e.g.
  ForgeWire-internal) — wires :class:`ClusterTransport` over an existing
  bus implementation. The fabric repo never imports the embedding
  application.

Lineage: lifted from PhrenForge/ForgeWire todo 114 Phase 1 (LAN Loom).
The internal repository's ``core/services/cluster/`` package re-exports
this namespace and provides the :class:`ClusterTransport` adapter for
its in-process AgentBus / NCB substrate.
"""

from __future__ import annotations

from forgewire_fabric.cluster.protocol import (
    DEFAULT_IDEMPOTENCY_TTL_SECONDS,
    FabricEnvelope,
    MessagePriority,
    composite_envelope_id,
)
from forgewire_fabric.cluster.transport import (
    ClusterTransport,
    EnvelopeFilter,
    EnvelopeHandler,
    Subscription,
)
from forgewire_fabric.cluster._inmemory import InMemoryClusterTransport

__all__ = [
    "ClusterTransport",
    "DEFAULT_IDEMPOTENCY_TTL_SECONDS",
    "EnvelopeFilter",
    "EnvelopeHandler",
    "FabricEnvelope",
    "InMemoryClusterTransport",
    "MessagePriority",
    "Subscription",
    "composite_envelope_id",
]
