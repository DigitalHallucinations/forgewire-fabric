"""Wire-level types for the fabric cluster substrate.

The contracts here are deliberately minimal:

* :class:`FabricEnvelope` is the unit of transport. It carries a channel
  name, a JSON-serializable payload dict, an integer priority, an optional
  ``trace_id`` for correlation, and a free-form ``headers`` dict.
* :class:`MessagePriority` mirrors the integer scale used by the embedding
  application's bus so handlers can pass numeric priorities through
  without translation. Lower numeric value == more urgent.
* :func:`composite_envelope_id` is the canonical helper for building
  composite idempotency keys (e.g. chunk transfers, streaming deltas).

Cluster modules should serialize their domain types to ``payload`` dicts
and never embed transport-specific objects.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Mapping

DEFAULT_IDEMPOTENCY_TTL_SECONDS: float = 60.0


class MessagePriority:
    """Integer priority constants. Lower == more urgent."""

    URGENT = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3


@dataclass(frozen=True)
class FabricEnvelope:
    """Transport-neutral envelope for cluster traffic.

    Cluster modules construct envelopes and hand them to a
    :class:`~forgewire_fabric.cluster.transport.ClusterTransport`. The
    transport is responsible for routing, idempotency, and delivery.
    """

    channel: str
    payload: Mapping[str, Any]
    priority: int = MessagePriority.NORMAL
    trace_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    ts: float = field(default_factory=time.time)
    headers: Mapping[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "channel": self.channel,
            "payload": dict(self.payload),
            "priority": int(self.priority),
            "trace_id": self.trace_id,
            "ts": self.ts,
            "headers": dict(self.headers),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FabricEnvelope":
        return cls(
            channel=str(data["channel"]),
            payload=dict(data.get("payload") or {}),
            priority=int(data.get("priority") or MessagePriority.NORMAL),
            trace_id=str(data.get("trace_id") or uuid.uuid4().hex),
            ts=float(data.get("ts") or time.time()),
            headers=dict(data.get("headers") or {}),
        )


def composite_envelope_id(*parts: object) -> str:
    """Build a composite idempotency key from its parts.

    Used for traffic where a single logical request fans out to multiple
    envelopes (e.g. ``"{request_id}:{chunk_index}"`` for blob chunks,
    ``"{trace_id}:{seq}"`` for stream deltas). All parts are coerced to
    ``str`` and joined with ``:``.
    """

    return ":".join(str(p) for p in parts)
