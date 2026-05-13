"""Tests for the in-memory cluster transport."""

from __future__ import annotations


import pytest

from forgewire_fabric.cluster import (
    FabricEnvelope,
    InMemoryClusterTransport,
    MessagePriority,
    composite_envelope_id,
)


@pytest.fixture
async def transport() -> "InMemoryClusterTransport":
    t = InMemoryClusterTransport()
    await t.start()
    try:
        yield t
    finally:
        await t.stop()


async def test_publish_delivers_to_subscriber(transport: InMemoryClusterTransport) -> None:
    received: list[FabricEnvelope] = []

    async def handler(env: FabricEnvelope) -> None:
        received.append(env)

    await transport.subscribe("cluster.test", handler)
    env = FabricEnvelope(channel="cluster.test", payload={"k": 1})
    await transport.publish(env)

    assert len(received) == 1
    assert received[0].payload == {"k": 1}


async def test_filter_admits_only_matching_envelopes(
    transport: InMemoryClusterTransport,
) -> None:
    received: list[FabricEnvelope] = []

    async def handler(env: FabricEnvelope) -> None:
        received.append(env)

    await transport.subscribe(
        "cluster.test",
        handler,
        filter_fn=lambda e: e.payload.get("target") == "node-A",
    )
    await transport.publish(FabricEnvelope(channel="cluster.test", payload={"target": "node-A"}))
    await transport.publish(FabricEnvelope(channel="cluster.test", payload={"target": "node-B"}))

    assert len(received) == 1
    assert received[0].payload["target"] == "node-A"


async def test_idempotency_dedupes_by_configured_field(
    transport: InMemoryClusterTransport,
) -> None:
    received: list[FabricEnvelope] = []

    async def handler(env: FabricEnvelope) -> None:
        received.append(env)

    await transport.configure_channel("cluster.dedup", idempotency_key_field="request_id")
    await transport.subscribe("cluster.dedup", handler)

    await transport.publish(
        FabricEnvelope(channel="cluster.dedup", payload={"request_id": "r1", "n": 1})
    )
    await transport.publish(
        FabricEnvelope(channel="cluster.dedup", payload={"request_id": "r1", "n": 2})
    )
    await transport.publish(
        FabricEnvelope(channel="cluster.dedup", payload={"request_id": "r2", "n": 3})
    )

    assert [e.payload["n"] for e in received] == [1, 3]


async def test_subscription_cancel_stops_delivery(
    transport: InMemoryClusterTransport,
) -> None:
    received: list[FabricEnvelope] = []

    async def handler(env: FabricEnvelope) -> None:
        received.append(env)

    sub = await transport.subscribe("cluster.cancel", handler)
    await transport.publish(FabricEnvelope(channel="cluster.cancel", payload={"n": 1}))
    await sub.cancel()
    # Cancel must be idempotent.
    await sub.cancel()
    await transport.publish(FabricEnvelope(channel="cluster.cancel", payload={"n": 2}))

    assert [e.payload["n"] for e in received] == [1]


async def test_handler_exception_routes_to_dlq(
    transport: InMemoryClusterTransport,
) -> None:
    dlq: list[FabricEnvelope] = []

    async def bad_handler(env: FabricEnvelope) -> None:
        raise RuntimeError("boom")

    async def dlq_handler(env: FabricEnvelope) -> None:
        dlq.append(env)

    await transport.configure_channel("cluster.dlq.target", dead_letter_channel="cluster.dlq.sink")
    await transport.subscribe("cluster.dlq.target", bad_handler, handler_name="bad")
    await transport.subscribe("cluster.dlq.sink", dlq_handler)

    await transport.publish(FabricEnvelope(channel="cluster.dlq.target", payload={"n": 1}))

    assert len(dlq) == 1
    assert dlq[0].payload["original_channel"] == "cluster.dlq.target"
    assert dlq[0].payload["handler"] == "bad"


async def test_envelope_round_trip_through_dict() -> None:
    env = FabricEnvelope(
        channel="cluster.x",
        payload={"k": 1},
        priority=MessagePriority.HIGH,
        headers={"h": "v"},
    )
    restored = FabricEnvelope.from_dict(env.to_dict())
    assert restored.channel == env.channel
    assert restored.payload == env.payload
    assert restored.priority == env.priority
    assert restored.trace_id == env.trace_id
    assert restored.headers == env.headers


def test_composite_envelope_id_joins_with_colons() -> None:
    assert composite_envelope_id("req-1", 7) == "req-1:7"
    assert composite_envelope_id("trace-x", "delta", 0) == "trace-x:delta:0"


async def test_publish_before_start_raises() -> None:
    t = InMemoryClusterTransport()
    with pytest.raises(RuntimeError):
        await t.publish(FabricEnvelope(channel="x", payload={}))


async def test_stop_clears_subscribers() -> None:
    t = InMemoryClusterTransport()
    await t.start()
    received: list[FabricEnvelope] = []

    async def handler(env: FabricEnvelope) -> None:
        received.append(env)

    await t.subscribe("cluster.stop", handler)
    await t.stop()
    await t.start()
    # After stop+start, the prior subscription must be gone.
    await t.publish(FabricEnvelope(channel="cluster.stop", payload={}))
    assert received == []
    await t.stop()
