"""Tests for the small-blob fabric over the in-memory cluster transport."""

from __future__ import annotations

import asyncio
import base64
import hashlib

import pytest

from forgewire_fabric.cluster import (
    BlobAnnouncement,
    BlobFabric,
    BlobIntegrityError,
    BlobNotAllowed,
    BlobTransfer,
    BlobUnavailable,
    CLUSTER_BLOBS_CHANNEL,
    CLUSTER_BLOBS_REQUEST_CHANNEL,
    CLUSTER_BLOBS_TRANSFER_CHANNEL,
    ContentAddressedStore,
    FabricEnvelope,
    InMemoryClusterTransport,
    configure_blob_fabric_channels,
)


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# ContentAddressedStore tests
# ---------------------------------------------------------------------------


async def test_cas_put_get_round_trip(tmp_path) -> None:
    store = ContentAddressedStore(tmp_path)
    payload = b"hello world"
    digest = store.put_bytes(payload)

    assert digest == _digest(payload)
    assert store.has(digest)
    assert store.get_bytes(digest) == payload
    assert store.total_size() == len(payload)
    assert digest in store.list_digests()


async def test_cas_rejects_unknown_digest_when_allowlist_set(tmp_path) -> None:
    payload = b"weights"
    store = ContentAddressedStore(tmp_path, allowlist=[_digest(b"other")])

    with pytest.raises(BlobNotAllowed):
        store.put_bytes(payload)


async def test_cas_lru_eviction_keeps_recent(tmp_path) -> None:
    store = ContentAddressedStore(tmp_path, capacity_bytes=10)
    digest_a = store.put_bytes(b"AAAAA")  # 5 bytes
    digest_b = store.put_bytes(b"BBBBB")  # 5 bytes, total 10
    assert store.get_bytes(digest_a) == b"AAAAA"
    digest_c = store.put_bytes(b"CCCCC")  # evicts B

    assert store.has(digest_a)
    assert store.has(digest_c)
    assert not store.has(digest_b)


# ---------------------------------------------------------------------------
# BlobFabric integration tests over InMemoryClusterTransport
# ---------------------------------------------------------------------------


async def test_blob_fabric_pull_round_trip_with_sha_verification(tmp_path) -> None:
    transport = InMemoryClusterTransport()
    await transport.start()
    holder = requestor = None
    try:
        await configure_blob_fabric_channels(transport)

        holder_store = ContentAddressedStore(tmp_path / "holder-cas")
        requestor_store = ContentAddressedStore(tmp_path / "requestor-cas")

        holder = BlobFabric(transport=transport, node_id="holder-node", store=holder_store)
        requestor = BlobFabric(
            transport=transport,
            node_id="requestor-node",
            store=requestor_store,
            pull_timeout_seconds=2.0,
        )
        await holder.start()
        await requestor.start()

        payload = b"forgewire model weights v1" * 8
        digest = holder_store.put_bytes(payload, namespace="weights")

        await holder.announce(digest, namespace="weights")
        for _ in range(5):
            if requestor.known_holders(digest):
                break
            await asyncio.sleep(0.05)
        assert "holder-node" in requestor.known_holders(digest)

        fetched = await requestor.pull(digest, namespace="weights")

        assert fetched == payload
        assert requestor_store.has(digest)
        assert requestor_store.get_bytes(digest) == payload
    finally:
        if holder is not None:
            await holder.stop()
        if requestor is not None:
            await requestor.stop()
        await transport.stop()


async def test_blob_fabric_pull_unavailable_when_no_holder(tmp_path) -> None:
    transport = InMemoryClusterTransport()
    await transport.start()
    fabric = None
    try:
        store = ContentAddressedStore(tmp_path / "cas")
        fabric = BlobFabric(
            transport=transport, node_id="node", store=store, pull_timeout_seconds=0.5
        )
        await fabric.start()

        with pytest.raises(BlobUnavailable):
            await fabric.pull(_digest(b"missing"))
    finally:
        if fabric is not None:
            await fabric.stop()
        await transport.stop()


async def test_blob_fabric_pull_rejects_disallowed_digest(tmp_path) -> None:
    transport = InMemoryClusterTransport()
    await transport.start()
    fabric = None
    try:
        store = ContentAddressedStore(tmp_path / "cas", allowlist=[_digest(b"approved")])
        fabric = BlobFabric(transport=transport, node_id="node", store=store)
        await fabric.start()

        with pytest.raises(BlobNotAllowed):
            await fabric.pull(_digest(b"unknown"))
    finally:
        if fabric is not None:
            await fabric.stop()
        await transport.stop()


async def test_blob_fabric_configures_channels_with_idempotency() -> None:
    transport = InMemoryClusterTransport()
    await transport.start()
    try:
        await configure_blob_fabric_channels(transport)
        assert (
            transport._channel_config(CLUSTER_BLOBS_CHANNEL).idempotency_key_field == "blob_id"
        )
        assert (
            transport._channel_config(CLUSTER_BLOBS_REQUEST_CHANNEL).idempotency_key_field
            == "request_id"
        )
        assert (
            transport._channel_config(CLUSTER_BLOBS_TRANSFER_CHANNEL).idempotency_key_field
            == "request_id"
        )
    finally:
        await transport.stop()


async def test_blob_fabric_detects_corrupted_transfer(tmp_path) -> None:
    """A holder that lies about the digest must be rejected by SHA verification."""

    transport = InMemoryClusterTransport()
    await transport.start()
    requestor = None
    try:
        await configure_blob_fabric_channels(transport)
        store = ContentAddressedStore(tmp_path / "cas")
        requestor = BlobFabric(
            transport=transport,
            node_id="requestor",
            store=store,
            pull_timeout_seconds=2.0,
        )
        await requestor.start()

        claimed_digest = _digest(b"truth")
        evil_payload = b"lies"

        async def evil_serve(env: FabricEnvelope) -> None:
            payload = dict(env.payload or {})
            if payload.get("target_node_id") != "evil-node":
                return
            transfer = BlobTransfer(
                request_id=str(payload["request_id"]),
                blob_id=claimed_digest,
                holder_node_id="evil-node",
                requestor_node_id=str(payload["requestor_node_id"]),
                payload_b64=base64.b64encode(evil_payload).decode("ascii"),
                size=len(evil_payload),
            )
            await transport.publish(
                FabricEnvelope(
                    channel=CLUSTER_BLOBS_TRANSFER_CHANNEL,
                    payload=transfer.to_dict(),
                )
            )

        evil_sub = await transport.subscribe(
            CLUSTER_BLOBS_REQUEST_CHANNEL, evil_serve, handler_name="evil.serve"
        )
        try:
            announcement = BlobAnnouncement(
                blob_id=claimed_digest,
                holder_node_id="evil-node",
                size=len(b"truth"),
            )
            await transport.publish(
                FabricEnvelope(
                    channel=CLUSTER_BLOBS_CHANNEL, payload=announcement.to_dict()
                )
            )
            for _ in range(5):
                if requestor.known_holders(claimed_digest):
                    break
                await asyncio.sleep(0.05)

            with pytest.raises(BlobIntegrityError):
                await requestor.pull(claimed_digest)
        finally:
            await evil_sub.cancel()
    finally:
        if requestor is not None:
            await requestor.stop()
        await transport.stop()
