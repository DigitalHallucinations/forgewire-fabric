"""Tests for the chunked blob fabric over the in-memory cluster transport."""

from __future__ import annotations

import base64
import hashlib
from pathlib import Path

import pytest

from forgewire_fabric.cluster import (
    BlobChunk,
    BlobChunkRequest,
    BlobIntegrityError,
    BlobOffer,
    CLUSTER_BLOBS_CHUNK_CHANNEL,
    CLUSTER_BLOBS_CHUNK_REQUEST_CHANNEL,
    CLUSTER_BLOBS_OFFER_CHANNEL,
    CLUSTER_BLOBS_REQUEST_CHANNEL,
    ChunkedBlobFabric,
    ContentAddressedStore,
    FabricEnvelope,
    InMemoryClusterTransport,
    SqliteBlobIndex,
    configure_chunked_blob_channels,
)


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# SqliteBlobIndex
# ---------------------------------------------------------------------------


async def test_sqlite_index_round_trip(tmp_path: Path) -> None:
    index = SqliteBlobIndex(tmp_path / "cas.sqlite3")
    try:
        index.upsert(digest="abc123", size=42, namespace="weights")
        record = index.get("abc123")
        assert record is not None
        assert record["size"] == 42
        assert record["namespace"] == "weights"
        assert record["schema_version"] == 1
        assert "abc123" in index.list_digests()
        assert index.total_size() == 42
    finally:
        index.close()


async def test_sqlite_index_persists_across_reopens(tmp_path: Path) -> None:
    db_path = tmp_path / "cas.sqlite3"
    index = SqliteBlobIndex(db_path)
    index.upsert(digest="deadbeef", size=128, namespace="lora")
    index.close()

    reopened = SqliteBlobIndex(db_path)
    try:
        record = reopened.get("deadbeef")
        assert record is not None
        assert record["size"] == 128
        assert record["namespace"] == "lora"
    finally:
        reopened.close()


async def test_sqlite_index_least_recently_accessed_order(tmp_path: Path) -> None:
    index = SqliteBlobIndex(tmp_path / "cas.sqlite3")
    try:
        index.upsert(digest="aaa", size=1, last_accessed_at=100.0)
        index.upsert(digest="bbb", size=2, last_accessed_at=50.0)
        index.upsert(digest="ccc", size=3, last_accessed_at=200.0)
        order = list(index.least_recently_accessed())
        assert [d for d, _ in order] == ["bbb", "aaa", "ccc"]
    finally:
        index.close()


# ---------------------------------------------------------------------------
# ChunkedBlobFabric
# ---------------------------------------------------------------------------


async def test_chunked_pull_round_trip_multiple_chunks(tmp_path: Path) -> None:
    transport = InMemoryClusterTransport()
    await transport.start()
    holder = requestor = None
    try:
        holder_store = ContentAddressedStore(tmp_path / "holder-cas")
        requestor_store = ContentAddressedStore(tmp_path / "requestor-cas")

        # 7 chunks at chunk_size=10
        payload = b"".join(bytes([i] * 10) for i in range(7))
        assert len(payload) == 70

        digest = holder_store.put_bytes(payload, namespace="weights")

        holder = ChunkedBlobFabric(
            transport=transport,
            node_id="holder",
            store=holder_store,
            chunk_size=10,
        )
        requestor_index = SqliteBlobIndex(tmp_path / "cas.sqlite3")
        requestor = ChunkedBlobFabric(
            transport=transport,
            node_id="requestor",
            store=requestor_store,
            index=requestor_index,
            chunk_size=10,
            pull_timeout_seconds=5.0,
        )
        await holder.start()
        await requestor.start()

        fetched = await requestor.pull_chunked(
            digest,
            target_node_id="holder",
            namespace="weights",
        )
        assert fetched == payload
        assert requestor_store.get_bytes(digest) == payload

        record = requestor_index.get(digest)
        assert record is not None
        assert record["size"] == 70
        assert record["namespace"] == "weights"
        requestor_index.close()
    finally:
        if holder is not None:
            await holder.stop()
        if requestor is not None:
            await requestor.stop()
        await transport.stop()


async def test_chunked_pull_single_chunk_path(tmp_path: Path) -> None:
    transport = InMemoryClusterTransport()
    await transport.start()
    holder = requestor = None
    try:
        holder_store = ContentAddressedStore(tmp_path / "holder-cas")
        requestor_store = ContentAddressedStore(tmp_path / "requestor-cas")

        payload = b"small"
        digest = holder_store.put_bytes(payload)

        holder = ChunkedBlobFabric(
            transport=transport, node_id="holder", store=holder_store, chunk_size=64
        )
        requestor = ChunkedBlobFabric(
            transport=transport,
            node_id="requestor",
            store=requestor_store,
            chunk_size=64,
            pull_timeout_seconds=5.0,
        )
        await holder.start()
        await requestor.start()

        fetched = await requestor.pull_chunked(digest, target_node_id="holder")
        assert fetched == payload
    finally:
        if holder is not None:
            await holder.stop()
        if requestor is not None:
            await requestor.stop()
        await transport.stop()


async def test_chunked_channels_have_composite_idempotency() -> None:
    transport = InMemoryClusterTransport()
    await transport.start()
    try:
        await configure_chunked_blob_channels(transport)
        assert (
            transport._channel_config(CLUSTER_BLOBS_REQUEST_CHANNEL).idempotency_key_field
            == "request_id"
        )
        assert (
            transport._channel_config(CLUSTER_BLOBS_OFFER_CHANNEL).idempotency_key_field
            == "request_id"
        )
        assert (
            transport._channel_config(
                CLUSTER_BLOBS_CHUNK_REQUEST_CHANNEL
            ).idempotency_key_field
            == "envelope_id"
        )
        assert (
            transport._channel_config(CLUSTER_BLOBS_CHUNK_CHANNEL).idempotency_key_field
            == "envelope_id"
        )
    finally:
        await transport.stop()


async def test_chunked_pull_detects_corrupted_chunk(tmp_path: Path) -> None:
    """A malicious holder that returns mismatched chunk bytes is rejected."""

    transport = InMemoryClusterTransport()
    await transport.start()
    requestor = None
    try:
        requestor_store = ContentAddressedStore(tmp_path / "requestor-cas")
        requestor = ChunkedBlobFabric(
            transport=transport,
            node_id="requestor",
            store=requestor_store,
            chunk_size=4,
            pull_timeout_seconds=2.0,
        )
        await requestor.start()

        truth = b"truthtruth"  # 10 bytes
        claimed_digest = _digest(truth)

        async def serve_offer(env: FabricEnvelope) -> None:
            payload = dict(env.payload or {})
            if payload.get("target_node_id") != "evil-node":
                return
            offer = BlobOffer(
                blob_id=claimed_digest,
                holder_node_id="evil-node",
                requestor_node_id=str(payload["requestor_node_id"]),
                request_id=str(payload["request_id"]),
                size=10,
                chunk_size=4,
                chunk_count=3,
            )
            await transport.publish(
                FabricEnvelope(channel=CLUSTER_BLOBS_OFFER_CHANNEL, payload=offer.to_dict())
            )

        async def serve_chunk(env: FabricEnvelope) -> None:
            payload = dict(env.payload or {})
            if payload.get("target_node_id") != "evil-node":
                return
            chunk_req = BlobChunkRequest.from_dict(payload)
            evil_bytes = b"EVIL"[: 4 if chunk_req.chunk_index < 2 else 2]
            chunk = BlobChunk(
                request_id=chunk_req.request_id,
                blob_id=claimed_digest,
                chunk_index=chunk_req.chunk_index,
                chunk_count=3,
                payload_b64=base64.b64encode(evil_bytes).decode("ascii"),
                # Mismatched: digest claims something else
                chunk_digest=hashlib.sha256(b"good").hexdigest(),
                holder_node_id="evil-node",
                requestor_node_id=chunk_req.requestor_node_id,
            )
            await transport.publish(
                FabricEnvelope(channel=CLUSTER_BLOBS_CHUNK_CHANNEL, payload=chunk.to_dict())
            )

        offer_sub = await transport.subscribe(
            CLUSTER_BLOBS_REQUEST_CHANNEL, serve_offer, handler_name="evil.offer"
        )
        chunk_sub = await transport.subscribe(
            CLUSTER_BLOBS_CHUNK_REQUEST_CHANNEL, serve_chunk, handler_name="evil.chunk"
        )
        try:
            with pytest.raises(BlobIntegrityError):
                await requestor.pull_chunked(claimed_digest, target_node_id="evil-node")
        finally:
            await offer_sub.cancel()
            await chunk_sub.cancel()
    finally:
        if requestor is not None:
            await requestor.stop()
        await transport.stop()
