"""Chunked blob fabric — large-blob path.

Spec-compliant chunked offer/pull protocol over the transport-neutral
:class:`~forgewire_fabric.cluster.transport.ClusterTransport`. Each chunk
is independently SHA-256 verified and the assembled whole-blob digest is
verified before the body is committed to the local CAS.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from .blob_index import CHUNKED_CAS_SCHEMA_VERSION, SqliteBlobIndex
from .blobs import BlobRequest
from .cas import (
    BlobIntegrityError,
    BlobUnavailable,
    ContentAddressedStore,
    DEFAULT_CAS_NAMESPACE,
)
from .channels import (
    CLUSTER_BLOBS_CHUNK_CHANNEL,
    CLUSTER_BLOBS_CHUNK_REQUEST_CHANNEL,
    CLUSTER_BLOBS_OFFER_CHANNEL,
    CLUSTER_BLOBS_REQUEST_CHANNEL,
    SYSTEM_DLQ_CHANNEL,
)
from .protocol import FabricEnvelope, MessagePriority, composite_envelope_id
from .transport import ClusterTransport, Subscription

DEFAULT_CHUNK_SIZE_BYTES = 1024 * 1024  # 1 MiB chunks
DEFAULT_CHUNKED_PULL_TIMEOUT_SECONDS = 60.0


# ---------------------------------------------------------------------------
# Chunked wire envelopes
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class BlobOffer:
    """Holder advertises that it can serve a blob in chunks."""

    blob_id: str
    holder_node_id: str
    requestor_node_id: str
    request_id: str
    size: int
    chunk_size: int
    chunk_count: int
    namespace: str = DEFAULT_CAS_NAMESPACE
    offered_at: float = field(default_factory=time.time)
    schema_version: int = CHUNKED_CAS_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "kind": "offer",
            "blob_id": self.blob_id,
            "holder_node_id": self.holder_node_id,
            "requestor_node_id": self.requestor_node_id,
            "request_id": self.request_id,
            "size": self.size,
            "chunk_size": self.chunk_size,
            "chunk_count": self.chunk_count,
            "namespace": self.namespace,
            "offered_at": self.offered_at,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BlobOffer":
        return cls(
            schema_version=int(data.get("schema_version") or CHUNKED_CAS_SCHEMA_VERSION),
            blob_id=str(data["blob_id"]),
            holder_node_id=str(data["holder_node_id"]),
            requestor_node_id=str(data["requestor_node_id"]),
            request_id=str(data["request_id"]),
            size=int(data["size"]),
            chunk_size=int(data["chunk_size"]),
            chunk_count=int(data["chunk_count"]),
            namespace=str(data.get("namespace") or DEFAULT_CAS_NAMESPACE),
            offered_at=float(data.get("offered_at") or time.time()),
        )


@dataclass(frozen=True, slots=True)
class BlobChunkRequest:
    """Requestor pulls one chunk."""

    request_id: str
    blob_id: str
    chunk_index: int
    requestor_node_id: str
    target_node_id: str
    requested_at: float = field(default_factory=time.time)
    schema_version: int = CHUNKED_CAS_SCHEMA_VERSION

    @property
    def envelope_id(self) -> str:
        return composite_envelope_id(self.request_id, self.chunk_index)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "kind": "chunk_request",
            "request_id": self.request_id,
            "envelope_id": self.envelope_id,
            "blob_id": self.blob_id,
            "chunk_index": self.chunk_index,
            "requestor_node_id": self.requestor_node_id,
            "target_node_id": self.target_node_id,
            "requested_at": self.requested_at,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BlobChunkRequest":
        return cls(
            schema_version=int(data.get("schema_version") or CHUNKED_CAS_SCHEMA_VERSION),
            request_id=str(data["request_id"]),
            blob_id=str(data["blob_id"]),
            chunk_index=int(data["chunk_index"]),
            requestor_node_id=str(data["requestor_node_id"]),
            target_node_id=str(data["target_node_id"]),
            requested_at=float(data.get("requested_at") or time.time()),
        )


@dataclass(frozen=True, slots=True)
class BlobChunk:
    """One chunk of a blob, hashed independently."""

    request_id: str
    blob_id: str
    chunk_index: int
    chunk_count: int
    payload_b64: str
    chunk_digest: str
    holder_node_id: str
    requestor_node_id: str
    completed_at: float = field(default_factory=time.time)
    schema_version: int = CHUNKED_CAS_SCHEMA_VERSION

    @property
    def envelope_id(self) -> str:
        return composite_envelope_id(self.request_id, self.chunk_index)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "kind": "chunk",
            "request_id": self.request_id,
            "envelope_id": self.envelope_id,
            "blob_id": self.blob_id,
            "chunk_index": self.chunk_index,
            "chunk_count": self.chunk_count,
            "payload_b64": self.payload_b64,
            "chunk_digest": self.chunk_digest,
            "holder_node_id": self.holder_node_id,
            "requestor_node_id": self.requestor_node_id,
            "completed_at": self.completed_at,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BlobChunk":
        return cls(
            schema_version=int(data.get("schema_version") or CHUNKED_CAS_SCHEMA_VERSION),
            request_id=str(data["request_id"]),
            blob_id=str(data["blob_id"]),
            chunk_index=int(data["chunk_index"]),
            chunk_count=int(data["chunk_count"]),
            payload_b64=str(data["payload_b64"]),
            chunk_digest=str(data["chunk_digest"]),
            holder_node_id=str(data["holder_node_id"]),
            requestor_node_id=str(data["requestor_node_id"]),
            completed_at=float(data.get("completed_at") or time.time()),
        )


# ---------------------------------------------------------------------------
# Chunked fabric
# ---------------------------------------------------------------------------


class ChunkedBlobFabric:
    """Spec-compliant chunked blob transport.

    Acts as both holder (serves offers + chunks for blobs in the local
    store) and requestor (pulls a missing blob by digest).
    """

    def __init__(
        self,
        *,
        transport: ClusterTransport,
        node_id: str,
        store: ContentAddressedStore,
        index: SqliteBlobIndex | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE_BYTES,
        pull_timeout_seconds: float = DEFAULT_CHUNKED_PULL_TIMEOUT_SECONDS,
    ):
        if chunk_size <= 0:
            raise ValueError("chunk_size must be > 0")
        self.transport = transport
        self.node_id = node_id
        self.store = store
        self.index = index
        self.chunk_size = int(chunk_size)
        self.pull_timeout_seconds = float(pull_timeout_seconds)
        self._request_subscription: Subscription | None = None
        self._chunk_request_subscription: Subscription | None = None

    async def start(self) -> None:
        await configure_chunked_blob_channels(self.transport)
        self._request_subscription = await self.transport.subscribe(
            CLUSTER_BLOBS_REQUEST_CHANNEL,
            self._on_pull_request,
            handler_name=f"cluster.blobs.offer.{self.node_id}",
            filter_fn=lambda env: (
                dict(env.payload or {}).get("target_node_id") == self.node_id
                and dict(env.payload or {}).get("kind") == "request"
            ),
        )
        self._chunk_request_subscription = await self.transport.subscribe(
            CLUSTER_BLOBS_CHUNK_REQUEST_CHANNEL,
            self._on_chunk_request,
            handler_name=f"cluster.blobs.chunk.serve.{self.node_id}",
            filter_fn=lambda env: (
                dict(env.payload or {}).get("target_node_id") == self.node_id
            ),
        )

    async def stop(self) -> None:
        if self._request_subscription is not None:
            await self._request_subscription.cancel()
            self._request_subscription = None
        if self._chunk_request_subscription is not None:
            await self._chunk_request_subscription.cancel()
            self._chunk_request_subscription = None

    async def pull_chunked(
        self,
        blob_id: str,
        target_node_id: str,
        *,
        namespace: str = DEFAULT_CAS_NAMESPACE,
        timeout_seconds: float | None = None,
    ) -> bytes:
        """Pull ``blob_id`` from ``target_node_id`` over the chunked
        protocol, verifying every chunk and the assembled whole."""

        if not self.store.is_allowed(blob_id):
            raise BlobUnavailable(f"digest {blob_id} not in allowlist")
        deadline = time.monotonic() + float(
            timeout_seconds or self.pull_timeout_seconds
        )

        await configure_chunked_blob_channels(self.transport)
        request_id = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        offer_future: asyncio.Future[BlobOffer] = loop.create_future()

        async def on_offer(env: FabricEnvelope) -> None:
            offer = BlobOffer.from_dict(env.payload)
            if not offer_future.done():
                offer_future.set_result(offer)

        offer_sub = await self.transport.subscribe(
            CLUSTER_BLOBS_OFFER_CHANNEL,
            on_offer,
            handler_name=f"cluster.blobs.offer.recv.{request_id}",
            filter_fn=lambda env: (
                dict(env.payload or {}).get("request_id") == request_id
                and dict(env.payload or {}).get("requestor_node_id") == self.node_id
            ),
        )
        try:
            request = BlobRequest(
                request_id=request_id,
                blob_id=blob_id,
                requestor_node_id=self.node_id,
                target_node_id=target_node_id,
            )
            await self.transport.publish(
                FabricEnvelope(
                    channel=CLUSTER_BLOBS_REQUEST_CHANNEL,
                    payload=request.to_dict(),
                    priority=MessagePriority.HIGH,
                )
            )
            offer = await asyncio.wait_for(
                offer_future,
                timeout=max(0.0, deadline - time.monotonic()),
            )
        finally:
            await offer_sub.cancel()

        if offer.chunk_count <= 0:
            raise BlobIntegrityError(
                f"holder offered zero chunks for {blob_id!r}"
            )

        chunks: dict[int, bytes] = {}
        for index in range(offer.chunk_count):
            chunk = await self._pull_one_chunk(
                blob_id=blob_id,
                request_id=request_id,
                target_node_id=offer.holder_node_id,
                index=index,
                deadline=deadline,
            )
            chunk_bytes = base64.b64decode(chunk.payload_b64.encode("ascii"))
            actual_chunk_digest = hashlib.sha256(chunk_bytes).hexdigest()
            if actual_chunk_digest != chunk.chunk_digest.lower():
                raise BlobIntegrityError(
                    f"chunk {index} digest mismatch for {blob_id!r}: "
                    f"expected {chunk.chunk_digest}, got {actual_chunk_digest}"
                )
            chunks[index] = chunk_bytes

        assembled = b"".join(chunks[i] for i in range(offer.chunk_count))
        actual_blob_digest = hashlib.sha256(assembled).hexdigest()
        if actual_blob_digest != blob_id.lower():
            raise BlobIntegrityError(
                f"assembled digest mismatch: expected {blob_id}, got {actual_blob_digest}"
            )
        if len(assembled) != offer.size:
            raise BlobIntegrityError(
                f"assembled size mismatch: expected {offer.size}, got {len(assembled)}"
            )
        self.store.put_bytes(assembled, namespace=namespace or offer.namespace)
        if self.index is not None:
            self.index.upsert(
                digest=blob_id,
                size=len(assembled),
                namespace=namespace or offer.namespace,
            )
        return assembled

    async def _pull_one_chunk(
        self,
        *,
        blob_id: str,
        request_id: str,
        target_node_id: str,
        index: int,
        deadline: float,
    ) -> BlobChunk:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[BlobChunk] = loop.create_future()

        async def on_chunk(env: FabricEnvelope) -> None:
            chunk = BlobChunk.from_dict(env.payload)
            if not future.done():
                future.set_result(chunk)

        composite_id = composite_envelope_id(request_id, index)
        sub = await self.transport.subscribe(
            CLUSTER_BLOBS_CHUNK_CHANNEL,
            on_chunk,
            handler_name=f"cluster.blobs.chunk.recv.{request_id}.{index}",
            filter_fn=lambda env: (
                dict(env.payload or {}).get("envelope_id") == composite_id
                and dict(env.payload or {}).get("requestor_node_id") == self.node_id
            ),
        )
        try:
            chunk_req = BlobChunkRequest(
                request_id=request_id,
                blob_id=blob_id,
                chunk_index=index,
                requestor_node_id=self.node_id,
                target_node_id=target_node_id,
            )
            await self.transport.publish(
                FabricEnvelope(
                    channel=CLUSTER_BLOBS_CHUNK_REQUEST_CHANNEL,
                    payload=chunk_req.to_dict(),
                    priority=MessagePriority.HIGH,
                )
            )
            return await asyncio.wait_for(
                future,
                timeout=max(0.0, deadline - time.monotonic()),
            )
        finally:
            await sub.cancel()

    async def _on_pull_request(self, env: FabricEnvelope) -> None:
        request = BlobRequest.from_dict(env.payload)
        data = self.store.get_bytes(request.blob_id)
        if data is None:
            return
        meta = self.store.metadata(request.blob_id)
        namespace = meta.namespace if meta is not None else DEFAULT_CAS_NAMESPACE
        chunk_count = max(1, (len(data) + self.chunk_size - 1) // self.chunk_size)
        offer = BlobOffer(
            blob_id=request.blob_id,
            holder_node_id=self.node_id,
            requestor_node_id=request.requestor_node_id,
            request_id=request.request_id,
            size=len(data),
            chunk_size=self.chunk_size,
            chunk_count=chunk_count,
            namespace=namespace,
        )
        await self.transport.publish(
            FabricEnvelope(
                channel=CLUSTER_BLOBS_OFFER_CHANNEL,
                payload=offer.to_dict(),
                priority=MessagePriority.NORMAL,
            )
        )

    async def _on_chunk_request(self, env: FabricEnvelope) -> None:
        chunk_req = BlobChunkRequest.from_dict(env.payload)
        data = self.store.get_bytes(chunk_req.blob_id)
        if data is None:
            return
        chunk_count = max(1, (len(data) + self.chunk_size - 1) // self.chunk_size)
        if chunk_req.chunk_index >= chunk_count:
            return
        start = chunk_req.chunk_index * self.chunk_size
        end = min(start + self.chunk_size, len(data))
        chunk_bytes = data[start:end]
        chunk = BlobChunk(
            request_id=chunk_req.request_id,
            blob_id=chunk_req.blob_id,
            chunk_index=chunk_req.chunk_index,
            chunk_count=chunk_count,
            payload_b64=base64.b64encode(chunk_bytes).decode("ascii"),
            chunk_digest=hashlib.sha256(chunk_bytes).hexdigest(),
            holder_node_id=self.node_id,
            requestor_node_id=chunk_req.requestor_node_id,
        )
        await self.transport.publish(
            FabricEnvelope(
                channel=CLUSTER_BLOBS_CHUNK_CHANNEL,
                payload=chunk.to_dict(),
                priority=MessagePriority.HIGH,
            )
        )


async def configure_chunked_blob_channels(transport: ClusterTransport) -> None:
    await transport.configure_channel(
        CLUSTER_BLOBS_REQUEST_CHANNEL,
        idempotency_key_field="request_id",
        dead_letter_channel=SYSTEM_DLQ_CHANNEL,
    )
    await transport.configure_channel(
        CLUSTER_BLOBS_OFFER_CHANNEL,
        idempotency_key_field="request_id",
        dead_letter_channel=SYSTEM_DLQ_CHANNEL,
    )
    await transport.configure_channel(
        CLUSTER_BLOBS_CHUNK_REQUEST_CHANNEL,
        idempotency_key_field="envelope_id",
        dead_letter_channel=SYSTEM_DLQ_CHANNEL,
    )
    await transport.configure_channel(
        CLUSTER_BLOBS_CHUNK_CHANNEL,
        idempotency_key_field="envelope_id",
        dead_letter_channel=SYSTEM_DLQ_CHANNEL,
    )


__all__ = [
    "DEFAULT_CHUNK_SIZE_BYTES",
    "DEFAULT_CHUNKED_PULL_TIMEOUT_SECONDS",
    "BlobOffer",
    "BlobChunkRequest",
    "BlobChunk",
    "ChunkedBlobFabric",
    "configure_chunked_blob_channels",
]
