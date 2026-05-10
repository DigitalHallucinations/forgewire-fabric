"""Small-blob fabric over the transport-neutral cluster substrate.

Single-shot announce / pull / transfer of blobs whose payload fits in one
envelope. For larger blobs see :mod:`forgewire_fabric.cluster.blobs_chunked`.
"""

from __future__ import annotations

import asyncio
import base64
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from .cas import (
    BlobIntegrityError,
    BlobNotAllowed,
    BlobUnavailable,
    CAS_SCHEMA_VERSION,
    ContentAddressedStore,
    DEFAULT_CAS_NAMESPACE,
    sha256_hex,
)
from .channels import (
    CLUSTER_BLOBS_CHANNEL,
    CLUSTER_BLOBS_REQUEST_CHANNEL,
    CLUSTER_BLOBS_TRANSFER_CHANNEL,
    SYSTEM_DLQ_CHANNEL,
)
from .protocol import FabricEnvelope, MessagePriority
from .transport import ClusterTransport, Subscription

DEFAULT_BLOB_PULL_TIMEOUT_SECONDS = 30.0


# ---------------------------------------------------------------------------
# Wire envelopes (payload dataclasses)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class BlobAnnouncement:
    """Holder advertises a locally-cached blob on ``cluster.blobs``."""

    blob_id: str
    holder_node_id: str
    size: int
    namespace: str = DEFAULT_CAS_NAMESPACE
    announced_at: float = field(default_factory=time.time)
    schema_version: int = CAS_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "kind": "announce",
            "blob_id": self.blob_id,
            "holder_node_id": self.holder_node_id,
            "size": self.size,
            "namespace": self.namespace,
            "announced_at": self.announced_at,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BlobAnnouncement":
        return cls(
            schema_version=int(data.get("schema_version") or CAS_SCHEMA_VERSION),
            blob_id=str(data["blob_id"]),
            holder_node_id=str(data["holder_node_id"]),
            size=int(data["size"]),
            namespace=str(data.get("namespace") or DEFAULT_CAS_NAMESPACE),
            announced_at=float(data.get("announced_at") or time.time()),
        )


@dataclass(frozen=True, slots=True)
class BlobTransfer:
    """Inline blob payload returned to a requestor on the transfer channel."""

    request_id: str
    blob_id: str
    holder_node_id: str
    requestor_node_id: str
    payload_b64: str
    size: int
    namespace: str = DEFAULT_CAS_NAMESPACE
    completed_at: float = field(default_factory=time.time)
    schema_version: int = CAS_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "kind": "transfer",
            "request_id": self.request_id,
            "blob_id": self.blob_id,
            "holder_node_id": self.holder_node_id,
            "requestor_node_id": self.requestor_node_id,
            "payload_b64": self.payload_b64,
            "size": self.size,
            "namespace": self.namespace,
            "completed_at": self.completed_at,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BlobTransfer":
        return cls(
            schema_version=int(data.get("schema_version") or CAS_SCHEMA_VERSION),
            request_id=str(data["request_id"]),
            blob_id=str(data["blob_id"]),
            holder_node_id=str(data["holder_node_id"]),
            requestor_node_id=str(data["requestor_node_id"]),
            payload_b64=str(data["payload_b64"]),
            size=int(data["size"]),
            namespace=str(data.get("namespace") or DEFAULT_CAS_NAMESPACE),
            completed_at=float(data.get("completed_at") or time.time()),
        )


@dataclass(frozen=True, slots=True)
class BlobRequest:
    """Pull request published on the request channel."""

    request_id: str
    blob_id: str
    requestor_node_id: str
    target_node_id: str
    requested_at: float = field(default_factory=time.time)
    schema_version: int = CAS_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "kind": "request",
            "request_id": self.request_id,
            "blob_id": self.blob_id,
            "requestor_node_id": self.requestor_node_id,
            "target_node_id": self.target_node_id,
            "requested_at": self.requested_at,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BlobRequest":
        return cls(
            schema_version=int(data.get("schema_version") or CAS_SCHEMA_VERSION),
            request_id=str(data["request_id"]),
            blob_id=str(data["blob_id"]),
            requestor_node_id=str(data["requestor_node_id"]),
            target_node_id=str(data["target_node_id"]),
            requested_at=float(data.get("requested_at") or time.time()),
        )


# ---------------------------------------------------------------------------
# Coordinator-side fabric
# ---------------------------------------------------------------------------


class BlobFabric:
    """Per-node coordinator + holder for the small-blob path.

    Listens on ``cluster.blobs`` for peer announcements (tracking who has
    what), serves incoming pull requests from the local CAS, and provides
    ``announce``/``pull`` primitives for callers.
    """

    def __init__(
        self,
        *,
        transport: ClusterTransport,
        node_id: str,
        store: ContentAddressedStore,
        pull_timeout_seconds: float = DEFAULT_BLOB_PULL_TIMEOUT_SECONDS,
    ):
        self.transport = transport
        self.node_id = node_id
        self.store = store
        self.pull_timeout_seconds = float(pull_timeout_seconds)
        # blob_id -> {holder_node_id -> announcement}
        self._holders: dict[str, dict[str, BlobAnnouncement]] = {}
        self._announce_subscription: Subscription | None = None
        self._transfer_subscription: Subscription | None = None

    async def start(self) -> None:
        await configure_blob_fabric_channels(self.transport)
        self._announce_subscription = await self.transport.subscribe(
            CLUSTER_BLOBS_CHANNEL,
            self._on_announcement,
            handler_name=f"cluster.blobs.announce.{self.node_id}",
            filter_fn=lambda env: dict(env.payload or {}).get("kind") == "announce",
        )
        self._transfer_subscription = await self.transport.subscribe(
            CLUSTER_BLOBS_REQUEST_CHANNEL,
            self._on_transfer_request,
            handler_name=f"cluster.blobs.request.serve.{self.node_id}",
            filter_fn=lambda env: (
                dict(env.payload or {}).get("target_node_id") == self.node_id
            ),
        )

    async def stop(self) -> None:
        if self._announce_subscription is not None:
            await self._announce_subscription.cancel()
            self._announce_subscription = None
        if self._transfer_subscription is not None:
            await self._transfer_subscription.cancel()
            self._transfer_subscription = None

    def known_holders(self, blob_id: str) -> tuple[str, ...]:
        return tuple(self._holders.get(blob_id, {}).keys())

    async def announce(
        self,
        blob_id: str,
        *,
        namespace: str = DEFAULT_CAS_NAMESPACE,
    ) -> BlobAnnouncement:
        meta = self.store.metadata(blob_id)
        if meta is None:
            raise BlobUnavailable(f"local store has no blob {blob_id}")
        announcement = BlobAnnouncement(
            blob_id=blob_id,
            holder_node_id=self.node_id,
            size=meta.size,
            namespace=namespace or meta.namespace,
        )
        await self.transport.publish(
            FabricEnvelope(
                channel=CLUSTER_BLOBS_CHANNEL,
                payload=announcement.to_dict(),
                priority=MessagePriority.NORMAL,
            )
        )
        # Reflect our own announcement locally so single-node loops work.
        self._record_holder(announcement)
        return announcement

    async def pull(
        self,
        blob_id: str,
        *,
        namespace: str = DEFAULT_CAS_NAMESPACE,
        timeout_seconds: float | None = None,
    ) -> bytes:
        if not self.store.is_allowed(blob_id):
            raise BlobNotAllowed(f"digest {blob_id} not in allowlist")
        local = self.store.get_bytes(blob_id)
        if local is not None:
            return local
        holders = [h for h in self.known_holders(blob_id) if h != self.node_id]
        if not holders:
            raise BlobUnavailable(f"no remote holder advertises blob {blob_id}")
        target = holders[0]
        request_id = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        future: asyncio.Future[BlobTransfer] = loop.create_future()

        async def on_transfer(env: FabricEnvelope) -> None:
            transfer = BlobTransfer.from_dict(env.payload)
            if not future.done():
                future.set_result(transfer)

        subscription = await self.transport.subscribe(
            CLUSTER_BLOBS_TRANSFER_CHANNEL,
            on_transfer,
            handler_name=f"cluster.blobs.transfer.recv.{request_id}",
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
                target_node_id=target,
            )
            await self.transport.publish(
                FabricEnvelope(
                    channel=CLUSTER_BLOBS_REQUEST_CHANNEL,
                    payload=request.to_dict(),
                    priority=MessagePriority.HIGH,
                )
            )
            transfer = await asyncio.wait_for(
                future,
                timeout=float(timeout_seconds or self.pull_timeout_seconds),
            )
        finally:
            await subscription.cancel()

        data = base64.b64decode(transfer.payload_b64.encode("ascii"))
        actual_digest = sha256_hex(data)
        if actual_digest != blob_id.lower():
            raise BlobIntegrityError(
                f"digest mismatch: expected {blob_id}, got {actual_digest}"
            )
        if len(data) != transfer.size:
            raise BlobIntegrityError(
                f"size mismatch: expected {transfer.size}, got {len(data)}"
            )
        self.store.put_bytes(data, namespace=namespace or transfer.namespace)
        return data

    async def _on_announcement(self, env: FabricEnvelope) -> None:
        announcement = BlobAnnouncement.from_dict(env.payload)
        self._record_holder(announcement)

    def _record_holder(self, announcement: BlobAnnouncement) -> None:
        bucket = self._holders.setdefault(announcement.blob_id, {})
        bucket[announcement.holder_node_id] = announcement

    async def _on_transfer_request(self, env: FabricEnvelope) -> None:
        request = BlobRequest.from_dict(env.payload)
        data = self.store.get_bytes(request.blob_id)
        if data is None:
            return
        meta = self.store.metadata(request.blob_id)
        namespace = meta.namespace if meta is not None else DEFAULT_CAS_NAMESPACE
        transfer = BlobTransfer(
            request_id=request.request_id,
            blob_id=request.blob_id,
            holder_node_id=self.node_id,
            requestor_node_id=request.requestor_node_id,
            payload_b64=base64.b64encode(data).decode("ascii"),
            size=len(data),
            namespace=namespace,
        )
        await self.transport.publish(
            FabricEnvelope(
                channel=CLUSTER_BLOBS_TRANSFER_CHANNEL,
                payload=transfer.to_dict(),
                priority=MessagePriority.HIGH,
            )
        )


async def configure_blob_fabric_channels(transport: ClusterTransport) -> None:
    await transport.configure_channel(
        CLUSTER_BLOBS_CHANNEL,
        idempotency_key_field="blob_id",
        dead_letter_channel=SYSTEM_DLQ_CHANNEL,
    )
    await transport.configure_channel(
        CLUSTER_BLOBS_REQUEST_CHANNEL,
        idempotency_key_field="request_id",
        dead_letter_channel=SYSTEM_DLQ_CHANNEL,
    )
    await transport.configure_channel(
        CLUSTER_BLOBS_TRANSFER_CHANNEL,
        idempotency_key_field="request_id",
        dead_letter_channel=SYSTEM_DLQ_CHANNEL,
    )


__all__ = [
    "DEFAULT_BLOB_PULL_TIMEOUT_SECONDS",
    "BlobAnnouncement",
    "BlobTransfer",
    "BlobRequest",
    "BlobFabric",
    "configure_blob_fabric_channels",
]
