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

from forgewire_fabric.cluster.blob_index import (
    CHUNKED_CAS_SCHEMA_VERSION,
    SqliteBlobIndex,
)
from forgewire_fabric.cluster.blobs import (
    DEFAULT_BLOB_PULL_TIMEOUT_SECONDS,
    BlobAnnouncement,
    BlobFabric,
    BlobRequest,
    BlobTransfer,
    configure_blob_fabric_channels,
)
from forgewire_fabric.cluster.blobs_chunked import (
    DEFAULT_CHUNK_SIZE_BYTES,
    DEFAULT_CHUNKED_PULL_TIMEOUT_SECONDS,
    BlobChunk,
    BlobChunkRequest,
    BlobOffer,
    ChunkedBlobFabric,
    configure_chunked_blob_channels,
)
from forgewire_fabric.cluster.cas import (
    CAS_SCHEMA_VERSION,
    DEFAULT_CAS_CAPACITY_BYTES,
    DEFAULT_CAS_NAMESPACE,
    BlobFabricError,
    BlobIntegrityError,
    BlobMetadata,
    BlobNotAllowed,
    BlobUnavailable,
    ContentAddressedStore,
    sha256_hex,
)
from forgewire_fabric.cluster.channels import (
    CLUSTER_BLOBS_CHANNEL,
    CLUSTER_BLOBS_CHUNK_CHANNEL,
    CLUSTER_BLOBS_CHUNK_REQUEST_CHANNEL,
    CLUSTER_BLOBS_OFFER_CHANNEL,
    CLUSTER_BLOBS_REQUEST_CHANNEL,
    CLUSTER_BLOBS_TRANSFER_CHANNEL,
    CLUSTER_COORDINATOR_ELECT_CHANNEL,
    CLUSTER_EMBED_CHANNEL,
    CLUSTER_JOB_CHANNEL_PREFIX,
    CLUSTER_LLM_GENERATE_CHANNEL,
    CLUSTER_LLM_PRELOAD_CHANNEL,
    CLUSTER_LLM_STREAM_CHANNEL,
    CLUSTER_MEMBERSHIP_CHANNEL,
    CLUSTER_OPERATOR_COMMAND_CHANNEL,
    CLUSTER_RESERVED_CHANNELS,
    CLUSTER_RESULTS_CHANNEL,
    CLUSTER_TENSOR_OP_CHANNEL,
    CLUSTER_TOOL_EXEC_CHANNEL,
    SYSTEM_DLQ_CHANNEL,
)
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
    # Channel constants
    "CLUSTER_BLOBS_CHANNEL",
    "CLUSTER_BLOBS_CHUNK_CHANNEL",
    "CLUSTER_BLOBS_CHUNK_REQUEST_CHANNEL",
    "CLUSTER_BLOBS_OFFER_CHANNEL",
    "CLUSTER_BLOBS_REQUEST_CHANNEL",
    "CLUSTER_BLOBS_TRANSFER_CHANNEL",
    "CLUSTER_COORDINATOR_ELECT_CHANNEL",
    "CLUSTER_EMBED_CHANNEL",
    "CLUSTER_JOB_CHANNEL_PREFIX",
    "CLUSTER_LLM_GENERATE_CHANNEL",
    "CLUSTER_LLM_PRELOAD_CHANNEL",
    "CLUSTER_LLM_STREAM_CHANNEL",
    "CLUSTER_MEMBERSHIP_CHANNEL",
    "CLUSTER_OPERATOR_COMMAND_CHANNEL",
    "CLUSTER_RESERVED_CHANNELS",
    "CLUSTER_RESULTS_CHANNEL",
    "CLUSTER_TENSOR_OP_CHANNEL",
    "CLUSTER_TOOL_EXEC_CHANNEL",
    "SYSTEM_DLQ_CHANNEL",
    # CAS
    "CAS_SCHEMA_VERSION",
    "DEFAULT_CAS_CAPACITY_BYTES",
    "DEFAULT_CAS_NAMESPACE",
    "BlobFabricError",
    "BlobIntegrityError",
    "BlobMetadata",
    "BlobNotAllowed",
    "BlobUnavailable",
    "ContentAddressedStore",
    "sha256_hex",
    # SQLite index
    "CHUNKED_CAS_SCHEMA_VERSION",
    "SqliteBlobIndex",
    # Small-blob fabric
    "DEFAULT_BLOB_PULL_TIMEOUT_SECONDS",
    "BlobAnnouncement",
    "BlobFabric",
    "BlobRequest",
    "BlobTransfer",
    "configure_blob_fabric_channels",
    # Chunked fabric
    "DEFAULT_CHUNK_SIZE_BYTES",
    "DEFAULT_CHUNKED_PULL_TIMEOUT_SECONDS",
    "BlobChunk",
    "BlobChunkRequest",
    "BlobOffer",
    "ChunkedBlobFabric",
    "configure_chunked_blob_channels",
    # Transport contract
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
