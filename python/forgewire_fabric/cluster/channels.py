"""Canonical ``cluster.*`` channel-name constants.

This module is the **single source of truth** for cluster channel names across
both the standalone ``forgewire-fabric`` repo and the internal ForgeWire
application repo. The internal repo re-exports these names from
``core/messaging/messages.py``; do not duplicate the string literals.
"""

from __future__ import annotations

# System / operational
SYSTEM_DLQ_CHANNEL = "system.dlq"

# Membership / coordination
CLUSTER_MEMBERSHIP_CHANNEL = "cluster.membership"
CLUSTER_COORDINATOR_ELECT_CHANNEL = "cluster.coordinator.elect"
CLUSTER_RESULTS_CHANNEL = "cluster.results"
CLUSTER_OPERATOR_COMMAND_CHANNEL = "cluster.operator.command"

# Job channels
CLUSTER_JOB_CHANNEL_PREFIX = "cluster.jobs."
CLUSTER_TENSOR_OP_CHANNEL = f"{CLUSTER_JOB_CHANNEL_PREFIX}tensor.op"
CLUSTER_EMBED_CHANNEL = f"{CLUSTER_JOB_CHANNEL_PREFIX}embed"
CLUSTER_LLM_GENERATE_CHANNEL = f"{CLUSTER_JOB_CHANNEL_PREFIX}llm.generate"
CLUSTER_LLM_PRELOAD_CHANNEL = f"{CLUSTER_JOB_CHANNEL_PREFIX}llm.preload"
CLUSTER_TOOL_EXEC_CHANNEL = f"{CLUSTER_JOB_CHANNEL_PREFIX}tool.exec"
CLUSTER_LLM_STREAM_CHANNEL = "cluster.llm.stream"

# Blob fabric (small-blob path — single-shot transfer)
CLUSTER_BLOBS_CHANNEL = "cluster.blobs"
CLUSTER_BLOBS_REQUEST_CHANNEL = "cluster.blobs.request"
CLUSTER_BLOBS_TRANSFER_CHANNEL = "cluster.blobs.transfer"

# Blob fabric (chunked path — large-blob path)
CLUSTER_BLOBS_OFFER_CHANNEL = "cluster.blobs.offer"
CLUSTER_BLOBS_CHUNK_REQUEST_CHANNEL = "cluster.blobs.chunk.request"
CLUSTER_BLOBS_CHUNK_CHANNEL = "cluster.blobs.chunk"

CLUSTER_RESERVED_CHANNELS: tuple[str, ...] = (
    CLUSTER_MEMBERSHIP_CHANNEL,
    CLUSTER_COORDINATOR_ELECT_CHANNEL,
    CLUSTER_RESULTS_CHANNEL,
    CLUSTER_BLOBS_CHANNEL,
    CLUSTER_BLOBS_REQUEST_CHANNEL,
    CLUSTER_BLOBS_TRANSFER_CHANNEL,
    CLUSTER_BLOBS_OFFER_CHANNEL,
    CLUSTER_BLOBS_CHUNK_REQUEST_CHANNEL,
    CLUSTER_BLOBS_CHUNK_CHANNEL,
    CLUSTER_OPERATOR_COMMAND_CHANNEL,
    CLUSTER_LLM_STREAM_CHANNEL,
    CLUSTER_JOB_CHANNEL_PREFIX,
)

__all__ = [
    "SYSTEM_DLQ_CHANNEL",
    "CLUSTER_MEMBERSHIP_CHANNEL",
    "CLUSTER_COORDINATOR_ELECT_CHANNEL",
    "CLUSTER_RESULTS_CHANNEL",
    "CLUSTER_OPERATOR_COMMAND_CHANNEL",
    "CLUSTER_JOB_CHANNEL_PREFIX",
    "CLUSTER_TENSOR_OP_CHANNEL",
    "CLUSTER_EMBED_CHANNEL",
    "CLUSTER_LLM_GENERATE_CHANNEL",
    "CLUSTER_LLM_PRELOAD_CHANNEL",
    "CLUSTER_TOOL_EXEC_CHANNEL",
    "CLUSTER_LLM_STREAM_CHANNEL",
    "CLUSTER_BLOBS_CHANNEL",
    "CLUSTER_BLOBS_REQUEST_CHANNEL",
    "CLUSTER_BLOBS_TRANSFER_CHANNEL",
    "CLUSTER_BLOBS_OFFER_CHANNEL",
    "CLUSTER_BLOBS_CHUNK_REQUEST_CHANNEL",
    "CLUSTER_BLOBS_CHUNK_CHANNEL",
    "CLUSTER_RESERVED_CHANNELS",
]
