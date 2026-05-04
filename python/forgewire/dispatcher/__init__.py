"""Dispatcher-side identity and signing helpers.

A "dispatcher" is anything that hands a sealed brief to the ForgeWire hub:
a CLI user, an MCP client, the VS Code extension host, an automation
script. Dispatchers own an ed25519 keypair (see
:mod:`forgewire.dispatcher.identity`) and may register that key with the
hub to send signed dispatches via ``POST /tasks/v2``.
"""

from forgewire.dispatcher.identity import (
    DEFAULT_IDENTITY_PATH,
    DispatcherIdentity,
    load_or_create,
)

__all__ = [
    "DEFAULT_IDENTITY_PATH",
    "DispatcherIdentity",
    "load_or_create",
]
