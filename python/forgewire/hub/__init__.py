"""ForgeWire hub package.

The hub is a FastAPI service that owns the task graph: signed dispatch,
runner registration, scope-bounded claim, line-streamed task output, and
terminal results. Run it as ``forgewire hub start`` (or ``python -m
forgewire.hub``); embed it via :func:`forgewire.hub.server.create_app`.

Public surface:

* :class:`forgewire.hub.client.HubClient` — async HTTP client (canonical
  name; ``BlackboardClient`` is the legacy alias kept for one minor cycle).
* :func:`forgewire.hub.client.load_client_from_env` — convenience loader.
* :mod:`forgewire.hub.server` — FastAPI app + ``main()`` entry point.
* :mod:`forgewire.hub.discovery` — optional mDNS advertise/browse.
"""

from forgewire.hub.client import (
    BlackboardClient,
    HubClient,
    load_client_from_env,
)

__all__ = ["BlackboardClient", "HubClient", "load_client_from_env"]
