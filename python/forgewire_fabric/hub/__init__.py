"""ForgeWire hub package.

The hub is a FastAPI service that owns the task graph: signed dispatch,
runner registration, scope-bounded claim, line-streamed task output, and
terminal results. Run it as ``forgewire-fabric hub start`` (or ``python -m
forgewire_fabric.hub``); embed it via :func:`forgewire_fabric.hub.server.create_app`.

Public surface:

* :class:`forgewire_fabric.hub.client.HubClient` — async HTTP client (canonical
  name; ``BlackboardClient`` is the legacy alias kept for one minor cycle).
* :func:`forgewire_fabric.hub.client.load_client_from_env` — convenience loader.
* :mod:`forgewire_fabric.hub.server` — FastAPI app + ``main()`` entry point.
* :mod:`forgewire_fabric.hub.discovery` — optional mDNS advertise/browse.
"""

from forgewire_fabric.hub.client import (
    BlackboardClient,
    HubClient,
    load_client_from_env,
)

__all__ = ["BlackboardClient", "HubClient", "load_client_from_env"]
