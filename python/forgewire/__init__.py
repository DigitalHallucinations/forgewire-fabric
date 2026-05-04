"""ForgeWire — work-graph-aware compute fabric.

Top-level package. The two main entry points users care about are:

* :mod:`forgewire.hub` — the FastAPI hub server (dispatch, claim, streams,
  results). Run with ``forgewire hub start`` or ``python -m forgewire.hub``.
* :mod:`forgewire.runner` — runner identity + capability discovery helpers
  consumed by an embedding application (e.g. PhrenForge) to register itself
  with a hub. Standalone runners can be started with ``forgewire runner start``.

Public Python API surface is intentionally small. Everything heavy lives behind
:class:`forgewire.hub.client.HubClient` (HTTP, formerly ``BlackboardClient``,
which is retained as a one-cycle alias) and the FastAPI app at
:mod:`forgewire.hub.server`. The Rust acceleration crates are loaded
transparently as ``forgewire_runtime`` when available; pure-Python fallbacks
are always present.

License: Apache-2.0.
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "0.1.0"
