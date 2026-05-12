"""ForgeWire — work-graph-aware compute fabric.

Top-level package. The two main entry points users care about are:

* :mod:`forgewire_fabric.hub` — the FastAPI hub server (dispatch, claim, streams,
  results). Run with ``forgewire-fabric hub start`` or ``python -m forgewire_fabric.hub``.
* :mod:`forgewire_fabric.runner` — runner identity + capability discovery helpers
  consumed by an embedding application (e.g. PhrenForge) to register itself
  with a hub. Standalone runners can be started with ``forgewire-fabric runner start``.

Public Python API surface is intentionally small. Everything heavy lives behind
:class:`forgewire_fabric.hub.client.HubClient` (HTTP, formerly ``BlackboardClient``,
which is retained as a one-cycle alias) and the FastAPI app at
:mod:`forgewire_fabric.hub.server`. The Rust acceleration crates are loaded
transparently as ``forgewire_runtime`` when available; pure-Python fallbacks
are always present.

License: Apache-2.0.
"""

from __future__ import annotations

import logging as _logging

__all__ = [
    "__version__",
    "RUNTIME_COMPAT",
    "runtime_version",
    "check_runtime_compat",
]

__version__ = "0.11.2"

# Compat envelope for the ``forgewire-runtime`` (PyO3) wheel. The hub and
# runner check this at import time and log a loud warning if a mismatching
# wheel is on sys.path. Bump RUNTIME_COMPAT whenever the Rust workspace's
# major or minor version moves.
RUNTIME_COMPAT = (">=0.1", "<0.2")


def runtime_version() -> str | None:
    """Return the installed ``forgewire_runtime.__version__`` or ``None``.

    The Rust accelerator is optional — pure-Python fallbacks are always
    present — so a missing wheel is not an error.
    """
    try:
        import forgewire_runtime  # type: ignore[import-not-found]
    except Exception:
        return None
    return getattr(forgewire_runtime, "__version__", None)


def check_runtime_compat() -> tuple[bool, str | None]:
    """Best-effort version compatibility check for ``forgewire_runtime``.

    Returns ``(ok, version)``. ``ok`` is ``True`` when the wheel is absent
    (pure-Python fallback) or its version satisfies :data:`RUNTIME_COMPAT`.
    A mismatch logs a warning and returns ``False`` — callers decide
    whether to continue (hub/runner do, by design, so a stale wheel
    degrades gracefully).
    """
    ver = runtime_version()
    if ver is None:
        return True, None
    # Tiny inline semver check to avoid pulling packaging at import time.
    try:
        parts = tuple(int(p) for p in ver.split(".")[:3])
    except ValueError:
        _logging.getLogger(__name__).warning(
            "forgewire_runtime version %r is not parseable; skipping compat check",
            ver,
        )
        return True, ver
    lo = tuple(int(p) for p in RUNTIME_COMPAT[0].lstrip(">=").split("."))
    hi = tuple(int(p) for p in RUNTIME_COMPAT[1].lstrip("<").split("."))
    # Pad to 3 components.
    parts = (parts + (0, 0, 0))[:3]
    lo = (lo + (0, 0, 0))[:3]
    hi = (hi + (0, 0, 0))[:3]
    ok = lo <= parts < hi
    if not ok:
        _logging.getLogger(__name__).warning(
            "forgewire_runtime %s is outside compat range %s,%s for forgewire-fabric %s; "
            "falling back to pure-Python path",
            ver,
            RUNTIME_COMPAT[0],
            RUNTIME_COMPAT[1],
            __version__,
        )
    return ok, ver
