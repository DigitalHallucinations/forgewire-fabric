"""Optional mDNS / Zeroconf helpers for hub advertisement & dispatcher discovery.

The ``zeroconf`` package is an optional dependency. When it is unavailable we
degrade gracefully -- the hub simply skips advertisement and the dispatcher
falls back to ``BLACKBOARD_URL`` (or the configured default).

Service type: ``_phrenforge-hub._tcp.local.`` -- distinct from the legacy
``_phrenforge-runner._tcp.local`` brainstormed in todo 23 because the hub is
the always-on control plane; runners reach *it*, not the other way around.

TXT record fields:

* ``proto``  -- ``hub_protocol_version`` (e.g. ``2``)
* ``token``  -- short token preview (last 8 hex chars) for human verification
                only; the real auth is the bearer token shipped via SCP.
* ``path``   -- always ``/`` (reserved for future REST prefixes).
"""

from __future__ import annotations

import logging
import socket
from dataclasses import dataclass
from typing import Any

LOGGER = logging.getLogger("phrenforge.remote.discovery")

SERVICE_TYPE = "_phrenforge-hub._tcp.local."


@dataclass(slots=True)
class HubAdvertisement:
    """Handle returned by :func:`advertise_hub`. Call ``close`` on shutdown."""

    _zeroconf: Any
    _info: Any

    def close(self) -> None:
        try:
            self._zeroconf.unregister_service(self._info)
        except Exception as exc:  # pragma: no cover - shutdown best-effort
            LOGGER.debug("zeroconf unregister failed: %s", exc)
        try:
            self._zeroconf.close()
        except Exception as exc:  # pragma: no cover
            LOGGER.debug("zeroconf close failed: %s", exc)


def _local_ipv4() -> str:
    """Best-effort local LAN IPv4 address (no network traffic generated)."""

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.settimeout(0.0)
        # 8.8.8.8 is a sentinel; UDP socket connect() does not actually send.
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


def advertise_hub(
    *,
    port: int,
    protocol_version: int,
    token_preview: str = "",
    instance_name: str | None = None,
) -> HubAdvertisement | None:
    """Advertise the running hub on the local LAN via mDNS.

    Returns ``None`` if ``zeroconf`` is not installed (logs a debug message).
    Otherwise returns a handle whose ``close()`` should be called on shutdown.
    """

    try:
        from zeroconf import IPVersion, ServiceInfo, Zeroconf
    except ImportError:
        LOGGER.debug("zeroconf not installed; skipping hub mDNS advertisement")
        return None

    hostname = socket.gethostname().split(".", 1)[0] or "phrenforge-hub"
    name = instance_name or f"{hostname}.{SERVICE_TYPE}"
    address = _local_ipv4()
    properties = {
        "proto": str(protocol_version),
        "token": token_preview,
        "path": "/",
    }
    try:
        info = ServiceInfo(
            type_=SERVICE_TYPE,
            name=name,
            addresses=[socket.inet_aton(address)],
            port=port,
            properties=properties,
            server=f"{hostname}.local.",
        )
        zc = Zeroconf(ip_version=IPVersion.V4Only)
        zc.register_service(info)
        LOGGER.info(
            "advertising %s on %s:%d via mDNS", SERVICE_TYPE.rstrip("."), address, port
        )
        return HubAdvertisement(_zeroconf=zc, _info=info)
    except Exception as exc:
        LOGGER.warning("mDNS advertisement failed: %s", exc)
        return None


def discover_hubs(timeout: float = 3.0) -> list[dict[str, Any]]:
    """Browse the local LAN for ``_phrenforge-hub._tcp`` services.

    Returns a list of ``{host, port, protocol_version, addresses, name}`` dicts.
    Empty list if zeroconf is missing or no hubs answer in ``timeout`` seconds.
    """

    try:
        from zeroconf import IPVersion, ServiceBrowser, Zeroconf
    except ImportError:
        LOGGER.debug("zeroconf not installed; cannot discover hubs")
        return []

    found: list[dict[str, Any]] = []

    class _Listener:
        def add_service(self, zc: Any, type_: str, name: str) -> None:  # noqa: D401
            info = zc.get_service_info(type_, name, timeout=int(timeout * 1000))
            if not info:
                return
            addresses = [socket.inet_ntoa(a) for a in info.addresses or [] if len(a) == 4]
            host = addresses[0] if addresses else (info.server or "").rstrip(".")
            props = {
                k.decode("ascii", "ignore") if isinstance(k, bytes) else k: (
                    v.decode("ascii", "ignore") if isinstance(v, bytes) else v
                )
                for k, v in (info.properties or {}).items()
            }
            try:
                proto = int(props.get("proto", "0"))
            except ValueError:
                proto = 0
            found.append(
                {
                    "host": host,
                    "port": info.port,
                    "protocol_version": proto,
                    "addresses": addresses,
                    "name": name,
                    "token_preview": props.get("token", ""),
                }
            )

        def update_service(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def remove_service(self, *_args: Any, **_kwargs: Any) -> None:
            pass

    import time as _time

    zc = Zeroconf(ip_version=IPVersion.V4Only)
    try:
        ServiceBrowser(zc, SERVICE_TYPE, listener=_Listener())
        _time.sleep(timeout)
    finally:
        zc.close()
    return found
