"""Runtime probes used by the runner to populate registration + heartbeat
payloads.

Kept pure-stdlib so the runner does not gain new dependencies.

* :func:`describe_host` collects a static capability snapshot suitable for
  ``/runners/register``.
* :func:`sample_resources` collects the dynamic snapshot for
  ``/runners/<id>/heartbeat``.
* :func:`sign_payload` produces a hub-compatible canonical-JSON signature.
"""

from __future__ import annotations

import json
import os
import platform
import secrets
import shutil
import subprocess
import time
from typing import Any

from forgewire_fabric.runner.identity import RunnerIdentity


# ---------------------------------------------------------------------- info


def describe_host() -> dict[str, Any]:
    return {
        "hostname": platform.node() or "unknown",
        "os": platform.platform(),
        "arch": platform.machine() or "unknown",
        "cpu_model": platform.processor() or platform.machine() or "unknown",
        "cpu_count": os.cpu_count() or 1,
        "ram_mb": _ram_mb(),
        "gpu": _gpu_label(),
    }


def detect_tools() -> list[str]:
    candidates = ["git", "python", "py", "pytest", "node", "npm", "rustc", "cargo", "go"]
    return [t for t in candidates if shutil.which(t) is not None]


def _ram_mb() -> int | None:
    try:
        if hasattr(os, "sysconf") and "SC_PHYS_PAGES" in os.sysconf_names:
            pages = os.sysconf("SC_PHYS_PAGES")
            page_size = os.sysconf("SC_PAGE_SIZE")
            return int((pages * page_size) / (1024 * 1024))
    except (OSError, ValueError):
        pass
    if platform.system() == "Windows":
        try:
            out = subprocess.check_output(
                ["wmic", "ComputerSystem", "get", "TotalPhysicalMemory", "/value"],
                text=True,
                stderr=subprocess.DEVNULL,
                timeout=5,
            )
            for line in out.splitlines():
                if "=" in line:
                    val = line.split("=", 1)[1].strip()
                    if val.isdigit():
                        return int(int(val) / (1024 * 1024))
        except (subprocess.SubprocessError, FileNotFoundError, OSError):
            return None
    return None


def _gpu_label() -> str | None:
    if platform.system() == "Windows":
        try:
            out = subprocess.check_output(
                ["wmic", "path", "win32_VideoController", "get", "name"],
                text=True,
                stderr=subprocess.DEVNULL,
                timeout=5,
            )
            names = [l.strip() for l in out.splitlines() if l.strip() and l.strip() != "Name"]
            if names:
                return names[0][:120]
        except (subprocess.SubprocessError, FileNotFoundError, OSError):
            return None
    return None


# ----------------------------------------------------------------- resources


def sample_resources() -> dict[str, Any]:
    """Lightweight per-heartbeat resource snapshot.

    Avoids ``psutil`` by design — we don't want to add a wheel just for this.
    Values that can't be determined cheaply on the current OS are reported as
    ``None`` and the hub treats them as "unknown" rather than gating on them.
    """
    return {
        "cpu_load_pct": _cpu_load_pct(),
        "ram_free_mb": _ram_free_mb(),
        "battery_pct": _battery_pct(),
        "on_battery": _on_battery(),
    }


def _cpu_load_pct() -> float | None:
    try:
        load1, _, _ = os.getloadavg()
        return round(load1 / (os.cpu_count() or 1) * 100.0, 1)
    except (AttributeError, OSError):
        return None


def _ram_free_mb() -> int | None:
    if platform.system() == "Windows":
        try:
            out = subprocess.check_output(
                ["wmic", "OS", "get", "FreePhysicalMemory", "/value"],
                text=True,
                stderr=subprocess.DEVNULL,
                timeout=3,
            )
            for line in out.splitlines():
                if "=" in line:
                    val = line.split("=", 1)[1].strip()
                    if val.isdigit():
                        # FreePhysicalMemory is reported in KB.
                        return int(int(val) / 1024)
        except (subprocess.SubprocessError, FileNotFoundError, OSError):
            return None
    try:
        with open("/proc/meminfo", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("MemAvailable:"):
                    parts = line.split()
                    return int(int(parts[1]) / 1024)
    except OSError:
        return None
    return None


def _battery_pct() -> int | None:
    if platform.system() == "Windows":
        try:
            out = subprocess.check_output(
                ["wmic", "Path", "Win32_Battery", "get", "EstimatedChargeRemaining", "/value"],
                text=True,
                stderr=subprocess.DEVNULL,
                timeout=3,
            )
            for line in out.splitlines():
                if "=" in line:
                    val = line.split("=", 1)[1].strip()
                    if val.isdigit():
                        return int(val)
        except (subprocess.SubprocessError, FileNotFoundError, OSError):
            return None
    return None


def _on_battery() -> bool:
    if platform.system() == "Windows":
        try:
            out = subprocess.check_output(
                ["wmic", "Path", "Win32_Battery", "get", "BatteryStatus", "/value"],
                text=True,
                stderr=subprocess.DEVNULL,
                timeout=3,
            )
            for line in out.splitlines():
                if "=" in line:
                    val = line.split("=", 1)[1].strip()
                    if val.isdigit():
                        # 1 = on battery; 2 = on AC; others ~ charging/etc.
                        return val == "1"
        except (subprocess.SubprocessError, FileNotFoundError, OSError):
            return False
    return False


# -------------------------------------------------------------------- crypto


def canonical_payload(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sign_payload(identity: RunnerIdentity, payload: dict[str, Any]) -> str:
    return identity.sign(canonical_payload(payload))


def fresh_nonce() -> str:
    return secrets.token_hex(16)


def now_ts() -> int:
    return int(time.time())
