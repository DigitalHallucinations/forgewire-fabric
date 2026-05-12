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


# Allowed values for the runner's task-kind affinity. Mirrors the hub's
# ``DispatchTaskRequest.kind`` enum. Anything else is rejected at startup
# so a typo in a service env var is loud instead of silently routing the
# wrong work.
_VALID_KINDS = ("agent", "command")
_KIND_ENV_VARS = ("FORGEWIRE_RUNNER_KIND", "PHRENFORGE_RUNNER_KIND")


def apply_kind_tag(
    tags: list[str],
    *,
    default_kind: str,
    env_override: str | None = None,
) -> list[str]:
    """Return ``tags`` with exactly one ``kind:<value>`` entry present.

    Resolution order:

    1. ``env_override`` (typically ``FORGEWIRE_RUNNER_KIND``) wins. If set
       and valid, any existing ``kind:*`` tag is replaced.
    2. Otherwise, if ``tags`` already contains a ``kind:*`` (or
       ``kind=*``) entry, it is normalised and kept.
    3. Otherwise, ``kind:<default_kind>`` is appended.

    Raises :class:`ValueError` when the resolved kind is not one of
    :data:`_VALID_KINDS` -- a misconfigured shell-exec runner must
    refuse to start rather than fall back to ``agent`` and claim work
    it cannot safely execute.
    """
    if default_kind not in _VALID_KINDS:
        raise ValueError(f"invalid default_kind: {default_kind!r}")

    def _norm(raw: str) -> str:
        return raw.strip().lower().replace("=", ":")

    chosen: str | None = None
    if env_override is not None:
        candidate = env_override.strip().lower()
        if candidate:
            if candidate not in _VALID_KINDS:
                raise ValueError(
                    f"FORGEWIRE_RUNNER_KIND={env_override!r}: "
                    f"must be one of {_VALID_KINDS}"
                )
            chosen = candidate

    rebuilt: list[str] = []
    existing_kind: str | None = None
    for raw in tags or []:
        if not isinstance(raw, str):
            continue
        norm = _norm(raw)
        if norm.startswith("kind:"):
            value = norm.split(":", 1)[1].strip()
            if value in _VALID_KINDS and existing_kind is None:
                existing_kind = value
            # In all cases we drop the original so we can re-append a
            # single canonical entry at the end.
            continue
        rebuilt.append(raw)

    if chosen is None:
        chosen = existing_kind or default_kind

    if chosen not in _VALID_KINDS:
        raise ValueError(
            f"resolved runner kind {chosen!r} is not in {_VALID_KINDS}"
        )

    rebuilt.append(f"kind:{chosen}")
    return rebuilt


def resolve_kind_env() -> str | None:
    """Read the runner-kind env var (canonical name + legacy alias)."""
    for name in _KIND_ENV_VARS:
        val = os.environ.get(name)
        if val is not None:
            return val
    return None


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


def describe_capabilities(
    *,
    host: dict[str, Any] | None = None,
    tools: list[str] | None = None,
    region: str | None = None,
) -> dict[str, Any]:
    """Build the structured capability blob the M2.5.4 matcher consumes.

    Pure-stdlib derivation from already-collected facts (host snapshot +
    detected tool list + a bit of ``platform`` lookup). The returned
    shape is the canonical schema documented in
    `phase-2.5-operator-control-plane.md` (M2.5.4):

    .. code-block:: yaml

        python: "3.13.1"
        os: "windows-11"
        cpu: { cores: 16, arch: "x86_64" }
        ram_gb: 64
        gpu: ["nvidia:rtx-..."]
        toolchains: { rust: true, node: true, ... }
        services: []
        region: "homelab"
        sandbox_profile: "bare"

    Numeric values use the natural unit operators are most likely to
    write against (``ram_gb >= 32``, ``cpu.cores >= 8``).
    """
    host = host or describe_host()
    tools = tools if tools is not None else detect_tools()
    ram_mb = host.get("ram_mb")
    ram_gb = int(ram_mb / 1024) if isinstance(ram_mb, int) and ram_mb > 0 else None
    raw_os = (host.get("os") or "").lower()
    short_os = (
        "windows-11" if "windows-11" in raw_os
        else "windows-10" if "windows-10" in raw_os
        else "windows" if raw_os.startswith("windows") or "windows" in raw_os
        else "linux" if raw_os.startswith("linux") or "linux" in raw_os
        else "macos" if "macos" in raw_os or "darwin" in raw_os
        else raw_os.split("-", 1)[0] or "unknown"
    )
    py = ".".join(str(p) for p in platform.python_version_tuple()[:3])
    toolchains = {t: True for t in tools if t in {"rust", "rustc", "cargo", "node", "npm", "go", "python", "py", "uv", "pytest"}}
    # Normalise "rustc"/"cargo" -> "rust", "py"/"python" -> "python".
    if "rustc" in toolchains or "cargo" in toolchains:
        toolchains["rust"] = True
    if "python" in toolchains or "py" in toolchains:
        toolchains["python"] = True
    blob: dict[str, Any] = {
        "python": py,
        "os": short_os,
        "cpu": {"cores": int(host.get("cpu_count") or 1), "arch": host.get("arch") or "unknown"},
        "toolchains": toolchains,
        "services": [],
        "sandbox_profile": "bare",
    }
    if ram_gb is not None:
        blob["ram_gb"] = ram_gb
    gpu = host.get("gpu")
    if isinstance(gpu, str) and gpu:
        blob["gpu"] = [gpu]
    elif isinstance(gpu, list) and gpu:
        blob["gpu"] = list(gpu)
    if region:
        blob["region"] = region
    return blob


def detect_tools() -> list[str]:
    candidates = ["git", "python", "py", "pytest", "node", "npm", "rustc", "cargo", "go"]
    found: list[str] = []
    for t in candidates:
        if shutil.which(t) is None:
            continue
        if not _tool_works(t):
            # Tool is on PATH but its --version probe fails (e.g. Windows
            # ``py.exe`` launcher present but no installed Python visible to
            # the running account, which exits 103/112 with "No installed
            # Python found!"). Don't advertise capabilities we can't actually
            # exercise -- the hub would route work to us and every task
            # would die at the spawn step.
            continue
        found.append(t)
    return found


# Per-tool argv used to probe whether the binary can actually run as the
# current account. ``--version`` is universally cheap and side-effect-free
# for these tools.
_TOOL_PROBE_ARGS: dict[str, list[str]] = {
    "git": ["--version"],
    "python": ["--version"],
    "py": ["--version"],
    "pytest": ["--version"],
    "node": ["--version"],
    "npm": ["--version"],
    "rustc": ["--version"],
    "cargo": ["--version"],
    "go": ["version"],
}


def _tool_works(tool: str) -> bool:
    args = _TOOL_PROBE_ARGS.get(tool, ["--version"])
    try:
        proc = subprocess.run(
            [tool, *args],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            timeout=5,
            check=False,
        )
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        return False
    return proc.returncode == 0


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
