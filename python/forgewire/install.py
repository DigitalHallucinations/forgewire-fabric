"""Service installers for ForgeWire hub + runner.

Cross-platform install/uninstall helpers for the hub and runner. On Windows
this drives the bundled NSSM ``ps1`` scripts (NSSM must be on PATH). On Linux
it installs a systemd unit via ``systemctl``. On macOS it installs a launchd
plist into ``/Library/LaunchDaemons``.

These helpers are idempotent. The ``uninstall`` operation stops + removes the
service/unit but never touches ``~/.forgewire/`` config or DB files.
"""

from __future__ import annotations

import os
import secrets
import shutil
import subprocess
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _asset(*relparts: str) -> Path:
    """Return a filesystem path to a bundled installer asset.

    Assets are shipped inside the wheel under
    ``forgewire/_installer_assets/``. For source checkouts we also fall back
    to the top-level ``scripts/install/`` tree.
    """
    here = Path(__file__).resolve().parent  # python/forgewire/
    bundled = here / "_installer_assets" / Path(*relparts)
    if bundled.exists():
        return bundled
    repo = here.parent.parent / "scripts" / "install" / Path(*relparts)
    if repo.exists():
        return repo
    raise FileNotFoundError(
        f"Installer asset not found in either {bundled} or {repo}."
    )


def _require_root_unix() -> None:
    if hasattr(os, "geteuid") and os.geteuid() != 0:  # type: ignore[attr-defined]
        raise SystemExit("This command must be run as root (try sudo).")


def _python_exe() -> str:
    return sys.executable


def _new_token() -> str:
    return secrets.token_hex(16)


def _powershell_env() -> dict[str, str]:
    """Return an env dict suitable for invoking powershell.exe.

    Strips ``PSModulePath`` so a caller's mangled module path (e.g. from a
    venv ``Activate.ps1``) cannot prevent ``Microsoft.PowerShell.Security``
    from loading inside the installer script.
    """
    env = os.environ.copy()
    env.pop("PSModulePath", None)
    return env


# ---------------------------------------------------------------------------
# Windows (NSSM)
# ---------------------------------------------------------------------------


def _windows_install_hub(*, port: int, host: str, token: str | None) -> None:
    if shutil.which("nssm.exe") is None:
        raise SystemExit(
            "NSSM not found on PATH. Install with 'winget install nssm.nssm' "
            "or download from https://nssm.cc/."
        )
    script = _asset("nssm-install-hub.ps1")
    cmd = [
        "powershell.exe",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(script),
        "-PythonExe",
        _python_exe(),
        "-Token",
        token or _new_token(),
        "-Port",
        str(port),
        "-BindHost",
        host,
    ]
    subprocess.run(cmd, check=True, env=_powershell_env())


def _windows_install_runner(*, hub_url: str, hub_token: str, workspace_root: str) -> None:
    if shutil.which("nssm.exe") is None:
        raise SystemExit("NSSM not found on PATH.")
    script = _asset("nssm-install-runner.ps1")
    cmd = [
        "powershell.exe",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(script),
        "-PythonExe",
        _python_exe(),
        "-HubUrl",
        hub_url,
        "-Token",
        hub_token,
        "-WorkspaceRoot",
        workspace_root,
    ]
    subprocess.run(cmd, check=True, env=_powershell_env())


def _windows_uninstall(service: str) -> None:
    if shutil.which("nssm.exe") is None:
        raise SystemExit("NSSM not found on PATH.")
    subprocess.run(["nssm.exe", "stop", service], check=False)
    subprocess.run(["nssm.exe", "remove", service, "confirm"], check=False)


# ---------------------------------------------------------------------------
# Linux (systemd)
# ---------------------------------------------------------------------------


def _linux_install_unit(unit_name: str, asset_name: str) -> None:
    _require_root_unix()
    src = _asset("systemd", asset_name)
    dst = Path(f"/etc/systemd/system/{unit_name}")
    dst.write_bytes(src.read_bytes())
    subprocess.run(["systemctl", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "enable", unit_name], check=True)
    subprocess.run(["systemctl", "start", unit_name], check=True)
    print(f"Installed {dst}; started {unit_name}.")


def _linux_uninstall_unit(unit_name: str) -> None:
    _require_root_unix()
    subprocess.run(["systemctl", "stop", unit_name], check=False)
    subprocess.run(["systemctl", "disable", unit_name], check=False)
    p = Path(f"/etc/systemd/system/{unit_name}")
    if p.exists():
        p.unlink()
    subprocess.run(["systemctl", "daemon-reload"], check=False)


# ---------------------------------------------------------------------------
# macOS (launchd)
# ---------------------------------------------------------------------------


def _macos_install_plist(plist_name: str) -> None:
    _require_root_unix()
    src = _asset("launchd", plist_name)
    dst = Path(f"/Library/LaunchDaemons/{plist_name}")
    dst.write_bytes(src.read_bytes())
    os.chmod(dst, 0o644)
    subprocess.run(["launchctl", "load", "-w", str(dst)], check=True)
    print(f"Installed {dst}; loaded via launchctl.")


def _macos_uninstall_plist(plist_name: str) -> None:
    _require_root_unix()
    p = Path(f"/Library/LaunchDaemons/{plist_name}")
    if p.exists():
        subprocess.run(["launchctl", "unload", str(p)], check=False)
        p.unlink()


# ---------------------------------------------------------------------------
# public dispatchers
# ---------------------------------------------------------------------------


def install_hub(*, port: int, host: str, token: str | None) -> None:
    if sys.platform.startswith("win"):
        _windows_install_hub(port=port, host=host, token=token)
    elif sys.platform.startswith("linux"):
        _linux_install_unit("forgewire-hub.service", "forgewire-hub.service")
    elif sys.platform == "darwin":
        _macos_install_plist("com.forgewire.hub.plist")
    else:
        raise SystemExit(f"Unsupported platform: {sys.platform}")


def uninstall_hub() -> None:
    if sys.platform.startswith("win"):
        _windows_uninstall("ForgeWireHub")
    elif sys.platform.startswith("linux"):
        _linux_uninstall_unit("forgewire-hub.service")
    elif sys.platform == "darwin":
        _macos_uninstall_plist("com.forgewire.hub.plist")
    else:
        raise SystemExit(f"Unsupported platform: {sys.platform}")


def install_runner(*, hub_url: str, hub_token: str, workspace_root: str) -> None:
    if sys.platform.startswith("win"):
        _windows_install_runner(hub_url=hub_url, hub_token=hub_token, workspace_root=workspace_root)
    elif sys.platform.startswith("linux"):
        _linux_install_unit("forgewire-runner.service", "forgewire-runner.service")
    elif sys.platform == "darwin":
        _macos_install_plist("com.forgewire.runner.plist")
    else:
        raise SystemExit(f"Unsupported platform: {sys.platform}")


def uninstall_runner() -> None:
    if sys.platform.startswith("win"):
        _windows_uninstall("ForgeWireRunner")
    elif sys.platform.startswith("linux"):
        _linux_uninstall_unit("forgewire-runner.service")
    elif sys.platform == "darwin":
        _macos_uninstall_plist("com.forgewire.runner.plist")
    else:
        raise SystemExit(f"Unsupported platform: {sys.platform}")
