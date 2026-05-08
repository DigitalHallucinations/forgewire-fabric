"""Drift guard for installer assets.

The repository keeps two copies of the same PowerShell installer scripts:

* ``scripts/install/*.ps1`` — what humans / CI scripts edit.
* ``python/forgewire_fabric/_installer_assets/*.ps1`` — what gets bundled
  into the wheel and shipped to operators via ``forgewire_fabric.cli hub install``
  and ``forgewire_fabric.cli runner install``.

If these two trees drift, deployments via the published package silently
ship stale installer logic. That has bitten us at least once already
(rqlite flags landed in ``scripts/install/`` but never made it into the
bundled asset). This test fails loudly when the two diverge so future
PRs cannot land an out-of-band fix.

Source of truth is ``scripts/install/``. To update bundled copies::

    pwsh -File scripts/dev/sync_installer_assets.ps1
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = REPO_ROOT / "scripts" / "install"
BUNDLED_DIR = REPO_ROOT / "python" / "forgewire_fabric" / "_installer_assets"

# Mirrored files. Anything in scripts/install/ that ends in .ps1 *and* is
# legitimately bundle-only (none today) would be excluded here. Today every
# script in scripts/install/*.ps1 is mirrored.
MIRRORED = (
    "nssm-install-hub.ps1",
    "nssm-install-runner.ps1",
    "install-hub-watchdog.ps1",
    "install-runner-watchdog.ps1",
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize("name", MIRRORED)
def test_installer_asset_in_sync(name: str) -> None:
    src = SOURCE_DIR / name
    dst = BUNDLED_DIR / name
    assert src.exists(), f"missing source asset: {src}"
    assert dst.exists(), (
        f"missing bundled asset: {dst}. Run "
        f"`pwsh -File scripts/dev/sync_installer_assets.ps1`."
    )
    assert _sha256(src) == _sha256(dst), (
        f"installer asset drift: {name}\n"
        f"  source:  {src}\n"
        f"  bundled: {dst}\n"
        f"Run `pwsh -File scripts/dev/sync_installer_assets.ps1` to sync."
    )
