from __future__ import annotations

import json
from pathlib import Path

from forgewire_fabric import cli as cli_mod


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_mcp_runner_uses_configured_hub_url(tmp_path, monkeypatch) -> None:
    appdata = tmp_path / "AppData" / "Roaming"
    monkeypatch.setenv("APPDATA", str(appdata))
    monkeypatch.setattr(cli_mod.sys, "platform", "win32")

    cli_mod._write_vscode_user_mcp(
        hub_url="http://10.120.81.95:8765",
        install_runner=True,
        workspace_root="C:\\Projects\\forgewire",
    )

    mcp_path = appdata / "Code" / "User" / "mcp.json"
    payload = json.loads(mcp_path.read_text(encoding="utf-8"))
    servers = payload["servers"]
    assert servers["forgewire-dispatcher"]["env"]["FORGEWIRE_HUB_URL"] == "http://10.120.81.95:8765"
    assert servers["forgewire-runner"]["env"]["FORGEWIRE_HUB_URL"] == "http://10.120.81.95:8765"


def test_runner_mcp_registration_is_backgrounded() -> None:
    body = (REPO_ROOT / "python" / "forgewire_fabric" / "hub" / "runner_mcp.py").read_text(encoding="utf-8")
    assert "registration_task = asyncio.create_task(_register_with_retries(session))" in body
    assert "await _register_with_retries(session)\n    heartbeat_task" not in body


def test_dispatchers_view_collapsed_into_hosts() -> None:
    package = json.loads((REPO_ROOT / "vscode" / "package.json").read_text(encoding="utf-8"))
    view_ids = {view["id"] for view in package["contributes"]["views"]["forgewireFabric"]}
    assert "forgewireFabric.dispatchers" not in view_ids

    extension = (REPO_ROOT / "vscode" / "src" / "extension.ts").read_text(encoding="utf-8")
    assert "registerTreeDataProvider(\"forgewireFabric.dispatchers\"" not in extension

    providers = (REPO_ROOT / "vscode" / "src" / "treeProviders.ts").read_text(encoding="utf-8")
    assert "kind: \"dispatcher\"" in providers
    assert "hosts:dispatcher:" in providers
