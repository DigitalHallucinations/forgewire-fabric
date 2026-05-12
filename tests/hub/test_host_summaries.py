"""Host role summaries for the primary Hosts pane.

Exercises the real hub API: installer-reported host role facts, runner
heartbeats, dispatcher registration, and active hub/control identity all fuse
into /hosts without requiring the VSIX to infer roles from raw lists.
"""

from __future__ import annotations

import json
import secrets
import socket
import time
from pathlib import Path

from fastapi.testclient import TestClient

from forgewire_fabric.dispatcher.identity import load_or_create as load_dispatcher
from forgewire_fabric.hub.server import BlackboardConfig, create_app
from forgewire_fabric.runner.identity import load_or_create as load_runner
from forgewire_fabric.runner.runner_capabilities import sign_payload


HUB_TOKEN = "test-hub-token-aaaaaaaaaaaaaaaaa"


def _make_app(tmp_path: Path):
    cfg = BlackboardConfig(
        db_path=tmp_path / "hub.sqlite3",
        token=HUB_TOKEN,
        host="127.0.0.1",
        port=0,
    )
    return create_app(cfg)


def _auth() -> dict[str, str]:
    return {"authorization": f"Bearer {HUB_TOKEN}"}


def _canonical(body: dict) -> bytes:
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _register_runner(client: TestClient, tmp_path: Path, *, hostname: str, tags: list[str]) -> str:
    ident = load_runner(tmp_path / f"runner-{hostname}.json")
    ts = int(time.time())
    nonce = secrets.token_hex(16)
    body = {
        "op": "register",
        "runner_id": ident.runner_id,
        "public_key": ident.public_key_hex,
        "protocol_version": 2,
        "timestamp": ts,
        "nonce": nonce,
    }
    payload = {
        "runner_id": ident.runner_id,
        "public_key": ident.public_key_hex,
        "protocol_version": 2,
        "runner_version": "0.4.0",
        "hostname": hostname,
        "os": "Windows",
        "arch": "x86_64",
        "tools": ["shell"],
        "tags": tags,
        "scope_prefixes": [],
        "metadata": {},
        "timestamp": ts,
        "nonce": nonce,
        "signature": sign_payload(ident, body),
    }
    r = client.post("/runners/register", json=payload, headers=_auth())
    assert r.status_code == 200, r.text
    return ident.runner_id


def _register_dispatcher(client: TestClient, tmp_path: Path, *, hostname: str) -> str:
    ident = load_dispatcher(tmp_path / f"dispatcher-{hostname}.json", label="test-dispatcher")
    ts = int(time.time())
    nonce = secrets.token_hex(16)
    body = {
        "op": "register-dispatcher",
        "dispatcher_id": ident.dispatcher_id,
        "public_key": ident.public_key_hex,
        "timestamp": ts,
        "nonce": nonce,
    }
    payload = {
        "dispatcher_id": ident.dispatcher_id,
        "public_key": ident.public_key_hex,
        "label": ident.label,
        "hostname": hostname,
        "metadata": {"dispatch_enabled": True},
        "timestamp": ts,
        "nonce": nonce,
        "signature": ident.sign(_canonical(body)),
    }
    r = client.post("/dispatchers/register", json=payload, headers=_auth())
    assert r.status_code == 200, r.text
    return ident.dispatcher_id


def test_hosts_summary_fuses_roles_runners_dispatchers_and_active_hub(tmp_path: Path) -> None:
    app = _make_app(tmp_path)
    with TestClient(app) as client:
        host = "HOST-A"
        command_runner_id = _register_runner(client, tmp_path, hostname=host, tags=["kind:command"])
        dispatcher_id = _register_dispatcher(client, tmp_path, hostname=host)

        r = client.post(
            "/hosts/roles",
            json={
                "hostname": host,
                "role": "agent_runner",
                "enabled": True,
                "status": "registered",
                "metadata": {"mcp_server": "forgewire-runner"},
            },
            headers=_auth(),
        )
        assert r.status_code == 200, r.text

        r = client.get("/hosts", headers=_auth())
        assert r.status_code == 200, r.text
        payload = r.json()
        hosts = {h["hostname"]: h for h in payload["hosts"]}

        active = hosts[socket.gethostname()]
        assert active["roles"]["hub_head"]["status"] == "active"
        assert active["roles"]["control"]["status"] == "master"

        summary = hosts[host]
        assert summary["roles"]["command_runner"]["enabled"] is True
        assert summary["roles"]["command_runner"]["status"] == "online"
        assert summary["roles"]["command_runner"]["runner_ids"] == [command_runner_id]
        assert summary["roles"]["agent_runner"]["enabled"] is True
        assert summary["roles"]["agent_runner"]["status"] == "registered"
        assert summary["roles"]["agent_runner"]["metadata"]["mcp_server"] == "forgewire-runner"
        assert summary["roles"]["dispatch"]["enabled"] is True
        assert summary["roles"]["dispatch"]["status"] == "registered"
        assert summary["roles"]["dispatch"]["dispatcher_ids"] == [dispatcher_id]


def test_host_role_report_requires_auth(tmp_path: Path) -> None:
    app = _make_app(tmp_path)
    with TestClient(app) as client:
        r = client.post(
            "/hosts/roles",
            json={
                "hostname": "HOST-A",
                "role": "agent_runner",
                "enabled": True,
            },
        )
        assert r.status_code == 401


def test_dispatcher_only_rows_do_not_create_hosts(tmp_path: Path) -> None:
    app = _make_app(tmp_path)
    with TestClient(app) as client:
        _register_dispatcher(client, tmp_path, hostname="test-host")

        r = client.get("/hosts", headers=_auth())
        assert r.status_code == 200, r.text
        hostnames = {h["hostname"] for h in r.json()["hosts"]}
        assert socket.gethostname() in hostnames
        assert "test-host" not in hostnames
