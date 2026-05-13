"""Regression: hub-side aliases + runner routing intent must survive
code updates, schema upgrades, and hardware migrations.

The persistence model this file locks in:

* **Runner aliases** live in the hub's ``labels`` table, keyed by
  ``runner_alias:<runner_id>``. They have no FK to the ``runners`` row,
  so pruning a stale runner does not erase its alias; re-importing the
  same identity on different hardware automatically picks the alias
  back up.
* **Hub display name** is the ``hub_name`` row in the same table; it
  shares the same survivability properties.
* **Runner routing knobs** (``workspace_root``, ``tags``,
  ``scope_prefixes``, ``tenant``, ``max_concurrent``,
  ``poll_interval_seconds``) are persisted in a machine-wide
  ``runner_config.json`` sidecar next to the identity file. The sidecar
  is read by ``RunnerConfig.from_env`` as a *fallback* when the
  corresponding env var is unset, and is carried by the migration
  bundle produced by ``runner identity-export``.

Covered scenarios:

1. Schema re-init is idempotent (``CREATE TABLE IF NOT EXISTS labels``
   does not drop rows).
2. A fresh ``Blackboard`` instance pointed at the same DB sees the
   labels written by a previous instance (proxy for "hub restart").
3. The stale-same-host prune in ``upsert_runner`` does not affect the
   labels table; aliases survive a forced dedupe.
4. Sidecar load/save/merge/clear semantics.
5. ``RunnerConfig.from_env`` precedence: env wins, sidecar fills in.
6. Migration bundle round-trip preserves both the runner_id *and* the
   sidecar config.
7. Legacy bare-identity exports remain importable.
8. ``install_runner`` seeds the sidecar from operator-supplied flags.
9. ``labels`` CLI export → import round-trips via the in-memory hub.
"""

from __future__ import annotations

import importlib
import json
import secrets
import sys
import time
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
from click.testing import CliRunner
from fastapi.testclient import TestClient

from forgewire_fabric import cli as cli_mod
from forgewire_fabric.hub.server import (
    Blackboard,
    BlackboardConfig,
    HEARTBEAT_OFFLINE_SECONDS,
    create_app,
)
from forgewire_fabric.runner import agent as agent_mod
from forgewire_fabric.runner import identity as identity_mod
from forgewire_fabric.runner.runner_capabilities import sign_payload


HUB_TOKEN = "test-hub-token-aaaaaaaaaaaaaaaaa"


def _auth() -> dict[str, str]:
    return {"authorization": f"Bearer {HUB_TOKEN}"}


def _make_cfg(tmp_path: Path) -> BlackboardConfig:
    return BlackboardConfig(
        db_path=tmp_path / "hub.sqlite3",
        token=HUB_TOKEN,
        host="127.0.0.1",
        port=0,
    )


def _register(
    client: TestClient,
    ident,
    *,
    hostname: str,
    ts: int | None = None,
) -> None:
    ts = ts if ts is not None else int(time.time())
    nonce = secrets.token_hex(16)
    body = {
        "op": "register",
        "runner_id": ident.runner_id,
        "public_key": ident.public_key_hex,
        "protocol_version": 2,
        "timestamp": ts,
        "nonce": nonce,
    }
    sig = sign_payload(ident, body)
    payload: dict[str, Any] = {
        "runner_id": ident.runner_id,
        "public_key": ident.public_key_hex,
        "protocol_version": 2,
        "runner_version": "0.11.0",
        "hostname": hostname,
        "os": "test-os",
        "arch": "x86_64",
        "tools": ["py"],
        "tags": [],
        "scope_prefixes": [],
        "metadata": {},
        "timestamp": ts,
        "nonce": nonce,
        "signature": sig,
    }
    r = client.post("/runners/register", json=payload, headers=_auth())
    assert r.status_code == 200, r.text


def _backdate_heartbeat(db_path: Path, runner_id: str, seconds_ago: int) -> None:
    import sqlite3

    cutoff = time.strftime(
        "%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - seconds_ago)
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE runners SET last_heartbeat = ? WHERE runner_id = ?",
            (cutoff, runner_id),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Dispatcher view: GET /runners includes hub_name + per-runner alias
# ---------------------------------------------------------------------------


def test_runners_endpoint_includes_hub_name_and_aliases(tmp_path: Path) -> None:
    """The dispatcher MCP ``list_runners`` tool returns whatever the hub
    serves at GET /runners. Enriching that payload server-side is what
    lets a dispatcher identify machines by their operator-set names
    (``hub_name`` + per-runner ``alias``) without a second round trip.
    """
    cfg = _make_cfg(tmp_path)
    app = create_app(cfg)
    with TestClient(app) as client:
        ident = identity_mod.load_or_create(tmp_path / "id.json")
        _register(client, ident, hostname="DESKTOP-TEST")
        client.put(
            "/labels/hub", json={"name": "Test HUB 1"}, headers=_auth()
        )
        client.put(
            f"/labels/runners/{ident.runner_id}",
            json={"alias": "Precision 5520"},
            headers=_auth(),
        )
        body = client.get("/runners", headers=_auth()).json()
        assert body["hub_name"] == "Test HUB 1"
        assert len(body["runners"]) == 1
        row = body["runners"][0]
        assert row["runner_id"] == ident.runner_id
        assert row["alias"] == "Precision 5520"
        # A runner without an alias still gets the key for stable schema.
        client.put(
            f"/labels/runners/{ident.runner_id}",
            json={"alias": ""},
            headers=_auth(),
        )
        body = client.get("/runners", headers=_auth()).json()
        assert body["runners"][0]["alias"] == ""


# ---------------------------------------------------------------------------
# Hub-side persistence of labels
# ---------------------------------------------------------------------------


def test_labels_survive_schema_reinit(tmp_path: Path) -> None:
    """``_init_schema`` runs at every Blackboard construction; the
    ``CREATE TABLE IF NOT EXISTS labels`` statement must not drop rows."""
    db_path = tmp_path / "hub.sqlite3"
    bb = Blackboard(db_path)
    bb.set_hub_name("alpha-hub")
    bb.set_runner_alias("11111111-1111-1111-1111-111111111111", "precision")

    # Construct a second Blackboard against the same DB (re-runs
    # ``_init_schema``). Aliases must still be visible.
    bb2 = Blackboard(db_path)
    labels = bb2.get_labels()
    assert labels["hub_name"] == "alpha-hub"
    assert labels["runner_aliases"] == {
        "11111111-1111-1111-1111-111111111111": "precision"
    }


def test_labels_survive_hub_restart(tmp_path: Path) -> None:
    """Closing the TestClient and reopening it against the same DB path
    is the moral equivalent of restarting the hub service."""
    cfg = _make_cfg(tmp_path)
    with TestClient(create_app(cfg)) as c1:
        r = c1.put(
            "/labels/runners/22222222-2222-2222-2222-222222222222",
            json={"alias": "optiplex"},
            headers=_auth(),
        )
        assert r.status_code == 200, r.text
    with TestClient(create_app(cfg)) as c2:
        r = c2.get("/labels", headers=_auth())
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["runner_aliases"] == {
            "22222222-2222-2222-2222-222222222222": "optiplex"
        }


def test_alias_survives_runner_dedupe_prune(tmp_path: Path) -> None:
    """The stale-same-host prune in ``upsert_runner`` deletes rows from
    the ``runners`` table; the alias keyed by the pruned ``runner_id``
    must remain because labels carry no FK to runners.
    """
    cfg = _make_cfg(tmp_path)
    app = create_app(cfg)
    db_path = cfg.db_path
    with TestClient(app) as client:
        ghost = identity_mod.load_or_create(tmp_path / "ghost.json")
        fresh = identity_mod.load_or_create(tmp_path / "fresh.json")
        _register(client, ghost, hostname="HOST-A")
        # Operator names this runner.
        r = client.put(
            f"/labels/runners/{ghost.runner_id}",
            json={"alias": "named-ghost"},
            headers=_auth(),
        )
        assert r.status_code == 200, r.text
        # Backdate so the prune logic considers it stale.
        _backdate_heartbeat(
            db_path, ghost.runner_id, HEARTBEAT_OFFLINE_SECONDS + 30
        )
        # Re-register a *different* runner on the same host: this trips
        # the dedupe DELETE on the runners table.
        _register(client, fresh, hostname="HOST-A")
        # The original runner row is gone.
        r = client.get("/runners", headers=_auth())
        runner_ids = {row["runner_id"] for row in r.json().get("runners", [])}
        assert ghost.runner_id not in runner_ids
        assert fresh.runner_id in runner_ids
        # But the alias keyed by the pruned runner_id is still there.
        r = client.get("/labels", headers=_auth())
        assert r.json()["runner_aliases"][ghost.runner_id] == "named-ghost"


# ---------------------------------------------------------------------------
# Labels snapshot sidecar (filesystem mirror, auto-applied on startup)
# ---------------------------------------------------------------------------


def test_labels_snapshot_writethrough_on_every_label_change(
    tmp_path: Path,
) -> None:
    """Every successful ``_upsert_label`` (set hub_name / set alias /
    clear alias) must mirror the live ``labels`` table to the on-disk
    sidecar. The sidecar is the operator's lifeboat against an
    accidental rqlite table wipe.
    """
    db_path = tmp_path / "hub.sqlite3"
    snap = tmp_path / "labels.snapshot.json"
    bb = Blackboard(db_path)
    assert not snap.exists()
    bb.set_hub_name("Test hub 1")
    assert snap.exists()
    after_hub = json.loads(snap.read_text(encoding="utf-8"))
    assert after_hub["schema"] == "forgewire-labels-export/1"
    assert after_hub["labels"]["hub_name"] == "Test hub 1"
    assert after_hub["labels"]["runner_aliases"] == {}
    assert after_hub["labels"]["host_aliases"] == {}

    bb.set_host_alias("HOST-A", "Precision 5520")
    after_host_alias = json.loads(snap.read_text(encoding="utf-8"))
    assert after_host_alias["labels"]["host_aliases"] == {"HOST-A": "Precision 5520"}

    bb.set_runner_alias("rid-A", "Alpha")
    after_alias = json.loads(snap.read_text(encoding="utf-8"))
    assert after_alias["labels"]["runner_aliases"] == {"rid-A": "Alpha"}

    bb.set_runner_alias("rid-A", "")  # clear
    after_clear = json.loads(snap.read_text(encoding="utf-8"))
    assert after_clear["labels"]["runner_aliases"] == {}
    assert after_clear["labels"]["host_aliases"] == {"HOST-A": "Precision 5520"}
    assert after_clear["labels"]["hub_name"] == "Test hub 1"


def test_labels_snapshot_restore_re_applies_after_table_wipe(
    tmp_path: Path,
) -> None:
    """Simulate the ``labels`` table being wiped (the destructive bug
    that motivated this feature). After the wipe, calling
    ``restore_labels_from_snapshot`` must repopulate hub_name +
    aliases from the on-disk sidecar.
    """
    db_path = tmp_path / "hub.sqlite3"
    bb = Blackboard(db_path)
    bb.set_hub_name("Test hub 1")
    bb.set_host_alias("HOST-A", "Precision 5520")
    bb.set_runner_alias("rid-A", "Pecision 5520")
    bb.set_runner_alias("rid-B", "Optiplex 7050t")

    # Wipe the labels table directly, bypassing the sidecar.
    with bb._connect() as conn:
        conn.execute("DELETE FROM labels")
        conn.commit()
    assert bb.get_labels() == {"hub_name": "", "runner_aliases": {}, "host_aliases": {}}

    report = bb.restore_labels_from_snapshot()
    assert report["status"] == "applied"
    assert report["applied"] == 4  # hub_name + 2 runner aliases + 1 host alias
    assert bb.get_labels() == {
        "hub_name": "Test hub 1",
        "host_aliases": {"HOST-A": "Precision 5520"},
        "runner_aliases": {
            "rid-A": "Pecision 5520",
            "rid-B": "Optiplex 7050t",
        },
    }


def test_create_app_auto_restores_labels_snapshot_on_startup(
    tmp_path: Path,
) -> None:
    """The default ``create_app`` flow must auto-apply the sidecar at
    construction time, before serving any traffic. This is what
    protects operator names across redeploys.
    """
    cfg = _make_cfg(tmp_path)
    snap = cfg.db_path.parent / "labels.snapshot.json"
    # Hand-author a sidecar as if a previous hub had written it. The
    # DB is empty when create_app runs, so the only way the names
    # appear is via restore.
    snap.write_text(
        json.dumps(
            {
                "schema": "forgewire-labels-export/1",
                "labels": {
                    "hub_name": "Test hub 1",
                    "host_aliases": {"HOST-A": "Precision 5520"},
                    "runner_aliases": {
                        "rid-A": "Pecision 5520",
                        "rid-B": "Optiplex 7050t",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    app = create_app(cfg)
    with TestClient(app) as client:
        r = client.get("/labels", headers=_auth())
        assert r.status_code == 200, r.text
        assert r.json() == {
            "hub_name": "Test hub 1",
            "host_aliases": {"HOST-A": "Precision 5520"},
            "runner_aliases": {
                "rid-A": "Pecision 5520",
                "rid-B": "Optiplex 7050t",
            },
        }
    # The startup report is surfaced on app.state for log/inspection.
    report = app.state.labels_snapshot_report
    assert report["status"] == "applied"
    assert report["applied"] == 4


def test_labels_snapshot_absent_is_noop(tmp_path: Path) -> None:
    """A fresh install has no sidecar AND no DB labels. Startup must
    not error; the report must reflect ``status="absent"`` so an
    operator can tell a skipped restore from a successful one in the
    logs.
    """
    cfg = _make_cfg(tmp_path)
    app = create_app(cfg)
    report = app.state.labels_snapshot_report
    assert report["status"] == "absent"
    assert report["applied"] == 0


def test_labels_snapshot_seeds_from_db_when_sidecar_missing(
    tmp_path: Path,
) -> None:
    """Enterprise-deploy safety net.

    First-time deploy of the snapshot feature on a hub that already
    has operator-set labels must auto-seed the sidecar from the live
    DB so the *next* wipe is recoverable. Same path covers a standby
    promoted via /state/import (DB labels arrive in the SQLite blob,
    sidecar does not) and a reimaged host that restored only the DB
    from backup. Without this, the very first wipe after rollout
    would lose names because the sidecar never existed.
    """
    cfg = _make_cfg(tmp_path)
    snap = cfg.db_path.parent / "labels.snapshot.json"
    # Pre-populate the DB with operator labels but NO sidecar -- the
    # state a freshly-upgraded production hub is in on first boot.
    bb_seed = Blackboard(cfg.db_path, labels_snapshot_path=Path(""))
    bb_seed.set_hub_name("Test hub 1")
    bb_seed.set_host_alias("HOST-A", "Precision 5520")
    bb_seed.set_runner_alias("rid-A", "Pecision 5520")
    assert not snap.exists()
    # Now boot the app the way the service supervisor does. Restore
    # must auto-seed the sidecar from the DB.
    app = create_app(cfg)
    report = app.state.labels_snapshot_report
    assert report["status"] == "seeded_from_db"
    assert report["seeded_keys"] == 3  # hub_name + 1 host alias + 1 runner alias
    assert snap.exists()
    payload = json.loads(snap.read_text(encoding="utf-8"))
    assert payload["labels"] == {
        "hub_name": "Test hub 1",
        "host_aliases": {"HOST-A": "Precision 5520"},
        "runner_aliases": {"rid-A": "Pecision 5520"},
    }
    # And labels are still readable through the API.
    with TestClient(app) as client:
        r = client.get("/labels", headers=_auth())
        assert r.json()["hub_name"] == "Test hub 1"


def test_labels_snapshot_disabled_via_empty_path(tmp_path: Path) -> None:
    """Operators must be able to opt out (e.g. when the DB lives on a
    read-only volume). Passing ``Path("")`` disables both the
    write-through and the startup restore.
    """
    cfg = BlackboardConfig(
        db_path=tmp_path / "hub.sqlite3",
        token=HUB_TOKEN,
        host="127.0.0.1",
        port=0,
        labels_snapshot_path=Path(""),
    )
    app = create_app(cfg)
    report = app.state.labels_snapshot_report
    assert report["status"] == "disabled"
    with TestClient(app) as client:
        r = client.put(
            "/labels/hub", json={"name": "no-mirror"}, headers=_auth()
        )
        assert r.status_code == 200, r.text
    # No sidecar file should have been written anywhere.
    assert list(tmp_path.glob("*.snapshot.json")) == []


def test_labels_snapshot_rejects_unknown_schema(tmp_path: Path) -> None:
    """Unknown-schema sidecars must be skipped, not blindly imported.
    Same contract as the ``labels import`` CLI."""
    db_path = tmp_path / "hub.sqlite3"
    snap = tmp_path / "labels.snapshot.json"
    snap.write_text(
        json.dumps(
            {
                "schema": "something-else/9",
                "labels": {"hub_name": "should-not-apply"},
            }
        ),
        encoding="utf-8",
    )
    bb = Blackboard(db_path)
    report = bb.restore_labels_from_snapshot()
    assert report["status"] == "unknown_schema"
    assert bb.get_labels()["hub_name"] == ""


def test_labels_snapshot_tolerates_bare_payload(tmp_path: Path) -> None:
    """Tolerate a bare ``{hub_name, runner_aliases}`` JSON (no envelope)
    so hand-edited sidecars still work, matching ``labels import``."""
    db_path = tmp_path / "hub.sqlite3"
    snap = tmp_path / "labels.snapshot.json"
    snap.write_text(
        json.dumps(
            {"hub_name": "bare-ok", "runner_aliases": {"rid-X": "X"}}
        ),
        encoding="utf-8",
    )
    bb = Blackboard(db_path)
    report = bb.restore_labels_from_snapshot()
    assert report["status"] == "applied"
    assert bb.get_labels() == {
        "hub_name": "bare-ok",
        "host_aliases": {},
        "runner_aliases": {"rid-X": "X"},
    }


def test_labels_snapshot_unreadable_is_logged_not_fatal(
    tmp_path: Path,
) -> None:
    """A corrupt sidecar must not crash startup. Operators can re-export
    via the CLI to recover; the worst case is that operator names are
    not auto-restored on this boot."""
    db_path = tmp_path / "hub.sqlite3"
    snap = tmp_path / "labels.snapshot.json"
    snap.write_text("{not valid json", encoding="utf-8")
    bb = Blackboard(db_path)
    report = bb.restore_labels_from_snapshot()
    assert report["status"] == "unreadable"
    assert "error" in report


# ---------------------------------------------------------------------------
# Runner-config sidecar
# ---------------------------------------------------------------------------


def test_sidecar_save_load_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "runner_config.json"
    saved = identity_mod.save_runner_config_overrides(
        {
            "workspace_root": str(tmp_path),
            "tags": "gpu,west",
            "scope_prefixes": ["docs/", "tests/"],
            "max_concurrent": 4,
            "poll_interval_seconds": 2.5,
        },
        path=path,
    )
    assert saved["tags"] == ["gpu", "west"]
    assert saved["scope_prefixes"] == ["docs/", "tests/"]
    assert saved["max_concurrent"] == 4
    assert saved["poll_interval_seconds"] == 2.5
    reloaded = identity_mod.load_runner_config_overrides(path)
    assert reloaded == saved


def test_sidecar_merge_preserves_existing_keys(tmp_path: Path) -> None:
    path = tmp_path / "runner_config.json"
    identity_mod.save_runner_config_overrides(
        {"workspace_root": "/tmp/a", "tags": "gpu"}, path=path
    )
    merged = identity_mod.save_runner_config_overrides(
        {"tenant": "team-alpha"}, path=path, merge=True
    )
    assert merged["workspace_root"] == "/tmp/a"
    assert merged["tags"] == ["gpu"]
    assert merged["tenant"] == "team-alpha"


def test_sidecar_replace_overrides_existing(tmp_path: Path) -> None:
    path = tmp_path / "runner_config.json"
    identity_mod.save_runner_config_overrides(
        {"workspace_root": "/tmp/a", "tags": "gpu"}, path=path
    )
    replaced = identity_mod.save_runner_config_overrides(
        {"workspace_root": "/tmp/b"}, path=path, merge=False
    )
    assert replaced == {"workspace_root": "/tmp/b"}


def test_sidecar_clear_removes_file(tmp_path: Path) -> None:
    path = tmp_path / "runner_config.json"
    identity_mod.save_runner_config_overrides(
        {"workspace_root": "/tmp/a"}, path=path
    )
    assert path.exists()
    identity_mod.clear_runner_config_overrides(path)
    assert not path.exists()


def test_sidecar_rejects_malformed_input(tmp_path: Path) -> None:
    path = tmp_path / "runner_config.json"
    path.write_text(json.dumps({"tags": 42}), encoding="utf-8")
    with pytest.raises(ValueError):
        identity_mod.load_runner_config_overrides(path)


def test_sidecar_drops_unknown_keys(tmp_path: Path) -> None:
    """Forward-compatibility: a sidecar written by a newer CLI must not
    crash an older runner."""
    path = tmp_path / "runner_config.json"
    path.write_text(
        json.dumps({"workspace_root": "/tmp/x", "future_knob": "value"}),
        encoding="utf-8",
    )
    loaded = identity_mod.load_runner_config_overrides(path)
    assert "future_knob" not in loaded
    assert loaded["workspace_root"] == "/tmp/x"


def test_runner_config_from_env_uses_sidecar_when_env_unset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "runner_config.json"
    identity_mod.save_runner_config_overrides(
        {
            "workspace_root": str(tmp_path),
            "tags": "gpu,west",
            "scope_prefixes": "docs/,tests/",
            "tenant": "team-alpha",
            "max_concurrent": 3,
            "poll_interval_seconds": 7.5,
        },
        path=path,
    )
    monkeypatch.setattr(identity_mod, "DEFAULT_RUNNER_CONFIG_PATH", path)
    # Make sure the env is clean.
    for var in (
        "FORGEWIRE_RUNNER_WORKSPACE_ROOT",
        "FORGEWIRE_RUNNER_TAGS",
        "FORGEWIRE_RUNNER_SCOPE_PREFIXES",
        "FORGEWIRE_RUNNER_TENANT",
        "FORGEWIRE_RUNNER_MAX_CONCURRENT",
        "FORGEWIRE_RUNNER_POLL_INTERVAL",
        "FORGEWIRE_RUNNER_VERSION",
    ):
        monkeypatch.delenv(var, raising=False)
    cfg = agent_mod.RunnerConfig.from_env()
    assert cfg.workspace_root == str(tmp_path)
    assert cfg.tags == ["gpu", "west", "kind:command"]
    assert cfg.scope_prefixes == ["docs/", "tests/"]
    assert cfg.tenant == "team-alpha"
    assert cfg.max_concurrent == 3
    assert cfg.poll_interval_seconds == 7.5


def test_runner_config_env_wins_over_sidecar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "runner_config.json"
    identity_mod.save_runner_config_overrides(
        {"workspace_root": str(tmp_path), "tags": "from-sidecar"},
        path=path,
    )
    monkeypatch.setattr(identity_mod, "DEFAULT_RUNNER_CONFIG_PATH", path)
    monkeypatch.setenv("FORGEWIRE_RUNNER_WORKSPACE_ROOT", str(tmp_path))
    monkeypatch.setenv("FORGEWIRE_RUNNER_TAGS", "from-env")
    cfg = agent_mod.RunnerConfig.from_env()
    assert cfg.tags == ["from-env", "kind:command"]


# ---------------------------------------------------------------------------
# Migration bundle: identity + config travel together
# ---------------------------------------------------------------------------


def test_bundle_round_trip_preserves_identity_and_config(tmp_path: Path) -> None:
    src_id = tmp_path / "src" / "runner_identity.json"
    src_cfg = tmp_path / "src" / "runner_config.json"
    src_id.parent.mkdir(parents=True)
    # Seed identity + sidecar on the source host.
    ident_a = identity_mod.load_or_create(src_id)
    identity_mod.save_runner_config_overrides(
        {
            "workspace_root": str(tmp_path / "ws"),
            "tags": "gpu,west",
            "scope_prefixes": "docs/,tests/",
            "tenant": "team-alpha",
            "max_concurrent": 2,
        },
        path=src_cfg,
    )
    (tmp_path / "ws").mkdir()
    bundle_path = tmp_path / "bundle.json"
    identity_mod.export_runner_bundle(
        destination=bundle_path,
        identity_source=src_id,
        config_source=src_cfg,
    )
    # Verify the bundle on disk is well-formed.
    payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    assert payload["schema"].startswith("forgewire-runner-bundle/")
    assert payload["identity"]["runner_id"] == ident_a.runner_id
    assert payload["config"]["tags"] == ["gpu", "west"]

    # Import on the "new hardware" location.
    dst_id = tmp_path / "dst" / "runner_identity.json"
    dst_cfg = tmp_path / "dst" / "runner_config.json"
    result = identity_mod.import_runner_bundle(
        bundle_path,
        identity_target=dst_id,
        config_target=dst_cfg,
    )
    assert result["runner_id"] == ident_a.runner_id
    assert result["config"]["tags"] == ["gpu", "west"]
    assert result["config"]["max_concurrent"] == 2
    # The destination identity file must match the source byte-for-byte
    # in the persisted fields (created_at may differ but the keys must
    # round-trip).
    on_disk = json.loads(dst_id.read_text(encoding="utf-8"))
    assert on_disk["runner_id"] == ident_a.runner_id
    assert on_disk["public_key"] == ident_a.public_key_hex


def test_bundle_import_accepts_legacy_bare_identity(tmp_path: Path) -> None:
    """A file produced by ``runner identity-export --no-bundle`` (the
    pre-bundle format) is just the identity record. The new bundle
    importer must still accept it for backward compatibility."""
    src = tmp_path / "src" / "runner_identity.json"
    src.parent.mkdir(parents=True)
    ident = identity_mod.load_or_create(src)
    legacy_export = tmp_path / "legacy.json"
    identity_mod.export_identity(destination=legacy_export, source=src)
    dst_id = tmp_path / "dst" / "runner_identity.json"
    dst_cfg = tmp_path / "dst" / "runner_config.json"
    result = identity_mod.import_runner_bundle(
        legacy_export,
        identity_target=dst_id,
        config_target=dst_cfg,
    )
    assert result["runner_id"] == ident.runner_id
    # No sidecar should have been written when the legacy format had no
    # config section.
    assert not dst_cfg.exists()
    assert result["config"] == {}


def test_bundle_import_refuses_overwrite_different_runner_id(tmp_path: Path) -> None:
    """The refuse-overwrite guard from ``import_identity`` must apply
    to bundle imports too: an operator carrying a bundle to a host that
    already has a *different* identity must explicitly --force."""
    src = tmp_path / "src" / "runner_identity.json"
    src.parent.mkdir(parents=True)
    identity_mod.load_or_create(src)
    bundle_path = tmp_path / "bundle.json"
    identity_mod.export_runner_bundle(
        destination=bundle_path, identity_source=src
    )
    # Pre-seed the destination with a *different* identity.
    dst_id = tmp_path / "dst" / "runner_identity.json"
    identity_mod.load_or_create(dst_id)
    with pytest.raises(RuntimeError, match="refusing to overwrite"):
        identity_mod.import_runner_bundle(
            bundle_path,
            identity_target=dst_id,
            config_target=tmp_path / "dst" / "runner_config.json",
        )


def test_bundle_import_force_overwrites(tmp_path: Path) -> None:
    src = tmp_path / "src" / "runner_identity.json"
    src.parent.mkdir(parents=True)
    src_ident = identity_mod.load_or_create(src)
    bundle_path = tmp_path / "bundle.json"
    identity_mod.export_runner_bundle(destination=bundle_path, identity_source=src)
    dst_id = tmp_path / "dst" / "runner_identity.json"
    identity_mod.load_or_create(dst_id)
    result = identity_mod.import_runner_bundle(
        bundle_path,
        identity_target=dst_id,
        config_target=tmp_path / "dst" / "runner_config.json",
        force=True,
    )
    assert result["runner_id"] == src_ident.runner_id


# ---------------------------------------------------------------------------
# install_runner seeds the sidecar
# ---------------------------------------------------------------------------


def test_install_runner_seeds_sidecar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``forgewire-fabric runner install --tags gpu --scope-prefixes docs/``
    must persist those values into the machine-wide sidecar so a later
    service reinstall that omits the flags does not silently downgrade
    the runner's routing capabilities."""
    ws = tmp_path / "workspace"
    ws.mkdir()
    sidecar = tmp_path / "machine" / "runner_config.json"
    monkeypatch.setenv(
        "FORGEWIRE_RUNNER_IDENTITY_PATH",
        str(sidecar.parent / "runner_identity.json"),
    )
    # Reload identity so DEFAULT_* paths pick up the override, then
    # rebind install.py's import to the reloaded module by patching the
    # platform shim out (so no NSSM/systemctl is invoked).
    importlib.reload(identity_mod)
    from forgewire_fabric import install as install_mod

    importlib.reload(install_mod)
    try:
        if sys.platform.startswith("win"):
            target = install_mod._windows_install_runner
        elif sys.platform.startswith("linux"):
            target = install_mod._linux_install_unit
        elif sys.platform == "darwin":
            target = install_mod._macos_install_plist
        else:
            pytest.skip("unsupported platform")
        with mock.patch.object(install_mod, target.__name__) as m:
            m.return_value = None
            install_mod.install_runner(
                hub_url="http://127.0.0.1:8765",
                hub_token="token",
                workspace_root=str(ws),
                tags="gpu,west",
                scope_prefixes="docs/,tests/",
                tenant="team-alpha",
                max_concurrent=2,
                poll_interval=4.0,
            )
        assert sidecar.exists()
        loaded = identity_mod.load_runner_config_overrides(sidecar)
        assert loaded["workspace_root"] == str(ws)
        assert loaded["tags"] == ["gpu", "west"]
        assert loaded["scope_prefixes"] == ["docs/", "tests/"]
        assert loaded["tenant"] == "team-alpha"
        assert loaded["max_concurrent"] == 2
        assert loaded["poll_interval_seconds"] == 4.0
    finally:
        monkeypatch.delenv("FORGEWIRE_RUNNER_IDENTITY_PATH", raising=False)
        importlib.reload(identity_mod)


# ---------------------------------------------------------------------------
# ``labels`` CLI: export/import round-trip
# ---------------------------------------------------------------------------


def test_labels_cli_export_import_round_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Operators rebuilding a hub from scratch must be able to back up
    the labels state out-of-band and replay it onto the new hub."""
    cfg = _make_cfg(tmp_path)
    app = create_app(cfg)

    runner_ids = [
        "33333333-3333-3333-3333-333333333333",
        "44444444-4444-4444-4444-444444444444",
    ]

    # Seed the source hub with a hub name + two aliases.
    with TestClient(app) as client:
        client.put(
            "/labels/hub",
            json={"name": "old-hub"},
            headers=_auth(),
        )
        for rid, alias in zip(runner_ids, ["alpha", "beta"], strict=True):
            client.put(
                f"/labels/runners/{rid}",
                json={"alias": alias},
                headers=_auth(),
            )
        client.put(
            "/labels/hosts/HOST-A",
            json={"alias": "Precision"},
            headers=_auth(),
        )

        # Run ``labels export`` against this in-memory hub by emulating
        # what the CLI does internally: GET /labels, wrap it in the
        # canonical envelope, and write the JSON file. We exercise the
        # ``labels import`` path through the Click runner below.
        export_path = tmp_path / "labels.json"
        labels = client.get("/labels", headers=_auth()).json()
        envelope = {"schema": "forgewire-labels-export/1", "labels": labels}
        export_path.write_text(
            json.dumps(envelope, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    # Stand up a *fresh* hub on a different DB and run the CLI ``labels
    # import`` against it. We patch the CLI's ``_client`` to return an
    # object that proxies the four methods used by the import code path
    # through the new TestClient.
    cfg2 = _make_cfg(tmp_path / "new")
    (tmp_path / "new").mkdir()
    app2 = create_app(cfg2)

    with TestClient(app2) as client2:
        class _Proxy:
            async def __aenter__(self) -> "_Proxy":
                return self

            async def __aexit__(self, *exc: object) -> None:
                return None

            async def get_labels(self) -> dict[str, Any]:
                return client2.get("/labels", headers=_auth()).json()

            async def set_hub_name(
                self, name: str, *, updated_by: str | None = None
            ) -> dict[str, Any]:
                payload: dict[str, Any] = {"name": name}
                if updated_by:
                    payload["updated_by"] = updated_by
                return client2.put(
                    "/labels/hub", json=payload, headers=_auth()
                ).json()

            async def set_runner_alias(
                self,
                runner_id: str,
                alias: str,
                *,
                updated_by: str | None = None,
            ) -> dict[str, Any]:
                payload: dict[str, Any] = {"alias": alias}
                if updated_by:
                    payload["updated_by"] = updated_by
                return client2.put(
                    f"/labels/runners/{runner_id}",
                    json=payload,
                    headers=_auth(),
                ).json()

            async def set_host_alias(
                self,
                hostname: str,
                alias: str,
                *,
                updated_by: str | None = None,
            ) -> dict[str, Any]:
                payload: dict[str, Any] = {"alias": alias}
                if updated_by:
                    payload["updated_by"] = updated_by
                return client2.put(
                    f"/labels/hosts/{hostname}",
                    json=payload,
                    headers=_auth(),
                ).json()

        monkeypatch.setattr(cli_mod, "_client", lambda: _Proxy())
        runner = CliRunner()
        result = runner.invoke(
            cli_mod.cli, ["labels", "import", str(export_path)]
        )
        assert result.exit_code == 0, result.output
        body = client2.get("/labels", headers=_auth()).json()
        assert body["hub_name"] == "old-hub"
        assert body["runner_aliases"] == {
            runner_ids[0]: "alpha",
            runner_ids[1]: "beta",
        }
        assert body["host_aliases"] == {"HOST-A": "Precision"}


def test_labels_cli_import_rejects_unknown_schema(tmp_path: Path) -> None:
    src = tmp_path / "bad.json"
    src.write_text(
        json.dumps(
            {
                "schema": "from-the-future/9",
                "labels": {"hub_name": "x", "runner_aliases": {}, "host_aliases": {}},
            }
        ),
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(cli_mod.cli, ["labels", "import", str(src)])
    assert result.exit_code != 0
    assert "unknown labels schema" in result.output.lower()


def test_labels_cli_import_accepts_bare_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An operator hand-editing a labels JSON without the schema envelope
    should still be able to import it."""
    cfg = _make_cfg(tmp_path)
    app = create_app(cfg)
    bare_path = tmp_path / "bare.json"
    bare_path.write_text(
        json.dumps(
            {
                "hub_name": "bare-hub",
                "host_aliases": {"HOST-A": "Precision"},
                "runner_aliases": {
                    "55555555-5555-5555-5555-555555555555": "bare-alias"
                },
            }
        ),
        encoding="utf-8",
    )
    with TestClient(app) as client:
        class _Proxy:
            async def __aenter__(self) -> "_Proxy":
                return self

            async def __aexit__(self, *exc: object) -> None:
                return None

            async def get_labels(self) -> dict[str, Any]:
                return client.get("/labels", headers=_auth()).json()

            async def set_hub_name(
                self, name: str, *, updated_by: str | None = None
            ) -> dict[str, Any]:
                return client.put(
                    "/labels/hub", json={"name": name}, headers=_auth()
                ).json()

            async def set_runner_alias(
                self,
                runner_id: str,
                alias: str,
                *,
                updated_by: str | None = None,
            ) -> dict[str, Any]:
                return client.put(
                    f"/labels/runners/{runner_id}",
                    json={"alias": alias},
                    headers=_auth(),
                ).json()

            async def set_host_alias(
                self,
                hostname: str,
                alias: str,
                *,
                updated_by: str | None = None,
            ) -> dict[str, Any]:
                return client.put(
                    f"/labels/hosts/{hostname}",
                    json={"alias": alias},
                    headers=_auth(),
                ).json()

        monkeypatch.setattr(cli_mod, "_client", lambda: _Proxy())
        runner = CliRunner()
        result = runner.invoke(cli_mod.cli, ["labels", "import", str(bare_path)])
        assert result.exit_code == 0, result.output
        body = client.get("/labels", headers=_auth()).json()
        assert body["hub_name"] == "bare-hub"
        assert body["runner_aliases"] == {
            "55555555-5555-5555-5555-555555555555": "bare-alias"
        }
        assert body["host_aliases"] == {"HOST-A": "Precision"}


# ---------------------------------------------------------------------------
# /cluster/health endpoint (Hosts sidebar)
# ---------------------------------------------------------------------------


def test_cluster_health_sqlite_reports_backend_and_sidecar(tmp_path: Path) -> None:
    """The ``/cluster/health`` endpoint must report the active backend
    and labels-snapshot sidecar status so the vsix Hosts view can flag
    a stale or missing sidecar without rereading the file itself."""
    snapshot_path = tmp_path / "labels.snapshot.json"
    cfg = BlackboardConfig(
        db_path=tmp_path / "hub.sqlite3",
        token=HUB_TOKEN,
        host="127.0.0.1",
        port=0,
        labels_snapshot_path=snapshot_path,
    )
    app = create_app(cfg)
    with TestClient(app) as client:
        # Trigger a write so the sidecar exists.
        client.put("/labels/hub", json={"name": "ClusterHealth Test"}, headers=_auth())
        body = client.get("/cluster/health", headers=_auth()).json()
        assert body["backend"] == "sqlite"
        assert body["rqlite"] is None
        sidecar = body["labels_snapshot"]
        assert sidecar["status"] in ("absent", "seeded_from_db", "applied", "disabled")
        assert sidecar["path"] == str(snapshot_path)
        # File now exists thanks to the write-through.
        assert sidecar["exists"] is True
        assert isinstance(sidecar["size_bytes"], int) and sidecar["size_bytes"] > 0
        assert isinstance(sidecar["mtime"], float)


def test_cluster_health_unauthorized(tmp_path: Path) -> None:
    cfg = BlackboardConfig(
        db_path=tmp_path / "hub.sqlite3",
        token=HUB_TOKEN,
        host="127.0.0.1",
        port=0,
    )
    app = create_app(cfg)
    with TestClient(app) as client:
        r = client.get("/cluster/health")
        assert r.status_code == 401

