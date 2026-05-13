# Changelog

All notable changes to **forgewire-fabric** are tracked here. Format roughly
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project
uses [semantic versioning](https://semver.org/spec/v2.0.0.html) for the Python
package. The VSIX (`vscode/`) is versioned independently.

## [0.13.0] - 2026-05-13

### Added

- **Secret broker end-to-end** on the hub:
  - `POST /secrets`, `GET /secrets`, `DELETE /secrets/{name}` — auth-gated
    put/rotate/list/delete. Put-or-rotate is path-collapsed: existing names
    rotate, new names create. Values are never echoed; list returns metadata
    only (`name`, `version`, `created_at`, `last_rotated_at`).
  - Per-task `secrets_needed` column in the tasks schema. Dispatch records
    the requested secret **names** (never values) in the audit log.
  - `claim-v2` flow resolves `secrets_needed` against the broker and injects
    resolved values into the runner-side claim payload.
  - **Redaction** in `submit_result` / stream-append / progress paths:
    `log_tail` and `error` fields are scanned for secret values and replaced
    with `***SECRET:<name>***` markers before persistence.
  - `BlackboardClient` gained `put_secret`, `rotate_secret`, `list_secrets`,
    `delete_secret`, `resolve_secrets`.
  - CLI `forgewire-fabric secrets {put,rotate,list,delete}` group.
- **Live smoke script** `scripts/live_smoke_secrets.py` covering put → rotate
  → list-redaction → dispatch-with-secret → submit-with-redaction → cleanup.
  Validated against the OptiPlex 7050 hub (10.120.81.95:8765) on 2026-05-13.

### Internal

- `tests/hub/test_secret_broker.py` — 21 tests covering put/rotate/delete
  semantics, redaction substring matching, name-only audit recording,
  unknown-secret rejection at claim time, and version monotonicity.
- Full suite: **208 passed, 12 skipped** (0.12.0 baseline: 71 passed; the
  delta reflects expanded coverage across secret broker + adjacent paths
  that were previously thinner).
- `ops(install): resync bundled nssm-install-runner.ps1 with canonical script`
  — drift caught by `test_installer_assets_in_sync`; bundled installer asset
  now matches `scripts/install/nssm-install-runner.ps1` at commit `7a2b346`.

## [0.12.0] - 2026-05-13

### Added

- **Deregister endpoints** on the hub:
  - `DELETE /runners/{runner_id}` — removes a runner registration. Tasks with
    a dangling `worker_id` are intentionally preserved for audit replay.
  - `DELETE /dispatchers/{dispatcher_id}` — removes a dispatcher registration
    and also clears the `host_roles[dispatch]` row when no other dispatcher
    remains on that hostname. Prevents ghost host rows in `/hosts`.
  - Both endpoints are auth-gated and idempotent (re-delete returns 404).
- **`kind:agent` runner** + interactive approval roundtrip (`a59f303`). Adds a
  self-driving runner kind that participates in the claim → start → progress
  → result cycle while gated on approval, plus the live smoke harness at
  `scripts/live_smoke_approvals.py` exercising both `kind:agent` and
  `kind:command` end-to-end.
- **`ForgeWireAgentRunner` NSSM service installer** (`b3057e4`) and a remote
  wrapper (`4704361`) so a single command stands up the agent-runner kind
  on a Windows host alongside the existing command runner.
- **`package_version`** field on `/healthz` as an explicit alias for the
  existing `version` field. Clients can now read the hub's package version
  without guessing what `version` refers to.

### Changed

- **Routes package split** (`1bae1db`): hub HTTP routes moved from
  `forgewire_fabric.hub.server` into per-domain `forgewire_fabric.hub.routes.*`
  `APIRouter` modules (`admin`, `approvals`, `audit`, `auth`, `cluster`,
  `runners`, `secrets`, `streams`, `tasks`). The public route surface is
  byte-identical and pinned by `tests/hub/test_routes_layout.py`.
- **NSSM start-loop hardening** (`7a2b346`): runner services no longer
  thrash when the hub is briefly unreachable on boot.
- **`live_smoke_approvals.py`** now deregisters its own ephemeral runner +
  dispatcher in `_cleanup`, so repeated runs no longer accumulate ghost
  host rows.

### Fixed

- Ghost host rows (`live-approval-smoke`, `live-agent-approval-smoke`) that
  accumulated on every smoke run because the hub had no deregister path.
  Existing rows on long-lived hubs can now be removed with
  `DELETE /runners/{id}` and `DELETE /dispatchers/{id}`.

### Internal

- `Blackboard.delete_runner` and `Blackboard.delete_dispatcher` added to the
  persistence layer with the host-roles cleanup invariant noted above.
- 4 new tests in `tests/hub/test_host_summaries.py` cover the deregister
  paths (success, idempotency, auth, host-row cleanup).

---

## [0.11.6] and earlier

Pre-changelog releases — see `git log` for full history. Notable milestones:

- `0.11.6` — `c986074` `fix(hub): M2.6.3 preserve exception causes`
- `M2.6.4` — `f3628ff` startup migrated to FastAPI `lifespan`
- `M2.6.2` — `bd7215d` ruff floor added
- Earlier: dispatcher host-role registration, host-role summaries,
  machine-label promotion, rqlite cluster path, runner v2 protocol.
