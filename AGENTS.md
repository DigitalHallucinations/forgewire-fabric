# AGENTS.md — forgewire-fabric (root)

> **Audience.** Any agent (human or LLM) editing this repository. This
> file is the *how*: invariants, supervision discipline, and shipping
> conventions that survive milestone churn. For the *what* — milestone
> slicing, roadmap, phase docs — see todo 114 in the sibling repo at
> [`forgewire/todos/114-forgewire-fabric/`](../forgewire/todos/114-forgewire-fabric/).
>
> The thesis at [`forgewire/docs/thesis.md`](../forgewire/docs/thesis.md)
> is binding here: **graceful degradation, parity paths, audit trails,
> ownership boundaries, and substrate replaceability** are the
> properties that determine whether this code is useful. A request to
> weaken any of them must be flagged before implementation.

---

## 1. Repo identity & boundaries

- **This repo (`forgewire-fabric`)** owns: the hub server, runner
  agent, CLI, installer scripts, VS Code extension, and Rust crates
  under `crates/`.
- **Sibling repo (`forgewire`)** owns: substrate
  (`forgewire_core/**`), planning surface (`todos/`), and the binding
  thesis. Substrate is **read-only from this repo's perspective**.
  Never `pip install -e ../forgewire/...` into this venv; never import
  from `forgewire_core` here.
- **Two checkouts, one developer.** If you find yourself editing
  `C:\Projects\forgewire\python\forgewire_fabric\...`, stop. That tree
  is stale planning content — implementation lives only here.

---

## 2. Source-of-truth hierarchy

When two answers disagree, take the higher item:

1. The live hub's `/healthz` and `/audit/tail` responses.
2. `git log` on `main` in this repo.
3. This `AGENTS.md` and the per-area `AGENTS.md` files under it.
4. The phase docs in `forgewire/todos/114-forgewire-fabric/`.

The version reported by `/healthz` **must** match
`python/forgewire_fabric/__init__.py.__version__`. If they disagree the
last deploy was stale; fix that before doing anything else.

---

## 3. Frozen surface — never edit from this repo

- `forgewire_core/**` (does not live here; never re-introduce a copy).
- `.github/workflows/**` (CI changes require human review).
- The v2 signed canonical payload schema:
  `{op, dispatcher_id, title, prompt, scope_globs, base_commit,
  branch, timestamp, nonce}`. New brief fields go **out-of-band of the
  signature** (precedent: `required_capabilities`, `secrets_needed`,
  `network_egress`). Touching the canonical breaks every existing
  signed dispatcher.

---

## 4. Installer two-copy mirror — non-negotiable

The repo ships installer scripts twice. They drift silently if you
edit only one.

| Edit here (source of truth)         | Mirror copy (shipped in wheel)                         |
|-------------------------------------|--------------------------------------------------------|
| `scripts/install/*.ps1`             | `python/forgewire_fabric/_installer_assets/*.ps1`      |

Workflow:

1. Edit `scripts/install/<file>.ps1`.
2. Run `pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/dev/sync_installer_assets.ps1`.
3. Run `pytest tests/test_installer_assets_in_sync.py -q` — must be all green.

That test file is also where you add **content drift-guards** for any
new installer contract (e.g. required parameter names, required path
fragments). The pattern is `assert "<token>" in body` with a comment
explaining why losing the token is a real bug. See the existing
`test_runner_installer_exposes_hub_ssh_failover_params` and
`test_watchdogs_use_system_reachable_pwsh_host` for shape.

---

## 5. OOTB-or-nothing wiring rule

If a feature requires manual per-host setup *after*
`forgewire-fabric setup` or `forgewire-fabric runner install`, it is
not shipped. The full chain must be wired end-to-end:

```
CLI click option
  → python/forgewire_fabric/cli.py command kwarg
    → python/forgewire_fabric/install.py install_runner/install_hub kwarg
      → _windows_install_runner / _windows_install_hub passes -Param
        → scripts/install/<name>.ps1 receives [string]$Param
          → on-disk artifact (NSSM service, scheduled task, ACL'd file)
```

Drift-guard the final hop with a `test_installer_assets_in_sync.py`
content test (see §4). The CLI hop is covered by Click's own type
system; the Python hop is covered by signature kwargs with defaults
so existing callers don't break.

---

## 6. Service supervision & reboot recovery

The fabric runs as **NSSM** services on every node. Reboot recovery is
part of the product, not an afterthought.

### Hard-won rules

- **NSSM only sees process death, not loop death.** An asyncio runner
  can lose its event loop (`anyio.NoEventLoopError`) and still hold a
  live PID with frozen logs. NSSM will never restart it. Liveness must
  be probed *from outside the process* — the runner watchdog probes
  the hub's `/runners` view and restarts the local NSSM service when
  this host's `last_heartbeat` is stale > 120s for 3 consecutive
  minutes.
- **Scheduled-task `-Execute` must resolve to a SYSTEM-reachable
  path.** Bare `"pwsh.exe"` picks up the Microsoft Store install
  under `C:\Program Files\WindowsApps\`, which SYSTEM cannot launch
  (`ERROR_FILE_NOT_FOUND`). The watchdog *appears* installed but
  never fires. Resolve `$psHost` at install time in order:
  1. `$env:ProgramFiles\PowerShell\7\pwsh.exe`
  2. `${env:ProgramFiles(x86)}\PowerShell\7\pwsh.exe`
  3. `$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe`

  Drift-guarded by `test_watchdogs_use_system_reachable_pwsh_host`.
- **Validate by `LastTaskResult`, not by existence.**
  `Get-ScheduledTaskInfo -TaskName ForgeWire*Watchdog` and assert
  `LastTaskResult -eq 0`. `267011` ("task has not yet run") on a
  scheduled task that should have fired is the symptom of the
  bare-`pwsh.exe` bug above.

### Cross-host hub watchdog (mutual failover)

The hub is a single point of failure unless every peer can restart it.
Every fabric node carries a `ForgeWireHubWatchdog` that probes the
hub's `/healthz` and, on sustained failure, restarts the hub service —
locally if the hub lives on this host, or **over SSH on a peer host**
otherwise.

Cross-host restart requires all of:

1. SSH key staged under
   `C:\ProgramData\forgewire\ssh\hub-restart.ed25519`. ACL locked to
   `NT AUTHORITY\SYSTEM` + `BUILTIN\Administrators` FullControl with
   inheritance disabled.
2. `known_hosts` at
   `C:\ProgramData\forgewire\ssh\known_hosts`, seeded at install time
   with `ssh-keyscan -T 5 -t ed25519,rsa,ecdsa <hub-host>`.
3. `ssh.exe` resolved from `$env:SystemRoot\System32\OpenSSH\ssh.exe`
   (do not rely on `$PATH` under SYSTEM). Flags:
   `-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10`.
4. Remote command:
   `powershell -NoProfile -Command "Restart-Service $RemoteServiceName -Force"`.

### OOTB chain (must remain intact)

```
forgewire-fabric setup --role runner \
  --hub-url http://<hub>:8765 \
  --hub-ssh-host <hub> --hub-ssh-user <user> \
  --hub-ssh-key-file <path-to-key>
```

→ `install_runner(hub_ssh_*=...)`
→ `nssm-install-runner.ps1 -HubSshHost ... -HubSshUser ... -HubSshKeyFile ...`
→ key staging + ACL + `ssh-keyscan`
→ `install-hub-watchdog.ps1 -SshHost ... -SshUser ... -SshKeyFile ... -RemoteServiceName ForgeWireHub`

`--role hub-and-runner` auto-suppresses the cross-host watchdog (local
hub watchdog already covers it). `--no-hub-watchdog` is the explicit
opt-out.

### Backfill for existing hosts

When you add a new supervisor or guard to the install chain, hosts
that were installed before the change will *not* acquire it
passively. Every milestone that adds a supervisor must include a
one-liner in its ship report that brings legacy hosts into compliance,
runnable against an SSH alias.

---

## 7. Hard-won bug rules

These are bugs we've already paid for. Don't pay again.

- **rqlite has no cross-statement transactions.** Each HTTP request is
  the transaction boundary. `BEGIN IMMEDIATE` + `SELECT` + `UPDATE`
  silently fails. Use single-statement
  `UPDATE ... WHERE id = (SELECT ... LIMIT 1) RETURNING id` for atomic
  claim-style operations.
- **No `SELECT` inside `BEGIN/COMMIT`** under the rqlite shim
  (`python/forgewire_fabric/hub/_rqlite_db.py` forbids it). Audit
  appenders must not wrap reads in transactions.
- **`datetime('now')` returns local time on rqlite.** Always pass an
  explicit UTC ISO string via `_now_iso()`.
- **Audit chain invariant.**
  `event_id_hash = sha256(prev_hash || kind || canonical_json(payload))`,
  `AUDIT_GENESIS_HASH = "0" * 64`. Tampering with any payload byte
  must break the chain on read — keep `verify_audit_chain` covered.
- **Column-order discipline.** When you add a column to an
  `upsert_runner`-style statement, audit the bind tuple end-to-end
  before running tests. The #1 source of "tests register but nothing
  matches" symptoms.
- **Route ordering matters in FastAPI.** Specific paths like
  `/tasks/waiting` must register *before* parameterized siblings like
  `/tasks/{task_id:int}`, or path-param validation 422s first.
- **Additive migrations only.** `_migrate_v2_columns` adds columns
  with defaults. No drops, no renames, no type changes. To rename:
  add new column, dual-write, deprecate old, drop only in a major
  bump.

---

## 8. Tests: the no-mocks rule

`tests/AGENTS.md` (this repo) bans mocking. In practice:

- Hub tests build a real FastAPI app:
  `TestClient(create_app(cfg))` with `BlackboardConfig(db_path=tmp/'b.db', ...)`.
- v2 signed flows: real `runner.identity.load_or_create(tmp/'id.json')`
  + real `sign_payload(ident, body)`. Nonces via
  `secrets.token_hex(16)`; timestamps via `int(time.time())`.
- A lightweight in-process fake is allowed *only* when the external
  service is genuinely unreachable (e.g. an OS keychain in CI).
  Document the exemption in the test docstring.
- Floor: the suite has been at **178 passed / 12 skipped** as of the
  installer OOTB landing. New tests must add to the passed count,
  never decrease it.

---

## 9. Commit & version conventions

- **Branch:** `main`. Direct commits are fine for milestone slices;
  reserve branches for risky refactors that need bisecting room.
- **Version bumps:** feature → minor (`0.X.0 → 0.(X+1).0`); bug-fix
  follow-up on the same milestone → patch. Bump
  `python/forgewire_fabric/__init__.py.__version__` and
  `pyproject.toml.version` in the **same commit**.
- **Commit body shape:**

  ```
  feat(<scope>): M2.5.X <one-line summary>

  <one-paragraph context>

  Hub:
  - bullet
  Runner:
  - bullet
  CLI:
  - forgewire-fabric <new command>
  Client:
  - BlackboardClient.<new_method>()

  Tests: N new tests — N passed / M skipped.
  Version bump <old> -> <new>.
  ```
- **One push per milestone close.** Group fix-ups during a milestone
  into amends or fresh commits, but never `--force` history that's
  already been deployed.

---

## 10. When to escalate

The thesis is binding. Stop and ask before:

- Weakening the audit chain invariant or making audit "best-effort"
  in a way that changes the chain shape.
- Removing a `_rust` / `_py` parity pair.
- Bypassing `require_auth` on any new endpoint.
- Storing secret **values** in the audit chain (names only).
- Touching anything in §3.
- Changing the v2 canonical payload shape.
- Dropping the cross-host hub watchdog from the OOTB chain in §6.
- Skipping the installer asset mirror in §4.

---

## 11. Pointers

- **Milestones / roadmap:** [`forgewire/todos/114-forgewire-fabric/`](../forgewire/todos/114-forgewire-fabric/)
- **Phase doc (operator control plane):**
  [`phase-2.5-operator-control-plane.md`](../forgewire/todos/114-forgewire-fabric/phase-2.5-operator-control-plane.md)
- **Test policy:** [`tests/AGENTS.md`](tests/AGENTS.md)
- **Live smoke pattern:** any `scripts/live_smoke_*.py` already in
  the repo is a copyable template.

Keep this file terse. A long AGENTS.md is a stale AGENTS.md.
