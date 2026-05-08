# `/state/snapshot` and `/state/import` — parity-only endpoints

> **Status:** PARITY-ONLY. Under the production rqlite backend, these
> endpoints are kept as exit hatches and are not part of the routine
> DR or failover paths.

## Why they exist

These endpoints predate the rqlite consensus tier. In the legacy
single-node SQLite deployment they were the only way to:

1. Snapshot the hub's blackboard for offline DR.
2. Bootstrap a freshly-promoted standby hub from a snapshot.

Both responsibilities now live in the Raft cluster:

| Concern | Legacy path | Current primary path |
|---|---|---|
| Routine backup | scheduled `GET /state/snapshot` | scheduled `GET /db/backup?redirect=true` against rqlite voters via [`scripts/dr/backup_rqlite.ps1`](../../scripts/dr/backup_rqlite.ps1) |
| Hub failover | manual "promote" + `POST /state/import` | rqlite Raft elects a new leader transparently; hubs are stateless against the cluster |
| Bulk restore from DR | `POST /state/import` | rqlite-native `POST /db/load` |
| Cross-region replication | snapshot ship | rqlite voters / read-only learners |

See [dr-rqlite-backups.md](dr-rqlite-backups.md) for the operational
DR runbook.

## When the parity endpoints are still acceptable

The endpoints remain authenticated and functional for:

- **Legacy `--backend sqlite` deployments.** Single-node hubs without
  an rqlite cluster still depend on this path for any DR at all.
- **Bootstrapping an empty rqlite cluster** from a DR backup. Under
  `--backend rqlite`, `POST /state/import` proxies straight to the
  cluster's `/db/load` and is a thin convenience wrapper around it.
- **Authenticated, network-restricted operators** who can reach the
  hub's HTTPS port but not the rqlite voters' HTTP ports directly.
- **One-shot exit-hatch dumps** when migrating *off* rqlite (e.g.
  rolling back to single-node SQLite for forensics).

## When they MUST NOT be used

- **New automation.** Any new scheduled job, runner script, or CI
  hook should target the rqlite cluster directly. The parity
  endpoints are not guaranteed to remain in future major versions.
- **Production failover orchestration.** Raft handles leader election
  and there is no "promote a hub" step in the rqlite topology — every
  hub points at the cluster and the cluster decides who serves writes.
- **High-frequency snapshots.** `/state/snapshot` under sqlite uses
  `VACUUM INTO` which holds a read transaction; running it on a busy
  hub will pile up. Use the rqlite-native path which is offloaded to
  followers via the redirect chain.

## Safety contract that still applies

`/state/import` retains its existing safety check: it refuses to
overwrite a hub that has claimed any tasks since startup unless the
operator sends `X-Force: 1`. This applies under both backends.

## Removal timeline

There is **no** scheduled removal of these endpoints. They are
demoted, not deprecated for deletion. The next major version may add
a `405 Method Not Allowed` response under `--backend rqlite` if
operator opt-out becomes desirable, but until then the only change is
the docstring contract documented here.
