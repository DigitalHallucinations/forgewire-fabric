# Disaster recovery — rqlite backups

ForgeWire's rqlite cluster is the consensus tier for hub state. Raft
already replicates writes across the voters, so a backup is **not**
required for normal availability — the cluster survives any single
voter failure. Backups exist for catastrophic-failure DR (whole-site
loss, accidental `/db/load` of bad data, ransomware, etc.).

This page describes the **generic** backup setup that any ForgeWire
host (hub, runner, dedicated DR box) can install.

## Topology source of truth

`config/cluster.yaml` lists every voter, their priority, and a default
preferred control node. Every script in `scripts/dr/` reads it; adding
or removing a voter is one config edit + a `git pull` on each host.

```yaml
voters:
  - label: node1
    host: 10.120.81.95
    port: 4001
    priority: 1
  - label: witness
    host: 10.120.81.95
    port: 4011
    priority: 2
  - label: node2
    host: 10.120.81.56
    port: 4001
    priority: 3

preferred_node: node1
backups:
  root: 'C:\ProgramData\forgewire\rqlite-backups'
  cadence_minutes: 5
  retention_hours: 24
```

## Preferred node + failover

Each host picks one voter as its preferred control node. The DR
script tries that voter first, then falls back to the rest of the
list **in priority order**. Followers are valid targets because
rqlite's `/db/backup?redirect=true` 301s to the leader internally.

Order of resolution:

1. `-PreferredNode <label>` on the script.
2. `$env:FORGEWIRE_PREFERRED_NODE`.
3. `cluster.yaml` → `preferred_node`.

A host's preferred node **does not** have to be the cluster leader.
Pick whichever voter is on the same LAN segment as the host running
the backup — the script will still succeed if it's a follower because
of the redirect.

## Install on a new host

1. Clone or pull `forgewire-fabric` to a known location (e.g.
   `C:\Projects\forgewire-fabric`).
2. Verify `config/cluster.yaml` matches your topology. If this host
   should prefer a non-default voter, either edit `preferred_node`
   for the whole cluster or pass `-PreferredNode` at install time.
3. Run the installer (it self-elevates):

   ```powershell
   pwsh -File scripts\dr\install_rqlite_backup_task.ps1 `
       -PreferredNode node2          # optional
   ```

The installer:

- Registers a Windows Task Scheduler job named `ForgeWireRqliteBackup`
  running as `SYSTEM` at the configured cadence (default 5 min).
- Points it at `scripts\dr\backup_rqlite.ps1` with the chosen
  preferred-node label and the configured backup root.
- Replaces any existing task with the same name (idempotent).

## What the script writes

```
<BackupRoot>\YYYYMMDD-HHmmss.sqlite3   # one per cadence tick
<BackupRoot>\backup.log.jsonl          # one JSON line per attempt
```

Each backup is verified against the SQLite magic bytes before the
`.partial` file is renamed into place, so a failed transfer never
leaves a corrupt blob in the rotation.

## Restore

To rehydrate a fresh rqlite cluster from a backup:

```powershell
$body = [IO.File]::ReadAllBytes("C:\ProgramData\forgewire\rqlite-backups\20260507-153000.sqlite3")
Invoke-RestMethod -Method Post `
    -Uri "http://<leader>:4001/db/load" `
    -ContentType "application/octet-stream" `
    -Body $body
```

Or via the hub's authenticated `/state/import` endpoint if the hub is
already up against an empty cluster (it proxies to `/db/load` under
the rqlite backend; see [`server.py`](../../python/forgewire_fabric/hub/server.py)).

## Manual run / verify

```powershell
# Run once, against the default chain.
pwsh -File scripts\dr\backup_rqlite.ps1

# Run with an explicit, ordered chain.
pwsh -File scripts\dr\backup_rqlite.ps1 `
    -Nodes "node1=10.120.81.95:4001,node2=10.120.81.56:4001"

# Trigger the scheduled task on demand.
Start-ScheduledTask -TaskName ForgeWireRqliteBackup
Get-ScheduledTaskInfo -TaskName ForgeWireRqliteBackup
```

The `backup.log.jsonl` file contains one JSON record per attempt
(per-voter warnings and a final summary record), suitable for
shipping to any log aggregator.
