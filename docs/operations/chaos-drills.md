# Chaos drills — rqlite cluster

These drills validate that the 3-voter rqlite Raft cluster behaves
correctly under common failure modes. Run them periodically (or after
substantive cluster changes) and archive the resulting JSONL log.

## Topology under test

| Node | Host | API | Role |
|---|---|---|---|
| node1-optiplex | 10.120.81.95 | :4001 | voter |
| node3-witness  | 10.120.81.95 | :4011 | voter (witness on same host) |
| node2-dell     | 10.120.81.56 | :4001 | voter |

Leader is whichever Raft elected last. node3-witness is a full voter
sharing the OptiPlex host — quorum is 2 of 3.

## Drills

The runner is [`scripts/dr/chaos_drills.ps1`](../../scripts/dr/chaos_drills.ps1).
It writes one JSONL record per phase to `logs/chaos/chaos.<UTC>.jsonl`.

```powershell
# Drills that involve OptiPlex services use the SSH alias 'forgewire'.
pwsh -File scripts\dr\chaos_drills.ps1 -Drills 'kill-leader,lose-quorum'
```

### Drill 1 — kill leader

1. Identify the current leader via `GET /nodes`.
2. Stop its Windows service (`Stop-Service` over SSH if remote).
3. Poll the surviving nodes' `/nodes` until a new `leader: true` voter
   appears that isn't the old leader. Record elapsed time.
4. Issue a write to the new leader; expect 200.
5. Restart the old service; wait until `/nodes` reports it `reachable`.

**Pass criteria:** new leader within 30 s; post-failover write succeeds;
old node rejoins as a follower.

### Drill 2 — lose quorum

1. Stop two of three voters (Node1 + Node3 on OptiPlex).
2. Issue a write to the surviving Node2; expect non-2xx (no quorum).
3. Issue a query at `consistency=none`; expect success (local raft log).
4. Restart Node3 → quorum restored at 2/3.
5. Issue a write; expect 200.
6. Restart Node1; verify `/nodes` shows all three reachable.

**Pass criteria:** writes refused while quorum is lost; weak reads
served from the survivor; quorum and writes recover at 2/3.

### Drill 3 — partition recovery (manual)

> Not run automatically because it touches the local Windows Firewall
> and assumes the Dell host can be temporarily isolated from OptiPlex
> without operator impact.

1. `New-NetFirewallRule -DisplayName ForgeWireChaosBlockOptiPlex
   -Direction Outbound -Action Block -RemoteAddress 10.120.81.95`.
2. From Dell, observe `/nodes` at `http://10.120.81.56:4001` — Node1
   and Node3 should go `reachable: false` after a few seconds.
3. Remove the firewall rule.
4. Wait for the cluster to reform. Expect a leader visible on all
   nodes again within ~10 s.

## Recorded outcomes (2026-05-08 UTC)

Live run on the production 3-voter cluster against `v10.0.3`.

### Drill 1 — kill leader

| Step | Outcome |
|---|---|
| Initial leader | `node1-optiplex` |
| Stop service over SSH | 1.997 s |
| New leader elected | `node3-witness` in **4.105 s** |
| Post-failover write | `200 OK`, `last_insert_id=1` |
| Old node rejoin | reachable again **26 ms** after `Start-Service` returned |

### Drill 2 — lose quorum

| Step | Outcome |
|---|---|
| Stop Node1 + Node3 | success |
| Write attempt against Node2 | **HTTP 500** (no quorum — correct) |
| Read at `consistency=none` | served `COUNT(*) = 1` (the d1 row) |
| Restart Node3 → quorum | `node2-dell` elected leader in 9.170 s |
| Write after restore | `200 OK`, `last_insert_id=2` |
| Restart Node1 | all three nodes reachable, cluster fully restored |

Strong-consistency `SELECT * FROM chaos_drill` on the new leader after
the drill ended returned both rows (`d1-after-failover`,
`d2-after-quorum-restored`), confirming Raft preserved both commits
across the failover and the quorum-loss window.

## Log retention

Chaos logs live at `logs/chaos/chaos.<UTC>.jsonl` and are not
auto-pruned. Treat them like any other operational log artifact and
ship them to the central log store if available.
