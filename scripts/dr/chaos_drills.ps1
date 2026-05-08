<#
.SYNOPSIS
  Chaos drills against the rqlite cluster (topology-driven).

.DESCRIPTION
  Drives controlled failure modes against the cluster declared in
  config/cluster.yaml and records structured JSONL outcomes:

    * kill-leader        : portable. POSTs /leader to the current leader's
                           HTTP API to trigger a voluntary stepdown, waits
                           for the new election, writes to the new leader,
                           and verifies the old node is back as a
                           follower. No host access required — works
                           cluster-wide over the rqlite HTTP API alone.
    * kill-leader-hard   : stop the current leader's Windows service,
                           time Raft re-election, write to the new leader,
                           verify the old node rejoins. Requires the
                           leader's host to be local to this driver (or
                           reachable via voter.ssh_alias from SYSTEM).
                           Skipped with a hint otherwise.
    * lose-quorum        : stop two of three voters, verify writes are
                           refused on the survivor, restore one node
                           and verify quorum + writes recover.
    * partition-recovery : (manual / opt-in) firewall-block the remote
                           voters from this host, verify local view,
                           then unblock and verify cluster reforms.

  Topology and service mappings come from config/cluster.yaml. Each
  voter entry must declare:
    * label, node_id, host, port
    * service     (Windows service name on its host)
    * ssh_alias   ('local' if running on this host, else SSH alias)

  Output:
    - Console: human-readable progress (per phase).
    - <LogDir>\chaos.<UTC>.jsonl: one JSON record per phase with timings.
    - Old logs pruned past -RetentionDays (default 30).

  Exit code 0 on completion; 2 if no leader was visible at start;
  per-phase failures are logged but do not fail the run.

.PARAMETER ConfigPath
  Path to cluster.yaml. Default: <repo>\config\cluster.yaml.

.PARAMETER LogDir
  JSONL output directory. Default: cfg.chaos.log_root or <repo>\logs\chaos\.

.PARAMETER Drills
  Comma-separated list of drills to run. Default: cfg.chaos.drills.
  Available: kill-leader, lose-quorum, partition-recovery.

.PARAMETER RetentionDays
  Prune chaos.*.jsonl files older than this. Default: cfg.chaos.retention_days or 30.
#>

[CmdletBinding()]
param(
    [string]$ConfigPath,
    [string]$LogDir,
    [string]$Drills,
    [Nullable[int]]$RetentionDays
)

$ErrorActionPreference = 'Stop'

# ---- Self-elevation -------------------------------------------------------
# Stop-Service / Start-Service against Windows services require Administrator.
# When invoked under Task Scheduler as SYSTEM this is already satisfied; for
# interactive runs we relaunch elevated so manual smoke tests work.
$identity  = [System.Security.Principal.WindowsIdentity]::GetCurrent()
$principal = [System.Security.Principal.WindowsPrincipal]::new($identity)
if (-not $principal.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)) {
    $shellExe = (Get-Process -Id $PID).Path
    $argList = @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $PSCommandPath)
    foreach ($kv in $PSBoundParameters.GetEnumerator()) {
        if ($null -eq $kv.Value) { continue }
        $argList += "-$($kv.Key)"
        $argList += [string]$kv.Value
    }
    Start-Process -FilePath $shellExe -ArgumentList $argList -Verb RunAs -Wait
    return
}

. (Join-Path $PSScriptRoot '_cluster_config.ps1')

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$cfg = Get-ForgeWireClusterConfig -Path $ConfigPath
$chaosCfg = if ($cfg.chaos) { $cfg.chaos } else { @{} }

if (-not $LogDir) {
    if ($chaosCfg.log_root) { $LogDir = [string]$chaosCfg.log_root }
    else { $LogDir = Join-Path $repoRoot 'logs\chaos' }
}
if (-not $Drills) {
    if ($chaosCfg.drills) { $Drills = [string]$chaosCfg.drills }
    else { $Drills = 'kill-leader,lose-quorum' }
}
if (-not $RetentionDays.HasValue) {
    $RetentionDays = if ($chaosCfg.retention_days) { [int]$chaosCfg.retention_days } else { 30 }
}

New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
$ts = (Get-Date).ToUniversalTime().ToString('yyyyMMdd-HHmmss')
$logPath = Join-Path $LogDir "chaos.$ts.jsonl"

# Index voters by node_id (the value rqlite reports in /nodes) for O(1)
# lookup during drills.
$voterById = @{}
$baseUrls = New-Object System.Collections.ArrayList
foreach ($v in $cfg.voters) {
    $nodeId = if ($v.node_id) { [string]$v.node_id } else { [string]$v.label }
    $voterById[$nodeId] = $v
    [void]$baseUrls.Add("http://$($v.host):$($v.port)")
}
$baseUrls = @($baseUrls)

function Write-Event {
    param([string]$Phase, [hashtable]$Data)
    $rec = [ordered]@{
        ts    = (Get-Date).ToUniversalTime().ToString('o')
        phase = $Phase
    }
    foreach ($k in $Data.Keys) { $rec[$k] = $Data[$k] }
    $line = ($rec | ConvertTo-Json -Compress -Depth 6)
    Add-Content -Path $logPath -Value $line
    Write-Host "[$Phase] $line"
}

function Get-ClusterNodes {
    param([string]$BaseUrl)
    try {
        return (Invoke-RestMethod -Uri "$BaseUrl/nodes" -TimeoutSec 5 -MaximumRedirection 5)
    } catch {
        return $null
    }
}

function Get-Leader {
    param([string]$BaseUrl)
    $nodes = Get-ClusterNodes -BaseUrl $BaseUrl
    if (-not $nodes) { return $null }
    foreach ($name in $nodes.PSObject.Properties.Name) {
        $n = $nodes.$name
        if ($n.leader -and $n.reachable) { return $n }
    }
    return $null
}

function Wait-ForLeader {
    param([string[]]$BaseUrls, [int]$TimeoutMs = 30000, [string]$NotEqualToId)
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    while ($sw.ElapsedMilliseconds -lt $TimeoutMs) {
        foreach ($u in $BaseUrls) {
            $l = Get-Leader -BaseUrl $u
            if ($l -and (-not $NotEqualToId -or $l.id -ne $NotEqualToId)) {
                return @{ leader = $l; elapsed_ms = [int]$sw.ElapsedMilliseconds }
            }
        }
        Start-Sleep -Milliseconds 200
    }
    return @{ leader = $null; elapsed_ms = [int]$sw.ElapsedMilliseconds }
}

function Invoke-Write {
    param([string]$BaseUrl, [string]$Sql)
    $body = ConvertTo-Json @(,$Sql) -Compress
    try {
        $r = Invoke-WebRequest -Uri "$BaseUrl/db/execute?redirect=true" `
            -Method POST -Body $body -ContentType 'application/json' `
            -TimeoutSec 8 -MaximumRedirection 5 -ErrorAction Stop
        return @{ ok = $true; status = $r.StatusCode; body = $r.Content }
    } catch {
        $resp = $_.Exception.Response
        $status = if ($resp) { [int]$resp.StatusCode } else { -1 }
        return @{ ok = $false; status = $status; error = $_.Exception.Message }
    }
}

function Invoke-Query {
    param([string]$BaseUrl, [string]$Sql, [string]$Consistency = 'strong')
    try {
        $r = Invoke-RestMethod -Uri ("$BaseUrl/db/query?level=$Consistency&redirect=true") `
            -Method POST -Body (ConvertTo-Json @(,$Sql) -Compress) `
            -ContentType 'application/json' -TimeoutSec 8 -MaximumRedirection 5
        return @{ ok = $true; data = $r }
    } catch {
        return @{ ok = $false; error = $_.Exception.Message }
    }
}

# POST /leader to the leader's API endpoint (rqlite v8+). The leader
# steps down voluntarily, an election runs, and a different voter takes
# over. The stepped-down node remains alive as a follower, so no rejoin
# is required. Works cluster-wide over the same HTTP port that clients
# use. The optional 'wait' URL param blocks until the new leader is
# elected; we use it so the call timing is meaningful.
function Invoke-LeaderStepdown {
    param([Parameter(Mandatory)][string]$BaseUrl)
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        $r = Invoke-WebRequest -Uri "$BaseUrl/leader?wait" -Method POST `
            -TimeoutSec 30 -MaximumRedirection 5 -ErrorAction Stop
        return @{ ok = $true; status = [int]$r.StatusCode; ms = [int]$sw.ElapsedMilliseconds }
    } catch {
        $resp = $_.Exception.Response
        $status = if ($resp) { [int]$resp.StatusCode } else { -1 }
        return @{ ok = $false; status = $status; error = $_.Exception.Message; ms = [int]$sw.ElapsedMilliseconds }
    }
}

# Stop or start a voter's Windows service. We auto-detect whether the
# voter's host is local to this machine by comparing voter.host to this
# host's IPv4 addresses. If local, run Stop-/Start-Service in-process;
# otherwise dispatch over SSH using voter.ssh_alias. The 'local' string
# in cluster.yaml is also honoured for backwards compatibility.
$script:_localIPs = $null
function Get-LocalIPv4Set {
    if ($null -ne $script:_localIPs) { return $script:_localIPs }
    $set = @{}
    try {
        Get-NetIPAddress -AddressFamily IPv4 -ErrorAction SilentlyContinue |
            ForEach-Object { $set[$_.IPAddress] = $true }
    } catch {}
    $set['127.0.0.1'] = $true
    $set['localhost'] = $true
    $script:_localIPs = $set
    return $set
}

function Test-VoterIsLocal {
    param($Voter)
    $alias = if ($Voter.ssh_alias) { [string]$Voter.ssh_alias } else { '' }
    if ($alias -ieq 'local') { return $true }
    $local = Get-LocalIPv4Set
    if ($Voter.host -and $local.ContainsKey([string]$Voter.host)) { return $true }
    return $false
}

function Invoke-VoterService {
    param([Parameter(Mandatory)]$Voter, [Parameter(Mandatory)][ValidateSet('Stop','Start')] [string]$Action)
    if (-not $Voter.service) {
        throw "voter $($Voter.label) has no 'service' configured in cluster.yaml"
    }
    $isLocal = Test-VoterIsLocal -Voter $Voter
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    if ($isLocal) {
        if ($Action -eq 'Stop') { Stop-Service -Name $Voter.service -ErrorAction Stop }
        else { Start-Service -Name $Voter.service -ErrorAction Stop }
    } else {
        $alias = [string]$Voter.ssh_alias
        if (-not $alias -or $alias -ieq 'local') {
            throw "voter $($Voter.label) host=$($Voter.host) is not local and has no ssh_alias"
        }
        $cmd = if ($Action -eq 'Stop') { "Stop-Service $($Voter.service)" } else { "Start-Service $($Voter.service)" }
        $remote = "powershell -NoProfile -Command `"$cmd`""
        & ssh $alias $remote 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "ssh $alias '$cmd' exited $LASTEXITCODE" }
    }
    return [int]$sw.ElapsedMilliseconds
}

# Prune old chaos logs.
if ($RetentionDays -gt 0) {
    $cutoff = (Get-Date).AddDays(-$RetentionDays)
    Get-ChildItem -Path $LogDir -Filter 'chaos.*.jsonl' -ErrorAction SilentlyContinue |
        Where-Object { $_.LastWriteTime -lt $cutoff } |
        ForEach-Object { Remove-Item -LiteralPath $_.FullName -Force -ErrorAction SilentlyContinue }
}

# --- Setup ----------------------------------------------------------------
Write-Event -Phase 'setup' -Data @{
    log              = $logPath
    drills           = $Drills
    voters           = (@($voterById.Keys) | Sort-Object)
    retention_days   = [int]$RetentionDays
}

$initial = $null
foreach ($u in $baseUrls) {
    $initial = Get-Leader -BaseUrl $u
    if ($initial) { break }
}
if (-not $initial) {
    Write-Event -Phase 'setup.error' -Data @{ message = 'no leader visible from any voter' }
    exit 2
}
Write-Event -Phase 'setup.leader' -Data @{ id = $initial.id; api = $initial.api_addr }

$null = Invoke-Write -BaseUrl $initial.api_addr -Sql `
    'CREATE TABLE IF NOT EXISTS chaos_drill (id INTEGER PRIMARY KEY AUTOINCREMENT, phase TEXT, ts TEXT)'

$drillList = $Drills -split '[,;]' | ForEach-Object { $_.Trim() } | Where-Object { $_ }

# --- Drill 1: kill leader (soft, via /raft/stepdown) ----------------------
# This is the portable variant. It POSTs to the leader's HTTP API to
# trigger a voluntary stepdown, then waits for a new leader. The old
# leader stays alive as a follower, so no service restart is needed
# and no host-level access (SSH, Stop-Service) is required.
if ($drillList -contains 'kill-leader') {
    Write-Event -Phase 'd1.start' -Data @{ mode = 'stepdown' }
    $leaderId = $initial.id
    $leaderApi = $initial.api_addr
    $stepdown = Invoke-LeaderStepdown -BaseUrl $leaderApi
    Write-Event -Phase 'd1.stepdown' -Data $stepdown
    if (-not $stepdown.ok) {
        Write-Event -Phase 'd1.fail' -Data @{ message = 'stepdown call failed'; status = $stepdown.status }
    } else {
        $other = $baseUrls | Where-Object { $_ -ne $leaderApi }
        if (-not $other) { $other = $baseUrls }
        $elect = Wait-ForLeader -BaseUrls $other -TimeoutMs 30000 -NotEqualToId $leaderId
        if ($elect.leader) {
            Write-Event -Phase 'd1.reelected' -Data @{
                new_leader  = $elect.leader.id
                api         = $elect.leader.api_addr
                elapsed_ms  = $elect.elapsed_ms
            }
            $w = Invoke-Write -BaseUrl $elect.leader.api_addr -Sql `
                "INSERT INTO chaos_drill(phase, ts) VALUES('d1-after-stepdown', datetime('now'))"
            Write-Event -Phase 'd1.write' -Data $w
            # Confirm the old leader is back in the cluster as a follower.
            $rsw = [System.Diagnostics.Stopwatch]::StartNew()
            $followerOk = $false
            while ($rsw.ElapsedMilliseconds -lt 15000 -and -not $followerOk) {
                $nodes = Get-ClusterNodes -BaseUrl $elect.leader.api_addr
                if ($nodes -and $nodes.$leaderId -and $nodes.$leaderId.reachable) {
                    $followerOk = $true; break
                }
                Start-Sleep -Milliseconds 250
            }
            Write-Event -Phase 'd1.restored' -Data @{
                old_leader = $leaderId
                follower_ok = $followerOk
                rejoin_ms   = [int]$rsw.ElapsedMilliseconds
            }
        } else {
            Write-Event -Phase 'd1.fail' -Data @{ message = 'no new leader within timeout'; elapsed_ms = $elect.elapsed_ms }
        }
    }
}

# --- Drill 1b: kill leader (hard, via Stop-Service) -----------------------
# Process-level failure. Requires the leader's host to be local to this
# driver, or reachable via SSH from the SYSTEM principal. Skipped with a
# hint otherwise so the drill is not partially executed.
if ($drillList -contains 'kill-leader-hard') {
    Write-Event -Phase 'd1h.start' -Data @{ mode = 'stop-service' }
    # Re-read the leader; a previous drill may have rotated it.
    $current = $null
    foreach ($u in $baseUrls) { $current = Get-Leader -BaseUrl $u; if ($current) { break } }
    if (-not $current) {
        Write-Event -Phase 'd1h.skip' -Data @{ reason = 'no leader visible' }
    } else {
        $leaderId = $current.id
        $leaderVoter = $voterById[$leaderId]
        if (-not $leaderVoter) {
            Write-Event -Phase 'd1h.skip' -Data @{ reason = "leader id $leaderId not in cluster.yaml" }
        } elseif (-not (Test-VoterIsLocal -Voter $leaderVoter) -and -not $leaderVoter.ssh_alias) {
            Write-Event -Phase 'd1h.skip' -Data @{
                reason   = 'leader is on a remote host with no ssh_alias from this driver'
                leader   = $leaderId
                host     = [string]$leaderVoter.host
                hint     = 'set ssh_alias on this voter (reachable from the SYSTEM principal) to enable cross-host kill-leader-hard, or rely on the soft kill-leader drill'
            }
        } else {
            try {
                $stopMs = Invoke-VoterService -Voter $leaderVoter -Action 'Stop'
                Write-Event -Phase 'd1h.stopped' -Data @{
                    service   = $leaderVoter.service
                    ssh_alias = $leaderVoter.ssh_alias
                    ms        = $stopMs
                }

                $other = $baseUrls | Where-Object { $_ -ne $current.api_addr }
                $elect = Wait-ForLeader -BaseUrls $other -TimeoutMs 30000 -NotEqualToId $leaderId
                if ($elect.leader) {
                    Write-Event -Phase 'd1h.reelected' -Data @{
                        new_leader  = $elect.leader.id
                        api         = $elect.leader.api_addr
                        elapsed_ms  = $elect.elapsed_ms
                    }
                    $w = Invoke-Write -BaseUrl $elect.leader.api_addr -Sql `
                        "INSERT INTO chaos_drill(phase, ts) VALUES('d1h-after-failover', datetime('now'))"
                    Write-Event -Phase 'd1h.write' -Data $w
                } else {
                    Write-Event -Phase 'd1h.fail' -Data @{ message = 'no new leader within timeout'; elapsed_ms = $elect.elapsed_ms }
                }
            } finally {
                try {
                    $startMs = Invoke-VoterService -Voter $leaderVoter -Action 'Start'
                    $rejoinMs = -1
                    $rsw = [System.Diagnostics.Stopwatch]::StartNew()
                    while ($rsw.ElapsedMilliseconds -lt 30000) {
                        foreach ($u in $baseUrls) {
                            $nodes = Get-ClusterNodes -BaseUrl $u
                            if ($nodes -and $nodes.$leaderId -and $nodes.$leaderId.reachable) {
                                $rejoinMs = [int]$rsw.ElapsedMilliseconds; break
                            }
                        }
                        if ($rejoinMs -ge 0) { break }
                        Start-Sleep -Milliseconds 250
                    }
                    Write-Event -Phase 'd1h.restored' -Data @{
                        service    = $leaderVoter.service
                        rejoin_ms  = $rejoinMs
                        start_ms   = $startMs
                    }
                } catch {
                    Write-Event -Phase 'd1h.restore_error' -Data @{ error = $_.Exception.Message }
                }
            }
        }
    }
}

# --- Drill 2: lose quorum -------------------------------------------------
if ($drillList -contains 'lose-quorum') {
    Write-Event -Phase 'd2.start' -Data @{ }

    $current = $null
    foreach ($u in $baseUrls) { $current = Get-Leader -BaseUrl $u; if ($current) { break } }
    if (-not $current) {
        Write-Event -Phase 'd2.skip' -Data @{ reason = 'no current leader visible' }
    } else {
        # Pick a survivor that ISN'T the current leader so the drill
        # exercises both quorum loss AND a forced re-election attempt.
        $survivor = $null
        foreach ($v in $cfg.voters) {
            $vid = if ($v.node_id) { [string]$v.node_id } else { [string]$v.label }
            if ($vid -ne $current.id) { $survivor = $v; break }
        }
        if (-not $survivor) { $survivor = $cfg.voters[0] }
        $survivorId = if ($survivor.node_id) { [string]$survivor.node_id } else { [string]$survivor.label }
        $survivorUrl = "http://$($survivor.host):$($survivor.port)"

        $toStop = @($cfg.voters | Where-Object {
            $vid = if ($_.node_id) { [string]$_.node_id } else { [string]$_.label }
            $vid -ne $survivorId
        })

        Write-Event -Phase 'd2.plan' -Data @{
            survivor    = $survivorId
            stopping    = (@($toStop) | ForEach-Object { $_.label })
        }

        # Pre-flight: every voter we plan to stop must be controllable
        # from this driver (local or has ssh_alias). Otherwise the drill
        # would only partially execute and "lose-quorum" wouldn't.
        $unreachable = @($toStop | Where-Object {
            -not (Test-VoterIsLocal -Voter $_) -and -not $_.ssh_alias
        })
        $skipDrill = $false
        if ($unreachable.Count -gt 0) {
            Write-Event -Phase 'd2.skip' -Data @{
                reason = 'one or more target voters are remote with no ssh_alias from this driver'
                unreachable = (@($unreachable) | ForEach-Object { $_.label })
                hint   = 'set ssh_alias on those voters (reachable from the SYSTEM principal) to enable lose-quorum'
            }
            $skipDrill = $true
        }

        if (-not $skipDrill) {
        $stopped = New-Object System.Collections.ArrayList
        try {
            foreach ($v in $toStop) {
                try {
                    $ms = Invoke-VoterService -Voter $v -Action 'Stop'
                    [void]$stopped.Add($v)
                    Write-Event -Phase 'd2.stopped' -Data @{ label = $v.label; service = $v.service; ms = $ms }
                } catch {
                    Write-Event -Phase 'd2.stop_error' -Data @{ label = $v.label; error = $_.Exception.Message }
                }
            }

            Start-Sleep -Seconds 3
            $w = Invoke-Write -BaseUrl $survivorUrl -Sql `
                "INSERT INTO chaos_drill(phase, ts) VALUES('d2-no-quorum', datetime('now'))"
            Write-Event -Phase 'd2.write_attempt' -Data $w

            $qNone = Invoke-Query -BaseUrl $survivorUrl `
                -Sql 'SELECT COUNT(*) FROM chaos_drill' -Consistency 'none'
            Write-Event -Phase 'd2.read_none' -Data $qNone

            if ($stopped.Count -gt 0) {
                $restoreFirst = $stopped[0]
                $msStart = Invoke-VoterService -Voter $restoreFirst -Action 'Start'
                Write-Event -Phase 'd2.restore_one' -Data @{ label = $restoreFirst.label; ms = $msStart }
                $r = Wait-ForLeader -BaseUrls $baseUrls -TimeoutMs 30000
                Write-Event -Phase 'd2.partial_quorum' -Data @{
                    leader     = ($r.leader.id ?? '<none>')
                    elapsed_ms = $r.elapsed_ms
                }
                $w2 = Invoke-Write -BaseUrl $survivorUrl -Sql `
                    "INSERT INTO chaos_drill(phase, ts) VALUES('d2-after-quorum-restored', datetime('now'))"
                Write-Event -Phase 'd2.write_after_restore' -Data $w2
            }
        } finally {
            foreach ($v in @($stopped)) {
                try { Invoke-VoterService -Voter $v -Action 'Start' | Out-Null } catch { }
            }
            Start-Sleep -Seconds 3
            $reach = @{}
            foreach ($u in $baseUrls) {
                $nodes = Get-ClusterNodes -BaseUrl $u
                if ($nodes) {
                    foreach ($n in $nodes.PSObject.Properties.Name) { $reach[$n] = $nodes.$n.reachable }
                    break
                }
            }
            Write-Event -Phase 'd2.full_restore' -Data @{ reachable = $reach }
        }
        }  # end if (-not $skipDrill)
    }
}

# --- Drill 3: partition recovery (manual / opt-in) -----------------------
if ($drillList -contains 'partition-recovery') {
    Write-Event -Phase 'd3.start' -Data @{ }
    $remoteHosts = @($cfg.voters | Where-Object { $_.ssh_alias -and ($_.ssh_alias -ine 'local') } |
                     ForEach-Object { [string]$_.host } | Select-Object -Unique)
    if ($remoteHosts.Count -eq 0) {
        Write-Event -Phase 'd3.skip' -Data @{ reason = 'no remote voters to partition from' }
    } else {
        $ruleName = 'ForgeWireChaosBlockRemoteVoters'
        try {
            New-NetFirewallRule -DisplayName $ruleName -Direction Outbound `
                -Action Block -RemoteAddress $remoteHosts -ErrorAction Stop | Out-Null
            Write-Event -Phase 'd3.partitioned' -Data @{ rule = $ruleName; blocked = $remoteHosts }

            Start-Sleep -Seconds 5
            $localUrl = $null
            foreach ($v in $cfg.voters) {
                if (-not $v.ssh_alias -or ($v.ssh_alias -ieq 'local')) {
                    $localUrl = "http://$($v.host):$($v.port)"; break
                }
            }
            if ($localUrl) {
                $localView = Get-ClusterNodes -BaseUrl $localUrl
                $reach = @{}
                if ($localView) {
                    foreach ($n in $localView.PSObject.Properties.Name) { $reach[$n] = $localView.$n.reachable }
                }
                Write-Event -Phase 'd3.local_view' -Data @{ reachable = $reach }
            }
        } finally {
            Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue | Remove-NetFirewallRule
            Write-Event -Phase 'd3.unpartitioned' -Data @{ rule = $ruleName }
        }

        Start-Sleep -Seconds 5
        $r = Wait-ForLeader -BaseUrls $baseUrls -TimeoutMs 30000
        Write-Event -Phase 'd3.recovered' -Data @{ leader = ($r.leader.id ?? '<none>'); elapsed_ms = $r.elapsed_ms }
    }
}

Write-Event -Phase 'done' -Data @{ log = $logPath }
Write-Host "Chaos drills complete. Log: $logPath"
