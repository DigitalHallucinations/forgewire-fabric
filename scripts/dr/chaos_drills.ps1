<#
.SYNOPSIS
  Phase 5 chaos drills against the live rqlite cluster.

.DESCRIPTION
  Runs three drills against the 3-voter rqlite cluster and records
  structured JSONL outcomes:

    1. kill-leader         : stop the current leader, time re-election.
    2. lose-quorum         : stop two of three voters, verify writes fail.
    3. partition-recovery  : (optional) firewall-block the leader from
                             this host, verify local hub failover, restore.

  Drills 1 and 2 require SSH access to the OptiPlex host (alias
  'forgewire' in ~/.ssh/config) where Node1 + Node3 (witness) run.
  Drill 3 is local (touches Windows Firewall on this host).

  Output:
    - Console: human-readable progress.
    - <LogDir>\chaos.<UTC>.jsonl: one JSON record per phase with timings.

.PARAMETER LogDir
  Where to write the JSONL log. Defaults to repo's logs\chaos\.

.PARAMETER OptiPlexAlias
  SSH alias for the OptiPlex host (default: forgewire).

.PARAMETER LeaderApi
  HTTP base URL for the current leader (default: http://10.120.81.95:4001).
  The script auto-discovers the actual leader via /nodes.

.PARAMETER Drills
  Comma-separated list: kill-leader, lose-quorum, partition-recovery.
  Default: kill-leader,lose-quorum.
#>

[CmdletBinding()]
param(
    [string]$LogDir,
    [string]$OptiPlexAlias = 'forgewire',
    [string]$LeaderApi = 'http://10.120.81.95:4001',
    [string]$Drills = 'kill-leader,lose-quorum'
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $LogDir) { $LogDir = Join-Path $repoRoot 'logs\chaos' }
New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
$ts = (Get-Date).ToUniversalTime().ToString('yyyyMMdd-HHmmss')
$logPath = Join-Path $LogDir "chaos.$ts.jsonl"

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

function Invoke-Remote {
    param([string]$Alias, [string]$PSCommand)
    $remote = "powershell -NoProfile -Command `"$PSCommand`""
    & ssh $Alias $remote 2>&1
    return $LASTEXITCODE
}

$allUrls = @(
    'http://10.120.81.95:4001',  # node1
    'http://10.120.81.95:4011',  # witness
    'http://10.120.81.56:4001'   # node2
)

# --- Setup ----------------------------------------------------------------
Write-Event -Phase 'setup' -Data @{ log = $logPath; drills = $Drills }

$initial = Get-Leader -BaseUrl $LeaderApi
if (-not $initial) {
    Write-Event -Phase 'setup.error' -Data @{ message = 'no leader visible from initial endpoint'; endpoint = $LeaderApi }
    exit 2
}
Write-Event -Phase 'setup.leader' -Data @{ id = $initial.id; api = $initial.api_addr }

# Ensure chaos table exists.
$null = Invoke-Write -BaseUrl $LeaderApi -Sql `
    'CREATE TABLE IF NOT EXISTS chaos_drill (id INTEGER PRIMARY KEY AUTOINCREMENT, phase TEXT, ts TEXT)'

$drillList = $Drills -split '[,;]' | ForEach-Object { $_.Trim() } | Where-Object { $_ }

# --- Drill 1: kill leader -------------------------------------------------
if ($drillList -contains 'kill-leader') {
    Write-Event -Phase 'd1.start' -Data @{ }
    $leaderId = $initial.id

    # Map node id to service name on OptiPlex; node2 lives on this Dell.
    $svc = switch ($leaderId) {
        'node1-optiplex'  { 'ForgeWireRqliteNode1' }
        'node3-witness'   { 'ForgeWireRqliteNode3' }
        'node2-dell'      { 'ForgeWireRqliteNode2' }
        default           { $null }
    }
    if (-not $svc) {
        Write-Event -Phase 'd1.skip' -Data @{ reason = "unknown leader id $leaderId" }
    } else {
        $isRemote = $svc -ne 'ForgeWireRqliteNode2'
        $stopSw = [System.Diagnostics.Stopwatch]::StartNew()
        if ($isRemote) {
            Invoke-Remote -Alias $OptiPlexAlias -PSCommand "Stop-Service $svc" | Out-Null
        } else {
            Stop-Service -Name $svc
        }
        Write-Event -Phase 'd1.stopped' -Data @{ service = $svc; remote = $isRemote; ms = $stopSw.ElapsedMilliseconds }

        $other = $allUrls | Where-Object { $_ -ne $initial.api_addr }
        $elect = Wait-ForLeader -BaseUrls $other -TimeoutMs 30000 -NotEqualToId $leaderId
        if ($elect.leader) {
            Write-Event -Phase 'd1.reelected' -Data @{ new_leader = $elect.leader.id; api = $elect.leader.api_addr; elapsed_ms = $elect.elapsed_ms }
            $w = Invoke-Write -BaseUrl $elect.leader.api_addr -Sql `
                "INSERT INTO chaos_drill(phase, ts) VALUES('d1-after-failover', datetime('now'))"
            Write-Event -Phase 'd1.write' -Data $w
        } else {
            Write-Event -Phase 'd1.fail' -Data @{ message = 'no new leader within timeout'; elapsed_ms = $elect.elapsed_ms }
        }

        # Restore.
        $startSw = [System.Diagnostics.Stopwatch]::StartNew()
        if ($isRemote) {
            Invoke-Remote -Alias $OptiPlexAlias -PSCommand "Start-Service $svc" | Out-Null
        } else {
            Start-Service -Name $svc
        }
        # Wait for it to show reachable=true again.
        $rejoin = $null
        $rsw = [System.Diagnostics.Stopwatch]::StartNew()
        while ($rsw.ElapsedMilliseconds -lt 30000) {
            $nodes = Get-ClusterNodes -BaseUrl $allUrls[0]
            if ($nodes -and $nodes.$leaderId -and $nodes.$leaderId.reachable) {
                $rejoin = @{ ms = [int]$rsw.ElapsedMilliseconds }; break
            }
            Start-Sleep -Milliseconds 250
        }
        Write-Event -Phase 'd1.restored' -Data @{ service = $svc; rejoin_ms = ($rejoin.ms ?? -1); start_ms = $startSw.ElapsedMilliseconds }
    }
}

# --- Drill 2: lose quorum -------------------------------------------------
if ($drillList -contains 'lose-quorum') {
    Write-Event -Phase 'd2.start' -Data @{ }
    # Stop Node1 + Node3 (both on OptiPlex). Node2 alone = 1/3, no quorum.
    Invoke-Remote -Alias $OptiPlexAlias -PSCommand 'Stop-Service ForgeWireRqliteNode1; Stop-Service ForgeWireRqliteNode3' | Out-Null
    Write-Event -Phase 'd2.stopped_two' -Data @{ remote_services = @('ForgeWireRqliteNode1','ForgeWireRqliteNode3') }

    Start-Sleep -Seconds 3
    $w = Invoke-Write -BaseUrl 'http://10.120.81.56:4001' -Sql `
        "INSERT INTO chaos_drill(phase, ts) VALUES('d2-no-quorum', datetime('now'))"
    Write-Event -Phase 'd2.write_attempt' -Data $w

    # Try a query at consistency=none (should still serve from local raft log).
    $qNone = Invoke-Query -BaseUrl 'http://10.120.81.56:4001' -Sql 'SELECT COUNT(*) FROM chaos_drill' -Consistency 'none'
    Write-Event -Phase 'd2.read_none' -Data $qNone

    # Restore Node3 only -> quorum is back at 2/3.
    Invoke-Remote -Alias $OptiPlexAlias -PSCommand 'Start-Service ForgeWireRqliteNode3' | Out-Null
    $r = Wait-ForLeader -BaseUrls $allUrls -TimeoutMs 30000
    Write-Event -Phase 'd2.partial_restore' -Data @{ leader = ($r.leader.id ?? '<none>'); elapsed_ms = $r.elapsed_ms }

    $w2 = Invoke-Write -BaseUrl 'http://10.120.81.56:4001' -Sql `
        "INSERT INTO chaos_drill(phase, ts) VALUES('d2-after-quorum-restored', datetime('now'))"
    Write-Event -Phase 'd2.write_after_restore' -Data $w2

    # Bring Node1 back too.
    Invoke-Remote -Alias $OptiPlexAlias -PSCommand 'Start-Service ForgeWireRqliteNode1' | Out-Null
    Start-Sleep -Seconds 3
    $nodes = Get-ClusterNodes -BaseUrl $allUrls[0]
    $reachable = @{}
    foreach ($n in $nodes.PSObject.Properties.Name) { $reachable[$n] = $nodes.$n.reachable }
    Write-Event -Phase 'd2.full_restore' -Data @{ reachable = $reachable }
}

# --- Drill 3: partition recovery -----------------------------------------
if ($drillList -contains 'partition-recovery') {
    Write-Event -Phase 'd3.start' -Data @{ }
    $ruleName = 'ForgeWireChaosBlockOptiPlex'
    try {
        New-NetFirewallRule -DisplayName $ruleName -Direction Outbound `
            -Action Block -RemoteAddress 10.120.81.95 -ErrorAction Stop | Out-Null
        Write-Event -Phase 'd3.partitioned' -Data @{ rule = $ruleName }

        Start-Sleep -Seconds 5
        $localView = Get-ClusterNodes -BaseUrl 'http://10.120.81.56:4001'
        $reach = @{}
        foreach ($n in $localView.PSObject.Properties.Name) { $reach[$n] = $localView.$n.reachable }
        Write-Event -Phase 'd3.local_view' -Data @{ reachable = $reach }
    } finally {
        Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue | Remove-NetFirewallRule
        Write-Event -Phase 'd3.unpartitioned' -Data @{ rule = $ruleName }
    }

    Start-Sleep -Seconds 5
    $r = Wait-ForLeader -BaseUrls $allUrls -TimeoutMs 30000
    Write-Event -Phase 'd3.recovered' -Data @{ leader = ($r.leader.id ?? '<none>'); elapsed_ms = $r.elapsed_ms }
}

Write-Event -Phase 'done' -Data @{ log = $logPath }
Write-Host "Chaos drills complete. Log: $logPath"
