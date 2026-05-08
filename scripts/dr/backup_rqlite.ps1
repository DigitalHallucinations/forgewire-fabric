<#
.SYNOPSIS
    Pull a consistent SQLite backup from the rqlite cluster, with
    preferred-node + failover semantics. Generic: runs on any host.

.DESCRIPTION
    Phase 3 disaster-recovery snapshots. Reads cluster topology from
    config\cluster.yaml so adding/removing voters is a config edit.

    Resolution order for the control-node chain:
      1. -Nodes (explicit override, comma/semicolon separated specs)
      2. config\cluster.yaml ``voters`` ordered by ``priority`` with the
         preferred voter floated to the head. Preferred is taken from
         (-PreferredNode || $env:FORGEWIRE_PREFERRED_NODE ||
          cfg.preferred_node).

    For each run:
      1. Walks the chain in order, GETting ``/db/backup?redirect=true``.
         Followers transparently 301 to the leader, so any reachable
         voter can serve the call. First 200 wins.
      2. Verifies the SQLite magic bytes and minimum size before commit.
      3. Renames into ``<BackupRoot>\YYYYMMDD-HHmmss.sqlite3``.
      4. Prunes files older than -RetentionHours.
      5. Appends a one-line JSON summary to backup.log.jsonl.

    Designed to be invoked by Windows Task Scheduler as SYSTEM, but
    will run interactively too. Exit code 0 on success, 2 if every
    voter in the chain failed.

.PARAMETER ConfigPath
    Path to cluster.yaml. Defaults to repo-root\config\cluster.yaml.

.PARAMETER PreferredNode
    Label of the voter to try first. Overrides cfg.preferred_node and
    $env:FORGEWIRE_PREFERRED_NODE.

.PARAMETER Nodes
    Explicit comma- or semicolon-separated chain "label=host:port,...".
    When given, ConfigPath / PreferredNode are ignored.

.PARAMETER BackupRoot
    Output directory. Defaults to cfg.backups.root.

.PARAMETER RetentionHours
    Default cfg.backups.retention_hours (24).

.PARAMETER MinBytes
    Default cfg.backups.min_bytes (1024).

.PARAMETER TimeoutSeconds
    Per-voter HTTP timeout. Default cfg.backups.timeout_seconds (60).

.EXAMPLE
    pwsh -File backup_rqlite.ps1
    pwsh -File backup_rqlite.ps1 -PreferredNode node2
    pwsh -File backup_rqlite.ps1 -Nodes "node1=10.0.0.1:4001,node2=10.0.0.2:4001"
#>
[CmdletBinding()]
param(
    [string]$ConfigPath,
    [string]$PreferredNode,
    [string]$Nodes,
    [string]$BackupRoot,
    [Nullable[int]]$RetentionHours,
    [Nullable[int]]$MinBytes,
    [Nullable[int]]$TimeoutSeconds
)

$ErrorActionPreference = "Continue"
. "$PSScriptRoot\_cluster_config.ps1"

# ---- Resolve chain --------------------------------------------------------
if ($Nodes) {
    $chain = $Nodes -split '[,;]' | ForEach-Object { $_.Trim() } | Where-Object { $_ }
    $cfg = $null
} else {
    $cfg = Get-ForgeWireClusterConfig -Path $ConfigPath
    $chain = Get-ForgeWireFailoverChain -Config $cfg -Preferred $PreferredNode
}
if (-not $chain -or $chain.Count -eq 0) {
    Write-Error "no voters resolved"
    exit 2
}

# ---- Resolve backup defaults ---------------------------------------------
function Coalesce { param($a, $b, $c) foreach ($v in @($a,$b,$c)) { if ($null -ne $v -and $v -ne '') { return $v } } return $null }

$bk = if ($cfg) { $cfg["backups"] } else { @{} }
if (-not $bk) { $bk = @{} }

if (-not $BackupRoot)     { $BackupRoot     = Coalesce $BackupRoot $bk["root"] 'C:\ProgramData\forgewire\rqlite-backups' }
if ($null -eq $RetentionHours) { $RetentionHours = if ($null -ne $bk["retention_hours"]) { [int]$bk["retention_hours"] } else { 24 } }
if ($null -eq $MinBytes)       { $MinBytes       = if ($null -ne $bk["min_bytes"])       { [int]$bk["min_bytes"]       } else { 1024 } }
if ($null -eq $TimeoutSeconds) { $TimeoutSeconds = if ($null -ne $bk["timeout_seconds"]) { [int]$bk["timeout_seconds"] } else { 60 } }

# ---- Helpers --------------------------------------------------------------
function Write-DRLog {
    param([string]$Path, [hashtable]$Record)
    $Record["ts"] = (Get-Date).ToUniversalTime().ToString("o")
    $line = ($Record | ConvertTo-Json -Compress -Depth 5)
    Add-Content -Path $Path -Value $line -Encoding utf8
}

function Test-SqliteBlob {
    param([string]$Path, [int]$MinBytes)
    if (-not (Test-Path $Path)) { return $false }
    $info = Get-Item $Path
    if ($info.Length -lt $MinBytes) { return $false }
    $fs = [IO.File]::OpenRead($Path)
    try {
        $hdr = New-Object byte[] 16
        [void]$fs.Read($hdr, 0, 16)
    } finally {
        $fs.Dispose()
    }
    $expected = [byte[]]@(83,81,76,105,116,101,32,102,111,114,109,97,116,32,51,0)
    for ($i = 0; $i -lt 16; $i++) {
        if ($hdr[$i] -ne $expected[$i]) { return $false }
    }
    return $true
}

# ---- Run ------------------------------------------------------------------
if (-not (Test-Path $BackupRoot)) {
    New-Item -ItemType Directory -Path $BackupRoot -Force | Out-Null
}
$logPath = Join-Path $BackupRoot "backup.log.jsonl"
$ts      = (Get-Date).ToUniversalTime().ToString("yyyyMMdd-HHmmss")
$cutoff  = (Get-Date).AddHours(-$RetentionHours)
$outPath = Join-Path $BackupRoot "$ts.sqlite3"
$tmpPath = "$outPath.partial"

$attempts = @()
$winner   = $null

foreach ($spec in $chain) {
    $label, $endpoint = $spec.Split("=", 2)
    if (-not $endpoint) {
        $attempts += @{ node=$label; ok=$false; error="bad spec '$spec'" }
        continue
    }
    $url = "http://$endpoint/db/backup?redirect=true"
    $start = Get-Date
    try {
        Invoke-WebRequest -Uri $url `
            -OutFile $tmpPath `
            -TimeoutSec $TimeoutSeconds `
            -UseBasicParsing `
            -MaximumRedirection 5 `
            -ErrorAction Stop | Out-Null
        if (-not (Test-SqliteBlob -Path $tmpPath -MinBytes $MinBytes)) {
            Remove-Item -Path $tmpPath -Force -ErrorAction SilentlyContinue
            throw "blob failed sqlite-magic / min-size verification"
        }
        Move-Item -Path $tmpPath -Destination $outPath -Force
        $size = (Get-Item $outPath).Length
        $dur  = ((Get-Date) - $start).TotalMilliseconds
        $winner = @{ node=$label; endpoint=$endpoint; bytes=$size; ms=[int]$dur; path=$outPath }
        $attempts += @{ node=$label; ok=$true; bytes=$size; ms=[int]$dur }
        break
    } catch {
        Remove-Item -Path $tmpPath -Force -ErrorAction SilentlyContinue
        $attempts += @{ node=$label; ok=$false; error=$_.Exception.Message }
        Write-DRLog -Path $logPath -Record @{
            level="warn"; phase="backup"; node=$label; endpoint=$endpoint
            error=$_.Exception.Message
        }
    }
}

# Rotation: always run, even on full-failure, so old backups still age out.
try {
    $stale = Get-ChildItem -Path $BackupRoot -File -Filter "*.sqlite3" -ErrorAction SilentlyContinue `
        | Where-Object { $_.LastWriteTime -lt $cutoff }
    foreach ($f in $stale) {
        Remove-Item -Path $f.FullName -Force -ErrorAction SilentlyContinue
    }
    $partials = Get-ChildItem -Path $BackupRoot -File -Filter "*.partial" -ErrorAction SilentlyContinue `
        | Where-Object { $_.LastWriteTime -lt (Get-Date).AddMinutes(-30) }
    foreach ($f in $partials) {
        Remove-Item -Path $f.FullName -Force -ErrorAction SilentlyContinue
    }
} catch {
    Write-DRLog -Path $logPath -Record @{
        level="warn"; phase="prune"; error=$_.Exception.Message
    }
}

try {
    if ((Test-Path $logPath) -and ((Get-Item $logPath).Length -gt 5MB)) {
        Move-Item -Path $logPath -Destination "$logPath.1" -Force
    }
} catch {}

if ($null -ne $winner) {
    Write-DRLog -Path $logPath -Record @{
        level="info"; phase="summary"; ok=$true
        winner=$winner; attempts=$attempts; chain=$chain
    }
    exit 0
} else {
    Write-DRLog -Path $logPath -Record @{
        level="error"; phase="summary"; ok=$false
        attempts=$attempts; chain=$chain
    }
    Write-Error "all $($chain.Count) node(s) in failover chain failed"
    exit 2
}
