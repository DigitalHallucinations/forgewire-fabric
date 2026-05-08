<#
.SYNOPSIS
    Install / update the ForgeWire rqlite backup scheduled task on this host.

.DESCRIPTION
    Phase 3 disaster-recovery installer. Generic: registers a Windows
    Task Scheduler job that periodically invokes
    ``scripts\dr\backup_rqlite.ps1`` with this host's chosen failover
    chain.

    Idempotent. If the task already exists it is replaced.
    Self-elevating; relaunches with UAC if not already Administrator.

    Topology lives in ``config\cluster.yaml`` -- this script does NOT
    bake hostnames into the scheduled task. Instead the task records
    only the preferred-node label, so adding a voter is a config edit
    + a normal git pull on the host.

.PARAMETER PreferredNode
    Label of the preferred control node for outbound DR ops on this
    host (e.g. "node1" on the OptiPlex, "node2" on the Dell). If not
    given, falls back to cfg.preferred_node.

.PARAMETER CadenceMinutes
    Repetition interval. Default = cfg.backups.cadence_minutes (5).

.PARAMETER BackupRoot
    Output directory. Default = cfg.backups.root.

.PARAMETER RetentionHours
    Retention. Default = cfg.backups.retention_hours (24).

.PARAMETER TaskName
    Default "ForgeWireRqliteBackup".

.PARAMETER RepoRoot
    Path to the forgewire-fabric repo on this host. Defaults to the
    parent-of-parent of this installer (so when invoked in-place from
    a clone, no parameter is needed).

.PARAMETER PwshExe
    Full path to pwsh.exe. Defaults to (Get-Command pwsh).Source.

.EXAMPLE
    pwsh -File install_rqlite_backup_task.ps1
    pwsh -File install_rqlite_backup_task.ps1 -PreferredNode node2 `
        -CadenceMinutes 5 -RetentionHours 48
#>
[CmdletBinding()]
param(
    [string]$PreferredNode,
    [Nullable[int]]$CadenceMinutes,
    [string]$BackupRoot,
    [Nullable[int]]$RetentionHours,
    [string]$TaskName = "ForgeWireRqliteBackup",
    [string]$RepoRoot,
    [string]$PwshExe
)

$ErrorActionPreference = "Stop"

# ---- Self-elevation -------------------------------------------------------
$identity  = [System.Security.Principal.WindowsIdentity]::GetCurrent()
$principal = [System.Security.Principal.WindowsPrincipal]::new($identity)
if (-not $principal.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)) {
    $shellExe = (Get-Process -Id $PID).Path
    $argList = @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $PSCommandPath)
    foreach ($kv in $PSBoundParameters.GetEnumerator()) {
        $argList += "-$($kv.Key)"
        $argList += [string]$kv.Value
    }
    Start-Process -FilePath $shellExe -ArgumentList $argList -Verb RunAs -Wait
    return
}

# ---- Resolve paths --------------------------------------------------------
if (-not $RepoRoot) {
    # This script lives at <repo>\scripts\dr\install_rqlite_backup_task.ps1.
    $RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}
$RepoRoot = (Resolve-Path $RepoRoot).Path
$BackupScript = Join-Path $RepoRoot "scripts\dr\backup_rqlite.ps1"
$ConfigScript = Join-Path $RepoRoot "scripts\dr\_cluster_config.ps1"
$ClusterYaml  = Join-Path $RepoRoot "config\cluster.yaml"

foreach ($p in @($BackupScript, $ConfigScript, $ClusterYaml)) {
    if (-not (Test-Path $p)) { throw "missing required file: $p" }
}

if (-not $PwshExe) {
    $cmd = Get-Command pwsh -ErrorAction SilentlyContinue
    if ($cmd) { $PwshExe = $cmd.Source } else {
        $PwshExe = "C:\Program Files\PowerShell\7\pwsh.exe"
    }
}
if (-not (Test-Path $PwshExe)) { throw "pwsh.exe not found at: $PwshExe" }

# ---- Resolve config defaults ---------------------------------------------
. $ConfigScript
$cfg = Get-ForgeWireClusterConfig -Path $ClusterYaml

if (-not $PreferredNode)   { $PreferredNode = $cfg["preferred_node"] }
$bk = $cfg["backups"]; if (-not $bk) { $bk = @{} }
if ($null -eq $CadenceMinutes) {
    $CadenceMinutes = if ($null -ne $bk["cadence_minutes"]) { [int]$bk["cadence_minutes"] } else { 5 }
}
if ($null -eq $RetentionHours) {
    $RetentionHours = if ($null -ne $bk["retention_hours"]) { [int]$bk["retention_hours"] } else { 24 }
}
if (-not $BackupRoot) {
    $BackupRoot = if ($bk["root"]) { $bk["root"] } else { 'C:\ProgramData\forgewire\rqlite-backups' }
}

if (-not (Test-Path $BackupRoot)) {
    New-Item -ItemType Directory -Path $BackupRoot -Force | Out-Null
}

# ---- Build action --------------------------------------------------------
$argList = @(
    "-NoProfile", "-NonInteractive",
    "-ExecutionPolicy", "Bypass",
    "-File", "`"$BackupScript`"",
    "-ConfigPath", "`"$ClusterYaml`"",
    "-BackupRoot", "`"$BackupRoot`"",
    "-RetentionHours", $RetentionHours
)
if ($PreferredNode) { $argList += @("-PreferredNode", $PreferredNode) }
$argString = $argList -join " "

$action = New-ScheduledTaskAction `
    -Execute $PwshExe `
    -Argument $argString `
    -WorkingDirectory $RepoRoot

$trigger = New-ScheduledTaskTrigger `
    -Once `
    -At ([DateTime]::Now.AddSeconds(60)) `
    -RepetitionInterval (New-TimeSpan -Minutes $CadenceMinutes)

$principalObj = New-ScheduledTaskPrincipal `
    -UserId "SYSTEM" `
    -LogonType ServiceAccount `
    -RunLevel Highest

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10) `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -Compatibility Win8

# ---- Register / replace --------------------------------------------------
$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Principal $principalObj `
    -Settings $settings `
    -Description "ForgeWire rqlite DR backup. Pulls /db/backup with preferred-node failover. Generic to host." | Out-Null

Write-Host "Registered scheduled task '$TaskName':" -ForegroundColor Green
Write-Host "  Cadence:        every $CadenceMinutes minute(s)"
Write-Host "  Preferred node: $(if ($PreferredNode) { $PreferredNode } else { '(none)' })"
Write-Host "  Backup root:    $BackupRoot"
Write-Host "  Retention:      $RetentionHours h"
Write-Host "  Repo root:      $RepoRoot"
Write-Host ""
Write-Host "First run scheduled at $(([DateTime]::Now.AddSeconds(60)).ToString('s'))"
Write-Host "Trigger now with:  Start-ScheduledTask -TaskName '$TaskName'"
