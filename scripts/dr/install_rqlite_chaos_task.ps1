<#
.SYNOPSIS
    Install / update the ForgeWire rqlite chaos-drill scheduled task.

.DESCRIPTION
    Registers a Windows Task Scheduler job that periodically invokes
    ``scripts\dr\chaos_drills.ps1`` against the live cluster. Defaults
    are pulled from the ``chaos:`` block in ``config\cluster.yaml`` so
    cadence, drill set, log root, and retention are config-driven.

    Idempotent. If the task already exists it is replaced.
    Self-elevating; relaunches with UAC if not already Administrator.

    Single-driver rule
    ------------------
    Only one host should run the chaos drills at a time (multiple
    drivers would race each other restarting services). The configured
    driver is ``cfg.chaos.driver_node`` (a voter label). This installer
    refuses to register the task unless the local host's preferred
    voter matches that driver, unless ``-Force`` is passed.

    Cross-host service control
    --------------------------
    Drills stop/start the leader's Windows service. When the leader is
    on a remote host the script SSHs to ``cfg.voters[].ssh_alias``. The
    scheduled task runs under -Principal (default SYSTEM); if SSH must
    reach a remote voter, install the SSH key into that principal's
    profile (``%WINDIR%\System32\config\systemprofile\.ssh\`` for
    SYSTEM) or pass ``-Principal <DOMAIN\user>`` and let the task
    prompt for a stored credential.

.PARAMETER TaskName
    Default "ForgeWireRqliteChaos".

.PARAMETER CadenceMinutes
    Repetition interval. Default = cfg.chaos.cadence_minutes (1440 = 24 h).

.PARAMETER Drills
    Comma-separated drill list. Default = cfg.chaos.drills.

.PARAMETER LogDir
    JSONL output directory. Default = cfg.chaos.log_root.

.PARAMETER RetentionDays
    Default = cfg.chaos.retention_days (30).

.PARAMETER Principal
    Task principal. "SYSTEM" (default) or a "DOMAIN\user" name.

.PARAMETER Force
    Skip the driver_node guard.

.PARAMETER RepoRoot
    Path to the forgewire-fabric repo. Defaults to the parent-of-parent
    of this installer.

.PARAMETER PwshExe
    Full path to pwsh.exe. Defaults to (Get-Command pwsh).Source.

.EXAMPLE
    pwsh -File install_rqlite_chaos_task.ps1
    pwsh -File install_rqlite_chaos_task.ps1 -CadenceMinutes 720 -Force
#>
[CmdletBinding()]
param(
    [string]$TaskName = "ForgeWireRqliteChaos",
    [Nullable[int]]$CadenceMinutes,
    [string]$Drills,
    [string]$LogDir,
    [Nullable[int]]$RetentionDays,
    [string]$Principal = "SYSTEM",
    [switch]$Force,
    [string]$RepoRoot,
    [string]$PwshExe
)

$ErrorActionPreference = "Stop"

# ---- Self-elevation -------------------------------------------------------
$id   = [System.Security.Principal.WindowsIdentity]::GetCurrent()
$prin = [System.Security.Principal.WindowsPrincipal]::new($id)
if (-not $prin.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)) {
    $shellExe = (Get-Process -Id $PID).Path
    $argList = @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $PSCommandPath)
    foreach ($kv in $PSBoundParameters.GetEnumerator()) {
        if ($null -eq $kv.Value) { continue }
        if ($kv.Value -is [switch]) {
            if ($kv.Value.IsPresent) { $argList += "-$($kv.Key)" }
        } else {
            $argList += "-$($kv.Key)"
            $argList += [string]$kv.Value
        }
    }
    Start-Process -FilePath $shellExe -ArgumentList $argList -Verb RunAs -Wait
    return
}

# ---- Resolve paths --------------------------------------------------------
if (-not $RepoRoot) {
    $RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}
$RepoRoot = (Resolve-Path $RepoRoot).Path
$ChaosScript  = Join-Path $RepoRoot "scripts\dr\chaos_drills.ps1"
$ConfigScript = Join-Path $RepoRoot "scripts\dr\_cluster_config.ps1"
$ClusterYaml  = Join-Path $RepoRoot "config\cluster.yaml"
foreach ($p in @($ChaosScript, $ConfigScript, $ClusterYaml)) {
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
$ch  = $cfg["chaos"]; if (-not $ch) { $ch = @{} }

if ($null -eq $ch["enabled"]) {
    # default enabled
} elseif ([string]$ch["enabled"] -in @("false","False","0","no","off")) {
    Write-Host "cfg.chaos.enabled=false; skipping installation." -ForegroundColor Yellow
    return
}

if ($null -eq $CadenceMinutes) {
    $CadenceMinutes = if ($null -ne $ch["cadence_minutes"]) { [int]$ch["cadence_minutes"] } else { 1440 }
}
if (-not $Drills) {
    $Drills = if ($ch["drills"]) { [string]$ch["drills"] } else { "kill-leader,lose-quorum" }
}
if (-not $LogDir) {
    $LogDir = if ($ch["log_root"]) { [string]$ch["log_root"] } else { 'C:\ProgramData\forgewire\rqlite-chaos' }
}
if ($null -eq $RetentionDays) {
    $RetentionDays = if ($null -ne $ch["retention_days"]) { [int]$ch["retention_days"] } else { 30 }
}
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

# ---- Driver guard --------------------------------------------------------
$driver = $ch["driver_node"]
$preferred = $cfg["preferred_node"]
if ($driver -and -not $Force) {
    if ($driver -ne $preferred) {
        throw ("This host's preferred_node is '$preferred' but cfg.chaos.driver_node='$driver'. " +
               "Refusing to install. Pass -Force to override, or run on the driver host.")
    }
}

# ---- SSH-for-SYSTEM provisioning (automatic when running as SYSTEM) -------
# When the chaos task runs under SYSTEM and a drill needs to reach a remote
# voter, SYSTEM needs its own SSH key. We provision that here unless the
# operator has opted out via cfg.chaos.ssh.provision_for_system=false or
# selected -Principal user (in which case the user's own ~/.ssh applies).
$sshCfg = $ch["ssh"]
$provisionSsh = $true
if ($sshCfg -and ($null -ne $sshCfg["provision_for_system"])) {
    $val = $sshCfg["provision_for_system"]
    if ($val -is [bool]) { $provisionSsh = $val }
    elseif ([string]$val -in @("false","False","0","no","off")) { $provisionSsh = $false }
}
if ($Principal -ine "SYSTEM") { $provisionSsh = $false }
if ($provisionSsh) {
    $sshInstaller = Join-Path $RepoRoot "scripts\dr\install_ssh_for_system.ps1"
    if (Test-Path $sshInstaller) {
        Write-Host "Provisioning SSH identity for SYSTEM (cfg.chaos.ssh)..." -ForegroundColor Cyan
        try {
            & $sshInstaller -ConfigPath $ClusterYaml
        } catch {
            Write-Warning "SSH-for-SYSTEM provisioning failed: $($_.Exception.Message). Cross-host drills will skip with a hint until this is resolved."
        }
    }
}

# ---- Build action --------------------------------------------------------
$argList = @(
    "-NoProfile", "-NonInteractive",
    "-ExecutionPolicy", "Bypass",
    "-File", "`"$ChaosScript`"",
    "-ConfigPath", "`"$ClusterYaml`"",
    "-LogDir", "`"$LogDir`"",
    "-Drills", "`"$Drills`"",
    "-RetentionDays", $RetentionDays
)
$argString = $argList -join " "

$action = New-ScheduledTaskAction `
    -Execute $PwshExe `
    -Argument $argString `
    -WorkingDirectory $RepoRoot

# Repeat for ~10 years; Task Scheduler treats RepetitionDuration=0 as
# "no repetition", so we use a long horizon instead.
$trigger = New-ScheduledTaskTrigger `
    -Once `
    -At ([DateTime]::Now.AddMinutes(10)) `
    -RepetitionInterval (New-TimeSpan -Minutes $CadenceMinutes) `
    -RepetitionDuration (New-TimeSpan -Days 3650)

if ($Principal -ieq "SYSTEM") {
    $principalObj = New-ScheduledTaskPrincipal `
        -UserId "SYSTEM" `
        -LogonType ServiceAccount `
        -RunLevel Highest
} else {
    $principalObj = New-ScheduledTaskPrincipal `
        -UserId $Principal `
        -LogonType S4U `
        -RunLevel Highest
}

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 15) `
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
    -Description "ForgeWire rqlite chaos drills. Topology-driven via config\cluster.yaml." | Out-Null

Write-Host "Registered scheduled task '$TaskName':" -ForegroundColor Green
Write-Host "  Cadence:       every $CadenceMinutes minute(s)"
Write-Host "  Drills:        $Drills"
Write-Host "  Log dir:       $LogDir"
Write-Host "  Retention:     $RetentionDays day(s)"
Write-Host "  Principal:     $Principal"
Write-Host "  Driver node:   $(if ($driver) { $driver } else { '(unset)' }) (this host preferred=$preferred)"
Write-Host "  Repo root:     $RepoRoot"
Write-Host ""
Write-Host "First run scheduled at $(([DateTime]::Now.AddMinutes(10)).ToString('s'))"
Write-Host "Trigger now with:  Start-ScheduledTask -TaskName '$TaskName'"
