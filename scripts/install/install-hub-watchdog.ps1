<#
.SYNOPSIS
    Install a Windows scheduled task that probes the ForgeWire hub /healthz
    every minute and force-restarts the NSSM service after N consecutive
    failures.

.DESCRIPTION
    Belt-and-suspenders for the hub:
      * If the hub process dies, NSSM restarts it (AppExit Default Restart).
      * If the hub process is alive but the listening socket is dead (the
        Windows IOCP "Accept failed" / WinError 64 failure mode), this
        watchdog detects it via /healthz and forces a service restart.

    Idempotent: re-running updates the existing task in place.

.PARAMETER ServiceName
    NSSM service to restart on failure. Default: ForgeWireHub.

.PARAMETER HealthzUrl
    HTTP URL to probe. Default: http://127.0.0.1:8765/healthz.

.PARAMETER IntervalMinutes
    Probe interval. Default: 1.

.PARAMETER FailureThreshold
    Consecutive failures before restart. Default: 3.

.PARAMETER TimeoutSeconds
    Per-probe timeout. Default: 5.

.PARAMETER LogPath
    JSONL log path. Default: C:\ProgramData\forgewire\logs\hub-watchdog.log.

.PARAMETER StateFile
    Failure-count state file. Default:
    C:\ProgramData\forgewire\hub-watchdog.state.

.PARAMETER TaskName
    Scheduled task name. Default: ForgeWireHubWatchdog.

.EXAMPLE
    pwsh -File install-hub-watchdog.ps1
#>
[CmdletBinding()]
param(
    [string]$ServiceName     = "ForgeWireHub",
    [string]$HealthzUrl      = "http://127.0.0.1:8765/healthz",
    [int]   $IntervalMinutes = 1,
    [int]   $FailureThreshold = 3,
    [int]   $TimeoutSeconds  = 5,
    [string]$LogPath         = "C:\ProgramData\forgewire\logs\hub-watchdog.log",
    [string]$StateFile       = "C:\ProgramData\forgewire\hub-watchdog.state",
    [string]$TaskName        = "ForgeWireHubWatchdog"
)

$ErrorActionPreference = "Stop"

# ---- Self-elevation -------------------------------------------------------
$identity  = [System.Security.Principal.WindowsIdentity]::GetCurrent()
$principal = [System.Security.Principal.WindowsPrincipal]::new($identity)
if (-not $principal.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)) {
    $shellExe = (Get-Process -Id $PID).Path
    $forwarded = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $PSCommandPath)
    foreach ($k in $PSBoundParameters.Keys) {
        $v = $PSBoundParameters[$k]
        if ($v -is [switch]) { if ($v.IsPresent) { $forwarded += "-$k" } }
        else                 { $forwarded += "-$k"; $forwarded += $v }
    }
    Write-Host "Elevating: $shellExe $($forwarded -join ' ')"
    $proc = Start-Process -FilePath $shellExe -Verb RunAs -Wait -PassThru -ArgumentList $forwarded
    exit $proc.ExitCode
}

$ProbeScript = "C:\ProgramData\forgewire\hub-watchdog-probe.ps1"
$ProbeDir    = Split-Path $ProbeScript
New-Item -ItemType Directory -Force -Path $ProbeDir, (Split-Path $LogPath) | Out-Null

$probeBody = @'
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$ServiceName,
    [Parameter(Mandatory)][string]$HealthzUrl,
    [Parameter(Mandatory)][int]$FailureThreshold,
    [Parameter(Mandatory)][int]$TimeoutSeconds,
    [Parameter(Mandatory)][string]$LogPath,
    [Parameter(Mandatory)][string]$StateFile
)

$ErrorActionPreference = "Stop"
$ts = (Get-Date).ToUniversalTime().ToString("o")

function Write-Log([string]$status, [hashtable]$extra) {
    $rec = @{ ts = $ts; status = $status }
    foreach ($k in $extra.Keys) { $rec[$k] = $extra[$k] }
    $line = ($rec | ConvertTo-Json -Compress -Depth 4)
    try { Add-Content -Path $LogPath -Value $line -Encoding utf8 } catch {}
}

$count = 0
if (Test-Path $StateFile) {
    try { $count = [int](Get-Content $StateFile -ErrorAction Stop | Select-Object -First 1) } catch { $count = 0 }
}

$ok = $false; $code = $null; $err = $null
try {
    $resp = Invoke-WebRequest -UseBasicParsing -Uri $HealthzUrl -TimeoutSec $TimeoutSeconds
    $code = [int]$resp.StatusCode
    $ok   = ($code -ge 200 -and $code -lt 500)
} catch {
    $err = $_.Exception.Message
}

if ($ok) {
    if ($count -ne 0) { Set-Content -Path $StateFile -Value "0" -Encoding ASCII -NoNewline }
    Write-Log "ok" @{ code = $code; consecutive_failures = 0 }
    exit 0
}

$count++
Set-Content -Path $StateFile -Value "$count" -Encoding ASCII -NoNewline
Write-Log "fail" @{ code = $code; error = $err; consecutive_failures = $count }

if ($count -ge $FailureThreshold) {
    Write-Log "restart" @{ service = $ServiceName; consecutive_failures = $count }
    try {
        $nssm = (Get-Command nssm.exe -ErrorAction SilentlyContinue).Source
        if (-not $nssm) { $nssm = "nssm.exe" }
        & $nssm restart $ServiceName 2>&1 | Out-String | ForEach-Object { Add-Content -Path $LogPath -Value ("# nssm: " + $_.Trim()) -Encoding utf8 }
    } catch {
        Write-Log "restart_error" @{ error = $_.Exception.Message }
    } finally {
        Set-Content -Path $StateFile -Value "0" -Encoding ASCII -NoNewline
    }
}
'@

Set-Content -Path $ProbeScript -Value $probeBody -Encoding utf8

$probeArgs = @(
    "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $ProbeScript,
    "-ServiceName",      $ServiceName,
    "-HealthzUrl",       $HealthzUrl,
    "-FailureThreshold", $FailureThreshold,
    "-TimeoutSeconds",   $TimeoutSeconds,
    "-LogPath",          $LogPath,
    "-StateFile",        $StateFile
) -join " "

$action    = New-ScheduledTaskAction -Execute "pwsh.exe" -Argument $probeArgs
$trigger   = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
                -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes)
$principalT = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
$settings  = New-ScheduledTaskSettingsSet `
                -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
                -StartWhenAvailable `
                -ExecutionTimeLimit (New-TimeSpan -Minutes 5) `
                -MultipleInstances IgnoreNew

Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
    -Principal $principalT -Settings $settings `
    -Description "ForgeWire hub /healthz watchdog. Restarts $ServiceName after $FailureThreshold consecutive failures." | Out-Null

Write-Host "Installed scheduled task '$TaskName'."
Write-Host "  Probe:     $HealthzUrl every $IntervalMinutes min, threshold=$FailureThreshold, timeout=${TimeoutSeconds}s"
Write-Host "  Service:   $ServiceName"
Write-Host "  Log:       $LogPath"
Write-Host "  State:     $StateFile"
