<#
.SYNOPSIS
    Install a Windows scheduled task that monitors the ForgeWire runner
    against the hub's authoritative view and force-restarts the NSSM
    service when the runner has gone silent.

.DESCRIPTION
    Belt-and-suspenders for the runner side, mirroring install-hub-watchdog.ps1:
      * If the runner process dies, NSSM restarts it (AppExit Default Restart).
      * If the runner process is alive but failing to heartbeat (stuck client,
        DNS flap, lingering "runner not registered" loop after a hub state
        reset, or any other path where the service is Running but invisible
        to the hub), this watchdog detects it via the hub's /runners view
        and forces a service restart.

    Liveness signal: GET <HubUrl>/runners (bearer), filter by local
    COMPUTERNAME (case-insensitive), check that an entry exists and that
    last_heartbeat is within $StalenessSeconds. The hub view is the single
    source of truth -- the runner could lie about itself locally, so we
    don't ask it.

    Idempotent: re-running updates the existing task in place.

.PARAMETER ServiceName
    NSSM service to restart on failure. Default: ForgeWireRunner.

.PARAMETER HubUrl
    Hub base URL. Default: http://10.120.81.95:8765 (override per host
    if the cluster moves).

.PARAMETER TokenFile
    Path to the bearer token file. Default:
    C:\ProgramData\forgewire\hub.token.

.PARAMETER RunnerHostname
    Hostname to match against the hub's runner records. Default:
    $env:COMPUTERNAME (resolved at probe time, not install time).

.PARAMETER IntervalMinutes
    Probe interval. Default: 1.

.PARAMETER FailureThreshold
    Consecutive failures before restart. Default: 3 (3 minutes of silence).

.PARAMETER StalenessSeconds
    Maximum acceptable age of last_heartbeat before the runner is
    considered silent. Default: 120.

.PARAMETER TimeoutSeconds
    Per-probe HTTP timeout. Default: 5.

.PARAMETER LogPath
    JSONL log path. Default:
    C:\ProgramData\forgewire\logs\runner-watchdog.log.

.PARAMETER StateFile
    Failure-count state file. Default:
    C:\ProgramData\forgewire\runner-watchdog.state.

.PARAMETER TaskName
    Scheduled task name. Default: ForgeWireRunnerWatchdog.

.EXAMPLE
    pwsh -File install-runner-watchdog.ps1
    pwsh -File install-runner-watchdog.ps1 -HubUrl http://10.120.81.95:8765 -StalenessSeconds 90
#>
[CmdletBinding()]
param(
    [string]$ServiceName       = "ForgeWireRunner",
    [string]$HubUrl            = "http://10.120.81.95:8765",
    [string]$TokenFile         = "C:\ProgramData\forgewire\hub.token",
    [string]$RunnerHostname    = "",
    [int]   $IntervalMinutes   = 1,
    [int]   $FailureThreshold  = 3,
    [int]   $StalenessSeconds  = 120,
    [int]   $TimeoutSeconds    = 5,
    [string]$LogPath           = "C:\ProgramData\forgewire\logs\runner-watchdog.log",
    [string]$StateFile         = "C:\ProgramData\forgewire\runner-watchdog.state",
    [string]$TaskName          = "ForgeWireRunnerWatchdog"
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

$ProbeScript = "C:\ProgramData\forgewire\runner-watchdog-probe.ps1"
$ProbeDir    = Split-Path $ProbeScript
New-Item -ItemType Directory -Force -Path $ProbeDir, (Split-Path $LogPath) | Out-Null

$probeBody = @'
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$ServiceName,
    [Parameter(Mandatory)][string]$HubUrl,
    [Parameter(Mandatory)][string]$TokenFile,
    [Parameter()]         [string]$RunnerHostname = "",
    [Parameter(Mandatory)][int]$FailureThreshold,
    [Parameter(Mandatory)][int]$StalenessSeconds,
    [Parameter(Mandatory)][int]$TimeoutSeconds,
    [Parameter(Mandatory)][string]$LogPath,
    [Parameter(Mandatory)][string]$StateFile
)

$ErrorActionPreference = "Stop"
$ts = (Get-Date).ToUniversalTime().ToString("o")
if (-not $RunnerHostname) { $RunnerHostname = $env:COMPUTERNAME }

function Write-Log([string]$status, [hashtable]$extra) {
    $rec = @{ ts = $ts; status = $status; hostname = $RunnerHostname }
    foreach ($k in $extra.Keys) { $rec[$k] = $extra[$k] }
    $line = ($rec | ConvertTo-Json -Compress -Depth 4)
    try { Add-Content -Path $LogPath -Value $line -Encoding utf8 } catch {}
}

$count = 0
if (Test-Path $StateFile) {
    try { $count = [int](Get-Content $StateFile -ErrorAction Stop | Select-Object -First 1) } catch { $count = 0 }
}

# Liveness gate 1: NSSM service has to be Running. If it is anything else
# (Stopped, Paused, StartPending) we treat it as a hard failure regardless
# of what the hub view says, because the hub view will be stale.
$svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if (-not $svc) {
    Write-Log "fail" @{ reason = "service_missing"; consecutive_failures = ($count + 1) }
    $count++
} elseif ($svc.Status -ne "Running") {
    Write-Log "fail" @{ reason = "service_status"; status = $svc.Status.ToString(); consecutive_failures = ($count + 1) }
    $count++
} else {
    # Liveness gate 2: hub must see us with a recent heartbeat.
    $token = $null
    try {
        $token = (Get-Content -Path $TokenFile -Raw -ErrorAction Stop).Trim()
    } catch {
        Write-Log "skip" @{ reason = "token_unreadable"; error = $_.Exception.Message }
        # Without a token we cannot adjudicate liveness; do not increment.
        if ($count -ne 0) { Set-Content -Path $StateFile -Value "0" -Encoding ASCII -NoNewline }
        exit 0
    }
    $hdrs = @{ Authorization = "Bearer $token" }
    $url  = $HubUrl.TrimEnd('/') + "/runners"
    $entry = $null
    $err   = $null
    $code  = $null
    try {
        $resp = Invoke-WebRequest -UseBasicParsing -Headers $hdrs -Uri $url -TimeoutSec $TimeoutSeconds
        $code = [int]$resp.StatusCode
        $body = $resp.Content | ConvertFrom-Json
        $entry = @($body.runners | Where-Object {
            $_.hostname -and ($_.hostname.ToLowerInvariant() -eq $RunnerHostname.ToLowerInvariant())
        }) | Select-Object -First 1
    } catch {
        $err = $_.Exception.Message
    }

    if ($err) {
        # Hub unreachable: we cannot adjudicate liveness. Do NOT count this
        # as a runner failure -- it would falsely restart the runner during
        # a hub outage. Log and skip.
        Write-Log "skip" @{ reason = "hub_unreachable"; error = $err }
        if ($count -ne 0) { Set-Content -Path $StateFile -Value "0" -Encoding ASCII -NoNewline }
        exit 0
    }

    if (-not $entry) {
        $count++
        Write-Log "fail" @{ reason = "not_registered"; code = $code; consecutive_failures = $count }
    } else {
        $stale = $true
        $ageSeconds = $null
        try {
            # Parse via DateTimeOffset so the 'Z' (or any +HH:MM) suffix the
            # hub emits is honoured. [DateTime]::Parse silently coerces ISO
            # 'Z' strings into local-kind DateTime on some Windows locales,
            # producing a 5- to 8-hour offset error vs (Get-Date).ToUniversalTime().
            $hbUtc = [DateTimeOffset]::Parse($entry.last_heartbeat).UtcDateTime
            $ageSeconds = [int]((Get-Date).ToUniversalTime() - $hbUtc).TotalSeconds
            $stale = $ageSeconds -gt $StalenessSeconds
        } catch {}

        if ($stale) {
            $count++
            Write-Log "fail" @{
                reason = "heartbeat_stale";
                last_heartbeat = $entry.last_heartbeat;
                age_seconds = $ageSeconds;
                staleness_threshold = $StalenessSeconds;
                consecutive_failures = $count
            }
        } else {
            if ($count -ne 0) { Set-Content -Path $StateFile -Value "0" -Encoding ASCII -NoNewline }
            Write-Log "ok" @{
                runner_id = $entry.runner_id;
                state = $entry.state;
                last_heartbeat = $entry.last_heartbeat;
                age_seconds = $ageSeconds;
                consecutive_failures = 0
            }
            exit 0
        }
    }
}

Set-Content -Path $StateFile -Value "$count" -Encoding ASCII -NoNewline

if ($count -ge $FailureThreshold) {
    Write-Log "restart" @{ service = $ServiceName; consecutive_failures = $count }
    $restarted = $false
    $nssm = $null
    foreach ($cand in @(
            (Get-Command nssm.exe -ErrorAction SilentlyContinue).Source,
            "$env:ProgramData\chocolatey\bin\nssm.exe",
            "$env:ProgramFiles\nssm\nssm.exe",
            "$env:ProgramFiles(x86)\nssm\nssm.exe",
            "C:\Users\*\AppData\Local\Microsoft\WinGet\Links\nssm.exe"
        )) {
        if ($cand) {
            $resolved = @(Resolve-Path -Path $cand -ErrorAction SilentlyContinue) | Select-Object -First 1
            if ($resolved -and (Test-Path $resolved.Path)) { $nssm = $resolved.Path; break }
        }
    }
    try {
        if ($nssm) {
            & $nssm restart $ServiceName 2>&1 | Out-String | ForEach-Object { Add-Content -Path $LogPath -Value ("# nssm: " + $_.Trim()) -Encoding utf8 }
            $restarted = $true
            Write-Log "restart_via" @{ method = "nssm" }
        } else {
            Restart-Service -Name $ServiceName -Force -ErrorAction Stop
            $restarted = $true
            Write-Log "restart_via" @{ method = "Restart-Service" }
        }
    } catch {
        $methodName = if ($nssm) { "nssm" } else { "Restart-Service" }
        Write-Log "restart_error" @{ error = $_.Exception.Message; method = $methodName }
        try {
            Get-Service -Name $ServiceName -ErrorAction SilentlyContinue | Stop-Service -Force -ErrorAction SilentlyContinue
            Start-Sleep -Seconds 2
            Start-Service -Name $ServiceName -ErrorAction SilentlyContinue
            $restarted = $true
            Write-Log "restart_via" @{ method = "Stop+Start" }
        } catch {
            Write-Log "restart_error_final" @{ error = $_.Exception.Message }
        }
    } finally {
        if ($restarted) { Set-Content -Path $StateFile -Value "0" -Encoding ASCII -NoNewline }
    }
}
'@

Set-Content -Path $ProbeScript -Value $probeBody -Encoding utf8

# Build argument list for the scheduled task. RunnerHostname is optional;
# omit when blank so the probe falls back to $env:COMPUTERNAME at runtime.
$argParts = @(
    "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $ProbeScript,
    "-ServiceName",      $ServiceName,
    "-HubUrl",           $HubUrl,
    "-TokenFile",        $TokenFile,
    "-FailureThreshold", $FailureThreshold,
    "-StalenessSeconds", $StalenessSeconds,
    "-TimeoutSeconds",   $TimeoutSeconds,
    "-LogPath",          $LogPath,
    "-StateFile",        $StateFile
)
if ($RunnerHostname) {
    $argParts += @("-RunnerHostname", $RunnerHostname)
}
$probeArgs = $argParts -join " "

# Resolve a PowerShell host that SYSTEM can launch. pwsh 7 from the
# Microsoft Store ships under C:\Program Files\WindowsApps\... which is
# NOT executable by SYSTEM scheduled tasks (ERROR_FILE_NOT_FOUND). Prefer
# pwsh 7 only at its msi/zip install location; otherwise fall back to the
# built-in Windows PowerShell 5.1 which is always present at a fixed path.
$pwshCandidates = @(
    "$env:ProgramFiles\PowerShell\7\pwsh.exe",
    "$env:ProgramFiles(x86)\PowerShell\7\pwsh.exe",
    "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"
)
$psHost = $null
foreach ($c in $pwshCandidates) { if ($c -and (Test-Path $c)) { $psHost = $c; break } }
if (-not $psHost) { throw "No SYSTEM-reachable PowerShell host found." }

$action     = New-ScheduledTaskAction -Execute $psHost -Argument $probeArgs
$trigger    = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
                -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes)
$principalT = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
$settings   = New-ScheduledTaskSettingsSet `
                -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
                -StartWhenAvailable `
                -ExecutionTimeLimit (New-TimeSpan -Minutes 5) `
                -MultipleInstances IgnoreNew

Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
    -Principal $principalT -Settings $settings `
    -Description "ForgeWire runner liveness watchdog. Restarts $ServiceName after $FailureThreshold consecutive heartbeat-staleness failures (>$StalenessSeconds s) observed via the hub's /runners view." | Out-Null

Write-Host "Installed scheduled task '$TaskName'."
Write-Host "  Hub:       $HubUrl"
Write-Host "  Hostname:  $(if ($RunnerHostname) { $RunnerHostname } else { '(env:COMPUTERNAME at probe time)' })"
Write-Host "  Probe:     every $IntervalMinutes min, threshold=$FailureThreshold, staleness=${StalenessSeconds}s, timeout=${TimeoutSeconds}s"
Write-Host "  Service:   $ServiceName"
Write-Host "  Log:       $LogPath"
Write-Host "  State:     $StateFile"
