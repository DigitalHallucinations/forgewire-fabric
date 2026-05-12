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
    [string]$TaskName        = "ForgeWireHubWatchdog",
    # ---- Cross-host failover (option B) ---------------------------------
    # When set, the probe restarts the hub via OpenSSH instead of
    # Restart-Service locally. All four params must be supplied together.
    # The HealthzUrl should target the remote hub (not 127.0.0.1).
    # The key file must be readable by SYSTEM (the scheduled task account).
    [string]$SshHost           = "",
    [string]$SshUser           = "",
    [string]$SshKeyFile        = "",
    [string]$RemoteServiceName = "",
    [string]$KnownHostsFile    = "C:\ProgramData\forgewire\ssh\known_hosts"
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
    [Parameter(Mandatory)][string]$StateFile,
    [Parameter()][string]$SshHost           = "",
    [Parameter()][string]$SshUser           = "",
    [Parameter()][string]$SshKeyFile        = "",
    [Parameter()][string]$RemoteServiceName = "",
    [Parameter()][string]$KnownHostsFile    = ""
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
    Write-Log "restart" @{ service = $ServiceName; consecutive_failures = $count; remote = [bool]$SshHost }
    $restarted = $false

    # --- Cross-host failover via OpenSSH --------------------------------
    if ($SshHost -and $SshUser -and $SshKeyFile -and $RemoteServiceName) {
        $sshExe = $null
        foreach ($cand in @(
                (Get-Command ssh.exe -ErrorAction SilentlyContinue).Source,
                "$env:SystemRoot\System32\OpenSSH\ssh.exe",
                "$env:ProgramFiles\OpenSSH\ssh.exe"
            )) {
            if ($cand -and (Test-Path $cand)) { $sshExe = $cand; break }
        }
        if (-not $sshExe) {
            Write-Log "restart_error" @{ error = "ssh.exe not found"; method = "ssh" }
        } elseif (-not (Test-Path $SshKeyFile)) {
            Write-Log "restart_error" @{ error = "SshKeyFile not found: $SshKeyFile"; method = "ssh" }
        } else {
            $remoteCmd = "powershell -NoProfile -Command `"Restart-Service $RemoteServiceName -Force`""
            $sshArgs = @(
                "-i", $SshKeyFile,
                "-o", "BatchMode=yes",
                "-o", "StrictHostKeyChecking=accept-new",
                "-o", "ConnectTimeout=10"
            )
            if ($KnownHostsFile) {
                $khDir = Split-Path $KnownHostsFile
                if ($khDir -and -not (Test-Path $khDir)) { New-Item -ItemType Directory -Force -Path $khDir | Out-Null }
                $sshArgs += @("-o", "UserKnownHostsFile=$KnownHostsFile")
            }
            $sshArgs += @("$SshUser@$SshHost", $remoteCmd)
            try {
                $sshOut = & $sshExe @sshArgs 2>&1 | Out-String
                $sshOut.Trim().Split([Environment]::NewLine) | ForEach-Object {
                    if ($_) { Add-Content -Path $LogPath -Value ("# ssh: " + $_) -Encoding utf8 }
                }
                if ($LASTEXITCODE -eq 0) {
                    $restarted = $true
                    Write-Log "restart_via" @{ method = "ssh"; target = "$SshUser@$SshHost"; service = $RemoteServiceName }
                } else {
                    Write-Log "restart_error" @{ method = "ssh"; exit_code = $LASTEXITCODE }
                }
            } catch {
                Write-Log "restart_error" @{ method = "ssh"; error = $_.Exception.Message }
            }
        }
        if ($restarted) { Set-Content -Path $StateFile -Value "0" -Encoding ASCII -NoNewline }
        return
    }

    # --- Local restart (NSSM preferred, Restart-Service fallback) -------
    # Try nssm first (preserves rotation/log config), falling back to the
    # built-in Restart-Service which is always available under SYSTEM PATH.
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
        } else {
            Restart-Service -Name $ServiceName -Force -ErrorAction Stop
            $restarted = $true
            Write-Log "restart_via" @{ method = "Restart-Service" }
        }
    } catch {
        $methodName = if ($nssm) { "nssm" } else { "Restart-Service" }
        Write-Log "restart_error" @{ error = $_.Exception.Message; method = $methodName }
        # Last-ditch: kill the process so the supervisor brings us back.
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

# Validate cross-host failover params (all-or-nothing).
$remote = [bool]($SshHost -or $SshUser -or $SshKeyFile -or $RemoteServiceName)
if ($remote) {
    foreach ($pair in @(@('SshHost',$SshHost), @('SshUser',$SshUser), @('SshKeyFile',$SshKeyFile), @('RemoteServiceName',$RemoteServiceName))) {
        if (-not $pair[1]) { throw "Cross-host failover requires all of -SshHost/-SshUser/-SshKeyFile/-RemoteServiceName. Missing: $($pair[0])." }
    }
    if (-not (Test-Path $SshKeyFile)) {
        throw "SshKeyFile not found at $SshKeyFile. Place the SYSTEM-readable private key there before installing the watchdog."
    }
}

$argList = @(
    "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $ProbeScript,
    "-ServiceName",      $ServiceName,
    "-HealthzUrl",       $HealthzUrl,
    "-FailureThreshold", $FailureThreshold,
    "-TimeoutSeconds",   $TimeoutSeconds,
    "-LogPath",          $LogPath,
    "-StateFile",        $StateFile
)
if ($remote) {
    $argList += @(
        "-SshHost",           $SshHost,
        "-SshUser",           $SshUser,
        "-SshKeyFile",        $SshKeyFile,
        "-RemoteServiceName", $RemoteServiceName,
        "-KnownHostsFile",    $KnownHostsFile
    )
}
$probeArgs = $argList -join " "

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

$action    = New-ScheduledTaskAction -Execute $psHost -Argument $probeArgs
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
if ($remote) {
    Write-Host "  Restart:   remote via ssh $SshUser@$SshHost -i $SshKeyFile (service: $RemoteServiceName)"
} else {
    Write-Host "  Restart:   local service '$ServiceName'"
}
Write-Host "  Log:       $LogPath"
Write-Host "  State:     $StateFile"
