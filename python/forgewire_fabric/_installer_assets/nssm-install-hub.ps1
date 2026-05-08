<#
.SYNOPSIS
    Install the ForgeWire Hub as a Windows service via NSSM.

.DESCRIPTION
    Idempotent. If the service already exists it is updated in place.
    NSSM must be on PATH (https://nssm.cc/). The script:
      1. Writes the bearer token to a file (default: C:\ProgramData\forgewire\hub.token).
      2. Creates/updates a service named "ForgeWireHub" that runs:
           <PythonExe> -m forgewire_fabric.cli hub start --host 0.0.0.0 --port <Port> --db-path <DbPath>
         with FORGEWIRE_HUB_TOKEN_FILE pointing at the token file.
      3. Configures auto-start, restart-on-failure (10s back-off), and rotating logs.

    Run as Administrator.

.EXAMPLE
    pwsh -File nssm-install-hub.ps1 -PythonExe C:\Python311\python.exe `
        -Token (Get-Content hub.token -Raw) -Port 8765
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$PythonExe,
    [Parameter(Mandatory)][string]$Token,
    [int]$Port = 8765,
    [string]$BindHost = "0.0.0.0",
    [string]$DbPath = "C:\ProgramData\forgewire\hub.sqlite3",
    [string]$DataDir = "C:\ProgramData\forgewire",
    [string]$ServiceName = "ForgeWireHub",
    [ValidateSet("sqlite","rqlite")][string]$Backend = "sqlite",
    [string]$RqliteHost = "",
    [int]$RqlitePort = 4001,
    [ValidateSet("none","weak","strong","linearizable")][string]$RqliteConsistency = "weak",
    [switch]$NoWatchdog
)

$ErrorActionPreference = "Stop"

# ---- Self-elevation -------------------------------------------------------
# If we are not running with the Administrator token, relaunch ourselves
# elevated (UAC prompt) and wait for completion. This means a non-admin user
# can run the installer directly without remembering to open an admin shell.
$identity  = [System.Security.Principal.WindowsIdentity]::GetCurrent()
$principal = [System.Security.Principal.WindowsPrincipal]::new($identity)
if (-not $principal.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)) {
    $shellExe = (Get-Process -Id $PID).Path  # whichever pwsh/powershell launched us
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

if (-not (Get-Command nssm.exe -ErrorAction SilentlyContinue)) {
    throw "nssm.exe not found on PATH. Install from https://nssm.cc/ or via 'winget install nssm.nssm'."
}
if (-not (Test-Path $PythonExe)) {
    throw "Python interpreter not found at: $PythonExe"
}

$LogDir = Join-Path $DataDir "logs"
$TokenFile = Join-Path $DataDir "hub.token"
New-Item -ItemType Directory -Force -Path $DataDir, $LogDir, (Split-Path $DbPath) | Out-Null

# Write token with restrictive ACL (owner + SYSTEM + Administrators only).
[System.IO.File]::WriteAllText($TokenFile, $Token.Trim())
$fileInfo = [System.IO.FileInfo]::new($TokenFile)
$acl = $fileInfo.GetAccessControl()
$acl.SetAccessRuleProtection($true, $false)
foreach ($rule in @($acl.Access)) { [void]$acl.RemoveAccessRule($rule) }
foreach ($principal in @("NT AUTHORITY\SYSTEM", "BUILTIN\Administrators")) {
    $rule = New-Object System.Security.AccessControl.FileSystemAccessRule(
        $principal, "FullControl", "Allow")
    $acl.AddAccessRule($rule)
}
$fileInfo.SetAccessControl($acl)

$prevPref = $ErrorActionPreference
$ErrorActionPreference = "Continue"
& nssm.exe status $ServiceName *>$null
$exists = ($LASTEXITCODE -eq 0)
$ErrorActionPreference = $prevPref
if ($exists) {
    Write-Host "Service '$ServiceName' exists; updating in place."
    & nssm.exe stop $ServiceName confirm | Out-Null
} else {
    Write-Host "Installing service '$ServiceName'."
    & nssm.exe install $ServiceName $PythonExe | Out-Null
}

$cliArgs = @(
    "-m", "forgewire_fabric.cli", "hub", "start",
    "--host", $BindHost,
    "--port", $Port,
    "--db-path", "`"$DbPath`""
)
if ($Backend -eq "rqlite") {
    if ([string]::IsNullOrWhiteSpace($RqliteHost)) {
        throw "Backend=rqlite requires -RqliteHost (any cluster member host)."
    }
    $cliArgs += @(
        "--backend", "rqlite",
        "--rqlite-host", $RqliteHost,
        "--rqlite-port", $RqlitePort,
        "--rqlite-consistency", $RqliteConsistency
    )
} else {
    $cliArgs += @("--backend", "sqlite")
}
$cliArgs = $cliArgs -join " "

& nssm.exe set $ServiceName Application $PythonExe              | Out-Null
& nssm.exe set $ServiceName AppParameters $cliArgs               | Out-Null
& nssm.exe set $ServiceName AppDirectory $DataDir                | Out-Null
& nssm.exe set $ServiceName DisplayName "ForgeWire Hub"          | Out-Null
& nssm.exe set $ServiceName Description "ForgeWire signed remote dispatch hub" | Out-Null
& nssm.exe set $ServiceName Start SERVICE_AUTO_START             | Out-Null
& nssm.exe set $ServiceName AppExit Default Restart              | Out-Null
& nssm.exe set $ServiceName AppRestartDelay 10000                | Out-Null
& nssm.exe set $ServiceName AppStdout (Join-Path $LogDir "hub.out.log") | Out-Null
& nssm.exe set $ServiceName AppStderr (Join-Path $LogDir "hub.err.log") | Out-Null
& nssm.exe set $ServiceName AppRotateFiles 1                     | Out-Null
& nssm.exe set $ServiceName AppRotateOnline 1                    | Out-Null
& nssm.exe set $ServiceName AppRotateBytes 10485760              | Out-Null
& nssm.exe set $ServiceName AppEnvironmentExtra `
    "FORGEWIRE_HUB_TOKEN_FILE=$TokenFile" `
    "PYTHONUNBUFFERED=1" | Out-Null

# ---- Start + resume (idempotent) -----------------------------------------
# `nssm continue` is a no-op when the service is not paused; `nssm start` is
# ---- Start + resume (idempotent) -----------------------------------------
# In PowerShell 7+ native non-zero exit codes raise a terminating error when
# $PSNativeCommandUseErrorActionPreference is on (the default). Disable that
# locally so we can poll nssm without aborting on benign exit codes (e.g.
# 'continue' against a service that has not started yet).
$prevNative = $PSNativeCommandUseErrorActionPreference
$PSNativeCommandUseErrorActionPreference = $false
try {
    function Get-NssmStatus {
        return (& nssm.exe status $ServiceName 2>&1 | Out-String).Trim()
    }

    $status = Get-NssmStatus
    switch -Regex ($status) {
        'SERVICE_PAUSED'  { & nssm.exe continue $ServiceName 2>&1 | Out-Null }
        'SERVICE_STOPPED' { & nssm.exe start    $ServiceName 2>&1 | Out-Null }
        'SERVICE_RUNNING' { } # already running, no-op
        default           { & nssm.exe start    $ServiceName 2>&1 | Out-Null }
    }
    Start-Sleep -Seconds 2
    $status = Get-NssmStatus
    if ($status -eq 'SERVICE_PAUSED') {
        & nssm.exe continue $ServiceName 2>&1 | Out-Null
        Start-Sleep -Seconds 1
        $status = Get-NssmStatus
    }
    if ($status -ne 'SERVICE_RUNNING') {
        throw "Service '$ServiceName' is in unexpected state: '$status'. Check logs in $LogDir."
    }
} finally {
    $PSNativeCommandUseErrorActionPreference = $prevNative
}
Write-Host "Service status: $status"
Write-Host ""
Write-Host "Hub URL:    http://${env:COMPUTERNAME}:${Port}"
Write-Host "Token file: $TokenFile (RW only by SYSTEM + Administrators)"
Write-Host "Logs:       $LogDir"

# ---- Watchdog (belt-and-suspenders liveness) -----------------------------
# The hub process supervised by NSSM can become silently unreachable on
# Windows IOCP when the listening socket dies but the process keeps
# running (WinError 64 / 'Accept failed'). Install the /healthz watchdog
# scheduled task so it force-restarts the service after N consecutive
# probe failures. Pass -NoWatchdog to suppress.
if (-not $NoWatchdog) {
    $watchdog = Join-Path $PSScriptRoot "install-hub-watchdog.ps1"
    if (Test-Path $watchdog) {
        Write-Host ""
        Write-Host "Installing /healthz watchdog scheduled task..."
        & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $watchdog `
            -ServiceName $ServiceName `
            -HealthzUrl  "http://127.0.0.1:$Port/healthz"
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "Watchdog install returned exit $LASTEXITCODE; service is up but auto-recovery is disabled."
        }
    } else {
        Write-Warning "install-hub-watchdog.ps1 not found alongside this script ($watchdog); skipping watchdog install."
    }
}
