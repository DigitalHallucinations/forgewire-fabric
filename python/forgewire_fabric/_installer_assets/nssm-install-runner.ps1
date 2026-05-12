<#
.SYNOPSIS
    Install a ForgeWire Runner as a Windows service via NSSM.

.DESCRIPTION
    Idempotent. Creates/updates the "ForgeWireRunner" service to run:
        <PythonExe> -m forgewire_fabric.cli runner start
    with FORGEWIRE_HUB_URL, FORGEWIRE_HUB_TOKEN_FILE, FORGEWIRE_RUNNER_*
    set in the service environment.

.EXAMPLE
    pwsh -File nssm-install-runner.ps1 `
        -PythonExe C:\Python311\python.exe `
        -HubUrl https://hub.local `
        -Token (Get-Content hub.token -Raw) `
        -WorkspaceRoot C:\Work\repo `
        -Tags "windows,gpu:nvidia,python:3.11" `
        -ScopePrefixes "src/,tests/"
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$PythonExe,
    [Parameter(Mandatory)][string]$HubUrl,
    [Parameter(Mandatory)][string]$Token,
    [Parameter(Mandatory)][string]$WorkspaceRoot,
    [string]$Tags = "",
    [string]$ScopePrefixes = "",
    [int]$MaxConcurrent = 1,
    [string]$DataDir = "C:\ProgramData\forgewire",
    [string]$ServiceName = "ForgeWireRunner",
    [switch]$NoWatchdog,
    # ---- Cross-host hub failover (OOTB) ---------------------------------
    # When -HubSshHost + -HubSshUser + -HubSshKeyFile are supplied, the
    # installer also stages the hub watchdog on THIS machine, so any node
    # in the fabric can restart a wedged hub on a peer over SSH. The key
    # file is copied to a SYSTEM-readable location under DataDir; we never
    # leave it where the runner service account could read it.
    [string]$HubSshHost       = "",
    [string]$HubSshUser       = "",
    [string]$HubSshKeyFile    = "",
    [string]$HubServiceName   = "ForgeWireHub",
    [string]$HubHealthzUrl    = "",
    [switch]$NoHubWatchdog
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

if (-not (Get-Command nssm.exe -ErrorAction SilentlyContinue)) {
    throw "nssm.exe not found on PATH. Install from https://nssm.cc/."
}
if (-not (Test-Path $PythonExe)) { throw "Python not found: $PythonExe" }
if (-not (Test-Path $WorkspaceRoot)) { throw "Workspace not found: $WorkspaceRoot" }

$LogDir = Join-Path $DataDir "logs"
$TokenFile = Join-Path $DataDir "hub.token"
New-Item -ItemType Directory -Force -Path $DataDir, $LogDir | Out-Null

[System.IO.File]::WriteAllText($TokenFile, $Token.Trim())
$acl = Get-Acl $TokenFile
$acl.SetAccessRuleProtection($true, $false)
$acl.Access | ForEach-Object { $acl.RemoveAccessRule($_) | Out-Null }
foreach ($principal in @("NT AUTHORITY\SYSTEM", "BUILTIN\Administrators")) {
    $rule = New-Object System.Security.AccessControl.FileSystemAccessRule(
        $principal, "FullControl", "Allow")
    $acl.AddAccessRule($rule)
}
Set-Acl -Path $TokenFile -AclObject $acl

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

$cliArgs = @("-m", "forgewire_fabric.cli", "runner", "start") -join " "

& nssm.exe set $ServiceName Application $PythonExe       | Out-Null
& nssm.exe set $ServiceName AppParameters $cliArgs       | Out-Null
& nssm.exe set $ServiceName AppDirectory $WorkspaceRoot  | Out-Null
& nssm.exe set $ServiceName DisplayName "ForgeWire Runner" | Out-Null
& nssm.exe set $ServiceName Description "ForgeWire claim-loop runner" | Out-Null
& nssm.exe set $ServiceName Start SERVICE_AUTO_START     | Out-Null
& nssm.exe set $ServiceName AppExit Default Restart      | Out-Null
& nssm.exe set $ServiceName AppRestartDelay 10000        | Out-Null
& nssm.exe set $ServiceName AppStdout (Join-Path $LogDir "runner.out.log") | Out-Null
& nssm.exe set $ServiceName AppStderr (Join-Path $LogDir "runner.err.log") | Out-Null
& nssm.exe set $ServiceName AppRotateFiles 1             | Out-Null
& nssm.exe set $ServiceName AppRotateOnline 1            | Out-Null
& nssm.exe set $ServiceName AppRotateBytes 10485760      | Out-Null

$envVars = @(
    "FORGEWIRE_HUB_URL=$HubUrl",
    "FORGEWIRE_HUB_TOKEN_FILE=$TokenFile",
    "FORGEWIRE_RUNNER_WORKSPACE_ROOT=$WorkspaceRoot",
    "FORGEWIRE_RUNNER_MAX_CONCURRENT=$MaxConcurrent",
    "PYTHONUNBUFFERED=1"
)
if ($Tags)          { $envVars += "FORGEWIRE_RUNNER_TAGS=$Tags" }
if ($ScopePrefixes) { $envVars += "FORGEWIRE_RUNNER_SCOPE_PREFIXES=$ScopePrefixes" }

& nssm.exe set $ServiceName AppEnvironmentExtra @envVars | Out-Null

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
Write-Host "Logs: $LogDir"

# ---- Watchdog (belt-and-suspenders liveness) -----------------------------
# NSSM only sees process death. The runner can be "Running" but its
# heartbeat thread silently dead (DNS flap, hung httpx client, or a
# 'runner not registered' loop after a hub state reset). Install the
# hub-view-based liveness watchdog so it force-restarts the service
# after N consecutive heartbeat staleness windows. Pass -NoWatchdog to
# suppress.
if (-not $NoWatchdog) {
    $watchdog = Join-Path $PSScriptRoot "install-runner-watchdog.ps1"
    if (Test-Path $watchdog) {
        Write-Host ""
        Write-Host "Installing runner liveness watchdog scheduled task..."
        & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $watchdog `
            -ServiceName $ServiceName `
            -HubUrl      $HubUrl `
            -TokenFile   $TokenFile
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "Watchdog install returned exit $LASTEXITCODE; service is up but auto-recovery is disabled."
        }
    } else {
        Write-Warning "install-runner-watchdog.ps1 not found alongside this script ($watchdog); skipping watchdog install."
    }
}

# ---- Cross-host hub watchdog (OOTB) --------------------------------------
# Every fabric node carries a hub watchdog so any peer can restart a wedged
# hub. We chain into install-hub-watchdog.ps1 with SSH params when the
# operator supplied an SSH target. Key material is staged here so SYSTEM
# can read it from the scheduled task context (the runner-install caller
# typically holds the key in $env:USERPROFILE\.ssh\ which is per-user).
$wantHubWatchdog = $false
if (-not $NoHubWatchdog) {
    $sshGiven = [bool]($HubSshHost -or $HubSshUser -or $HubSshKeyFile)
    if ($sshGiven) {
        foreach ($pair in @(@('HubSshHost',$HubSshHost), @('HubSshUser',$HubSshUser), @('HubSshKeyFile',$HubSshKeyFile))) {
            if (-not $pair[1]) {
                throw "Cross-host hub watchdog requires all of -HubSshHost/-HubSshUser/-HubSshKeyFile. Missing: $($pair[0])."
            }
        }
        if (-not (Test-Path $HubSshKeyFile)) {
            throw "HubSshKeyFile not found: $HubSshKeyFile"
        }
        $wantHubWatchdog = $true
    }
}

if ($wantHubWatchdog) {
    $sshDir = Join-Path $DataDir "ssh"
    New-Item -ItemType Directory -Force -Path $sshDir | Out-Null

    # Copy the private key into a SYSTEM-readable location with an ACL
    # restricted to SYSTEM + BUILTIN\Administrators only.
    $stagedKey = Join-Path $sshDir "hub-restart.ed25519"
    Copy-Item -Force -Path $HubSshKeyFile -Destination $stagedKey
    $keyAcl = New-Object System.Security.AccessControl.FileSecurity
    $keyAcl.SetAccessRuleProtection($true, $false)
    foreach ($p in @("NT AUTHORITY\SYSTEM", "BUILTIN\Administrators")) {
        $keyAcl.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($p, "FullControl", "Allow")))
    }
    Set-Acl -Path $stagedKey -AclObject $keyAcl
    Write-Host "Staged hub-restart key at $stagedKey (SYSTEM/Administrators only)."

    # Seed known_hosts so the SYSTEM probe doesn't fail the first run on
    # an interactive accept-new prompt. ssh-keyscan is part of OpenSSH and
    # ships with Windows 10/Server 2019+.
    $knownHosts = Join-Path $sshDir "known_hosts"
    try {
        $scan = & ssh-keyscan.exe -T 5 -t ed25519,rsa,ecdsa $HubSshHost 2>$null
        if ($scan) {
            Set-Content -Path $knownHosts -Value $scan -Encoding ascii
            Write-Host "Seeded $knownHosts with $HubSshHost host keys."
        } else {
            Write-Warning "ssh-keyscan returned no output for $HubSshHost. The first probe will fail StrictHostKeyChecking; preseed $knownHosts manually."
        }
    } catch {
        Write-Warning "ssh-keyscan failed for ${HubSshHost}: $($_.Exception.Message). Preseed $knownHosts manually."
    }

    # Default the probe URL when the operator didn't override it.
    $effectiveHealthz = $HubHealthzUrl
    if (-not $effectiveHealthz) { $effectiveHealthz = ($HubUrl.TrimEnd('/') + "/healthz") }

    $hubWatchdog = Join-Path $PSScriptRoot "install-hub-watchdog.ps1"
    if (Test-Path $hubWatchdog) {
        Write-Host ""
        Write-Host "Installing cross-host hub liveness watchdog scheduled task..."
        & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $hubWatchdog `
            -HealthzUrl        $effectiveHealthz `
            -SshHost           $HubSshHost `
            -SshUser           $HubSshUser `
            -SshKeyFile        $stagedKey `
            -RemoteServiceName $HubServiceName `
            -KnownHostsFile    $knownHosts
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "Hub watchdog install returned exit $LASTEXITCODE; cross-host failover is disabled."
        }
    } else {
        Write-Warning "install-hub-watchdog.ps1 not found alongside this script ($hubWatchdog); skipping hub watchdog install."
    }
}
