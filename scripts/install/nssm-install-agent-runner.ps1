<#
.SYNOPSIS
    Install a ForgeWire kind:agent runner as a Windows service via NSSM.

.DESCRIPTION
    Idempotent. Creates/updates the "ForgeWireAgentRunner" service to run:
        <PythonExe> -m forgewire_fabric.runner.agent_kind
    with FORGEWIRE_HUB_URL, FORGEWIRE_HUB_TOKEN_FILE, and agent-runner
    workspace/identity env vars set in the service environment.

    The kind:agent runner uses a built-in marker-file harness executor
    (see python/forgewire_fabric/runner/agent_kind.py); it is the
    persistent sibling of the shell-exec kind:command runner installed
    by nssm-install-runner.ps1.

.EXAMPLE
    pwsh -File nssm-install-agent-runner.ps1 `
        -PythonExe C:\Python311\python.exe `
        -HubUrl http://10.120.81.95:8765 `
        -Token (Get-Content $env:USERPROFILE\.forgewire\hub.token -Raw)
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$PythonExe,
    [Parameter(Mandatory)][string]$HubUrl,
    [Parameter(Mandatory)][string]$Token,
    [string]$WorkspaceRoot = "C:\ProgramData\forgewire\agent-sandbox",
    [string]$IdentityFile  = "C:\ProgramData\forgewire\agent_runner_identity.json",
    [int]$MaxConcurrent    = 1,
    [string]$Tags          = "",
    [string]$DataDir       = "C:\ProgramData\forgewire",
    [string]$ServiceName   = "ForgeWireAgentRunner",
    [string]$DisplayName   = "ForgeWire Agent Runner",
    [string]$Description   = "Persistent kind:agent claim-loop runner (marker-file harness executor)."
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

# ---- Pre-flight ----------------------------------------------------------
# NSSM marks a service SERVICE_PAUSED via its throttle-on-rapid-exit heuristic
# whenever the child process exits faster than AppRestartDelay. The #1 cause
# in the field is a Python env that cannot import the runner module (e.g. a
# stale `git pull` on the forgewire-fabric checkout). Catch that here so we
# never install a service that's pre-destined to crash-loop.
Write-Host "Pre-flight: importing forgewire_fabric.runner.agent_kind via $PythonExe ..."
$prevPref0 = $ErrorActionPreference
$ErrorActionPreference = "Continue"
$preflight = & $PythonExe -c "import forgewire_fabric.runner.agent_kind" 2>&1
$preflightExit = $LASTEXITCODE
$ErrorActionPreference = $prevPref0
if ($preflightExit -ne 0) {
    $preflightText = ($preflight | Out-String).Trim()
    throw @"
Pre-flight import failed (exit $preflightExit) for forgewire_fabric.runner.agent_kind
using interpreter:
    $PythonExe

Output:
$preflightText

Resolve the environment before installing the service. Common causes:
  * The forgewire-fabric checkout this venv was built against is behind
    origin/main: run 'git pull' in that repo and re-run this installer.
  * The 'forgewire-fabric' package is not installed in this venv at all:
    run 'pip install -e <path-to-forgewire-fabric>/python' (editable) or
    'pip install forgewire-fabric'.

Installing anyway would produce a crash-looping service that NSSM reports as
SERVICE_PAUSED, which is harder to diagnose after the fact.
"@
}

$LogDir       = Join-Path $DataDir "logs"
$TokenFile    = Join-Path $DataDir "agent_runner_hub.token"
New-Item -ItemType Directory -Force -Path $DataDir, $LogDir, $WorkspaceRoot | Out-Null

# Stage hub token at a SYSTEM-only path (the runner service runs as
# LocalSystem; never leave the token where lesser principals can read it).
[System.IO.File]::WriteAllText($TokenFile, $Token.Trim())
$acl = Get-Acl $TokenFile
$acl.SetAccessRuleProtection($true, $false)
$acl.Access | ForEach-Object { $acl.RemoveAccessRule($_) | Out-Null }
foreach ($p in @("NT AUTHORITY\SYSTEM", "BUILTIN\Administrators")) {
    $rule = New-Object System.Security.AccessControl.FileSystemAccessRule(
        $p, "FullControl", "Allow")
    $acl.AddAccessRule($rule)
}
Set-Acl -Path $TokenFile -AclObject $acl

# ---- Install / update service --------------------------------------------
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

$cliArgs = @("-m", "forgewire_fabric.runner.agent_kind") -join " "

& nssm.exe set $ServiceName Application $PythonExe       | Out-Null
& nssm.exe set $ServiceName AppParameters $cliArgs       | Out-Null
& nssm.exe set $ServiceName AppDirectory $WorkspaceRoot  | Out-Null
& nssm.exe set $ServiceName DisplayName $DisplayName     | Out-Null
& nssm.exe set $ServiceName Description $Description     | Out-Null
& nssm.exe set $ServiceName Start SERVICE_AUTO_START     | Out-Null
& nssm.exe set $ServiceName AppExit Default Restart      | Out-Null
& nssm.exe set $ServiceName AppRestartDelay 10000        | Out-Null
& nssm.exe set $ServiceName AppStdout (Join-Path $LogDir "agent_runner.out.log") | Out-Null
& nssm.exe set $ServiceName AppStderr (Join-Path $LogDir "agent_runner.err.log") | Out-Null
& nssm.exe set $ServiceName AppRotateFiles 1             | Out-Null
& nssm.exe set $ServiceName AppRotateOnline 1            | Out-Null
& nssm.exe set $ServiceName AppRotateBytes 10485760      | Out-Null

$envVars = @(
    "FORGEWIRE_HUB_URL=$HubUrl",
    "FORGEWIRE_HUB_TOKEN_FILE=$TokenFile",
    "FORGEWIRE_AGENT_RUNNER_WORKSPACE_ROOT=$WorkspaceRoot",
    "FORGEWIRE_AGENT_RUNNER_IDENTITY_PATH=$IdentityFile",
    "FORGEWIRE_AGENT_RUNNER_MAX_CONCURRENT=$MaxConcurrent",
    "PYTHONUNBUFFERED=1"
)
if ($Tags) { $envVars += "FORGEWIRE_AGENT_RUNNER_TAGS=$Tags" }

& nssm.exe set $ServiceName AppEnvironmentExtra @envVars | Out-Null

# ---- Start + verify (idempotent) -----------------------------------------
# Hard rule: SERVICE_PAUSED is treated as a fatal install failure, never
# "resumed" with `nssm continue`. NSSM only sets PAUSED via its throttle-on-
# rapid-exit heuristic, which means the child process is crashing on launch.
# Issuing `nssm continue` masks that crash loop and ships broken services.
$prevNative = $PSNativeCommandUseErrorActionPreference
$PSNativeCommandUseErrorActionPreference = $false
try {
    function Get-NssmStatus {
        return (& nssm.exe status $ServiceName 2>&1 | Out-String).Trim()
    }
    function Get-ErrLogTail {
        $errLog = Join-Path $LogDir "agent_runner.err.log"
        if (-not (Test-Path $errLog)) { return "(no err log yet)" }
        try {
            $tail = Get-Content -Path $errLog -Tail 40 -ErrorAction Stop
            if (-not $tail) { return "(err log is empty)" }
            return ($tail -join "`n")
        } catch {
            return "(could not read $errLog`: $_)"
        }
    }

    $status = Get-NssmStatus
    if ($status -ne 'SERVICE_RUNNING') {
        & nssm.exe start $ServiceName 2>&1 | Out-Null
    }

    # Poll: require SERVICE_RUNNING and stable for >= StableSeconds. Any of
    # SERVICE_PAUSED or SERVICE_STOPPED inside the start window is a hard
    # failure with the err-log tail surfaced for diagnosis.
    $TimeoutSeconds = 45
    $StableSeconds  = 3
    $deadline   = (Get-Date).AddSeconds($TimeoutSeconds)
    $stableSince = $null
    do {
        Start-Sleep -Milliseconds 500
        $status = Get-NssmStatus
        if ($status -eq 'SERVICE_PAUSED') {
            $tail = Get-ErrLogTail
            throw @"
Service '$ServiceName' entered SERVICE_PAUSED during start.

NSSM marks a service PAUSED only when its child process exits faster than
AppRestartDelay (throttle-on-rapid-exit). The child is crash-looping; this
is NOT a recoverable pause, and `nssm continue` would only retrigger the
same crash. Tail of $LogDir\agent_runner.err.log:
$tail
"@
        }
        if ($status -eq 'SERVICE_STOPPED') {
            $tail = Get-ErrLogTail
            throw @"
Service '$ServiceName' is SERVICE_STOPPED after start. Tail of
$LogDir\agent_runner.err.log:
$tail
"@
        }
        if ($status -eq 'SERVICE_RUNNING') {
            if (-not $stableSince) { $stableSince = Get-Date }
            if (((Get-Date) - $stableSince).TotalSeconds -ge $StableSeconds) { break }
        } else {
            $stableSince = $null
        }
    } while ((Get-Date) -lt $deadline)

    if ($status -ne 'SERVICE_RUNNING') {
        $tail = Get-ErrLogTail
        throw @"
Service '$ServiceName' did not reach SERVICE_RUNNING within ${TimeoutSeconds}s.
Last status: '$status'. Tail of $LogDir\agent_runner.err.log:
$tail
"@
    }
} finally {
    $PSNativeCommandUseErrorActionPreference = $prevNative
}

Write-Host ""
Write-Host "Service status: $status"
Write-Host "Workspace:      $WorkspaceRoot"
Write-Host "Identity:       $IdentityFile"
Write-Host "Token file:     $TokenFile (SYSTEM/Administrators only)"
Write-Host "Logs:           $LogDir"
