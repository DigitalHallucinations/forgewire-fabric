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

# ---- Start + resume (idempotent) -----------------------------------------
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
        'SERVICE_RUNNING' { }
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

Write-Host ""
Write-Host "Service status: $status"
Write-Host "Workspace:      $WorkspaceRoot"
Write-Host "Identity:       $IdentityFile"
Write-Host "Token file:     $TokenFile (SYSTEM/Administrators only)"
Write-Host "Logs:           $LogDir"
