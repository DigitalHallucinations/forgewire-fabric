<#
.SYNOPSIS
    Install a ForgeWire Runner as a Windows service via NSSM.

.DESCRIPTION
    Idempotent. Creates/updates the "ForgeWireRunner" service to run:
        <PythonExe> -m forgewire.cli runner start
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
    [string]$ServiceName = "ForgeWireRunner"
)

$ErrorActionPreference = "Stop"

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

$existing = & nssm.exe status $ServiceName 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "Service '$ServiceName' exists; updating in place."
    & nssm.exe stop $ServiceName confirm | Out-Null
} else {
    Write-Host "Installing service '$ServiceName'."
    & nssm.exe install $ServiceName $PythonExe | Out-Null
}

$cliArgs = @("-m", "forgewire.cli", "runner", "start") -join " "

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

& nssm.exe start $ServiceName | Out-Null
Start-Sleep -Seconds 2
$status = & nssm.exe status $ServiceName
Write-Host "Service status: $status"
Write-Host "Logs: $LogDir"
