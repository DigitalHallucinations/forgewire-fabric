<#
.SYNOPSIS
    Install the ForgeWire Hub as a Windows service via NSSM.

.DESCRIPTION
    Idempotent. If the service already exists it is updated in place.
    NSSM must be on PATH (https://nssm.cc/). The script:
      1. Writes the bearer token to a file (default: C:\ProgramData\forgewire\hub.token).
      2. Creates/updates a service named "ForgeWireHub" that runs:
           <PythonExe> -m forgewire.cli hub start --host 0.0.0.0 --port <Port> --db-path <DbPath>
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
    [string]$ServiceName = "ForgeWireHub"
)

$ErrorActionPreference = "Stop"

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

$cliArgs = @(
    "-m", "forgewire.cli", "hub", "start",
    "--host", $BindHost,
    "--port", $Port,
    "--db-path", "`"$DbPath`""
) -join " "

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

& nssm.exe start $ServiceName | Out-Null
Start-Sleep -Seconds 2
$status = & nssm.exe status $ServiceName
Write-Host "Service status: $status"
Write-Host ""
Write-Host "Hub URL:    http://${env:COMPUTERNAME}:${Port}"
Write-Host "Token file: $TokenFile (RW only by SYSTEM + Administrators)"
Write-Host "Logs:       $LogDir"
