#Requires -Version 5.1
<#
Thin wrapper executed on a remote host. Reads the hub token from the local
filesystem (never sent across the wire) and invokes the agent-runner installer.
#>
param(
    [string]$Installer = "$env:USERPROFILE\nssm-install-agent-runner.ps1",
    [string]$PythonExe = "C:\Projects\forgewire-fabric\.venv\Scripts\python.exe",
    [string]$HubUrl    = "http://10.120.81.95:8765",
    [string]$TokenFile = "$env:USERPROFILE\.forgewire\hub.token"
)
$ErrorActionPreference = 'Stop'
if (-not (Test-Path $Installer))  { throw "Installer missing: $Installer" }
if (-not (Test-Path $PythonExe))  { throw "Python missing: $PythonExe" }
if (-not (Test-Path $TokenFile))  { throw "Token missing: $TokenFile" }
$token = (Get-Content -Raw -Path $TokenFile).Trim()
& powershell -NoProfile -ExecutionPolicy Bypass -File $Installer `
    -PythonExe $PythonExe -HubUrl $HubUrl -Token $token
exit $LASTEXITCODE
