<#
.SYNOPSIS
    Dispatcher-side teardown helper. Optionally also stops the remote hub
    via SSH.

.PARAMETER StopRemoteHub
    If set, runs ``stop_hub.ps1`` on the hub via SSH.

.PARAMETER RemoteHost
    SSH alias of the hub. Required with -StopRemoteHub.

.PARAMETER RemoteRepo
    Path of the forgewire checkout on the hub. Required with -StopRemoteHub.
#>
[CmdletBinding()]
param(
    [switch]$StopRemoteHub,
    [string]$RemoteHost = "",
    [string]$RemoteRepo = ""
)

$ErrorActionPreference = "Continue"

Write-Host "Dispatcher has no local processes to stop."

if ($StopRemoteHub) {
    if (-not $RemoteHost -or -not $RemoteRepo) {
        throw "-StopRemoteHub requires both -RemoteHost and -RemoteRepo."
    }
    Write-Host "Stopping hub on $RemoteHost via SSH..."
    $cmd = "powershell -NoProfile -ExecutionPolicy Bypass -File `"$RemoteRepo\scripts\stop_hub.ps1`""
    ssh $RemoteHost $cmd
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Remote stop_hub.ps1 exited $LASTEXITCODE"
    }
}
