<#
.SYNOPSIS
    Dispatcher-side teardown. With v2 topology there is nothing local to stop;
    the hub runs on the always-on OptiPlex. This script is kept as a
    convenience that informs the operator and (optionally) stops the hub
    remotely via SSH.

.PARAMETER StopRemoteHub
    If set, also runs scripts/remote/stop_hub.ps1 on the hub via SSH.

.PARAMETER RemoteHost
    SSH alias of the hub. Defaults to "phrenforge".

.PARAMETER RemoteRepo
    Repo path on the hub. Defaults to "C:\Users\jerem\Projects\PhrenForge".
#>
[CmdletBinding()]
param(
    [switch]$StopRemoteHub,
    [string]$RemoteHost = "phrenforge",
    [string]$RemoteRepo = "C:\Users\jerem\Projects\PhrenForge"
)

$ErrorActionPreference = "Continue"

Write-Host "Dispatcher v2 has no local processes to stop (no local server, no tunnel)."

if ($StopRemoteHub) {
    Write-Host "Stopping hub on $RemoteHost via SSH..."
    $cmd = "powershell -NoProfile -ExecutionPolicy Bypass -File `"$RemoteRepo\scripts\remote\stop_hub.ps1`""
    ssh $RemoteHost $cmd
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Remote stop_hub.ps1 exited $LASTEXITCODE"
    }
} else {
    Write-Host "Pass -StopRemoteHub to also stop the hub on $RemoteHost."
}
