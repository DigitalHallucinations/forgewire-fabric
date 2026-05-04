<#
.SYNOPSIS
    Stop a detached ForgeWire hub started by ``start_hub.ps1 -Detach``.

.DESCRIPTION
    Reads the pidfile at ``~/.forgewire/run/hub.pid`` and stops the matching
    process if it is still alive. Foreground hubs (no -Detach) are not
    managed by this script -- stop them with Ctrl-C.

    For service installs, use the platform service manager instead
    (``nssm stop ForgeWireHub``, ``systemctl stop forgewire-hub``,
    ``launchctl unload``).
#>
[CmdletBinding()]
param()

$ErrorActionPreference = "Continue"
$ConfigDir = Join-Path $env:USERPROFILE ".forgewire"
$PidPath   = Join-Path $ConfigDir "run\hub.pid"

if (-not (Test-Path $PidPath)) {
    Write-Host "No pidfile at $PidPath; nothing to stop."
    exit 0
}
$procId = (Get-Content $PidPath -ErrorAction SilentlyContinue | Select-Object -First 1)
if (-not $procId) {
    Write-Host "Empty pidfile."
    Remove-Item $PidPath -ErrorAction SilentlyContinue
    exit 0
}
$proc = Get-Process -Id $procId -ErrorAction SilentlyContinue
if ($proc) {
    Write-Host "Stopping hub pid $procId ($($proc.ProcessName))"
    Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
} else {
    Write-Host "Pid $procId not running."
}
Remove-Item $PidPath -ErrorAction SilentlyContinue
