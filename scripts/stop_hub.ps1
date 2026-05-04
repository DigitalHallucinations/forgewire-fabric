<#
.SYNOPSIS
    Stops the PhrenForge remote-subagent blackboard ("hub") on this host.
    Run on the always-on hub node (currently the OptiPlex 7050).
#>
[CmdletBinding()]
param()

$ErrorActionPreference = "Continue"
$ConfigDir = Join-Path $env:USERPROFILE ".phrenforge"
$PidDir = Join-Path $ConfigDir "run"
$ServerPidFile = Join-Path $PidDir "blackboard.pid"

function Stop-PidFile {
    param([string]$PidFile, [string]$Label)
    if (-not (Test-Path $PidFile)) { Write-Host "$Label: no pidfile, nothing to stop."; return }
    $procId = (Get-Content $PidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
    if (-not $procId) { Write-Host "$Label: empty pidfile."; return }
    $proc = Get-Process -Id $procId -ErrorAction SilentlyContinue
    if ($proc) {
        Write-Host "$Label: stopping pid $procId ($($proc.ProcessName))"
        Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
    } else {
        Write-Host "$Label: pid $procId not running."
    }
    Remove-Item $PidFile -ErrorAction SilentlyContinue
}

Stop-PidFile -PidFile $ServerPidFile -Label "Hub (blackboard)"
