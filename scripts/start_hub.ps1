<#
.SYNOPSIS
    Brings up the PhrenForge remote-subagent blackboard ("hub") on this host.
    Run on the always-on hub node (currently the OptiPlex 7050).

.DESCRIPTION
    1. Ensures a blackboard token exists at $env:USERPROFILE\.phrenforge\blackboard.token
       (generates a 256-bit hex token if missing).
    2. Starts the FastAPI/SSE blackboard service in the repo venv on $BindHost:$Port.
       Default bind is 0.0.0.0 so dispatchers on the LAN can reach it. Bearer auth is
       required on every endpoint except /healthz.
    3. Writes a pidfile and rotates basic logs under ~/.phrenforge/.

    Idempotent. Safe to re-run.

    The replaced workflow (v1) used a reverse SSH tunnel from the OptiPlex back to a
    blackboard running on the Dell laptop. v2 flips the topology: the hub lives on
    the always-on box and dispatchers connect outward over the LAN. See
    docs/operations/remote-subagent.md.

.PARAMETER BindHost
    Interface to bind. Defaults to 0.0.0.0 (all interfaces). Use 127.0.0.1 to scope
    the hub to localhost-only (useful when colocated with the runner only).

.PARAMETER Port
    TCP port. Defaults to 8765.

.PARAMETER Python
    Python interpreter. Defaults to the repo venv if present, else "python".
#>
[CmdletBinding()]
param(
    [string]$BindHost = "0.0.0.0",
    [int]$Port = 8765,
    [string]$Python = ""
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$ConfigDir = Join-Path $env:USERPROFILE ".phrenforge"
$TokenPath = Join-Path $ConfigDir "blackboard.token"
$LogDir = Join-Path $ConfigDir "logs"
$ServerLog = Join-Path $LogDir "blackboard.log"
$PidDir = Join-Path $ConfigDir "run"
$ServerPidFile = Join-Path $PidDir "blackboard.pid"

New-Item -ItemType Directory -Force -Path $ConfigDir, $LogDir, $PidDir | Out-Null

function Resolve-Python {
    if ($Python) { return $Python }
    $venvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
    if (Test-Path $venvPython) { return $venvPython }
    return "python"
}

function Ensure-Token {
    if (-not (Test-Path $TokenPath)) {
        $bytes = New-Object byte[] 32
        [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
        $hex = ($bytes | ForEach-Object { $_.ToString("x2") }) -join ""
        Set-Content -Path $TokenPath -Value $hex -Encoding ASCII -NoNewline
        Write-Host "Generated new blackboard token at $TokenPath"
    }
}

function Test-PidAlive {
    param([string]$PidFile)
    if (-not (Test-Path $PidFile)) { return $false }
    $procId = (Get-Content $PidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
    if (-not $procId) { return $false }
    return $null -ne (Get-Process -Id $procId -ErrorAction SilentlyContinue)
}

function Start-Blackboard {
    if (Test-PidAlive -PidFile $ServerPidFile) {
        Write-Host "Hub already running (pid $(Get-Content $ServerPidFile))."
        return
    }
    $py = Resolve-Python
    $env:BLACKBOARD_TOKEN = (Get-Content $TokenPath -Raw).Trim()
    $args = @(
        "-m", "scripts.remote.hub.server",
        "--host", $BindHost,
        "--port", "$Port"
    )
    Write-Host "Starting hub: $py $($args -join ' ')"
    $proc = Start-Process -FilePath $py -ArgumentList $args `
        -WorkingDirectory $RepoRoot `
        -RedirectStandardOutput $ServerLog `
        -RedirectStandardError "$ServerLog.err" `
        -WindowStyle Hidden -PassThru
    $proc.Id | Set-Content -Path $ServerPidFile
    Start-Sleep -Seconds 2
    if (-not (Test-PidAlive -PidFile $ServerPidFile)) {
        throw "Hub failed to start. See $ServerLog and $ServerLog.err."
    }
    Write-Host "Hub pid $($proc.Id), log: $ServerLog"
}

Ensure-Token
Start-Blackboard

$lanAddrs = @()
try {
    $lanAddrs = (Get-NetIPAddress -AddressFamily IPv4 -ErrorAction SilentlyContinue |
        Where-Object { $_.InterfaceAlias -notmatch 'Loopback' -and $_.IPAddress -notmatch '^169\.254\.' } |
        Select-Object -ExpandProperty IPAddress)
} catch {}

$tokenPreview = (Get-Content $TokenPath -Raw).Trim().Substring(0, 8)
Write-Host ""
Write-Host "PhrenForge hub is up."
Write-Host "  Bind:   $BindHost`:$Port"
if ($lanAddrs) {
    Write-Host "  LAN URLs the dispatcher can use:"
    foreach ($a in $lanAddrs) { Write-Host "    http://${a}:$Port" }
}
Write-Host "  Token:  starts with $tokenPreview..."
Write-Host ""
Write-Host "Dispatcher side: ensure ~/.phrenforge/blackboard.token matches and that"
Write-Host "BLACKBOARD_URL points at one of the LAN URLs above."
