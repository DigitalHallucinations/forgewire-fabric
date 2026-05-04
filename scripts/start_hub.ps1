<#
.SYNOPSIS
    Start the ForgeWire hub on this host (foreground or background).

.DESCRIPTION
    Thin PowerShell wrapper around ``forgewire hub start``. Ensures a token
    exists at ``~/.forgewire/hub.token`` (creating a fresh 256-bit hex token
    if missing) and either runs the hub in the foreground (default) or
    detached with logs and a pidfile under ``~/.forgewire/{logs,run}/``.

    Idempotent: re-running while a detached hub is up does nothing.

    For production use install as a service instead, via
    ``scripts/install/nssm-install-hub.ps1`` (Windows), the
    ``com.forgewire.hub.plist`` launchd job (macOS), or the
    ``forgewire-hub.service`` systemd unit (Linux).

.PARAMETER BindHost
    Interface to bind. Default 0.0.0.0 (all interfaces).

.PARAMETER Port
    TCP port. Default 8765.

.PARAMETER Detach
    If set, run in the background, redirect stdout/stderr into
    ``~/.forgewire/logs/hub.log``, and write a pidfile.

.PARAMETER MdnsAdvertise
    If set, advertise the hub via mDNS on the LAN.
#>
[CmdletBinding()]
param(
    [string]$BindHost = "0.0.0.0",
    [int]$Port = 8765,
    [switch]$Detach,
    [switch]$MdnsAdvertise
)

$ErrorActionPreference = "Stop"

$ConfigDir = Join-Path $env:USERPROFILE ".forgewire"
$TokenPath = Join-Path $ConfigDir "hub.token"
$LogDir    = Join-Path $ConfigDir "logs"
$PidDir    = Join-Path $ConfigDir "run"
$LogPath   = Join-Path $LogDir "hub.log"
$PidPath   = Join-Path $PidDir "hub.pid"

New-Item -ItemType Directory -Force -Path $ConfigDir, $LogDir, $PidDir | Out-Null

function Ensure-Token {
    if (Test-Path $TokenPath) { return }
    $bytes = New-Object byte[] 32
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    $hex = ($bytes | ForEach-Object { $_.ToString("x2") }) -join ""
    Set-Content -Path $TokenPath -Value $hex -Encoding ASCII -NoNewline
    Write-Host "Generated new hub token at $TokenPath"
}

function Resolve-ForgeWire {
    $cmd = Get-Command forgewire -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
    $venv = Join-Path $repoRoot ".venv\Scripts\forgewire.exe"
    if (Test-Path $venv) { return $venv }
    throw "forgewire CLI not found on PATH and no repo .venv\Scripts\forgewire.exe present. Install with: pip install forgewire"
}

function Test-PidAlive {
    if (-not (Test-Path $PidPath)) { return $false }
    $procId = (Get-Content $PidPath -ErrorAction SilentlyContinue | Select-Object -First 1)
    if (-not $procId) { return $false }
    return $null -ne (Get-Process -Id $procId -ErrorAction SilentlyContinue)
}

Ensure-Token
$forgewire = Resolve-ForgeWire

$argList = @(
    "hub", "start",
    "--host", $BindHost,
    "--port", "$Port",
    "--token-file", $TokenPath
)
if ($MdnsAdvertise) { $argList += "--mdns" }

if ($Detach) {
    if (Test-PidAlive) {
        Write-Host "Hub already running (pid $(Get-Content $PidPath))."
        exit 0
    }
    $proc = Start-Process -FilePath $forgewire -ArgumentList $argList `
        -RedirectStandardOutput $LogPath `
        -RedirectStandardError "$LogPath.err" `
        -WindowStyle Hidden -PassThru
    $proc.Id | Set-Content -Path $PidPath
    Start-Sleep -Seconds 2
    if (-not (Test-PidAlive)) {
        throw "Hub failed to start. See $LogPath and $LogPath.err."
    }
    $tokenPreview = (Get-Content $TokenPath -Raw).Trim().Substring(0, 8)
    Write-Host "Hub pid $($proc.Id), log: $LogPath"
    Write-Host "Token preview: $tokenPreview..."
} else {
    & $forgewire @argList
}
