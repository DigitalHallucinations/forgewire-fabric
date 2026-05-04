<#
.SYNOPSIS
    Verifies the PhrenForge remote-subagent control plane from a dispatcher
    (driver) host. v2 topology: the hub lives on the always-on OptiPlex.
    This script does NOT start a local server or open any tunnels.

.DESCRIPTION
    1. Ensures a local blackboard token file exists at
       $env:USERPROFILE\.phrenforge\blackboard.token. If missing, attempts
       to fetch the canonical token from the hub via SSH (-RemoteHost).
    2. Verifies the hub is reachable at $HubUrl with that bearer token by
       calling /healthz.
    3. Prints the BLACKBOARD_URL and BLACKBOARD_TOKEN_FILE values that the
       phrenforge-dispatcher MCP server expects.

    Run after a network change, after the hub has been (re)started, or as a
    sanity check before dispatching tasks.

.PARAMETER HubUrl
    Base URL of the hub. Defaults to http://10.220.190.95:8765 (OptiPlex
    static reservation). Override on guest WiFi or after a hub move.

.PARAMETER RemoteHost
    SSH alias of the hub. Used only if the local token file is missing and
    we need to copy it from the hub. Defaults to "phrenforge".
#>
[CmdletBinding()]
param(
    [string]$HubUrl = "http://10.220.190.95:8765",
    [string]$RemoteHost = "phrenforge"
)

$ErrorActionPreference = "Stop"

$ConfigDir = Join-Path $env:USERPROFILE ".phrenforge"
$TokenPath = Join-Path $ConfigDir "blackboard.token"
New-Item -ItemType Directory -Force -Path $ConfigDir | Out-Null

function Ensure-LocalToken {
    if (Test-Path $TokenPath) { return }
    Write-Host "Local token missing at $TokenPath; copying from hub via scp..."
    $remoteToken = "$RemoteHost`:.phrenforge/blackboard.token"
    & scp -q $remoteToken $TokenPath
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path $TokenPath)) {
        throw "Could not fetch token from $remoteToken. Run start_hub.ps1 on the hub first."
    }
    Write-Host "Token copied from hub."
}

function Test-Hub {
    $token = (Get-Content $TokenPath -Raw).Trim()
    $headers = @{ Authorization = "Bearer $token" }
    try {
        return Invoke-RestMethod -Uri "$HubUrl/healthz" -Headers $headers -TimeoutSec 5
    } catch {
        throw "Hub unreachable at $HubUrl/healthz: $($_.Exception.Message)`nIs start_hub.ps1 running on the hub? Is the firewall open?"
    }
}

Ensure-LocalToken
$health = Test-Hub

$tokenPreview = (Get-Content $TokenPath -Raw).Trim().Substring(0, 8)
Write-Host ""
Write-Host "Dispatcher session is ready."
Write-Host "  Hub URL:   $HubUrl"
Write-Host "  Health:    $($health | ConvertTo-Json -Compress)"
Write-Host "  Token:     starts with $tokenPreview... (file: $TokenPath)"
Write-Host ""
Write-Host "MCP env (already set in .vscode/mcp.json):"
Write-Host "  BLACKBOARD_URL        = $HubUrl"
Write-Host "  BLACKBOARD_TOKEN_FILE = `${userHome}/.phrenforge/blackboard.token"
Write-Host ""
Write-Host "Next step: dispatch via the phrenforge-dispatcher MCP tools, then"
Write-Host "wake the runner with:  pwsh -File scripts/remote/wake_runner.ps1"
