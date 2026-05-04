<#
.SYNOPSIS
    Verify a dispatcher's connection to a ForgeWire hub before dispatching.

.DESCRIPTION
    1. Ensures a local hub token exists at ``~/.forgewire/hub.token``. If
       missing, optionally fetches it from the hub via SSH (-RemoteHost).
    2. Calls ``$HubUrl/healthz`` with that bearer token.
    3. Prints the FORGEWIRE_HUB_URL / FORGEWIRE_HUB_TOKEN_FILE values that
       the forgewire-dispatcher MCP server expects.

.PARAMETER HubUrl
    Base URL of the hub. Required.

.PARAMETER RemoteHost
    SSH alias of the hub. Used only if the local token file is missing.
    Optional.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$HubUrl,
    [string]$RemoteHost = ""
)

$ErrorActionPreference = "Stop"

$ConfigDir = Join-Path $env:USERPROFILE ".forgewire"
$TokenPath = Join-Path $ConfigDir "hub.token"
New-Item -ItemType Directory -Force -Path $ConfigDir | Out-Null

function Ensure-LocalToken {
    if (Test-Path $TokenPath) { return }
    if (-not $RemoteHost) {
        throw "Local token missing at $TokenPath and -RemoteHost not provided."
    }
    Write-Host "Local token missing; copying from $RemoteHost via scp..."
    $remoteToken = "$RemoteHost`:.forgewire/hub.token"
    & scp -q $remoteToken $TokenPath
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path $TokenPath)) {
        throw "Could not fetch token from $remoteToken. Run start_hub.ps1 on the hub first."
    }
    Write-Host "Token copied from hub."
}

Ensure-LocalToken
$token = (Get-Content $TokenPath -Raw).Trim()
$headers = @{ Authorization = "Bearer $token" }
try {
    $health = Invoke-RestMethod -Uri "$HubUrl/healthz" -Headers $headers -TimeoutSec 5
} catch {
    throw "Hub unreachable at $HubUrl/healthz: $($_.Exception.Message)"
}

$tokenPreview = $token.Substring(0, 8)
Write-Host ""
Write-Host "Dispatcher session is ready."
Write-Host "  Hub URL:   $HubUrl"
Write-Host "  Health:    $($health | ConvertTo-Json -Compress)"
Write-Host "  Token:     starts with $tokenPreview... (file: $TokenPath)"
Write-Host ""
Write-Host "MCP env:"
Write-Host "  FORGEWIRE_HUB_URL        = $HubUrl"
Write-Host "  FORGEWIRE_HUB_TOKEN_FILE = `${userHome}/.forgewire/hub.token"
