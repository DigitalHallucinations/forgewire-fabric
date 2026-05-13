# Bootstrap script for the kind:agent runner (development / smoke harness).
# Sources hub URL + token from %USERPROFILE%\.forgewire\hub.token.
$ErrorActionPreference = "Stop"
$tokenPath = Join-Path $env:USERPROFILE ".forgewire\hub.token"
if (-not (Test-Path $tokenPath)) {
    throw "hub token not found at $tokenPath"
}
$token = (Get-Content $tokenPath -Raw).Trim()
$env:FORGEWIRE_HUB_URL = "http://10.120.81.95:8765"
$env:FORGEWIRE_HUB_TOKEN = $token
$env:BLACKBOARD_URL = $env:FORGEWIRE_HUB_URL
$env:BLACKBOARD_TOKEN = $token
Set-Location C:\Projects\forgewire-fabric
& .\.venv\Scripts\python.exe -m forgewire_fabric.runner.agent_kind
