<#
.SYNOPSIS
    Idempotently install BOTH ForgeWire runner flavors on this Windows host.

.DESCRIPTION
    Phase 6 / "both kinds always available" mandate. The fabric's task
    taxonomy splits work into two queues:

      * kind:command -- claimed by the shell-exec runner that ships as a
        background Windows service (NSSM 'ForgeWireRunner').
      * kind:agent   -- claimed by the Copilot-Chat MCP runner that lives
        inside an interactive VS Code window (chat mode 'forgewire-runner').

    Both are *binary* identities, not operator config: every host that
    can run a runner should expose both, and the dispatcher's explicit
    `kind` field is the only routing decision. This script makes that
    OOTB by chaining:

      1. scripts/install/nssm-install-runner.ps1
         -- installs/updates the always-on command runner service.
      2. `forgewire-fabric mcp install --with-runner`
        -- registers dispatcher + agent-runner MCP servers in the
         user-scope VS Code mcp.json.
     3. `forgewire-fabric dispatchers register`
        -- registers this host's signed dispatcher identity so Hosts can
         show Dispatch=registered, not merely installed.

    The agent runner is *not* a daemon by design. Copilot Chat is the
    execution surface, so "always available" for the agent kind means
    "always one click away in VS Code." Wake it with scripts/wake_runner.ps1
    or by opening the forgewire-runner chat mode manually.

.PARAMETER PythonExe
    Absolute path to the Python interpreter the runner service should use.

.PARAMETER HubUrl
    Hub base URL, e.g. http://10.120.81.95:8765.

.PARAMETER Token
    Bearer token. Trimmed and written to $DataDir\hub.token.

.PARAMETER WorkspaceRoot
    Absolute path the runner clones / executes inside.

.PARAMETER Tags
    Optional comma-separated tag list. The `kind:command` tag is appended
    automatically by the runner binary itself (operator-supplied kind:*
    tags are stripped); no need to set it here.

.PARAMETER ScopePrefixes
    Optional comma-separated scope prefix allowlist.

.PARAMETER NoAgentMcp
    Skip the `forgewire-runner` MCP entry. The dispatcher MCP entry still
    gets installed and registered because every headed operator machine may
    drive work even when it does not run an interactive agent runner.

.EXAMPLE
    pwsh -File install-host.ps1 `
        -PythonExe C:\Projects\forgewire-fabric\.venv\Scripts\python.exe `
        -HubUrl http://10.120.81.95:8765 `
        -Token (Get-Content $HOME\.forgewire\hub.token -Raw) `
        -WorkspaceRoot C:\Projects\fw-runner-workspace
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$PythonExe,
    [Parameter(Mandatory)][string]$HubUrl,
    [Parameter(Mandatory)][string]$Token,
    [Parameter(Mandatory)][string]$WorkspaceRoot,
    [string]$Tags = "",
    [string]$ScopePrefixes = "",
    [int]$MaxConcurrent = 1,
    [string]$DataDir = "C:\ProgramData\forgewire",
    [string]$ServiceName = "ForgeWireRunner",
    [switch]$NoWatchdog,
    [switch]$NoAgentMcp
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $PythonExe))     { throw "Python not found: $PythonExe" }
if (-not (Test-Path $WorkspaceRoot)) { throw "Workspace not found: $WorkspaceRoot" }

function Register-HostRole {
    param(
        [Parameter(Mandatory)][string]$Role,
        [Parameter(Mandatory)][bool]$Enabled,
        [string]$Status = "installed",
        [hashtable]$Metadata = @{}
    )

    $headers = @{ Authorization = "Bearer $($Token.Trim())" }
    $body = @{
        hostname = $env:COMPUTERNAME
        role = $Role
        enabled = $Enabled
        status = $Status
        metadata = $Metadata
    } | ConvertTo-Json -Depth 8
    try {
        Invoke-RestMethod -Method Post -Uri ($HubUrl.TrimEnd('/') + "/hosts/roles") -Headers $headers -ContentType "application/json" -Body $body | Out-Null
        Write-Host "  Reported host role: $Role=$Enabled ($Status)"
    } catch {
        Write-Warning "Could not report host role '$Role' to hub: $($_.Exception.Message)"
    }
}

# ---------------------------------------------------------------------------
# 1) Command runner (NSSM background service).
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "==[ Phase 6 / step 1 ]== Installing command runner (NSSM)..." -ForegroundColor Cyan

$nssmInstaller = Join-Path $PSScriptRoot "nssm-install-runner.ps1"
if (-not (Test-Path $nssmInstaller)) {
    throw "nssm-install-runner.ps1 not found alongside this script ($nssmInstaller)."
}

$nssmArgs = @{
    PythonExe       = $PythonExe
    HubUrl          = $HubUrl
    Token           = $Token
    WorkspaceRoot   = $WorkspaceRoot
    MaxConcurrent   = $MaxConcurrent
    DataDir         = $DataDir
    ServiceName     = $ServiceName
}
if ($Tags)          { $nssmArgs["Tags"] = $Tags }
if ($ScopePrefixes) { $nssmArgs["ScopePrefixes"] = $ScopePrefixes }
if ($NoWatchdog)    { $nssmArgs["NoWatchdog"] = $true }

& $nssmInstaller @nssmArgs
if ($LASTEXITCODE -ne 0 -and $LASTEXITCODE) {
    throw "nssm-install-runner.ps1 exited with code $LASTEXITCODE."
}
Write-Host "==[ Phase 6 / step 1 ]== Command runner installed." -ForegroundColor Green
Register-HostRole -Role "command_runner" -Enabled $true -Status "installed" -Metadata @{
    service_name = $ServiceName
    workspace_root = $WorkspaceRoot
    max_concurrent = $MaxConcurrent
}

# ---------------------------------------------------------------------------
# 2) Dispatcher + agent MCP server registration.
#
# The dispatcher is a signed task-creation identity. The agent runner is an
# interactive Copilot Chat MCP session, not a service. What we do here is make
# both discoverable in user-scope mcp.json, then register the dispatcher
# identity with the hub so the Hosts pane can show Dispatch=registered.
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "==[ Phase 6 / step 2 ]== Registering dispatcher MCP server in user-scope mcp.json..." -ForegroundColor Cyan
if (-not $NoAgentMcp) {
    Write-Host "  Also registering agent runner MCP server."
}

# `mcp install` writes to the *invoking user's* VS Code config dir. If this
# script self-elevated, the invoking user is the admin shell, which is probably
# NOT the user who runs Copilot Chat. Detect and warn instead of silently
# writing the wrong user's profile.
$whoami = (whoami).Trim()
Write-Host "  Writing mcp.json under: $whoami"
Write-Host "  (If this is the elevated admin and your normal Copilot user differs,"
Write-Host "   re-run this script unelevated or run 'forgewire-fabric mcp install"
Write-Host "   --with-runner --workspace-root $WorkspaceRoot' as your normal user.)"

$mcpArgs = @("-m", "forgewire_fabric.cli", "mcp", "install", "--hub-url", $HubUrl)
if (-not $NoAgentMcp) {
    $mcpArgs += @("--with-runner", "--workspace-root", $WorkspaceRoot)
}
& $PythonExe @mcpArgs
if ($LASTEXITCODE -ne 0) {
    throw "forgewire-fabric mcp install exited with code $LASTEXITCODE."
}
Write-Host "==[ Phase 6 / step 2 ]== MCP servers registered." -ForegroundColor Green
Register-HostRole -Role "dispatch" -Enabled $true -Status "installed" -Metadata @{
    mcp_server = "forgewire-dispatcher"
}

Write-Host "==[ Phase 6 / step 2 ]== Registering signed dispatcher identity with hub..." -ForegroundColor Cyan
$oldHubUrl = $env:FORGEWIRE_HUB_URL
$oldHubToken = $env:FORGEWIRE_HUB_TOKEN
try {
    $env:FORGEWIRE_HUB_URL = $HubUrl
    $env:FORGEWIRE_HUB_TOKEN = $Token.Trim()
    & $PythonExe -m forgewire_fabric.cli dispatchers register --hostname $env:COMPUTERNAME
    if ($LASTEXITCODE -ne 0) {
        throw "forgewire-fabric dispatchers register exited with code $LASTEXITCODE."
    }
    Write-Host "==[ Phase 6 / step 2 ]== Dispatcher registered." -ForegroundColor Green
} catch {
    Write-Warning "Dispatcher registration failed: $($_.Exception.Message)"
} finally {
    $env:FORGEWIRE_HUB_URL = $oldHubUrl
    $env:FORGEWIRE_HUB_TOKEN = $oldHubToken
}

if ($NoAgentMcp) {
    Write-Host ""
    Write-Host "==[ Phase 6 / step 2 ]== Skipped agent runner MCP entry (-NoAgentMcp)." -ForegroundColor Yellow
    Register-HostRole -Role "agent_runner" -Enabled $false -Status "skipped" -Metadata @{
        reason = "NoAgentMcp"
        workspace_root = $WorkspaceRoot
    }
} else {
    Write-Host "==[ Phase 6 / step 2 ]== Agent runner MCP registered." -ForegroundColor Green
    Register-HostRole -Role "agent_runner" -Enabled $true -Status "registered" -Metadata @{
        mcp_server = "forgewire-runner"
        workspace_root = $WorkspaceRoot
    }
}

Write-Host ""
Write-Host "Both runner kinds are now available on this host:" -ForegroundColor Green
Write-Host "  * dispatch     -- VS Code MCP server 'forgewire-dispatcher' (signed)."
Write-Host "  * kind:command -- Windows service '$ServiceName' (auto-start)."
if (-not $NoAgentMcp) {
    Write-Host "  * kind:agent   -- VS Code MCP server 'forgewire-runner'."
    Write-Host "                   Wake with scripts\wake_runner.ps1 or open the"
    Write-Host "                   'forgewire-runner' chat mode in VS Code."
}
