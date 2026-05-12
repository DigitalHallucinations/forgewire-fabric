<#
.SYNOPSIS
    Idempotently install BOTH ForgeWire runner flavors on this Windows host.

.DESCRIPTION
    Phase 6 / "both kinds always available" mandate. The fabric's task
    taxonomy splits work into two queues:

      * kind:command — claimed by the shell-exec runner that ships as a
        background Windows service (NSSM 'ForgeWireRunner').
      * kind:agent   — claimed by the Copilot-Chat MCP runner that lives
        inside an interactive VS Code window (chat mode 'forgewire-runner').

    Both are *binary* identities, not operator config: every host that
    can run a runner should expose both, and the dispatcher's explicit
    `kind` field is the only routing decision. This script makes that
    OOTB by chaining:

      1. scripts/install/nssm-install-runner.ps1
         — installs/updates the always-on command runner service.
      2. `forgewire-fabric mcp install --with-runner`
         — registers the agent runner MCP server in the user-scope
           VS Code mcp.json (forgewire-dispatcher + forgewire-runner).

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
    Skip the `mcp install --with-runner` step. Use this on hosts that
    have no human VS Code user (e.g. headless OptiPlex). The command
    runner service still gets installed.

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

# ---------------------------------------------------------------------------
# 2) Agent runner (VS Code MCP server registration).
#
# The agent runner is an interactive Copilot Chat MCP session, not a
# service. What we do here is make it *discoverable*: write the
# user-scope mcp.json entries so the chat mode + tools are wired the
# next time the user opens VS Code. The actual claim loop is driven by
# the operator opening the `forgewire-runner` chat mode (or by
# scripts/wake_runner.ps1 over SSH from another host).
# ---------------------------------------------------------------------------
if ($NoAgentMcp) {
    Write-Host ""
    Write-Host "==[ Phase 6 / step 2 ]== Skipping agent runner MCP install (-NoAgentMcp)." -ForegroundColor Yellow
} else {
    Write-Host ""
    Write-Host "==[ Phase 6 / step 2 ]== Registering agent runner MCP server in user-scope mcp.json..." -ForegroundColor Cyan

    # `mcp install --with-runner` writes to the *invoking user's* VS Code
    # config dir. If this script self-elevated, the invoking user is the
    # admin shell, which is probably NOT the user who runs Copilot Chat.
    # Detect and warn instead of silently writing the wrong user's profile.
    $whoami = (whoami).Trim()
    Write-Host "  Writing mcp.json under: $whoami"
    Write-Host "  (If this is the elevated admin and your normal Copilot user differs,"
    Write-Host "   re-run this script unelevated or run 'forgewire-fabric mcp install"
    Write-Host "   --with-runner --workspace-root $WorkspaceRoot' as your normal user.)"

    & $PythonExe -m forgewire_fabric.cli mcp install --with-runner --hub-url $HubUrl --workspace-root $WorkspaceRoot
    if ($LASTEXITCODE -ne 0) {
        throw "forgewire-fabric mcp install exited with code $LASTEXITCODE."
    }
    Write-Host "==[ Phase 6 / step 2 ]== Agent runner MCP registered." -ForegroundColor Green
}

Write-Host ""
Write-Host "Both runner kinds are now available on this host:" -ForegroundColor Green
Write-Host "  * kind:command — Windows service '$ServiceName' (auto-start)."
if (-not $NoAgentMcp) {
    Write-Host "  * kind:agent   — VS Code MCP server 'forgewire-runner'."
    Write-Host "                   Wake with scripts\wake_runner.ps1 or open the"
    Write-Host "                   'forgewire-runner' chat mode in VS Code."
}
