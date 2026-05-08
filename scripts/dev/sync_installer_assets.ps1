<#
.SYNOPSIS
    Sync installer scripts from scripts/install/ -> python/forgewire_fabric/_installer_assets/.

.DESCRIPTION
    The wheel bundles installer PowerShell scripts under
    python/forgewire_fabric/_installer_assets/ so that
    `forgewire-fabric hub install` and `forgewire-fabric runner install`
    work out of the box on Windows hosts that only have the published
    package. The same scripts are also kept under scripts/install/ for
    direct invocation in source checkouts and for editing in PRs.

    Single source of truth is scripts/install/. This script copies them
    into the bundled location so the two stay byte-identical. The
    drift-guard pytest at tests/test_installer_assets_in_sync.py will
    fail CI if you forget to run this.

.EXAMPLE
    pwsh -File scripts/dev/sync_installer_assets.ps1
#>
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$repo = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$src  = Join-Path $repo "scripts\install"
$dst  = Join-Path $repo "python\forgewire_fabric\_installer_assets"

$names = @(
    "nssm-install-hub.ps1",
    "nssm-install-runner.ps1",
    "install-hub-watchdog.ps1",
    "install-runner-watchdog.ps1"
)

if (-not (Test-Path $dst)) {
    New-Item -ItemType Directory -Force -Path $dst | Out-Null
}

$changed = 0
foreach ($n in $names) {
    $a = Join-Path $src $n
    $b = Join-Path $dst $n
    if (-not (Test-Path $a)) { Write-Warning "Source missing: $a"; continue }

    $hashA = (Get-FileHash $a).Hash
    $hashB = if (Test-Path $b) { (Get-FileHash $b).Hash } else { "<absent>" }
    if ($hashA -eq $hashB) {
        Write-Host ("{0,-32} unchanged" -f $n)
    } else {
        Copy-Item $a $b -Force
        Write-Host ("{0,-32} synced" -f $n)
        $changed++
    }
}

Write-Host ""
Write-Host "Updated $changed file(s) under $dst"
