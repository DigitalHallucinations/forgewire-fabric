[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$VsixPath,
    [string]$RemoteHost = 'forgewire',
    [string]$RemoteUserHome = 'C:\Users\jerem',
    [string]$RemoteCodeCli = 'C:\Users\jerem\AppData\Local\Programs\Microsoft VS Code\bin\code.cmd'
)

$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $VsixPath)) {
    throw "VSIX not found: $VsixPath"
}

$leaf = Split-Path -Leaf $VsixPath
$remotePath = (Join-Path $RemoteUserHome $leaf) -replace '/', '\\'
$scpTarget = "${RemoteHost}:/" + ($remotePath -replace '\\', '/')

Write-Host "Copying $leaf -> $RemoteHost:$remotePath"
& scp $VsixPath $scpTarget
if ($LASTEXITCODE -ne 0) { throw "scp failed with exit $LASTEXITCODE" }

$cmd = "`"$RemoteCodeCli`" --install-extension `"$remotePath`" --force"
Write-Host "Installing on $RemoteHost: $cmd"
& ssh $RemoteHost $cmd
if ($LASTEXITCODE -ne 0) { throw "remote install failed with exit $LASTEXITCODE" }

& ssh $RemoteHost "`"$RemoteCodeCli`" --list-extensions --show-versions" | Select-String forgewire
