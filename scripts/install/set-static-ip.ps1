<#
.SYNOPSIS
    Pin this Windows host to a static IPv4 address on its primary interface.

.DESCRIPTION
    Idempotent. Designed to run on a ForgeWire Fabric coordinator/runner box
    (Windows) so its address never drifts under DHCP renewal. Detects the
    interface that currently holds the default IPv4 route, removes any
    existing IPv4 address bindings + DHCP lease on that interface, and then
    binds the requested static address, gateway, and DNS servers.

    Run as Administrator. Requires the host to be on the target subnet
    already (this script does not change SSID or media). On Wi-Fi the new
    address survives DHCP releases but the AP must be reachable from the
    static address (most home APs are fine; many hotspots are not).

.PARAMETER IPv4
    The static IPv4 address to assign, e.g. 10.120.81.50.

.PARAMETER PrefixLength
    Subnet prefix length, e.g. 24 for /24. Defaults to 24.

.PARAMETER Gateway
    The default gateway to set, e.g. 10.120.81.86.

.PARAMETER Dns
    One or more DNS server IPs. Defaults to (Gateway, 1.1.1.1).

.PARAMETER InterfaceAlias
    Optional. Name of the NIC to configure. Defaults to the interface that
    currently owns the default IPv4 route.

.PARAMETER WhatIf
    Standard. Shows the actions without applying.

.EXAMPLE
    # Pin the OptiPlex to 10.120.81.50/24 via the hotspot gateway.
    pwsh -File set-static-ip.ps1 -IPv4 10.120.81.50 -Gateway 10.120.81.86

.NOTES
    Re-run safely. To revert to DHCP:
        Set-NetIPInterface -InterfaceAlias <NIC> -Dhcp Enabled
        Set-DnsClientServerAddress -InterfaceAlias <NIC> -ResetServerAddresses
        Restart-NetAdapter -Name <NIC>
#>

[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory)][string]$IPv4,
    [int]$PrefixLength = 24,
    [Parameter(Mandatory)][string]$Gateway,
    [string[]]$Dns,
    [string]$InterfaceAlias
)

$ErrorActionPreference = 'Stop'

# Require admin
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    throw "Run this script in an elevated PowerShell (Run as Administrator)."
}

if (-not $Dns) { $Dns = @($Gateway, '1.1.1.1') }

if (-not $InterfaceAlias) {
    $defaultRoute = Get-NetRoute -DestinationPrefix '0.0.0.0/0' -ErrorAction Stop |
        Sort-Object RouteMetric | Select-Object -First 1
    $InterfaceAlias = (Get-NetAdapter -InterfaceIndex $defaultRoute.InterfaceIndex).Name
    Write-Host "Auto-detected interface: $InterfaceAlias"
}

$nic = Get-NetAdapter -Name $InterfaceAlias
if ($nic.Status -ne 'Up') {
    throw "Interface '$InterfaceAlias' is not Up (current: $($nic.Status))."
}

Write-Host ""
Write-Host "Current configuration on '$InterfaceAlias':"
Get-NetIPAddress -InterfaceAlias $InterfaceAlias -AddressFamily IPv4 -ErrorAction SilentlyContinue |
    Select-Object IPAddress, PrefixLength, PrefixOrigin | Format-Table -AutoSize

if ($PSCmdlet.ShouldProcess($InterfaceAlias, "Set static IPv4 $IPv4/$PrefixLength gw $Gateway")) {

    # 1. Stop DHCP on this interface
    Set-NetIPInterface -InterfaceAlias $InterfaceAlias -Dhcp Disabled -ErrorAction Stop

    # 2. Drop any existing IPv4 addresses on the interface
    Get-NetIPAddress -InterfaceAlias $InterfaceAlias -AddressFamily IPv4 -ErrorAction SilentlyContinue |
        Remove-NetIPAddress -Confirm:$false -ErrorAction SilentlyContinue

    # 3. Drop existing default routes on this interface (will be replaced)
    Get-NetRoute -InterfaceAlias $InterfaceAlias -DestinationPrefix '0.0.0.0/0' -ErrorAction SilentlyContinue |
        Remove-NetRoute -Confirm:$false -ErrorAction SilentlyContinue

    # 4. Bind the static address (this also installs the gateway route)
    New-NetIPAddress -InterfaceAlias $InterfaceAlias `
        -IPAddress $IPv4 -PrefixLength $PrefixLength -DefaultGateway $Gateway `
        -AddressFamily IPv4 -ErrorAction Stop | Out-Null

    # 5. DNS
    Set-DnsClientServerAddress -InterfaceAlias $InterfaceAlias -ServerAddresses $Dns -ErrorAction Stop

    Write-Host ""
    Write-Host "New configuration on '$InterfaceAlias':"
    Get-NetIPAddress -InterfaceAlias $InterfaceAlias -AddressFamily IPv4 |
        Select-Object IPAddress, PrefixLength, PrefixOrigin | Format-Table -AutoSize
    Get-DnsClientServerAddress -InterfaceAlias $InterfaceAlias -AddressFamily IPv4 |
        Select-Object InterfaceAlias, ServerAddresses | Format-Table -AutoSize

    Write-Host "Static IP applied. Verify with: Test-Connection $Gateway -Count 2" -ForegroundColor Green
}
