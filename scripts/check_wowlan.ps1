<#
.SYNOPSIS
    Reports Wake-on-WLAN / Wake-on-LAN feasibility for the local machine.

.DESCRIPTION
    Wave E todo 20. This is an *operational* check, not code: PhrenForge does
    not initiate WoL/WoWLAN itself. Use the output to decide whether a runner
    machine can be woken from S3/S4 by sending a magic packet from the hub
    (or another always-on box).

    What it inspects:

      * NICs that report ``WakeOnMagicPacket`` enabled (Get-NetAdapterAdvancedProperty)
      * NICs Windows currently has armed for wake (powercfg /devicequery wake_armed)
      * Devices with wake programmable firmware (powercfg /devicequery wake_programmable)
      * Current sleep / hibernate / hybrid sleep policy
      * Whether 'Allow this device to wake the computer' is enabled per-NIC

    BIOS/UEFI: still verify ``Wake on LAN`` / ``Wake on Wireless`` is enabled
    in firmware (this script cannot read that). On laptops, AC-only wake is
    common; the BIOS often hides the WoWLAN toggle when on battery.

.NOTES
    Run as a non-elevated user; ``Get-NetAdapter*`` and ``powercfg /devicequery``
    do not need admin rights.
#>

[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

function Write-Section($title) {
    Write-Host ""
    Write-Host ("=" * 78) -ForegroundColor Cyan
    Write-Host $title -ForegroundColor Cyan
    Write-Host ("=" * 78) -ForegroundColor Cyan
}

Write-Section "Host"
Write-Host ("Hostname: {0}" -f [System.Net.Dns]::GetHostName())
Write-Host ("OS:       {0}" -f (Get-CimInstance Win32_OperatingSystem).Caption)

Write-Section "NetAdapter wake capabilities"
try {
    $adapters = Get-NetAdapter -Physical | Where-Object { $_.Status -ne "Disabled" }
    foreach ($a in $adapters) {
        Write-Host ("- {0}  ({1}, {2}, MAC {3})" -f $a.Name, $a.InterfaceDescription, $a.MediaType, $a.MacAddress)
        try {
            $power = Get-NetAdapterPowerManagement -Name $a.Name -ErrorAction Stop
            Write-Host ("    WakeOnMagicPacket : {0}" -f $power.WakeOnMagicPacket)
            Write-Host ("    WakeOnPattern     : {0}" -f $power.WakeOnPattern)
            Write-Host ("    AllowComputerToTurnOff : {0}" -f $power.AllowComputerToTurnOffDevice)
        } catch {
            Write-Host "    (Get-NetAdapterPowerManagement unsupported on this NIC)"
        }
    }
} catch {
    Write-Warning "Get-NetAdapter failed: $_"
}

Write-Section "Devices Windows has armed for wake"
& powercfg /devicequery wake_armed

Write-Section "Devices Windows considers wake-programmable"
& powercfg /devicequery wake_programmable

Write-Section "Sleep / hibernate state"
$power = & powercfg /a
Write-Host $power

Write-Section "Reminders / next steps"
Write-Host "1. Verify in BIOS/UEFI: 'Wake on LAN', 'Wake on Wireless LAN', 'Deep Sleep = disabled'."
Write-Host "2. On a laptop: confirm WoL works on AC and (separately) on battery."
Write-Host "3. From the hub, test with: wol <MAC>  or  Send-MagicPacket -MacAddress <MAC>."
Write-Host "4. Some access points strip multicast magic packets; if WoWLAN fails, sniff with"
Write-Host "   wireshark on the runner subnet to see whether the packet reached the NIC at all."
