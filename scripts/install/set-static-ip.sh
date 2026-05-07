#!/usr/bin/env bash
# set-static-ip.sh — pin this Linux host to a static IPv4 address.
#
# Idempotent. Designed for ForgeWire Fabric coordinator/runner boxes (Linux)
# so the address never drifts under DHCP renewal. Detects the interface that
# currently holds the default IPv4 route, then writes a NetworkManager (nmcli)
# or systemd-networkd profile depending on what's installed.
#
# Run as root (or via sudo). Re-run is safe; the new profile replaces the old.
#
# Usage:
#   sudo ./set-static-ip.sh --ipv4 10.120.81.50/24 --gateway 10.120.81.86 \
#        [--dns 10.120.81.86,1.1.1.1] [--iface eth0]
#
# To revert to DHCP (NetworkManager):
#   sudo nmcli connection modify forgewire-static ipv4.method auto
#   sudo nmcli connection up forgewire-static

set -euo pipefail

ipv4=""
gateway=""
dns=""
iface=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ipv4)    ipv4="$2"; shift 2 ;;
        --gateway) gateway="$2"; shift 2 ;;
        --dns)     dns="$2"; shift 2 ;;
        --iface)   iface="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,15p' "$0"; exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "$ipv4" || -z "$gateway" ]]; then
    echo "error: --ipv4 and --gateway are required" >&2
    exit 2
fi

if [[ $EUID -ne 0 ]]; then
    echo "error: must run as root (use sudo)" >&2
    exit 1
fi

if [[ -z "$iface" ]]; then
    iface="$(ip -4 route show default | awk '{print $5; exit}')"
    if [[ -z "$iface" ]]; then
        echo "error: could not detect default-route interface; pass --iface" >&2
        exit 1
    fi
    echo "auto-detected interface: $iface"
fi

if [[ -z "$dns" ]]; then
    dns="${gateway},1.1.1.1"
fi

profile="forgewire-static"

if command -v nmcli >/dev/null 2>&1 && systemctl is-active --quiet NetworkManager; then
    echo "Using NetworkManager (nmcli)."

    # Drop any existing profile with this name; recreate cleanly.
    nmcli -t -f NAME connection show | grep -Fxq "$profile" && \
        nmcli connection delete "$profile" || true

    nmcli connection add type ethernet ifname "$iface" con-name "$profile" \
        ipv4.method manual \
        ipv4.addresses "$ipv4" \
        ipv4.gateway "$gateway" \
        ipv4.dns "$(echo "$dns" | tr ',' ' ')" \
        connection.autoconnect yes >/dev/null

    # If the iface is wireless, switch the type
    if [[ "$(cat "/sys/class/net/$iface/wireless" 2>/dev/null; echo)" != "" ]]; then
        nmcli connection modify "$profile" connection.type 802-11-wireless
    fi

    nmcli connection up "$profile"

    echo
    echo "New configuration:"
    ip -4 addr show dev "$iface"
    ip -4 route show default
    exit 0
fi

# Fallback: systemd-networkd
if [[ -d /etc/systemd/network ]]; then
    echo "Using systemd-networkd."
    file="/etc/systemd/network/10-${profile}.network"
    cat > "$file" <<EOF
[Match]
Name=$iface

[Network]
Address=$ipv4
Gateway=$gateway
DNS=$(echo "$dns" | tr ',' ' ')
EOF
    chmod 0644 "$file"
    systemctl restart systemd-networkd
    echo "Wrote $file and restarted systemd-networkd."
    ip -4 addr show dev "$iface"
    ip -4 route show default
    exit 0
fi

echo "error: neither NetworkManager nor systemd-networkd is available" >&2
exit 1
