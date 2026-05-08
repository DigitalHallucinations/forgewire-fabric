<#
.SYNOPSIS
    Common helpers for parsing config\cluster.yaml on any host.

.DESCRIPTION
    Dot-source this file from any DR / chaos / failover script:

        . "$PSScriptRoot\..\..\scripts\dr\_cluster_config.ps1"
        $cfg = Get-ForgeWireClusterConfig
        $chain = Get-ForgeWireFailoverChain -Config $cfg -Preferred "node2"

    Provides:

      Get-ForgeWireClusterConfig [-Path <path>]
        Returns a hashtable representation of cluster.yaml. Tries the
        explicit -Path first, then $env:FORGEWIRE_CLUSTER_CONFIG, then
        the repo-root default ``config\cluster.yaml``.

      Get-ForgeWireFailoverChain -Config <hashtable> [-Preferred <label>]
        Returns an ordered array of "label=host:port" specs. The
        preferred voter is first, the rest follow in priority order.
        ``-Preferred`` overrides ``cfg.preferred_node`` and
        ``$env:FORGEWIRE_PREFERRED_NODE``.

    The YAML parser is deliberately minimal -- enough to read the
    well-formed cluster.yaml shipped with the repo without taking a
    dependency on PowerShell-Yaml. If the schema gets richer, swap in
    ConvertFrom-Yaml.
#>

function Get-ForgeWireClusterConfig {
    [CmdletBinding()]
    param([string]$Path)

    if (-not $Path) { $Path = $env:FORGEWIRE_CLUSTER_CONFIG }
    if (-not $Path) {
        # Repo default: <repo-root>\config\cluster.yaml. This file lives
        # at <repo-root>\scripts\dr\_cluster_config.ps1.
        $repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
        $Path = Join-Path $repoRoot "config\cluster.yaml"
    }
    if (-not (Test-Path $Path)) {
        throw "cluster config not found: $Path"
    }
    return ConvertFrom-MinimalYaml -Path $Path
}

function Get-ForgeWireFailoverChain {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Config,
        [string]$Preferred
    )
    if (-not $Preferred) { $Preferred = $env:FORGEWIRE_PREFERRED_NODE }
    if (-not $Preferred) { $Preferred = $Config["preferred_node"] }

    $voters = $Config["voters"]
    if (-not $voters -or $voters.Count -eq 0) {
        throw "cluster config has no voters"
    }

    $sorted = $voters | Sort-Object {
        if ($null -ne $_["priority"]) { [int]$_["priority"] } else { 9999 }
    }

    if ($Preferred) {
        $head = @($sorted | Where-Object { $_["label"] -eq $Preferred })
        $tail = @($sorted | Where-Object { $_["label"] -ne $Preferred })
        $sorted = @() + $head + $tail
    }

    return @($sorted | ForEach-Object {
        "{0}={1}:{2}" -f $_["label"], $_["host"], $_["port"]
    })
}

# ---------------------------------------------------------------------------
# Minimal YAML parser
# ---------------------------------------------------------------------------
#
# Handles the subset used by cluster.yaml:
#   * top-level scalar:    key: value
#   * top-level mapping:   key:\n  k1: v1\n  k2: v2
#   * top-level sequence-of-mappings:
#         key:
#           - k1: v1
#             k2: v2
#   * inline flow seq:     [a, b, c]
#   * comments (# ...)
#   * quoted and unquoted scalars
#   * 2-space indentation
#
# This is intentionally NOT a general YAML parser. Don't extend the schema
# in cluster.yaml past what's exercised here without swapping for the real
# library.

function ConvertFrom-MinimalYaml {
    param([Parameter(Mandatory)][string]$Path)

    $lines = Get-Content -Path $Path -Encoding utf8
    $root = @{}
    # Each stack frame represents an open scope for child indent > Indent.
    # - Indent:    the indent of *this* frame's keys (children indent more)
    # - Container: the hashtable to write child keys into
    # - Owner:     the parent map (or $null for root)
    # - OwnerKey:  the key in Owner that points at Container (so we can
    #              promote Container from a map to a sequence-of-maps when
    #              we encounter '- ' at the current indent's children)
    $stack = New-Object System.Collections.Stack
    $stack.Push(@{ Indent = -1; Container = $root; Owner = $null; OwnerKey = $null })

    foreach ($raw in $lines) {
        if ($raw -match '^\s*$') { continue }
        if ($raw -match '^\s*#') { continue }
        $line = $raw -replace '\s+#.*$', ''
        if ($line -match '^\s*$') { continue }

        $indent = ($line -replace '^(\s*).*$', '$1').Length
        $body   = $line.Substring($indent)

        # Pop until the top frame's child indent (Indent + 2 in 2-space yaml,
        # but we tolerate any deeper) accommodates this line.
        while ($stack.Peek().Indent -ge $indent) {
            [void]$stack.Pop()
        }
        $top = $stack.Peek()

        if ($body.StartsWith("- ")) {
            $itemBody = $body.Substring(2)
            # Promote Owner[OwnerKey] from @{} to an array if needed.
            if ($null -eq $top.Owner -or -not $top.OwnerKey) {
                throw "unexpected '- ' at indent $indent (no owning key)"
            }
            $existing = $top.Owner[$top.OwnerKey]
            if ($existing -isnot [System.Collections.IList]) {
                $existing = New-Object System.Collections.ArrayList
                $top.Owner[$top.OwnerKey] = $existing
                $top.Container = $existing
            }
            $itemMap = @{}
            [void]$existing.Add($itemMap)
            if ($itemBody -match '^([^:]+):\s*(.*)$') {
                $k = $Matches[1].Trim()
                $v = $Matches[2].Trim()
                $itemMap[$k] = if ($v -ne "") { ConvertFrom-MinimalYamlScalar $v } else { $null }
                # Push a frame whose container is the new map, so further
                # "k: v" lines at the matching child indent flow into it.
                $stack.Push(@{
                    Indent    = $indent
                    Container = $itemMap
                    Owner     = $existing
                    OwnerKey  = $existing.Count - 1
                })
            } else {
                $stack.Push(@{
                    Indent    = $indent
                    Container = $itemMap
                    Owner     = $existing
                    OwnerKey  = $existing.Count - 1
                })
            }
            continue
        }

        if ($body -match '^([^:]+):\s*(.*)$') {
            $k = $Matches[1].Trim()
            $v = $Matches[2].Trim()
            $container = $top.Container
            if ($container -isnot [System.Collections.IDictionary]) {
                throw "cannot set key '$k' on non-mapping container at indent $indent"
            }
            if ($v -eq "") {
                $container[$k] = @{}
                $stack.Push(@{
                    Indent    = $indent
                    Container = $container[$k]
                    Owner     = $container
                    OwnerKey  = $k
                })
            } else {
                $container[$k] = ConvertFrom-MinimalYamlScalar $v
            }
        } else {
            throw "could not parse YAML line: $raw"
        }
    }

    return $root
}

function ConvertFrom-MinimalYamlScalar {
    param([string]$Value)
    $v = $Value.Trim()
    if ($v -eq "") { return "" }
    # Quoted strings.
    if ($v.StartsWith("'") -and $v.EndsWith("'")) {
        return $v.Substring(1, $v.Length - 2)
    }
    if ($v.StartsWith('"') -and $v.EndsWith('"')) {
        return $v.Substring(1, $v.Length - 2)
    }
    # Inline flow sequence: [a, b, c]
    if ($v.StartsWith('[') -and $v.EndsWith(']')) {
        $inner = $v.Substring(1, $v.Length - 2).Trim()
        if ($inner -eq "") { return ,@() }
        return ,@($inner -split ',' | ForEach-Object {
            ConvertFrom-MinimalYamlScalar $_.Trim()
        })
    }
    # Booleans / null.
    switch -Regex ($v) {
        '^(true|True|TRUE)$'   { return $true }
        '^(false|False|FALSE)$' { return $false }
        '^(null|Null|NULL|~)$' { return $null }
    }
    # Numbers.
    [int]$intVal = 0
    if ([int]::TryParse($v, [ref]$intVal)) { return $intVal }
    [double]$dblVal = 0
    if ([double]::TryParse($v, [ref]$dblVal)) { return $dblVal }
    return $v
}
