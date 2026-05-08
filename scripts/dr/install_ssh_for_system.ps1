<#
.SYNOPSIS
    Provision an SSH identity into the SYSTEM principal's profile so
    Windows scheduled tasks running as SYSTEM (e.g. ForgeWireRqliteChaos)
    can SSH to remote hosts.

.DESCRIPTION
    Reads ``cfg.chaos.ssh`` from config\cluster.yaml and writes:

      * <SYSTEM>\.ssh\<key-name>        (private key, copied from -KeySource)
      * <SYSTEM>\.ssh\config            (Host blocks for cfg.chaos.ssh.aliases)
      * <SYSTEM>\.ssh\known_hosts       (auto-populated via ssh-keyscan)

    SYSTEM's home is ``%WINDIR%\System32\config\systemprofile``. The .ssh
    directory and key file ACLs are tightened to SYSTEM-only (matching
    what OpenSSH's StrictModes expects), with a copy left at the
    operator-readable companion path so a human Administrator can
    inspect/repair it.

    Idempotent: re-running replaces the key and config in place.
    Self-elevating.

    The public half of -KeySource must already be in authorized_keys on
    every remote voter host. This script does NOT push the public key.

.PARAMETER ConfigPath
    Path to cluster.yaml. Default: <repo>\config\cluster.yaml.

.PARAMETER KeySource
    Override path to the private key to copy. Default = cfg.chaos.ssh.key_source.

.PARAMETER Test
    After install, run ``ssh <alias> hostname`` as SYSTEM (via a one-shot
    scheduled task) for each alias and report the result.

.EXAMPLE
    pwsh -File install_ssh_for_system.ps1
    pwsh -File install_ssh_for_system.ps1 -Test
#>
[CmdletBinding()]
param(
    [string]$ConfigPath,
    [string]$KeySource,
    [switch]$Test
)

$ErrorActionPreference = 'Stop'

# ---- Self-elevation -------------------------------------------------------
$id   = [System.Security.Principal.WindowsIdentity]::GetCurrent()
$prin = [System.Security.Principal.WindowsPrincipal]::new($id)
if (-not $prin.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)) {
    $shellExe = (Get-Process -Id $PID).Path
    $argList = @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $PSCommandPath)
    foreach ($kv in $PSBoundParameters.GetEnumerator()) {
        if ($null -eq $kv.Value) { continue }
        if ($kv.Value -is [switch]) {
            if ($kv.Value.IsPresent) { $argList += "-$($kv.Key)" }
        } else {
            $argList += "-$($kv.Key)"
            $argList += [string]$kv.Value
        }
    }
    Start-Process -FilePath $shellExe -ArgumentList $argList -Verb RunAs -Wait
    return
}

. (Join-Path $PSScriptRoot '_cluster_config.ps1')

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$cfg = Get-ForgeWireClusterConfig -Path $ConfigPath
$ch  = $cfg["chaos"]; if (-not $ch) { $ch = @{} }
$sshCfg = $ch["ssh"]; if (-not $sshCfg) { $sshCfg = @{} }

if (($null -ne $sshCfg["provision_for_system"]) -and -not $sshCfg["provision_for_system"]) {
    Write-Host "cfg.chaos.ssh.provision_for_system=false; nothing to do." -ForegroundColor Yellow
    return
}

# ---- Resolve key source --------------------------------------------------
if (-not $KeySource) {
    $KeySource = if ($sshCfg["key_source"]) { [string]$sshCfg["key_source"] } else { '~\.ssh\id_ed25519_forgewire' }
}
# Expand ~ relative to the *invoking* operator's profile, not SYSTEM's.
if ($KeySource.StartsWith('~')) {
    $KeySource = Join-Path $env:USERPROFILE $KeySource.Substring(2)
}
# When invoked from a SYSTEM-scoped scheduled task, $env:USERPROFILE points at
# C:\Windows\system32\config\systemprofile, which never holds the operator's
# key. Fall back to scanning C:\Users\*\.ssh\<basename> for the first match.
if (-not (Test-Path -LiteralPath $KeySource)) {
    $basename = Split-Path -Leaf $KeySource
    $usersRoot = Join-Path $env:SystemDrive 'Users'
    if (Test-Path -LiteralPath $usersRoot) {
        $candidates = Get-ChildItem -LiteralPath $usersRoot -Directory -ErrorAction SilentlyContinue |
            ForEach-Object { Join-Path $_.FullName ".ssh\$basename" } |
            Where-Object { Test-Path -LiteralPath $_ }
        if ($candidates) {
            Write-Host "Key not found at expanded path; using $($candidates[0])" -ForegroundColor DarkYellow
            $KeySource = $candidates[0]
        }
    }
}
$KeySource = (Resolve-Path -LiteralPath $KeySource -ErrorAction Stop).Path
$keyPub = "$KeySource.pub"
if (-not (Test-Path $keyPub)) {
    throw "public key not found alongside private key: $keyPub"
}

# ---- SYSTEM profile paths -------------------------------------------------
$sysHome = Join-Path $env:WINDIR 'System32\config\systemprofile'
$sysSsh  = Join-Path $sysHome '.ssh'
if (-not (Test-Path $sysSsh)) {
    New-Item -ItemType Directory -Path $sysSsh -Force | Out-Null
}

$keyName    = Split-Path -Leaf $KeySource
$keyDest    = Join-Path $sysSsh $keyName
$keyPubDest = "$keyDest.pub"
$cfgDest    = Join-Path $sysSsh 'config'
$khDest     = Join-Path $sysSsh 'known_hosts'

Copy-Item -LiteralPath $KeySource -Destination $keyDest -Force
Copy-Item -LiteralPath $keyPub    -Destination $keyPubDest -Force

# ---- Write ssh config -----------------------------------------------------
$aliases = @($sshCfg["aliases"])
if ($aliases.Count -eq 0) {
    Write-Warning "cfg.chaos.ssh.aliases is empty; SYSTEM will have keys but no Host entries."
}

$configLines = New-Object System.Collections.ArrayList
[void]$configLines.Add("# Auto-generated by scripts\dr\install_ssh_for_system.ps1.")
[void]$configLines.Add("# Source: config\cluster.yaml :: chaos.ssh.aliases")
[void]$configLines.Add("# Re-run install_ssh_for_system.ps1 to regenerate.")
[void]$configLines.Add("")
foreach ($a in $aliases) {
    if (-not $a -or -not $a["alias"] -or -not $a["hostname"]) {
        Write-Warning "skipping malformed alias entry: $($a | ConvertTo-Json -Compress)"
        continue
    }
    [void]$configLines.Add("Host $($a["alias"])")
    [void]$configLines.Add("    HostName $($a["hostname"])")
    if ($a["user"]) { [void]$configLines.Add("    User $($a["user"])") }
    [void]$configLines.Add("    IdentityFile `"$keyDest`"")
    [void]$configLines.Add("    IdentitiesOnly yes")
    [void]$configLines.Add("    StrictHostKeyChecking accept-new")
    [void]$configLines.Add("    UserKnownHostsFile `"$khDest`"")
    [void]$configLines.Add("    ServerAliveInterval 60")
    [void]$configLines.Add("")
}
Set-Content -Path $cfgDest -Value ($configLines -join "`n") -Encoding ascii -Force

# ---- Pre-populate known_hosts via ssh-keyscan ----------------------------
$sshKeyscan = Get-Command ssh-keyscan -ErrorAction SilentlyContinue
if ($sshKeyscan) {
    $hosts = @($aliases | Where-Object { $_ -and $_["hostname"] } | ForEach-Object { [string]$_["hostname"] } | Select-Object -Unique)
    if ($hosts.Count -gt 0) {
        try {
            $scan = & $sshKeyscan.Source -t ed25519,rsa,ecdsa $hosts 2>$null
            if ($scan) {
                Set-Content -Path $khDest -Value $scan -Encoding ascii -Force
            }
        } catch {
            Write-Warning "ssh-keyscan failed: $($_.Exception.Message); known_hosts will be auto-populated on first connect (StrictHostKeyChecking=accept-new)."
        }
    }
} else {
    Write-Warning "ssh-keyscan not on PATH; known_hosts will be auto-populated on first connect."
}

# ---- Tighten ACLs --------------------------------------------------------
# OpenSSH on Windows enforces that the private key is owned and
# readable only by the running principal (SYSTEM here). Reset the ACL
# accordingly using icacls.
$acls = @($keyDest, $cfgDest)
foreach ($p in $acls) {
    if (-not (Test-Path $p)) { continue }
    & icacls.exe $p /inheritance:r /grant:r 'SYSTEM:R' 'Administrators:R' 2>&1 | Out-Null
}

Write-Host "Provisioned SSH identity for SYSTEM:" -ForegroundColor Green
Write-Host "  Key:       $keyDest"
Write-Host "  Config:    $cfgDest"
Write-Host "  KnownHosts:$khDest"
Write-Host "  Aliases:   $((@($aliases) | ForEach-Object { $_["alias"] }) -join ', ')"

# ---- Optional: verify SYSTEM can SSH -------------------------------------
if ($Test) {
    Write-Host ""
    Write-Host "Verifying SSH-as-SYSTEM..." -ForegroundColor Cyan
    foreach ($a in $aliases) {
        if (-not $a -or -not $a["alias"]) { continue }
        $alias = [string]$a["alias"]
        $taskName = "ForgeWireSSHTest_$alias"
        $outFile = Join-Path $env:TEMP "fw_ssh_test_$alias.txt"
        Remove-Item $outFile -ErrorAction SilentlyContinue
        $sshExe = (Get-Command ssh).Source
        $argStr = "$alias hostname"
        $action = New-ScheduledTaskAction -Execute 'cmd.exe' -Argument "/c `"$sshExe $argStr > `"$outFile`" 2>&1`""
        $trigger = New-ScheduledTaskTrigger -Once -At ([DateTime]::Now.AddYears(10))
        $principalObj = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest
        $existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
        if ($existing) { Unregister-ScheduledTask -TaskName $taskName -Confirm:$false }
        Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Principal $principalObj | Out-Null
        try {
            Start-ScheduledTask -TaskName $taskName
            $sw = [Diagnostics.Stopwatch]::StartNew()
            while ($sw.Elapsed.TotalSeconds -lt 20) {
                $info = Get-ScheduledTaskInfo -TaskName $taskName
                if ($info.LastTaskResult -ne 267009 -and $info.LastTaskResult -ne $null) { break }
                Start-Sleep -Milliseconds 250
            }
            $info = Get-ScheduledTaskInfo -TaskName $taskName
            $output = if (Test-Path $outFile) { (Get-Content $outFile -Raw).Trim() } else { '<no output>' }
            $ok = ($info.LastTaskResult -eq 0)
            $color = if ($ok) { 'Green' } else { 'Red' }
            Write-Host ("  [{0}] {1} -> rc={2} output={3}" -f ($(if ($ok){'OK'}else{'FAIL'})), $alias, $info.LastTaskResult, $output) -ForegroundColor $color
        } finally {
            Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
        }
    }
}
