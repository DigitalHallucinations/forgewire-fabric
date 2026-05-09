<#
.SYNOPSIS
    Grant a non-admin user the right to start/stop/pause specific Windows
    services without UAC, by editing the per-service security descriptor.

.DESCRIPTION
    Self-elevates if needed (one-time UAC prompt), then for each named
    service grants the target account the SDDL access mask:

        RP  -- SERVICE_START
        WP  -- SERVICE_STOP
        DT  -- SERVICE_PAUSE_CONTINUE

    No other rights are granted (no SERVICE_CHANGE_CONFIG, no
    SERVICE_ALL_ACCESS, etc.). The change is scoped strictly to the
    listed services; system-wide UAC behavior, file ACLs, and other
    services are untouched.

    After this runs, ``Restart-Service ForgeWireRunner`` (or whichever
    services were granted) works from any normal, non-elevated PowerShell
    for that user.

.PARAMETER Services
    One or more service short names. Missing services are skipped with
    a warning, not an error.

.PARAMETER Account
    DOMAIN\user (or .\user, or just user@domain) to grant rights to.
    Defaults to the *original* invoking user when self-elevation occurs;
    otherwise to the current user.

.EXAMPLE
    pwsh -File grant-service-control.ps1 `
        -Services ForgeWireRunner,ForgeWireHub
        # grants the current user start/stop/pause on those two services

.EXAMPLE
    pwsh -File grant-service-control.ps1 `
        -Services ForgeWireRunner -Account 'DESKTOP-228U8GL\jerem'

.NOTES
    Reversible. To revoke, edit the SDDL again and remove the
    ``(A;;RPWPDT;;;<sid>)`` ACE, or run:

        sc.exe sdset <ServiceName> "$(sc.exe sdshow <ServiceName>)"
        # then manually delete the unwanted ACE.

    Or simply re-create the service: NSSM-managed services rebuild
    their default DACL on (re)install.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string[]]$Services,
    [string]$Account
)

$ErrorActionPreference = 'Stop'

# ---- Self-elevation -------------------------------------------------------
$identity  = [System.Security.Principal.WindowsIdentity]::GetCurrent()
$principal = [System.Security.Principal.WindowsPrincipal]::new($identity)
if (-not $principal.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)) {
    # Default Account to the *original* (pre-elevation) caller so the
    # elevated child grants control to the right principal.
    if (-not $Account) { $Account = "$env:USERDOMAIN\$env:USERNAME" }

    $shellExe  = (Get-Process -Id $PID).Path
    $forwarded = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $PSCommandPath,
                   '-Services', ($Services -join ','),
                   '-Account',  $Account)
    Write-Host "Elevating to apply per-service ACLs (one-time UAC prompt)..."
    $proc = Start-Process -FilePath $shellExe -Verb RunAs -Wait -PassThru -ArgumentList $forwarded
    exit $proc.ExitCode
}

# Comma-split if forwarded as a single string (Start-Process flattens arrays).
if ($Services.Count -eq 1 -and $Services[0] -match ',') {
    $Services = $Services[0] -split '\s*,\s*'
}

if (-not $Account) { $Account = "$env:USERDOMAIN\$env:USERNAME" }

# Resolve account -> SID (fail loud if it doesn't exist).
try {
    $sid = (New-Object System.Security.Principal.NTAccount $Account).
            Translate([System.Security.Principal.SecurityIdentifier]).Value
} catch {
    throw "Could not resolve account '$Account' to a SID: $($_.Exception.Message)"
}

$ace = "(A;;RPWPDT;;;$sid)"   # SERVICE_START | SERVICE_STOP | SERVICE_PAUSE_CONTINUE

foreach ($svc in $Services) {
    if (-not (Get-Service $svc -ErrorAction SilentlyContinue)) {
        Write-Warning "Service '$svc' not found; skipping."
        continue
    }

    $sd = ((sc.exe sdshow $svc) -join '').Trim()
    if (-not $sd) {
        Write-Warning "sc.exe sdshow returned empty SDDL for '$svc'; skipping."
        continue
    }

    if ($sd -match [regex]::Escape($ace)) {
        Write-Host "$svc already grants RP/WP/DT to $Account" -ForegroundColor DarkGray
        continue
    }

    # Insert the ACE in the DACL section. If a SACL ('S:') is present,
    # splice in *before* it; otherwise append.
    if ($sd -match 'S:') {
        $new = $sd -replace 'S:', "${ace}S:"
    } else {
        $new = $sd + $ace
    }

    & sc.exe sdset $svc $new | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "sc.exe sdset failed for '$svc' (exit $LASTEXITCODE); SDDL was: $new"
        continue
    }
    Write-Host "Granted RP/WP/DT to $Account on $svc" -ForegroundColor Cyan
}

Write-Host ""
Write-Host "Done. From a normal (non-elevated) shell, $Account can now:" -ForegroundColor Green
foreach ($svc in $Services) {
    Write-Host "  Restart-Service $svc"
}
