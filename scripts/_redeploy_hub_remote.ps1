Set-Location C:\Projects\forgewire-fabric
git fetch origin 2>&1 | Out-Host
git checkout main 2>&1 | Out-Host
git reset --hard origin/main 2>&1 | Out-Host
.venv\Scripts\python.exe -m pip install -e . --no-deps --quiet 2>&1 | Out-Host
nssm restart ForgewireHub 2>&1 | Out-Host
Start-Sleep -Seconds 3
nssm status ForgewireHub 2>&1 | Out-Host
try {
    $r = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:8765/healthz" -TimeoutSec 5
    Write-Host "HEALTHZ_STATUS=$($r.StatusCode)"
    Write-Host "HEALTHZ_BODY=$($r.Content)"
} catch {
    Write-Host "HEALTHZ_ERROR=$($_.Exception.Message)"
}
$ver = .venv\Scripts\python.exe -c "import forgewire_fabric; print(forgewire_fabric.__version__)"
Write-Host "FABRIC_VERSION=$ver"
