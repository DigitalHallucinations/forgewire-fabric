$ErrorActionPreference = 'Continue'
Write-Host '=== host ==='
hostname
Write-Host '=== forgewire processes ==='
Get-CimInstance Win32_Process | Where-Object {
    $_.CommandLine -match 'forgewire|hub|uvicorn|rqlite'
} | Select-Object ProcessId, Name, CommandLine, CreationDate | Format-List
Write-Host '=== service ==='
Get-Service | Where-Object Name -match 'forgewire' | Format-List Name, Status, StartType
Write-Host '=== programdata ==='
Get-ChildItem 'C:\ProgramData\forgewire' -Recurse -ErrorAction SilentlyContinue | Select-Object FullName, Length, LastWriteTime
Write-Host '=== hub dirs ==='
Get-ChildItem 'C:\Users\jerem\' -Directory -ErrorAction SilentlyContinue | Where-Object Name -match 'forgewire' | ForEach-Object {
    Write-Host $_.FullName
    Get-ChildItem $_.FullName -ErrorAction SilentlyContinue | Select-Object Name, Length, LastWriteTime
}
Write-Host '=== sqlite files ==='
Get-ChildItem -Path C:\,C:\Users\jerem\ -Recurse -Filter '*.sqlite*' -ErrorAction SilentlyContinue -Depth 6 | Select-Object FullName, Length, LastWriteTime | Format-Table -AutoSize
Write-Host '=== rqlite query labels ==='
$rq = Invoke-RestMethod -Uri 'http://localhost:4001/db/query?pretty&level=strong' -Method Post -Body '["SELECT key,value,updated_by,updated_at FROM labels"]' -ContentType 'application/json' -ErrorAction SilentlyContinue
$rq | ConvertTo-Json -Depth 8
