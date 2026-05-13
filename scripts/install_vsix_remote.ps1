$ErrorActionPreference = 'Stop'
$vsix = 'C:\Users\jerem\forgewire-fabric-0.1.15.vsix'
$cli = 'C:\Users\jerem\AppData\Local\Programs\Microsoft VS Code\bin\code.cmd'
Get-ChildItem $vsix | Select-Object Length, LastWriteTime
& $cli --install-extension $vsix --force
& $cli --list-extensions --show-versions | Select-String forgewire
