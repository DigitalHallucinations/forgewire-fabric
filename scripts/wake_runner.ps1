<#
.SYNOPSIS
    Wakes the OptiPlex Copilot runner by sending one `code chat` invocation
    over SSH. The runner chat mode then drains the blackboard queue.

.DESCRIPTION
    v2 topology: the hub and the runner are colocated on the always-on
    OptiPlex. Precondition: the hub is up (scripts/remote/start_hub.ps1 has
    been run on the OptiPlex). No tunnel is required.

    The remote command opens a Copilot Chat in the OptiPlex's existing VS Code
    window pointed at the runner's repo path with the `phrenforge-runner`
    chat mode pre-selected and a single wake prompt: "drain the queue".

.PARAMETER RemoteHost
    SSH alias of the OptiPlex. Defaults to "phrenforge".

.PARAMETER RemoteRepo
    Repo path on the OptiPlex. Defaults to "C:\Projects\PhrenForge".

.PARAMETER WakeMessage
    The prompt to fire into Copilot Chat. The runner mode treats any wake
    message as "start the loop". Defaults to "drain the queue".
#>
[CmdletBinding()]
param(
    [string]$RemoteHost = "phrenforge",
    [string]$RemoteRepo = "C:\Users\jerem\Projects\PhrenForge",
    [string]$WakeMessage = "drain the queue"
)

$ErrorActionPreference = "Stop"

$escapedMessage = $WakeMessage.Replace('"', '\"')
# `code` on the remote is cmd.exe-friendly. -r reuses the active window;
# --mode picks the custom chat mode by id (filename stem).
$cmd = "cd /d `"$RemoteRepo`" && code chat -r --mode phrenforge-runner `"$escapedMessage`""

Write-Host "Waking ${RemoteHost} runner..."
Write-Host "  remote: $cmd"
ssh $RemoteHost $cmd
$exit = $LASTEXITCODE
if ($exit -ne 0) {
    Write-Warning "ssh exit code $exit -- the wake call may have failed. Check the OptiPlex VS Code window."
} else {
    Write-Host "Wake dispatched. The runner will now loop until the queue is empty."
}
