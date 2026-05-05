# Operations: install hub & runner as a service

> Run `forgewire-fabric hub start` and `forgewire-fabric runner start` as long-lived,
> auto-restarting services on Windows, Linux, and macOS.

This guide assumes you have already run [`pip install forgewire-fabric`](../QUICKSTART.md#1-install)
in a Python environment of your choice and have a hub bearer token saved
somewhere (the install scripts will pick it up via env var or a token file).

For TLS termination, see [`tls.md`](tls.md).

---

## Windows — NSSM

[NSSM](https://nssm.cc/) wraps any console program as a Windows service. The
installer scripts below are idempotent: re-running them will update the
service definition in place.

### Install the hub

```powershell
# As Administrator, from the forgewire repo (or anywhere with the scripts on disk)
pwsh -File scripts\install\nssm-install-hub.ps1 `
    -PythonExe "C:\Python311\python.exe" `
    -Token "<paste your hub bearer token>" `
    -Port 8765 `
    -DbPath "C:\ProgramData\forgewire\hub.sqlite3"
```

Result:

- A Windows service named **ForgeWireHub** that runs
  `python -m forgewire_fabric.cli hub start --host 0.0.0.0 --port 8765 --db-path ...`
- Auto-start on boot, auto-restart on crash (10s back-off, no throttle).
- Logs at `C:\ProgramData\forgewire\logs\hub.{out,err}.log` with NSSM's
  built-in rotation (10 MB, keep 5 generations).
- Token loaded from `C:\ProgramData\forgewire\hub.token` so it never appears
  in the service args.

### Install a runner

```powershell
pwsh -File scripts\install\nssm-install-runner.ps1 `
    -PythonExe "C:\Python311\python.exe" `
    -HubUrl "https://hub.local" `
    -Token "<bearer token>" `
    -WorkspaceRoot "C:\Work\repo" `
    -Tags "windows,gpu:nvidia,python:3.11" `
    -ScopePrefixes "src/,tests/"
```

Service name: **ForgeWireRunner**.

### Manage

```powershell
nssm status ForgeWireHub
nssm restart ForgeWireHub
nssm edit ForgeWireHub      # GUI to tweak args/env after install
nssm remove ForgeWireHub confirm
```

---

## Linux — systemd

The unit files in `scripts/install/systemd/` are templates. Edit the
`User=`, `WorkingDirectory=`, and `Environment=` lines before installing.

### Install the hub

```bash
sudo install -d -o forgewire -g forgewire /var/lib/forgewire /var/log/forgewire
sudo cp scripts/install/systemd/forgewire-hub.service /etc/systemd/system/
sudo install -m 0640 -o forgewire -g forgewire \
    your-token-file /etc/forgewire/hub.token

# Edit /etc/systemd/system/forgewire-hub.service to point Python at your venv
sudo systemctl daemon-reload
sudo systemctl enable --now forgewire-hub.service
sudo systemctl status forgewire-hub.service
```

### Install a runner

```bash
sudo cp scripts/install/systemd/forgewire-runner.service /etc/systemd/system/
sudo systemctl edit forgewire-runner.service     # set HUB_URL, WORKSPACE_ROOT, TAGS, SCOPE_PREFIXES
sudo systemctl daemon-reload
sudo systemctl enable --now forgewire-runner.service
```

### Logs

```bash
journalctl -u forgewire-hub.service -f
journalctl -u forgewire-runner.service -f
```

The unit files set `StandardOutput=journal` and `Restart=on-failure` with a
10-second back-off. They run under a dedicated `forgewire` user with
`ProtectSystem=strict`, `ProtectHome=true`, `NoNewPrivileges=true`, and a
restricted system-call filter.

---

## macOS — launchd

```bash
sudo cp scripts/install/launchd/com.forgewire_fabric.hub.plist /Library/LaunchDaemons/
sudo chown root:wheel /Library/LaunchDaemons/com.forgewire_fabric.hub.plist
sudo chmod 0644 /Library/LaunchDaemons/com.forgewire_fabric.hub.plist
sudo launchctl load -w /Library/LaunchDaemons/com.forgewire_fabric.hub.plist
```

Edit the plist to point at your Python interpreter and to set
`FORGEWIRE_HUB_TOKEN_FILE`.

For a runner, use `com.forgewire_fabric.runner.plist` and set the
`FORGEWIRE_HUB_URL`, `FORGEWIRE_RUNNER_WORKSPACE_ROOT`,
`FORGEWIRE_RUNNER_TAGS`, and `FORGEWIRE_RUNNER_SCOPE_PREFIXES`
environment variables in the `<dict>` block.

Logs land at `/var/log/forgewire/{hub,runner}.{out,err}.log` (rotated by
`newsyslog`; see `scripts/install/launchd/forgewire.newsyslog.conf`).

---

## Backups

The hub state is a single SQLite database at `--db-path`. Recommended
backup approach:

```bash
sqlite3 /var/lib/forgewire/hub.sqlite3 ".backup '/var/backups/forgewire/hub-$(date -u +%Y%m%dT%H%M%SZ).sqlite3'"
```

Run via cron / Task Scheduler. The `.backup` command is online-safe and
works while the hub is serving requests.

---

## Uninstall

| Platform | Command |
| --- | --- |
| Windows | `nssm remove ForgeWireHub confirm` (and `ForgeWireRunner`) |
| Linux | `sudo systemctl disable --now forgewire-hub.service && sudo rm /etc/systemd/system/forgewire-hub.service && sudo systemctl daemon-reload` |
| macOS | `sudo launchctl unload -w /Library/LaunchDaemons/com.forgewire_fabric.hub.plist && sudo rm /Library/LaunchDaemons/com.forgewire_fabric.hub.plist` |

Removing the service does not delete `--db-path`; remove that file
manually if you want to wipe the task graph.
