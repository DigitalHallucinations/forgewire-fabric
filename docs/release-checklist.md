# ForgeWire Fabric Release Readiness Checklist

> Use this checklist during release cut. Keep boxes unchecked until the specific item is verified for the target release.

## Documentation

- [ ] README accurately describes ForgeWire Fabric as the extracted remote dispatch / compute fabric layer.
- [ ] README no longer reads like the full ForgeWire/PhrenForge assistant platform.
- [ ] “What this is / What this is not” section is present.
- [ ] Project lineage is clearly documented.
- [ ] Current limitations are documented.
- [ ] Parent-platform references are removed or explicitly scoped.
- [ ] License badge matches the actual license.
- [ ] Install instructions are verified.
- [ ] Hub/runner smoke test instructions are verified.
- [ ] CLI examples are verified.
- [ ] VS Code workflow instructions are verified.

## Release

- [ ] Version number is finalized.
- [ ] Changelog/release notes are prepared.
- [ ] GitHub tag is prepared.
- [ ] GitHub release draft is prepared.
- [ ] Known limitations are included in release notes.
- [ ] Quick demo path is documented.
- [ ] Security model summary is documented.
- [ ] Signed dispatch envelope behavior is documented.
- [ ] Scope-bound capability token behavior is documented.
- [ ] Runner registration/trust behavior is documented.

## Verification

- [ ] Test suite passes.
- [ ] Hub starts successfully.
- [ ] Runner starts successfully.
- [ ] End-to-end dispatch smoke test passes.
- [ ] Structured event stream can be tailed or observed.
- [ ] VS Code or agent workflow integration path is verified, if present.

## Release cut execution log (fill during cut)

- Release target version:
- Release branch/commit:
- Release operator:
- Date (UTC):

### Recommended verification commands

```bash
pytest -q
forgewire-fabric hub start --host 127.0.0.1 --port 8765
forgewire-fabric runner start --workspace-root /path/to/repo --hub-url http://127.0.0.1:8765
forgewire-fabric dispatch "echo smoke" --scope "**/*"
forgewire-fabric tasks list
forgewire-fabric tasks stream <task-id>
```

### VS Code workflow verification

```bash
cd vscode
npm install
npm run package
```

Attach links to artifacts (release notes draft, demo run, checklist PR, and verification logs) before marking release complete.
