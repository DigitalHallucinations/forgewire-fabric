# forgewire-runtime

Rust acceleration core for the [ForgeWire](../todos/114-forgewire-fabric/README.md) control plane.

Hot paths migrated to Rust + PyO3, with a Python fallback that ships alongside until the standalone ForgeWire repo cuts wheels for all platforms (Stage E).

## Crates

| Crate | Purpose | Status |
| ----- | ------- | ------ |
| `fabric-protocol` | Wire types + ed25519 sign/verify + canonical JSON envelope | 🚧 Stage C.1 |
| `fabric-claim-router` | Glob set + capability matcher (planned) | 📋 Stage C.2 |
| `fabric-streams` | In-memory ring buffer + flush policy (planned) | 📋 Stage C.3 |
| `fabric-py` | PyO3 bindings; produces `forgewire_runtime` Python extension | 🚧 Stage C.1 |

## Build

```pwsh
# from PhrenForge root, with the venv active
maturin develop --release --manifest-path forgewire-runtime/crates/fabric-py/Cargo.toml
python -c "import forgewire_runtime; print(forgewire_runtime.__version__)"
```

## Python fallback selection

`scripts/remote/hub/_crypto.py` resolves the implementation at import time:

```python
try:
    from forgewire_runtime import verify_envelope, sign_envelope  # Rust
    HAS_RUST = True
except ImportError:
    from scripts.remote.hub._py_fallback import verify_envelope, sign_envelope
    HAS_RUST = False
```

Operators can opt out via `FORGEWIRE_FORCE_PYTHON=1` (mirrors PhrenForge's `PHRENFORGE_FORCE_PYTHON_GRAPH`).

## License

Apache-2.0. When this workspace moves to the standalone `forgewire/` repo at Stage E, the license carries.
