"""Quick microbench: forgewire_runtime (Rust) vs the Python ed25519 path.

Run from the repo root with the venv active:

    python -X dev scripts/remote/bench_crypto.py

Measures sign + verify + canonicalize over a representative envelope shape.
This is not a CI gate — the locked numbers live in
``forgewire-runtime/PERFORMANCE.md``.
"""

from __future__ import annotations

import json
import time
from statistics import median

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from scripts.remote.hub import _crypto

try:
    import forgewire_runtime as _rust
    HAS_RUST = bool(getattr(_rust, "HAS_RUST", False))
except ImportError:
    _rust = None
    HAS_RUST = False


def _gen_keypair() -> tuple[str, str]:
    sk = Ed25519PrivateKey.generate()
    sk_hex = sk.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    ).hex()
    pk_hex = sk.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    ).hex()
    return sk_hex, pk_hex


def _bench(label: str, fn, iterations: int) -> float:
    # Discard a small warmup, take median of N runs of `iterations` calls.
    fn()
    samples: list[float] = []
    for _ in range(5):
        t0 = time.perf_counter()
        for _ in range(iterations):
            fn()
        samples.append((time.perf_counter() - t0) / iterations)
    m = median(samples)
    print(f"  {label:<48s} {m * 1e6:8.2f} µs/op  ({iterations:,} iters x 5 runs)")
    return m


def _py_canonical(d: dict) -> bytes:
    return json.dumps(d, sort_keys=True, separators=(",", ":")).encode("utf-8")


def main() -> None:
    sk_hex, pk_hex = _gen_keypair()
    envelope = {
        "op": "register",
        "runner_id": "optiplex-7050-1234abcd",
        "ts": 1714742400,
        "nonce": "deadbeefcafef00d",
        "extra": {"k": 1, "tags": ["docs", "tests", "phrenforge:1"]},
    }
    canonical = _py_canonical(envelope)
    sig = _rust.sign_envelope(sk_hex, envelope) if HAS_RUST else _crypto._py_sign_payload(sk_hex, canonical)

    print(f"forgewire_runtime HAS_RUST = {HAS_RUST}\n")

    print("canonicalize:")
    if HAS_RUST:
        _bench("rust ", lambda: _rust.canonicalize(envelope), 50_000)
    _bench("python", lambda: _py_canonical(envelope), 50_000)

    print("\nsign (envelope, includes canonicalize):")
    if HAS_RUST:
        _bench("rust ", lambda: _rust.sign_envelope(sk_hex, envelope), 5_000)
    _bench("python", lambda: _crypto._py_sign_payload(sk_hex, _py_canonical(envelope)), 5_000)

    print("\nverify (envelope, includes canonicalize):")
    if HAS_RUST:
        _bench("rust ", lambda: _rust.verify_envelope(pk_hex, envelope, sig), 5_000)
    _bench(
        "python",
        lambda: _crypto._py_verify_signature(pk_hex, _py_canonical(envelope), sig),
        5_000,
    )


if __name__ == "__main__":
    main()
