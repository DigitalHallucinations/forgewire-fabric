"""Parity tests: Rust forgewire_runtime ≡ Python ed25519 path.

Stage C.1 of PhrenForge todo 113. Skips automatically when the Rust extension
is not built.
"""

from __future__ import annotations

import json
import secrets

import pytest

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from scripts.remote.hub import _crypto

try:
    import forgewire_runtime as _rust  # type: ignore[import-not-found]
    _HAS_RUST = bool(getattr(_rust, "HAS_RUST", False))
except ImportError:
    _rust = None
    _HAS_RUST = False

pytestmark = pytest.mark.skipif(
    not _HAS_RUST,
    reason="forgewire_runtime Rust extension not built",
)


def _py_canonical(envelope: dict) -> bytes:
    return json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _fresh_keypair() -> tuple[str, str]:
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


@pytest.mark.parametrize(
    "envelope",
    [
        {"op": "register", "runner_id": "abc", "ts": 1234, "nonce": "n1"},
        {"b": 1, "a": 2, "c": [3, 4]},
        {"outer": {"z": 1, "a": 2}, "x": 3},
        {"unicode": "héllo", "nested": {"emoji": "🚀"}},
        {"empty_list": [], "empty_obj": {}, "null": None, "bool": True, "neg": -42},
    ],
)
def test_canonicalize_matches_python(envelope: dict) -> None:
    rust_bytes = _rust.canonicalize(envelope)
    py_bytes = _py_canonical(envelope)
    assert rust_bytes == py_bytes


def test_python_signature_verifies_under_rust() -> None:
    """Cross-check: Python signs, Rust verifies."""
    sk_hex, pk_hex = _fresh_keypair()
    envelope = {"op": "heartbeat", "runner_id": "host-1", "ts": 1700000000}
    sig_hex = _crypto._py_sign_payload(sk_hex, _py_canonical(envelope))
    assert _rust.verify_envelope(pk_hex, envelope, sig_hex) is True


def test_rust_signature_verifies_under_python() -> None:
    """Cross-check: Rust signs, Python verifies."""
    sk_hex, pk_hex = _fresh_keypair()
    envelope = {"op": "heartbeat", "runner_id": "host-2", "ts": 1700000000}
    sig_hex = _rust.sign_envelope(sk_hex, envelope)
    assert _crypto._py_verify_signature(pk_hex, _py_canonical(envelope), sig_hex) is True


def test_tampered_envelope_rejected_by_both() -> None:
    sk_hex, pk_hex = _fresh_keypair()
    envelope = {"op": "drain", "runner_id": "host-3", "ts": 1700000000}
    sig_hex = _rust.sign_envelope(sk_hex, envelope)
    tampered = {**envelope, "ts": 1700001234}
    assert _rust.verify_envelope(pk_hex, tampered, sig_hex) is False
    assert _crypto._py_verify_signature(pk_hex, _py_canonical(tampered), sig_hex) is False


def test_fuzz_random_envelopes() -> None:
    """100 random keypair × envelope combinations must agree."""
    rng = secrets.SystemRandom()
    for _ in range(100):
        sk_hex, pk_hex = _fresh_keypair()
        envelope = {
            "op": rng.choice(["register", "heartbeat", "drain", "claim"]),
            "runner_id": secrets.token_hex(8),
            "ts": rng.randint(1_600_000_000, 1_800_000_000),
            "nonce": secrets.token_hex(rng.randint(4, 16)),
            "extra": {"k": rng.randint(-1000, 1000), "s": secrets.token_hex(4)},
        }
        sig_rust = _rust.sign_envelope(sk_hex, envelope)
        sig_py = _crypto._py_sign_payload(sk_hex, _py_canonical(envelope))
        # ed25519 is deterministic → identical bytes either side.
        assert sig_rust == sig_py
        assert _rust.verify_envelope(pk_hex, envelope, sig_rust) is True
        assert _crypto._py_verify_signature(
            pk_hex, _py_canonical(envelope), sig_py
        ) is True


def test_facade_uses_rust_when_available() -> None:
    assert _crypto.HAS_RUST is True


def test_facade_force_python(monkeypatch: pytest.MonkeyPatch) -> None:
    """FORGEWIRE_FORCE_PYTHON=1 must bypass the Rust path on a fresh import."""
    import importlib

    monkeypatch.setenv("FORGEWIRE_FORCE_PYTHON", "1")
    fresh = importlib.reload(_crypto)
    try:
        assert fresh.HAS_RUST is False
        sk_hex, pk_hex = _fresh_keypair()
        envelope = {"op": "register", "ts": 1700000000}
        sig = fresh.sign_envelope(sk_hex, envelope)
        assert fresh.verify_envelope(pk_hex, envelope, sig) is True
    finally:
        monkeypatch.delenv("FORGEWIRE_FORCE_PYTHON", raising=False)
        importlib.reload(_crypto)
