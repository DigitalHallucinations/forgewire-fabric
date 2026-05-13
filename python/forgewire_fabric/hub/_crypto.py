"""ForgeWire crypto facade.

Resolves at import time to the Rust-accelerated `forgewire_runtime`
implementation when available, otherwise falls back to the existing
pure-Python ed25519 path.

Operators can force the Python path with ``FORGEWIRE_FORCE_PYTHON=1``.

Lineage: Stage C.1 of the forgewire-runtime extraction (formerly PhrenForge todo 113).
"""

from __future__ import annotations

import json
import os
from typing import Any
from collections.abc import Mapping

__all__ = [
    "HAS_RUST",
    "canonicalize",
    "sign_envelope",
    "sign_payload",
    "verify_envelope",
    "verify_signature",
]


def _force_python() -> bool:
    return os.environ.get("FORGEWIRE_FORCE_PYTHON", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


_use_rust = False
if not _force_python():
    try:
        import forgewire_runtime as _rust  # type: ignore[import-not-found]

        _use_rust = bool(getattr(_rust, "HAS_RUST", False))
    except ImportError:
        _use_rust = False


def _py_canonical(envelope: Mapping[str, Any]) -> bytes:
    return json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _py_verify_signature(public_key_hex: str, payload: bytes, signature_hex: str) -> bool:
    # Lazy import so the rust path doesn't pay the cryptography import cost.
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    try:
        pk = Ed25519PublicKey.from_public_bytes(bytes.fromhex(public_key_hex))
        pk.verify(bytes.fromhex(signature_hex), payload)
        return True
    except Exception:
        return False


def _py_sign_payload(secret_key_hex: str, payload: bytes) -> str:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    sk = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(secret_key_hex))
    return sk.sign(payload).hex()


if _use_rust:
    HAS_RUST = True

    def canonicalize(envelope: Mapping[str, Any]) -> bytes:
        return _rust.canonicalize(envelope)

    def verify_signature(public_key_hex: str, payload: bytes, signature_hex: str) -> bool:
        return _rust.verify_signature(public_key_hex, payload, signature_hex)

    def sign_payload(secret_key_hex: str, payload: bytes) -> str:
        return _rust.sign_payload(secret_key_hex, payload)

    def verify_envelope(
        public_key_hex: str,
        envelope: Mapping[str, Any],
        signature_hex: str,
    ) -> bool:
        return _rust.verify_envelope(public_key_hex, envelope, signature_hex)

    def sign_envelope(secret_key_hex: str, envelope: Mapping[str, Any]) -> str:
        return _rust.sign_envelope(secret_key_hex, envelope)

else:
    HAS_RUST = False

    def canonicalize(envelope: Mapping[str, Any]) -> bytes:
        return _py_canonical(envelope)

    def verify_signature(public_key_hex: str, payload: bytes, signature_hex: str) -> bool:
        return _py_verify_signature(public_key_hex, payload, signature_hex)

    def sign_payload(secret_key_hex: str, payload: bytes) -> str:
        return _py_sign_payload(secret_key_hex, payload)

    def verify_envelope(
        public_key_hex: str,
        envelope: Mapping[str, Any],
        signature_hex: str,
    ) -> bool:
        return _py_verify_signature(public_key_hex, _py_canonical(envelope), signature_hex)

    def sign_envelope(secret_key_hex: str, envelope: Mapping[str, Any]) -> str:
        return _py_sign_payload(secret_key_hex, _py_canonical(envelope))
