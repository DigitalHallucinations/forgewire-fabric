//! ForgeWire wire protocol primitives.
//!
//! Stage C.1: ed25519 sign + verify over canonical JSON envelopes, byte-compatible
//! with the existing Python implementation in `scripts/remote/runner/identity.py`
//! and `scripts/remote/runner/runner_capabilities.py::canonical_payload`.
//!
//! The canonical form is `serde_json` with sorted keys and the compact separators
//! `(',', ':')`, matching Python's
//! `json.dumps(payload, sort_keys=True, separators=(",", ":"))`.

use ed25519_dalek::{Signature, SigningKey, Verifier, VerifyingKey, SECRET_KEY_LENGTH};
use serde_json::{Map, Value};
use thiserror::Error;

/// Errors surfaced by the protocol layer.
///
/// Verification errors are deliberately collapsed to a single variant so callers
/// cannot use the discriminant to fingerprint why a given attempt failed; the
/// Python side returns a bare `False` for the same reason.
#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("invalid hex: {0}")]
    Hex(#[from] hex::FromHexError),

    #[error("invalid public key length: expected 32 bytes, got {0}")]
    PublicKeyLength(usize),

    #[error("invalid private key length: expected 32 bytes, got {0}")]
    PrivateKeyLength(usize),

    #[error("invalid signature length: expected 64 bytes, got {0}")]
    SignatureLength(usize),

    #[error("invalid public key bytes")]
    PublicKey,

    #[error("canonicalization failed: {0}")]
    Canonicalization(#[from] serde_json::Error),
}

/// Canonical-JSON encoding of a `serde_json::Value`.
///
/// Object keys are emitted in sorted order; separators are compact (`,` and `:`).
/// This matches `json.dumps(payload, sort_keys=True, separators=(",", ":"))`.
pub fn canonicalize(value: &Value) -> Result<Vec<u8>, ProtocolError> {
    let mut out = Vec::with_capacity(64);
    write_canonical(value, &mut out)?;
    Ok(out)
}

fn write_canonical(value: &Value, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
    match value {
        Value::Object(map) => {
            let sorted = sort_object(map);
            out.push(b'{');
            for (i, (k, v)) in sorted.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_ascii_string(k, out);
                out.push(b':');
                write_canonical(v, out)?;
            }
            out.push(b'}');
        }
        Value::Array(arr) => {
            out.push(b'[');
            for (i, v) in arr.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_canonical(v, out)?;
            }
            out.push(b']');
        }
        Value::String(s) => {
            write_ascii_string(s, out);
        }
        // Numbers/bools/null: serde_json's default encoding matches Python's
        // json.dumps for these scalar shapes (e.g. integers as bare digits,
        // booleans as `true`/`false`, null as `null`).
        other => {
            serde_json::to_writer(&mut *out, other)?;
        }
    }
    Ok(())
}

/// Encode a string the way `json.dumps(..., ensure_ascii=True)` does:
/// - quoted with `"`
/// - escapes for `"`, `\\`, and the standard short escapes (`\b\f\n\r\t`)
/// - control characters `< 0x20` as `\u00XX`
/// - any non-ASCII character as `\uXXXX` (with UTF-16 surrogate pairs above U+FFFF)
fn write_ascii_string(s: &str, out: &mut Vec<u8>) {
    out.push(b'"');
    for ch in s.chars() {
        match ch {
            '"' => out.extend_from_slice(b"\\\""),
            '\\' => out.extend_from_slice(b"\\\\"),
            '\u{0008}' => out.extend_from_slice(b"\\b"),
            '\u{0009}' => out.extend_from_slice(b"\\t"),
            '\u{000a}' => out.extend_from_slice(b"\\n"),
            '\u{000c}' => out.extend_from_slice(b"\\f"),
            '\u{000d}' => out.extend_from_slice(b"\\r"),
            c if (c as u32) < 0x20 => {
                write_unicode_escape(c as u32, out);
            }
            c if (c as u32) < 0x7f => {
                // Printable ASCII (excluding the controls / quote / backslash already handled).
                out.push(c as u8);
            }
            c => {
                let cp = c as u32;
                if cp <= 0xffff {
                    write_unicode_escape(cp, out);
                } else {
                    // Encode as UTF-16 surrogate pair to match Python's
                    // ensure_ascii output for non-BMP code points.
                    let v = cp - 0x10000;
                    let high = 0xd800 + (v >> 10);
                    let low = 0xdc00 + (v & 0x3ff);
                    write_unicode_escape(high, out);
                    write_unicode_escape(low, out);
                }
            }
        }
    }
    out.push(b'"');
}

fn write_unicode_escape(cp: u32, out: &mut Vec<u8>) {
    out.extend_from_slice(b"\\u");
    let nibbles = [
        (cp >> 12) & 0xf,
        (cp >> 8) & 0xf,
        (cp >> 4) & 0xf,
        cp & 0xf,
    ];
    for n in nibbles {
        let byte = if n < 10 {
            b'0' + n as u8
        } else {
            b'a' + (n as u8 - 10)
        };
        out.push(byte);
    }
}

fn sort_object(map: &Map<String, Value>) -> Vec<(&String, &Value)> {
    let mut entries: Vec<(&String, &Value)> = map.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    entries
}

fn decode_hex_fixed<const N: usize>(s: &str) -> Result<[u8; N], ProtocolError> {
    let raw = hex::decode(s)?;
    if raw.len() != N {
        return match N {
            32 if s.len() == 64 => Err(ProtocolError::PublicKeyLength(raw.len())),
            64 => Err(ProtocolError::SignatureLength(raw.len())),
            _ => Err(ProtocolError::PublicKeyLength(raw.len())),
        };
    }
    let mut out = [0u8; N];
    out.copy_from_slice(&raw);
    Ok(out)
}

/// Verify an ed25519 signature over `payload` using a hex-encoded public key.
///
/// Returns `Ok(true)` only if the signature is valid; `Ok(false)` for any
/// recoverable mismatch (invalid signature). Returns `Err` only for input
/// shape problems (bad hex, wrong length).
pub fn verify_signature_hex(
    public_key_hex: &str,
    payload: &[u8],
    signature_hex: &str,
) -> Result<bool, ProtocolError> {
    let pk_bytes = decode_hex_fixed::<32>(public_key_hex)
        .map_err(|_| ProtocolError::PublicKeyLength(public_key_hex.len() / 2))?;
    let sig_bytes_raw = hex::decode(signature_hex)?;
    if sig_bytes_raw.len() != 64 {
        return Err(ProtocolError::SignatureLength(sig_bytes_raw.len()));
    }
    let mut sig_bytes = [0u8; 64];
    sig_bytes.copy_from_slice(&sig_bytes_raw);

    let pk = VerifyingKey::from_bytes(&pk_bytes).map_err(|_| ProtocolError::PublicKey)?;
    let sig = Signature::from_bytes(&sig_bytes);
    Ok(pk.verify(payload, &sig).is_ok())
}

/// Sign `payload` with a hex-encoded 32-byte ed25519 secret key.
///
/// Returns the 64-byte signature as a lowercase hex string, matching the Python
/// runner identity's `sign(payload).hex()` output.
pub fn sign_payload_hex(
    secret_key_hex: &str,
    payload: &[u8],
) -> Result<String, ProtocolError> {
    let sk_raw = hex::decode(secret_key_hex)?;
    if sk_raw.len() != SECRET_KEY_LENGTH {
        return Err(ProtocolError::PrivateKeyLength(sk_raw.len()));
    }
    let mut sk_bytes = [0u8; SECRET_KEY_LENGTH];
    sk_bytes.copy_from_slice(&sk_raw);

    let signing = SigningKey::from_bytes(&sk_bytes);
    let sig: Signature = ed25519_dalek::Signer::sign(&signing, payload);
    Ok(hex::encode(sig.to_bytes()))
}

/// Convenience: verify a signed envelope (object) using a hex public key and
/// hex signature; canonicalizes the envelope before verifying.
pub fn verify_envelope_hex(
    public_key_hex: &str,
    envelope: &Value,
    signature_hex: &str,
) -> Result<bool, ProtocolError> {
    let canonical = canonicalize(envelope)?;
    verify_signature_hex(public_key_hex, &canonical, signature_hex)
}

/// Convenience: sign an envelope (object) with a hex secret key.
pub fn sign_envelope_hex(
    secret_key_hex: &str,
    envelope: &Value,
) -> Result<String, ProtocolError> {
    let canonical = canonicalize(envelope)?;
    sign_payload_hex(secret_key_hex, &canonical)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonicalize_sorts_keys() {
        let v = json!({"b": 1, "a": 2, "c": [3, 4]});
        let s = canonicalize(&v).unwrap();
        assert_eq!(std::str::from_utf8(&s).unwrap(), r#"{"a":2,"b":1,"c":[3,4]}"#);
    }

    #[test]
    fn canonicalize_nested_objects() {
        let v = json!({"outer": {"z": 1, "a": 2}, "x": 3});
        let s = canonicalize(&v).unwrap();
        assert_eq!(
            std::str::from_utf8(&s).unwrap(),
            r#"{"outer":{"a":2,"z":1},"x":3}"#
        );
    }

    #[test]
    fn canonicalize_ensure_ascii_escapes_non_bmp() {
        let v = json!({"unicode": "héllo", "emoji": "🚀"});
        let s = canonicalize(&v).unwrap();
        assert_eq!(
            std::str::from_utf8(&s).unwrap(),
            r#"{"emoji":"\ud83d\ude80","unicode":"h\u00e9llo"}"#
        );
    }

    #[test]
    fn canonicalize_escapes_control_chars() {
        let v = json!({"k": "a\nb\tc\u{0001}d\""});
        let s = canonicalize(&v).unwrap();
        assert_eq!(
            std::str::from_utf8(&s).unwrap(),
            r#"{"k":"a\nb\tc\u0001d\""}"#
        );
    }

    #[test]
    fn sign_then_verify_roundtrip() {
        // Deterministic: ed25519 from a fixed 32-byte seed.
        let sk_hex = "1".repeat(64);
        let signing = SigningKey::from_bytes(
            &<[u8; 32]>::try_from(hex::decode(&sk_hex).unwrap()).unwrap(),
        );
        let pk_hex = hex::encode(signing.verifying_key().to_bytes());
        let envelope = json!({"op": "register", "runner_id": "abc", "ts": 1234});

        let sig = sign_envelope_hex(&sk_hex, &envelope).unwrap();
        assert!(verify_envelope_hex(&pk_hex, &envelope, &sig).unwrap());

        // Tamper with the envelope and ensure verification fails.
        let tampered = json!({"op": "register", "runner_id": "abc", "ts": 9999});
        assert!(!verify_envelope_hex(&pk_hex, &tampered, &sig).unwrap());
    }

    #[test]
    fn bad_signature_length_is_err() {
        let pk_hex = "0".repeat(64);
        let payload = b"hello";
        let result = verify_signature_hex(&pk_hex, payload, "ab");
        assert!(matches!(result, Err(ProtocolError::SignatureLength(_))));
    }
}
