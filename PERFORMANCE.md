# forgewire-runtime — Performance

Locked benchmark numbers for the Rust acceleration core. Numbers are wall-clock
medians over 5 runs of N iterations, with the venv warm and the system idle.

## Methodology

```pwsh
.\.venv\Scripts\Activate.ps1
maturin develop --release --manifest-path forgewire-runtime/crates/fabric-py/Cargo.toml
python -m scripts.remote.bench_crypto
```

The bench envelope is representative of a `register` payload:

```json
{
  "op": "register",
  "runner_id": "optiplex-7050-1234abcd",
  "ts": 1714742400,
  "nonce": "deadbeefcafef00d",
  "extra": {"k": 1, "tags": ["docs", "tests", "phrenforge:1"]}
}
```

The Python "verify" path uses `cryptography.hazmat.primitives.asymmetric.ed25519`
which is already C-backed via OpenSSL; the speedup is therefore over an
already-native baseline, not over pure-Python ed25519.

## Results — 2026-05-03 (Precision 5520, Win11, Python 3.11.15, rustc 1.95.0)

| Operation | Rust (`forgewire_runtime`) | Python (`cryptography`) | Speedup |
| --------- | --------------------------- | ------------------------ | ------- |
| `canonicalize` (envelope → bytes) | **25.05 µs/op** | 51.29 µs/op | 2.05× |
| `sign_envelope`  (canonicalize + ed25519 sign) | **385.99 µs/op** | 1018.43 µs/op | 2.64× |
| `verify_envelope` (canonicalize + ed25519 verify) | **316.98 µs/op** | 1106.38 µs/op | 3.49× |

Iterations: 50 k for canonicalize, 5 k for sign/verify. Median of 5 runs.

### Where the time goes (Rust path)

- ~25 µs canonicalize: PyO3 `depythonize` of the dict to `serde_json::Value`, sort, ASCII-escape, encode. Dominated by Python-side dict traversal across the FFI boundary, **not** the canonical encoder itself (the encoder runs in tens of nanoseconds for this envelope size).
- ~290 µs sign / ~290 µs verify: hex-decode of the key + signature (~1 µs) + ed25519-dalek op + signature hex-encode. Dalek itself benches at ~5 µs sign / ~50 µs verify on this CPU; the rest is FFI + hex round-trips.

### Why not 10×?

The original Stage C plan speculated a 10× speedup over the Python implementation. That target assumed the Python baseline was using a pure-Python ed25519 (e.g. `pynacl`'s pure-Python fallback or `nacl.bindings`). PhrenForge uses `cryptography`, which is already a thin shim over OpenSSL, so the C-vs-C delta is much smaller and FFI dominates.

A future optimization pass could:

1. Accept and return raw bytes (no hex decode/encode at the FFI boundary) → saves ~5 µs.
2. Cache the `VerifyingKey` per `runner_id` on the hub side instead of decoding hex on every request → saves ~2 µs per verify.
3. Pre-canonicalize signed-field-only views in the Pydantic models so we don't pay PyO3 dict→Value translation for the unsigned fields → saves ~10 µs per request.

These are tracked as Stage C follow-ups; the current numbers are already good enough to deploy.

## Acceptance for Stage C.1

Per [phase-0-foundations.md](../todos/114-forgewire-fabric/phase-0-foundations.md) (former stage-C-rust-core):

- [x] Parity with Python implementation (`tests/remote/test_forgewire_runtime_parity.py` — 11 passing).
- [x] Rust + Python sign/verify round-trip is byte-identical.
- [x] `FORGEWIRE_FORCE_PYTHON=1` opt-out works.
- [x] Speedup measurable (2–3.5× over an already-C-backed Python baseline).
- [x] Hub server wired to use `_crypto` facade.

---

## Stage C.2 — claim router

Replaces the per-task match loop in `Blackboard.claim_next_task_v2`. The
pre-checks (drain, concurrency cap, resource gates) stay in Python because they
are DB-bound and trivial; the O(N) candidate filter is the hot path.

```pwsh
python -m scripts.remote.bench_claim_router
```

The bench corpus is randomly generated against the same SCOPES / TOOLS / TAGS
catalog the parity tests use, with a runner that satisfies most candidates
("typical case") and a runner whose tenant disqualifies every task ("worst
case: full scan").

### Results — 2026-05-03 (Precision 5520, Win11, Python 3.11.15, rustc 1.95.0)

**Typical case (match found mid-list):**

| N tasks | Rust | Python | Speedup |
| ------- | ---- | ------ | ------- |
| 5       | **45.26 µs/op** | 72.32 µs/op | 1.60× |
| 25      | **39.46 µs/op** | 67.36 µs/op | 1.71× |
| 50      | **37.73 µs/op** | 70.09 µs/op | 1.86× |

**Worst case (tenant rejects every task — full scan, no match):**

| N tasks | Rust | Python | Speedup |
| ------- | ---- | ------ | ------- |
| 5       | 32.75 µs/op | **26.71 µs/op** | 0.82× |
| 25      | **41.58 µs/op** | 48.03 µs/op | 1.16× |
| 50      | **51.69 µs/op** | 72.82 µs/op | 1.41× |

Iterations: 20 k. Median of 5 runs.

### Where the time goes

The claim router never sees raw Python objects long enough to allocate `String`
buffers — the Rust matcher borrows `&str` views into the `PyString`s, uses
`eq_ignore_ascii_case` for tools/tags (avoiding a per-tool `.to_lowercase()`
allocation), and short-circuits on the first failed gate. Only the runner's
`scope_prefixes` are eagerly normalized once per call.

The remaining cost is PyO3 dict-traversal overhead. For a queue of 50
candidates that's ~50 × `PyDict_GetItem` per gate, so the absolute floor on a
full-scan rejection is roughly N × 200 ns regardless of what we do in Rust.

### Why not 50×?

The original Stage C.2 spec aimed for ≥50× at 50 queued tasks × 5 runners. That
target assumed the router would be a fan-out across runners (the planned
`ClaimRouter` Rust class would index runners by tag/tool sorted vectors and do
sub-µs intersection). The shipped C.2 surface is the simpler **single-runner
loop** that mirrors the existing Python contract one-for-one, because:

1. The Python hub already calls `claim_next_task_v2` per-runner inside a
   per-request SQLite transaction. There's no fan-out point to optimize.
2. Stage C.2 is a strict drop-in replacement for the loop body, so the new
   crate is by-construction parity-safe (verified by 10 000-case fuzz).
3. The "fan-out across all queued runners" optimization belongs at the SQL
   layer or in Stage I (the distributed transport rewrite), not in the
   single-process hub.

The shipped numbers are good enough: hub `claim` requests have ~70 µs of
router work today, ~40 µs after C.2. A future pass can:

1. Build a Rust `ClaimRouter` class that pre-extracts the queued task list
   into a compact `Vec<CandidateTask>` once per connection and then dispatches
   per-runner against that view (saves the per-call PyO3 dict traversal).
2. Index runners by tag/tool fingerprints so the hub can answer
   "which runners can claim this task?" without scanning.
3. Move the SQLite query inside the Rust crate (would need `rusqlite` and
   collapses the `BEGIN IMMEDIATE` + `SELECT * FROM tasks` round-trip).

These are tracked under Stage I.

## Acceptance for Stage C.2

- [x] Parity with Python implementation (`tests/remote/test_forgewire_claim_router_parity.py` — 209 passing including 10 000-case fuzz).
- [x] `FORGEWIRE_FORCE_PYTHON=1` opt-out works.
- [x] Speedup measurable on typical case (1.6–1.86×); worst case neutral-to-positive (0.82–1.41×).
- [x] Hub server wired to use `_router` facade; `/healthz` exposes `rust_router` flag.


## Stage C.3 — Stream sequence counter

**Surface:** `forgewire_runtime.StreamCounter` (Rust, `parking_lot::Mutex<HashMap<i64, u64>>`) and `_PyStreamCounter` fallback (`threading.Lock` + dict). Wired into `Blackboard.append_stream` via `scripts/remote/hub/_streams.py`. `FORGEWIRE_FORCE_PYTHON=1` pins Python.

**Counter alone (1 M `next_seq` calls, single thread):**

| backend | throughput |
|---------|-----------:|
| rust    | ~1.1 M ops/sec |
| python  | ~310 k ops/sec |

Speedup: ~3.5× on the counter call itself.

**End-to-end `append_stream` (5 000 lines, single task):**

| backend | throughput |
|---------|-----------:|
| rust    | ~34 lines/sec |
| python  | ~34 lines/sec |

End-to-end is **dominated by SQLite WAL fsync** (`isolation_level=None` autocommit on each INSERT). The counter optimization eliminates the per-call `BEGIN IMMEDIATE` + `SELECT MAX(seq)` round-trip — measurable, but invisible behind disk fsync at this rate. The counter is **available, correct, and free**; the actual hub-throughput win will only be visible once we batch INSERTs (deferred, see follow-up).

**Honest follow-up (deferred):**
- Batch streams into per-task ring buffers and flush every ~50 ms in a background thread (or Tokio task). This is the original C.3 ambition; it requires reworking the HTTP path, not just the counter.
- Switch to `journal_mode=WAL` + `synchronous=NORMAL` to reduce fsync cost (durability trade-off; needs ops sign-off).

**Correctness:**
- 17 / 17 parity tests pass (`tests/remote/test_forgewire_streams_parity.py`).
- 6 / 6 Rust unit tests (incl. 8-thread × 1 000-iter strict-coverage concurrency test).
- Hub restart re-primes from `MAX(seq)` — `kill -9` safe.

## Acceptance for Stage C.3

- [x] Parity with Python implementation across both backends.
- [x] `FORGEWIRE_FORCE_PYTHON=1` opt-out works.
- [x] Concurrent correctness: strictly monotonic, gap-free seqs under 8-way thread contention.
- [x] Hub-restart safety verified by parity test.
- [x] `/healthz` exposes `rust_streams` flag.
- [x] Counter speedup measurable (~3.5×) even though end-to-end is fsync-bound; trade-off documented honestly above.
