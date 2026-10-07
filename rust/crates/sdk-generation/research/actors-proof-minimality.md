# Actors proof minimality and source-bound gaps

Status: source-owned audit and one focused source test. This note does not
qualify a generated SDK, UniFFI lifecycle, transport, or C ABI.

Date: 2026-10-07

## Existing formal coverage

The maintained Actors Kani module is
`rust/crates/actors/src/domain/kani_proofs.rs`. Its current eleven-harness
receipt is `rust/crates/actors/proofs/kani-domain-invariants-current.json`.
Those harnesses execute the real domain constructors and conversions, not a
second validation model. They cover:

- every symbolic `u64` for `PositiveU64`, including exact preservation and
  rejection of zero;
- every symbolic `u64` in each of the three `ActorLimits` fields, including
  `u64::MAX` preservation and zero rejection;
- exact 32-byte nonzero digest admission and byte preservation;
- cursor presence and the complete cursor `u64` range through the real
  `SubscriptionStart` conversion;
- valid `CurrentHead(true)` and rejection of missing or false current-head
  payloads;
- lossless known-enum numeric mappings and explicit unknown-value errors; and
- inverse wire round trips for valid subscription oneofs.

This is stronger evidence for those semantic values than the external opaque
handle prototype. The receipt does not cover the renderer or generated
descriptor bytes, and its source hashes must be refreshed after any source
change before being cited as a current whole-crate receipt.

## Smallest missing source proof

The existing descriptor integration test checks the fresh descriptor against
the archived wire contract and rejects wire-identity, option, and unrelated
unknown-field mutations. It does not check that the Rust-owned renderer emits
the same complete proto tree on two clean outputs.

`rust/crates/actors/src/contract.rs` now contains the focused test
`contract::tests::render_proto_files_is_byte_deterministic`. It invokes the
actual `render_proto_files` function twice into separate temporary directories,
recursively snapshots every generated file, and compares relative paths and
bytes. The targeted test passed on the source-foundation checkout:

```text
cargo test -p acyclic-actors --lib \
  contract::tests::render_proto_files_is_byte_deterministic -- --nocapture
test contract::tests::render_proto_files_is_byte_deterministic ... ok
```

This test is intentionally small: it adds no registry, no pointer handle, no
custom lifecycle object, and no duplicate validator. It proves deterministic
output for the current renderer invocation and local toolchain only. It does
not prove protoc determinism across versions or platforms, descriptor
compatibility (covered separately), transport behavior, or generated binding
ownership.

## Lifecycle and handle boundary

UniFFI-generated Kotlin `free()`/drop behavior remains the maintained native
object lifecycle. The external generational-handle prototype is useful only
for its separately stated pure state-machine obligations; it is not evidence
that Actors needs a custom registry. Any future opaque C consumer must be
qualified against the Rust-owned lifecycle and synchronization design rather
than importing that prototype as a runtime.

The remaining qualification work is therefore source and artifact specific:
refresh the Actors source-bound Kani receipt after a clean revision, retain the
descriptor compatibility test, and qualify generated binding cancellation and
release paths with the exact UniFFI cohort. No mathematical claim about
cryptographic handle unforgeability or arbitrary foreign pointers follows from
the current receipts.
