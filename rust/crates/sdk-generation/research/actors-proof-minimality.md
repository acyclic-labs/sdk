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

The source-to-obligation mapping is deliberately bounded to these APIs:

| Kani harness | Rust API exercised | Mathematical obligation |
| --- | --- | --- |
| `code_sha256_fixed_length_matches_contract` | `valid_code_sha256` | Exactly 32 bytes and not all zero are admitted. |
| `code_sha256_constructor_preserves_valid_bytes` | `CodeSha256::new`, `CodeSha256::as_bytes` | Every valid 32-byte input is preserved byte-for-byte. |
| `code_sha256_rejects_empty`, `code_sha256_rejects_31_bytes`, `code_sha256_rejects_33_bytes`, `code_sha256_rejects_64_bytes` | `valid_code_sha256` | Representative invalid lengths are rejected; this is not a claim about unrelated APIs. |
| `positive_u64_constructor_accepts_exactly_nonzero_values` | `PositiveU64::new`, `PositiveU64::get` | For the full symbolic `u64` domain, admission is equivalent to `value != 0` and preservation is exact. |
| `actor_limits_constructor_accepts_exactly_positive_values` | `ActorLimits::new` and field accessors | All three limit values independently admit exactly nonzero `u64`s and preserve `u64::MAX`. |
| `subscription_start_preserves_cursor_and_current_head_presence` | `SubscriptionStart::try_from`, `cursor_value`, `current_head_value` | Cursor values span the full `u64` domain; only `CurrentHead(true)` and a cursor variant are valid. |
| `subscription_start_valid_wire_round_trip_preserves_oneof_identity` | `SubscriptionStart` `TryFrom`/`From` | Each valid oneof variant round-trips without changing its variant or value. |
| `enum_numeric_mappings_are_inverse_and_lossless` | `SubscriptionState`, `ActorState`, `ErrorCode` `TryFrom<i32>`/`From` | Known numeric values are inverse and unknown values return the corresponding domain error. |

The length-specific digest harnesses are intentionally narrower than a full
length theorem; the constructor-preservation harness plus the source
predicate provide the exact 32-byte positive case. No row proves transport,
descriptor serialization, generated bindings, packaging, or foreign-pointer
ownership.

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
output for the current renderer invocation and local toolchain only. It is an
empirical regression test, not a mathematical proof. It does not quantify over
all renderer inputs, protoc versions, or platforms, and it does not prove
descriptor compatibility (covered separately), transport behavior, or
generated binding ownership. The mathematical claims in this note are limited
to the listed Kani harnesses that quantify symbolic domain values.

## Protify ordering argument and remaining gap

The installed Protify source is `protify 0.1.4`. Its non-inventory package
path preserves the declaration vectors supplied by the `proto_file`,
`proto_message`, and `proto_service` macros, then calls `ProtoFile::sort_items`.
That routine gives a structural ordering for imports, top-level and nested
messages/enums, extensions, and services; imports are explicitly emitted from
an ordered copy of the import set. The inventory path uses an insertion-order
map for registry collection and applies the same per-file sort before
rendering. These facts explain why the source renderer has a stable ordering
for the declarations covered by the current test.

This is not a complete formal determinism argument for every emitted byte:
message entries/oneofs, fields, service handlers, options, enum variants, and
the package file vector remain ordinary vectors, and the inventory iteration
order is an external collection detail. Consequently the repeated-render
test remains the source-bound guard; no claim of universal Protify or
cross-toolchain determinism is made without a stronger ordered-input contract.

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
