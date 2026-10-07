# Generational handle proof adequacy and next qualification plan

Status: source-owned research only. No C ABI, foreign pointer API, or
production dependency is qualified by this note.

Date: 2026-10-07

## Current receipt

The external prototype is at
`Q:\\sdk\\work\\rust-handle-proof-prototype`. Its source-bound receipt is
`proof-receipts/kani-receipt.json` and records:

- source SHA-256:
  `186d82171e4cc89b8e5c3ee1d3b0fe4495b1c93dd25d63e8f040ad63b86bcd60`;
- Kani 0.68.0 with CBMC 6.11.0;
- one verifier job and unwind bound 8; and
- eight successful harnesses, with zero failed checks.

The reproducible command is:

```text
wsl.exe -d Ubuntu --user root -- bash -lc \
  'cd /mnt/q/sdk/work/rust-handle-proof-prototype && \
   export CARGO_HOME=/mnt/q/sdk/work/foundation-kani-068/cargo-home \
          RUSTUP_HOME=/mnt/q/sdk/work/foundation-kani-068/rustup-home \
          CARGO_KANI=/mnt/q/sdk/work/foundation-kani-068/cargo-home/bin/cargo-kani \
          KANI_HOME=/mnt/q/sdk/work/foundation-kani-068/kani-home && \
   bash proofs/run-kani.sh'
```

The receipt is evidence about that exact source snapshot and those eight
finite harness executions. It is not a proof receipt for a production ABI.

## What the eight harnesses establish

The model has const-generic slot and lease arrays. The receipt instantiates
`Registry<2, 4>`, `Registry<1, 4>`, `Registry<1, 2>`, and `Registry<1, 1>`
through the eight harnesses. Those executions establish the checked
transitions in those bounded states:

1. an old released token, changed-generation token, changed-domain token, and
   every symbolic tuple other than the current live tuple do not resolve to
   the replacement object;
2. a handle issued by one explicit registry domain is rejected by another;
3. two bitwise copies of one live token cannot cause two releases or two drop
   events;
4. explicit clone and move operations mint distinct lease IDs and leave one
   final drop;
5. a vacant slot at `u64::MAX` generation is retired rather than reused;
6. releasing generation `u64::MAX - 1` retires the slot, and a direct
   `u64::MAX` creation attempt is rejected;
7. issuing lease ID `u64::MAX - 1` succeeds, the following `u64::MAX` attempt
   is rejected, and both existing leases remain usable; and
8. lease-table capacity failure leaves the live lease usable.

The symbolic tuple check is deliberately conditional on exact equality with
the current live tuple. A bitwise copy of a live token is the same token; the
model does not claim that an attacker cannot guess those bits. The proof also
does not quantify over arbitrary array capacities, all operation sequences, or
all interleavings.

## Adequacy findings and gaps

The prototype is useful as a pure transition model, but the following gaps
must be closed before any ABI qualification.

### Registry identity

The prototype now accepts a domain tag at construction and verifies that two
explicitly distinct domains reject cross-registry lookup. Production code
still needs a Rust-owned unique issuer rather than caller-selected tags. Even
then, a public domain and a valid live handle are not cryptographically
unforgeable; the C contract must describe handles as opaque capabilities and
make no security claim beyond exact state validation.

### Full-width arithmetic

The harnesses now cover the `u64::MAX - 1` generation release, direct
`u64::MAX` generation rejection, the final permitted lease ID, and the next
checked lease-ID failure. They still do not prove every intermediate
generation transition or quantify over arbitrary capacities and operation
sequences, so they are boundary evidence rather than a full-width theorem.

### Ledger capacity and atomicity

The bounded lease array makes consumed IDs persistent so repeated release is
observable. A production registry needs a dynamically sized consumed-token
ledger or an equivalent monotonic identity scheme; it must not silently reuse
released records. The prototype now verifies table-capacity and lease-ID
failure without mutating the live handle. Its `clone_lease` still issues the
new ledger entry before incrementing the active-lease count. The bounded
capacities make that counter overflow unreachable, but the production
transition should preflight the counter or derive it from the ledger so an
error cannot leave an active lease uncounted.

### Drop and concurrency

`drops` is an abstract destructor-event counter, not a real `Drop` witness. It
proves the state transition’s final-drop count only. A future model should add
a Rust-owned drop witness and specify whether destructor failure is possible.

All eight harnesses use exclusive `&mut Registry` access. They prove no
concurrent-release property. The ABI layer needs a separate synchronization
design and tests for two simultaneous releases, clone-versus-release,
shutdown-versus-lookup, and callback re-entry. Those tests must assert one
destructor event, never a negative/duplicate lease count, and no use after the
last release. No current receipt may be cited for this.

### Foreign handles and pointers

The model stores only integer object IDs and never dereferences a pointer. It
therefore proves neither arbitrary foreign pointer validity nor allocator,
calling-convention, callback, panic/unwind, or process-shutdown behavior. A
consumer fixture should pass handles only as opaque integers through the C
boundary and verify stale, unissued, duplicate-release, and cross-registry
errors without attempting to manufacture a Rust reference.

## Minimal next proof bundle

The remaining follow-up is:

1. make `clone_lease` failure-atomic for the active-lease count;
2. replace the abstract drop counter with a test-only drop witness while
   retaining the pure transition counter; and
3. add a separate mutex/consumer test lane for concurrent release and C ABI
   opaque-handle transport.

Until those remaining steps pass with a new source hash and receipt, the
prototype is a bounded state-machine qualification aid, not evidence of
full-width, concurrent, cryptographically unforgeable, or arbitrary-pointer-
safe handles.

