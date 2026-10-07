# Generational handle proof adequacy and next qualification plan

Status: source-owned research only. No C ABI, foreign pointer API, or
production dependency is qualified by this note.

Date: 2026-10-07

## Current receipt

The external prototype is at
`Q:\\sdk\\work\\rust-handle-proof-prototype`. Its source-bound receipt is
`proof-receipts/kani-receipt.json` and records:

- source SHA-256:
  `cccb02c70467b41e8f5e5a1226b933921aa9ecaa7565b07044797bf9e07bfc23`;
- Kani 0.68.0 with CBMC 6.11.0;
- one verifier job and unwind bound 8; and
- five successful harnesses, with zero failed checks.

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

The receipt is evidence about that exact source snapshot and those five
finite harness executions. It is not a proof receipt for a production ABI.

## What the five harnesses establish

The model has const-generic slot and lease arrays. The receipt instantiates
only `Registry<2, 4>`, `Registry<1, 4>`, and `Registry<1, 2>` through the five
harnesses. Those executions establish the checked transitions in those
bounded states:

1. an old released token, changed-generation token, changed-domain token, and
   every symbolic tuple other than the current live tuple do not resolve to
   the replacement object;
2. two bitwise copies of one live token cannot cause two releases or two drop
   events;
3. explicit clone and move operations mint distinct lease IDs and leave one
   final drop;
4. a vacant slot at `u64::MAX` generation is retired rather than reused; and
5. refusing a lease ID at the checked exhaustion boundary leaves the live
   object usable.

The symbolic tuple check is deliberately conditional on exact equality with
the current live tuple. A bitwise copy of a live token is the same token; the
model does not claim that an attacker cannot guess those bits. The proof also
does not quantify over arbitrary array capacities, all operation sequences, or
all interleavings.

## Adequacy findings and gaps

The prototype is useful as a pure transition model, but the following gaps
must be closed before any ABI qualification.

### Registry identity

The current prototype uses one public constant domain tag. Two independent
registries can therefore issue equal-looking handles, and the current proof
does not exercise cross-registry lookup. The next revision must assign a
registry domain at construction (or use a Rust-owned unique issuer) and add a
cross-registry rejection harness. Even then, a public domain and a valid live
handle are not cryptographically unforgeable; the C contract must describe
handles as opaque capabilities and make no security claim beyond exact state
validation.

### Full-width arithmetic

The exhaustion harness injects `u64::MAX` into a vacant slot and checks the
retirement branch. It does not prove every intermediate generation transition,
nor the `u64::MAX - 1` release boundary. The lease harness injects
`u64::MAX` before issuing a lease; it does not exercise the final permitted ID
and then the next checked failure. Add those neighboring-boundary harnesses
before claiming full-width transition coverage.

### Ledger capacity and atomicity

The bounded lease array makes consumed IDs persistent so repeated release is
observable. A production registry needs a dynamically sized consumed-token
ledger or an equivalent monotonic identity scheme; it must not silently reuse
released records. The prototype’s `clone_lease` issues the new ledger entry
before incrementing the active-lease count. The bounded capacities make that
counter overflow unreachable, but the production transition should preflight
the counter or derive it from the ledger so an error cannot leave an active
lease uncounted.

### Drop and concurrency

`drops` is an abstract destructor-event counter, not a real `Drop` witness. It
proves the state transition’s final-drop count only. A future model should add
a Rust-owned drop witness and specify whether destructor failure is possible.

All five harnesses use exclusive `&mut Registry` access. They prove no
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

The smallest adequate follow-up is:

1. parameterize the registry domain and add cross-registry rejection;
2. add the `MAX - 1`/`MAX` neighboring generation and lease-ID cases;
3. make `clone_lease` failure-atomic for the active-lease count;
4. replace the abstract drop counter with a test-only drop witness while
   retaining the pure transition counter; and
5. add a separate mutex/consumer test lane for concurrent release and C ABI
   opaque-handle transport.

Until those steps pass with a new source hash and receipt, the prototype is a
bounded state-machine qualification aid, not evidence of full-width,
concurrent, cryptographically unforgeable, or arbitrary-pointer-safe handles.

