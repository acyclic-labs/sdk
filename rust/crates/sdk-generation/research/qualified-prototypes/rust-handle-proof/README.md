# Rust opaque-handle proof prototype

This is an external, pure Rust model for a possible future C ABI registry. It
is not production code and is intentionally outside the SDK source checkout.
It stores integer object IDs only; it never accepts or dereferences pointers.

The model proves ten bounded-state obligations with Kani:

1. stale and unissued handles are rejected; a bitwise copy of a live handle is
   intentionally the same token and is covered by the exactly-once proof;
2. distinct registry domain tags reject cross-registry lookup;
3. releasing a copied lease token is idempotent and drops once;
4. moves mint distinct tokens and clones do not double-drop;
5. generation exhaustion retires a slot instead of reusing its generation;
6. the `MAX - 1` generation boundary retires without reuse;
7. lease-ID exhaustion leaves live state unchanged; and
8. lease-capacity failure leaves the live lease unchanged;
9. active-lease counter overflow is rejected before ledger mutation; and
10. a real `Drop` witness is emitted only at the final release.

`Registry<SLOTS, LEASES>` uses const-generic capacities solely to make the
finite model explicit. The production design must choose a dynamically sized
registry and retain the same checked transitions; it must not inherit these
small proof capacities.

The model does not claim cryptographic unforgeability, arbitrary foreign
pointer validity, allocator compatibility, or concurrent-release safety. A
future concurrent ABI needs a separate mutex/atomic implementation and tests.

Run `proofs/run-kani.sh` with `CARGO_KANI` and `KANI_HOME` pointing to an
already installed Kani bundle. The script emits the raw verifier log and a
separate receipt can record exact tool and source hashes after a successful run.
It uses one verifier job and unwind bound 8, which covers every explicit array
iteration in the finite model.
