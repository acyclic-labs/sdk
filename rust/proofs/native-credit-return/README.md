# Original native credit owners during unwind

The `tests::reservations` property uses the shipped operation, byte, codec and
allocation reservation owners and the shared physical `ReadBuffer` owner. It
crosses normal/panicking return callbacks with normal/panicking callers for each
of the five destructor paths. Original-ledger observations require one return,
exact unused credit, retained actual metadata/copy/allocation work, and the
original caller panic. The operation case queries a real temporary file; the
allocation case frees the production buffer. Unused byte and allocation
reservations intentionally have no physical transfer or allocation to invent.
The codec case copies three bytes and retains exactly that completed prefix.
These are synchronous public credit-owner contracts, not an IOCP completion or
whole-application failure proof.

The existing terminal delivery primitive calls each callback once. A normal
callback panic remains observable. A secondary panic during caller unwind
cannot abort that unwind or discard the remaining owners. Each drop retains
one callback, zero native calls, zero durability barriers, zero round trips,
zero allocations and zero copies before and after this change. The one-item
iterator adds no scan, wait, polling or history-dependent work.

The retained patch restores all three production files byte-for-byte to
`604d8449f8848b9edd5fcab77cc35560691f63cc`, retaining the property. Every one of
the five original paths must fail with its credit-return marker and a destructor
cleanup abort. A patch or compilation failure is not negative-control evidence.
The inverse also restores that revision's Darwin production dependency, retaining
the current tests. This keeps the original Linux-only borrowed constructor and
member storage consistent. Owner selection and shrinking also return to that
revision; select only the five destructor properties below.

Run from the repository root:

```sh
git apply --check rust/proofs/native-credit-return/original-destructors.patch
git apply rust/proofs/native-credit-return/original-destructors.patch
git diff --exit-code 604d8449f8848b9edd5fcab77cc35560691f63cc -- rust/crates/native-runtime/src/meter/allocation.rs rust/crates/native-runtime/src/meter/codec.rs rust/crates/native-runtime/src/meter/reservation.rs
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib --no-fail-fast -E 'test(/^tests::reservations::(operations|bytes|codec|allocation_reservation|allocation)_preserves?_original_unwind$/)'
# Require five failures: original credit return failure, then
# panic in a destructor during cleanup for every original credit owner.
git apply -R rust/proofs/native-credit-return/original-destructors.patch
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib --no-fail-fast -E 'test(/^tests::reservations::(operations|bytes|codec|allocation_reservation|allocation)_preserves?_original_unwind$/)'
# Require exactly five selected passes and no selected skips.
```

Restore the patch even if a command fails. Each property uses the shared
isolated self-execution control so an original destructor abort is finite and
cannot prevent the other four controls from executing.
