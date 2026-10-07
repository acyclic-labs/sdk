# .NET handle ABI minimality review

Status: research and external prototype evidence only. This note does not
qualify a production C ABI, managed SDK, transport, or ownership policy.

The source-only csbindgen probe is preserved at
`research/qualified-prototypes/dotnet-csbindgen-architecture`. Its external
verification used csbindgen `1.9.8`, a locked Cargo dependency set, generated
C# declarations, and a bundled .NET 8 build. The probe exercised a Rust-owned
opaque token, `ulong.MaxValue`, explicit optional presence, status returns,
and one typed `SafeHandle`. Its receipt binds the Rust source, manifest,
lockfile, generated declarations, and managed façade source.

## Smallest maintained boundary

The smallest maintainable .NET path is Rust-owned runtime state plus
csbindgen declarations and a thin managed façade. The generated declarations
remove handwritten P/Invoke signatures. A typed `SafeHandle` forwards release
to Rust and supplies managed finalization; it must not become a second object
registry or duplicate nominal validation.

The Haskell prototype at `Q:\sdk\work\rust-handle-proof-prototype` proves a
different layer: a pure integer lease state machine. Its current source has a
stale receipt, so that evidence must be rerun before qualification. Its
generational ledger remains the right Rust-side authority for stale handles,
lease counts, and exactly-once destruction transitions.

The pointer-shaped probe and integer-lease model should not be merged as two
ownership systems. Go, Haskell, and .NET need one Rust-owned handle contract.
The representation choice remains open: pointer-shaped opaque handles map
directly to `SafeHandle`, while an integer lease ABI maps naturally to Go and
the formal model but needs an explicit managed lifetime adapter. In either
case, Rust retains issuance, validation, release, synchronization, and drop
authority.

## Proof and FFI obligations

The pure model can establish exact token transitions, stale/unissued
rejection, duplicate-release rejection, generation retirement, and bounded
lease exhaustion. It does not establish pointer validity, allocator
compatibility, C calling conventions, panic containment, process shutdown,
callbacks, concurrent release, or managed finalizer races.

The ABI review must separately require:

- `u64` round-trips through zero and `UInt64.MaxValue` without sentinel use;
- optional values use an explicit presence bit rather than a numeric sentinel;
- every operation returns a documented status/result shape and never unwinds
  across C;
- release is idempotent at the Rust authority and safe under the selected
  concurrent shutdown policy;
- callbacks and cancellation have one terminal transition and defined
  re-entry behavior; and
- generated bindings remain declarations while Rust owns transport,
  validation, retry, recovery, and error meaning.

No new transport or parallel managed registry is justified by the probe.
