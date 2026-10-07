# csbindgen ownership architecture probe

This disposable external probe tests the smallest maintained .NET boundary
shape for Rust-owned nominal state. `csbindgen = 1.9.8` generates only the
P/Invoke declarations from `src/lib.rs`; the managed layer adds one typed
`SafeHandle` and nominal façade per Rust-owned opaque type.

The ABI conventions are explicit:

- `ulong` carries Rust `u64` without narrowing; the probe exercises
  `UInt64.MaxValue`.
- `OptionalU64` has a `present` byte and a `value` field, so absent and zero
  remain distinct.
- constructors and accessors return an integer status; no Rust panic or
  domain exception crosses the C boundary.
- Rust owns allocation and release. `SafeHandle.ReleaseHandle` forwards the
  one release call and does not maintain a second object registry; the probe
  treats only a null pointer as invalid, so it does not impose a `-1` or zero
  value sentinel on Rust-owned handles.

The receipt in `generated/binding-receipt.json` binds the Rust source,
manifest, generated declarations, and generator pin. It is probe evidence,
not an SDK ABI or transport contract. It deliberately omits requests,
responses, retry, async completion, cancellation, callbacks, and error
payloads; those require a separately reviewed shared Rust ABI design.
