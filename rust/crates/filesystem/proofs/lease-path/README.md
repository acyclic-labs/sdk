# Lease path validation

This crate includes the production formatter directly. The component Kani
harness checks arbitrary identity bytes against a hexadecimal nibble oracle.
The verified identity stubs carry only a write effect: the caller proofs cover
buffer safety, not complete path semantics or native accounting ownership.

The deterministic formatter property compares the actual writer with Rust's
independent hexadecimal and decimal formatting. It covers all byte values at
each identity position, distinct workspace/lease patterns, decimal boundaries,
the maximum expiry, and untouched bytes after the admitted write range. These
are generated formatter inputs, not captured external wire fixtures.

The existing filesystem `paths` fuzzer calls the same `check_formatter_input`
oracle on the first 41 bytes of each sufficiently long input: workspace bytes
0..16, lease bytes 16..32, a little-endian expiry at 32..40 and the initial
buffer byte at 40. All identity/expiry bits and initial buffer values can vary.
The portable-path oracle still receives the original input. No second lease
formatter or parser is introduced.

Three retained seeds in `../../fuzz/corpus/paths` are generated function inputs:

| Seed | Construction | SHA-256 |
| --- | --- | --- |
| `lease-zero` | Zero identities and expiry, initial byte `a5` | `6bd5928b2746115aa1f3f9a266d70cc9936c51dffae8b42608016f1f33e5fa88` |
| `lease-distinct-max` | Workspace byte `i`, lease byte `255-17*i`, maximum expiry, initial byte `5a` | `6a933a9a16d61e9d96937805ce6d42f775e0870610fcc88afe02dd1dc742b7bf` |
| `lease-decimal-boundary` | Workspace byte `37*i mod 256`, lease byte `(73*i+1) mod 256`, expiry `10^19`, initial byte `ff` | `1ad1a9366411b847724466d2d40e92f8d2b1e5bfa4d32dde3832f78ed2cce196` |

The deterministic property replays these exact seeds before its generated
boundary cases. These inputs are not attributed to an external producer. A
fuzz campaign and source branch coverage are separate from corpus replay.

Run the property with:

```sh
cargo test --locked -p acyclic-fs-lease-path-proof --lib tests::production_formatter_preserves_identities_expiry_and_exact_write_range -- --exact
```

The retained mutations change the original production source. From the
repository root, apply either patch, run the same property and require an
assertion failure, then reverse the patch. A compile error is not a successful
negative control. The prior zero-identity maximum-expiry example accepts the
zero-identity mutation; the replacement property must reject it.

```sh
git apply rust/crates/filesystem/proofs/lease-path/mutations/zero-identity.patch
git apply -R rust/crates/filesystem/proofs/lease-path/mutations/zero-identity.patch
git apply rust/crates/filesystem/proofs/lease-path/mutations/lost-expiry-digit.patch
git apply -R rust/crates/filesystem/proofs/lease-path/mutations/lost-expiry-digit.patch
```

The library's `distributed::native::lease_path_tests` properties separately
exercise ordinary/native admission, cancellation, exact receipts and live
custody. Formatter properties and arithmetic proofs do not replace those
public operation checks, source coverage or platform qualification.
