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
