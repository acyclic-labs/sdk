# Acyclic PHP transport package

This is the generated PHP transport package for the Rust-owned authority
families. It includes a thin `Acyclic\Runtime\RemoteClient` facade that
selects Rust-qualified transports before invoking an injected wire adapter.
Generated protobuf models and clients remain authoritative; the facade does
not add a handwritten HTTP encoder or retry policy.

Generation requires `protoc` and the `grpc_php_plugin` from the matching gRPC
release:

```powershell
composer install
php tools/generate.php --schema-root ..\\.tmp-rust-authority --manifest ..\\.tmp-rust-authority\\rust-authority.json
composer test
```

The package uses the official `grpc/grpc` runtime and `google/protobuf` runtime.
The generator writes SHA-256 hashes for both schema inputs and the optional Rust
authority manifest to `src/provenance.json`, so CI can reject stale or
cross-authority generated output before packaging.
The generated code is a client: PHP gRPC does not provide a server generator.
Actors is unary; Stream's Read, Follow, and Children methods are server-streaming.
The same generator emits every family listed in the authority manifest,
including client-streaming and server-streaming bindings.

PHP protobuf scalar accessors preserve uint64 values through `PHP_INT_MAX` and
reject larger values instead of narrowing them. Values in the remaining unsigned range must use
`Acyclic\\Runtime\\UInt64`, which preserves the decimal string, JSON string,
and exact protobuf varint without floating-point conversion or clamping. This
policy remains necessary on pure-PHP `google/protobuf` 5.36.2; a native
protobuf extension may be qualified separately when its official PHP ABI
package is available.
The official PECL 5.36.2 Windows asset and its qualification result are
recorded in `native-runtime.lock.json`.
With that extension loaded, `tests/uint64_rust_golden.php` decodes and
re-encodes a Rust `ActorLimits.memoryBytes = u64::MAX` message. It verifies
the Rust wire bytes `10ffffffffffffffffff01` and JSON string
`{"memoryBytes":"18446744073709551615"}` before passing the value through
the exact `UInt64` facade.
The pure-PHP runtime does not qualify for this generated-message maximum
fixture: it decodes the unsigned wire value through a signed integer and is
rejected by the package's no-clamp policy. That limitation is recorded rather
than hidden by a helper-only assertion.

The package is not published from this worktree. Generated output is a local
build artifact and should be validated from a clean checkout in CI.

`Acyclic\\Runtime\\RemoteClient` delegates transport selection and bearer
validation to the Rust-emitted `src/Acyclic/Runtime/GeneratedRemotePolicy.php`
snapshot. Its automatic resolver selects the native policy for the installed
PHP runtime; an embedded PHP/WASM host can set its browser runtime bridge
before construction. Refresh that snapshot with the
`sdk-contract-wire generate-products` command whenever the Rust transport
policy changes.
The package license is Apache-2.0; dependency license evidence is tracked in
`LICENSE-THIRD-PARTY.md`.

Native qualification receipt

With the pinned PECL `protobuf` 5.36.2 extension loaded, `tests/uint64_rust_golden.php` runs the clean installed consumer against `tests/fixtures/rust-family-goldens.json`. It covers all nine Rust authority families with exact wire and JSON round trips: eight family messages carry `u64::MAX`, while Protocol v1 has no uint64 field and checks its real `ProtocolIdentity.version` scalar. The fixture set is bound to the authority manifest hash in `src/provenance.json`.

The pinned official PECL `grpc` 1.82.0 and `protobuf` 5.36.2 Windows extensions are loaded together for the native consumer. `tests/native_transport_fixture.php` exercises a generated Actors unary call, Stream append, and server-stream cancellation against the Rust fixture; it passed with exit code 0. The exact runtime, package, fixture, output, and source-authority hashes are recorded in `tests/fixtures/native-qualification.receipt.json`. Pure PHP remains limited to the no-clamp uint64 policy described above; this native lane qualifies PHP transport and generated-message semantics for the pinned Windows target.

