# Actors descriptor fixture provenance

`actors-v1.bin` is copied from `compatibility/descriptors/actors-v1.bin`, emitted
by the pinned Rust/protoc pipeline:

```
cargo run --locked -p sdk-proto-codegen -- descriptors . OUTPUT
```

`actors-buf-v1.bin` is independently captured from the pinned `@bufbuild/buf`
1.75.0 tool installed by `bun install --frozen-lockfile`:

```
node_modules/.bin/buf build --path rust/crates/actors/proto/actors --exclude-source-info -o actors-buf.bin
cargo run --locked -p sdk-proto-codegen -- canonicalize actors actors-buf.bin rust/crates/actors/tests/fixtures/actors/v1/actors-buf-v1.bin
```

Keep the Buf image's file metadata (extension 8042). Exporting with
`--as-file-descriptor-set` drops it and fails the existing exact descriptor
comparison. The comparator removes source locations and declaration ordering;
it preserves unknown metadata and custom options, with mutation controls.

Both compiler outputs use the shared canonicalizer's pinned Google imports.
Buf 1.75 adds Google descriptor fields that differ from that retained input;
canonicalization keeps those compiler inputs stable. Project messages, fields,
services and custom options still come independently from Buf and are compared
exactly. The Rust descriptor is never substituted for the Buf output.

Both current fixtures include reference-only secret and Fetch selections.
`actors-pre-secrets.bin` retains the unmodified actual
predecessor descriptor; its separate provenance file identifies that capture.
