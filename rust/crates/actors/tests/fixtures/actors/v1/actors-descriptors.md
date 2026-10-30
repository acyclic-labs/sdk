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

Both current fixtures include reference-only secret and Fetch selections and
the selected immutable entry source on deployment, update and observations.
The 2026-10-30 Rust capture used the same compiled descriptor through the
existing Actors metadata exporter, whose maintained-proto comparison passed:

```
cargo run --locked -p acyclic-actors --example actors-http-routes -- target/entry-configuration/routes.json --proto-out target/entry-configuration/proto
cmp target/entry-configuration/proto/actors/v1/actors.proto rust/crates/actors/proto/actors/v1/actors.proto
```

Its emitted `target/entry-configuration/Actors.service.bin` was copied to both
Rust descriptor paths above. The independent Buf capture used the commands
above and the same maintained protobuf input.

`actors-pre-secrets.bin` retains the unmodified actual
predecessor descriptor; its separate provenance file identifies that capture.
