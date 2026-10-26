# Actors descriptor fixture provenance

`actors-v1.bin` is copied from `compatibility/descriptors/actors-v1.bin`, emitted
by the pinned Rust/protoc pipeline:

```
cargo run --locked -p sdk-proto-codegen -- descriptors . OUTPUT
```

`actors-buf-v1.bin` is independently captured from the pinned `@bufbuild/buf`
1.73.0 tool installed by `bun install --frozen-lockfile`:

```
node_modules/.bin/buf build --path rust/crates/actors/proto/actors --exclude-source-info -o rust/crates/actors/tests/fixtures/actors/v1/actors-buf-v1.bin
```

Keep the Buf image's file metadata (extension 8042). Exporting with
`--as-file-descriptor-set` drops it and fails the existing exact descriptor
comparison. The comparator removes source locations and declaration ordering;
it preserves unknown metadata and custom options, with mutation controls.

Both current fixtures include the reference-only secret selections added on top
of SDK main `b7333953cb`. `actors-pre-secrets.bin` retains the unmodified actual
predecessor descriptor; its separate provenance file identifies that capture.
