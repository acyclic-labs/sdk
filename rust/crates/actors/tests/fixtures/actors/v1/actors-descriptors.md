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

The 2026-10-31 capture removes the unused subscription retry counter while
retaining every other field's wire identity. The Rust descriptor came from:

```
cargo run --locked -p acyclic-actors --example actors-http-routes -- target/actor-observation/routes.json --proto-out target/actor-observation/proto
cmp target/actor-observation/proto/actors/v1/actors.proto rust/crates/actors/proto/actors/v1/actors.proto
```

The emitted `target/actor-observation/Actors.service.bin` was copied to both
Rust descriptor paths. The independent compiler capture was:

```
node_modules/.bin/buf build --path rust/crates/actors/proto/actors --exclude-source-info -o actors-observation-buf.bin
cargo run --locked -p sdk-proto-codegen -- canonicalize actors actors-observation-buf.bin rust/crates/actors/tests/fixtures/actors/v1/actors-buf-v1.bin
```

`actors-pre-secrets.bin` retains the unmodified actual
predecessor descriptor; its separate provenance file identifies that capture.

The composed 2026-10-31 capture at `f56588ca65` includes the original fork
admission fields and removes the subscription retry counter. The Rust fixture
uses the shared pinned compiler and canonicalizer directly:

```
cargo run --locked -p sdk-proto-codegen -- descriptors . target/actor-composed-descriptors
```

Its `actors-v1.bin` is copied to both Rust descriptor paths above. The
independent Buf capture and canonicalization used:

```
node_modules/.bin/buf build --path rust/crates/actors/proto/actors --exclude-source-info -o rust/crates/actors/tests/fixtures/actors/v1/actors-buf-v1.bin
cargo run --locked -p sdk-proto-codegen -- canonicalize actors rust/crates/actors/tests/fixtures/actors/v1/actors-buf-v1.bin rust/crates/actors/tests/fixtures/actors/v1/actors-buf-v1.bin
```

The Buf image remains independently generated, including its unknown file
metadata. Earlier capture receipts qualify only their original schemas.

The capture at `3ac4ee18fd6a` combines the canonical shared `SecretRef`
import, fork admission fields and removal of the subscription retry counter.
The Rust pipeline above selected only the `actors` family:

```
cargo run --locked -p sdk-proto-codegen -- descriptors . target/actor-reference-descriptors actors
```

Its descriptor was copied to both Rust paths. The independent Buf build and
canonicalization used the commands above on that same source. These captures
replace the predecessor schema outputs; no descriptor bytes were combined by
hand. Manifest digests were written by `scripts/check-compatibility-digests.mjs`.

The 2026-10-31 captures at source b52985ed1e combine the canonical shared
SecretRef import with the current Actor fork and selected-entry fields.
The Rust and independent Buf pipelines above were run on that source; the
Rust capture was copied to both Rust descriptor paths. No descriptor bytes
were combined by hand.

The 2026-10-31 live-update capture, based on main `909744ee57`, adds the v1
publication upload and deployment envelope fields. The Rust metadata exporter
and its maintained-proto comparison passed on bb2. The independent pinned Buf
build and canonicalizer above also ran there. Rust bytes were copied only to
the two Rust paths; Buf bytes were copied only to `actors-buf-v1.bin`.
