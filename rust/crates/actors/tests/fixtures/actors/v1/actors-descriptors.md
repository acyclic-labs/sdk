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

The 2026-10-31 publication capture at source `38dc7c95c22c` composes the v1
publication upload and deployment envelope fields with the subscription retry
counter removal. These commands captured the Rust and independent Buf outputs:

```
cargo run --locked -p acyclic-actors --example actors-http-routes -- target/actor-publication-contract/actors-service.ts --proto-out target/actor-publication-contract/proto
cmp target/actor-publication-contract/proto/actors/v1/actors.proto rust/crates/actors/proto/actors/v1/actors.proto
node_modules/.bin/buf build --path rust/crates/actors/proto/actors --exclude-source-info -o target/actor-publication-contract/actors-buf.bin
cargo run --locked -p sdk-proto-codegen -- canonicalize actors target/actor-publication-contract/actors-buf.bin target/actor-publication-contract/actors-buf-v1.bin
```

`Actors.service.bin` was copied only to the two Rust descriptor paths;
`actors-buf-v1.bin` was copied only to the Buf fixture path. The independent
descriptor comparison passed. `node scripts/check-compatibility-digests.mjs write`
recorded the emitted descriptor and maintained schema digests in the manifest.

The 2026-11-03 capture adds the shared `machines.read-results` extractor
declaration to the imported `validation/v1/options.proto`; Actors sources are
unchanged. The Rust descriptor came from
`cargo run --locked -p sdk-proto-codegen -- descriptors . OUTPUT` and was
copied to both Rust descriptor paths. The Buf fixture came from the Buf build
and `canonicalize actors` commands above. `node
scripts/check-compatibility-digests.mjs write` recorded the descriptor digest.

The 2026-11-04 attribution capture uses schema and exporter source
`732b61931ee4eb6c65c4a46a1233d178cc095ffc`. `ServiceError` supplies Error
fields 3-5 (`component`, `reason`, `request_id`). The original exporter output
matches the maintained `actors/v1/actors.proto` byte for byte. Its descriptor
and the selected Rust compiler output both have SHA-256
`c628e85818454161cc061a13073c6e6f1721841bd73b30fc8f9c22cd55dfb4dc`:

```sh
cargo run --locked -p acyclic-actors --example actors-http-routes -- target/actors-cause-capture/actors-service.ts --proto-out target/actors-cause-capture/proto
cmp target/actors-cause-capture/proto/actors/v1/actors.proto rust/crates/actors/proto/actors/v1/actors.proto
cargo run --locked -p sdk-proto-codegen -- descriptors . target/actors-cause-descriptors actors
node_modules/.bin/buf build --path rust/crates/actors/proto/actors --exclude-source-info -o actors-cause-buf-raw.bin
cargo run --locked -p sdk-proto-codegen -- canonicalize actors actors-cause-buf-raw.bin actors-cause-buf-v1.bin
node scripts/check-compatibility-digests.mjs write
```

The locked Rust pipeline uses `protoc-bin-vendored-linux-x86_64` 3.3.0 and
Rust 1.98.1. The independently installed Buf package is 1.75.0. Its original
image has SHA-256
`e225cde47e304a3692764ff7d071af525d1d4baae66e45d060e30cef5170d0cc`;
the shared canonicalizer produces SHA-256
`9794310409f2890c7528dc026cf2968acb89e8fabb5e5a1965e7df66bec654c8`.
Only the Rust output is copied to `compatibility/descriptors/actors-v1.bin`
and `actors-v1.bin` here. Only the independent canonical Buf image is copied
to `actors-buf-v1.bin`; its file metadata and unknown options are retained.
The comparison controls continue to qualify these outputs independently.
