# Runnable Rust metadata prototypes

This note records a pinned, runnable comparison of four Rust-authored metadata
libraries. The prototype source is in
[`research/metadata/rust-metadata-prototypes`](../../research/metadata/rust-metadata-prototypes/).
It uses a standalone Cargo workspace so the experiment does not modify the SDK
workspace or read an active protocol descriptor.

## Reproducible inputs

The package pins `utoipa = 6.0.0`, `schemars = 1.2.2`, `specta =
2.0.0-rc.25`, `specta-typescript = 0.0.12`, `specta-serde = 0.0.12`, and
`typeshare-core = 1.13.4`. Cargo resolved and locked these versions in the
prototype's `Cargo.lock` on 2026-10-03. The versions and licenses were checked
against primary project or crate sources:

| Library | Pinned release and license evidence |
| --- | --- |
| Utoipa | [crate 6.0.0](https://docs.rs/crate/utoipa/6.0.0) and [crate manifest](https://docs.rs/crate/utoipa/6.0.0/source/Cargo.toml.orig), MIT OR Apache-2.0 |
| Schemars | [crate 1.2.2](https://docs.rs/crate/schemars/1.2.2) and [crate manifest](https://docs.rs/crate/schemars/1.2.2/source/Cargo.toml.orig), MIT; the [upstream changelog](https://github.com/GREsau/schemars/blob/master/CHANGELOG.md) identifies 1.2.2 |
| Specta | [Specta 2.0.0-rc.25 docs](https://docs.rs/specta/2.0.0-rc.25/specta/) identifies the MIT license and marks TypeScript stable while OpenAPI and JSON Schema are work in progress; [release history](https://github.com/specta-rs/specta/releases) identifies the pinned release |
| Typeshare | [typeshare-core 1.13.4](https://docs.rs/crate/typeshare-core/1.13.4) and the [upstream repository](https://github.com/1Password/typeshare), whose license is Apache-2.0 OR MIT |

The dependency versions are intentionally explicit. Specta's release is a
release candidate, so it is evidence for a current prototype rather than a
stability recommendation.

## What the prototype runs

`src/main.rs` executes four small, independent examples and prints one JSON
summary. The tests also run each emitter, so a compile-only check cannot hide a
runtime exporter failure.

* **Actors unary:** Utoipa derives an OpenAPI 3.1 document from `ActorRequest`,
  `ActorResponse`, and a `POST /v1/actors` operation. The emitted document
  contains the path, HTTP method, operation ID, request body, response, and
  component schemas.
* **Stream typing:** Schemars derives a JSON Schema for `StreamRecord`; Specta
  registers `StreamRecord`, `FollowRequest`, and `FollowFrame` and emits
  TypeScript. The output correctly represents ordinary Rust fields and
  `Option<T>` as nullable TypeScript values.
* **Filesystem behavior:** Typeshare parses a Rust fixture containing
  `FilesystemRequest` and `FilesystemResult` and emits TypeScript. A separate
  serialized `FilesystemBehavior` value records the operation's idempotency,
  cancellation point, consistency rule, and output identity. Keeping this
  value separate is deliberate: it is behavior metadata, not a type schema.

The observed output is saved in
[`prototype-output.json`](../../research/metadata/rust-metadata-prototypes/prototype-output.json).
Representative emitted fragments are:

```text
Utoipa: openapi = 3.1.0; path = /v1/actors; method = post; operationId = create_actor
Specta: export type FollowRequest = { cursor: number | null }
Typeshare: export interface FilesystemRequest { path: string; recursive: boolean; include_hidden?: boolean }
Behavior: { operation: "list", idempotent: true, cancellation: "safe-before-commit", ... }
```

## Metadata comparison

The prototype's `marker_report` scans each emitted artifact for protocol keys;
it does not treat Rust type names such as `StreamRecord` as evidence of
streaming semantics. Every emitter produced useful type or HTTP metadata. None
of the emitted artifacts contained the protocol facts that the SDK contract
requires:

| Required contract fact | Utoipa | Schemars | Specta | Typeshare | Evidence from run |
| --- | --- | --- | --- | --- | --- |
| Stable protobuf field number/tag | absent | absent | absent | absent | `protobuf_field_tags: false` for every output |
| Presence versus default semantics | absent | absent | absent | absent | `field_presence_rules: false` for every output |
| Maps with protobuf key/value typing | JSON object only where represented | JSON object schema only | TypeScript object only | TypeScript object only | no protobuf key/value marker is emitted |
| Oneof/union wire identity | OpenAPI discriminator must be authored separately | JSON Schema union only | TypeScript union only | language union only | no wire oneof marker is emitted |
| Custom protobuf options | absent | absent | absent | absent | `custom_options: false` for every output |
| Service/RPC identity | HTTP path and operation ID only | absent | absent | absent | only Utoipa has HTTP operation metadata |
| Client/server streaming kind | absent | absent | absent | absent | `streaming_semantics: false` for every output |
| Behavior policy | absent | absent | absent | absent | `behavior_policy: false`; behavior was serialized manually |

This is the exact boundary demonstrated by working output: these tools can
describe a Rust type or web-facing operation, but they cannot author or recover
the deployed descriptor's field tags, presence rules, options, RPC identities,
streaming direction, or filesystem behavior policy. The generated output is
therefore suitable as a projection after the Rust protocol model is complete;
it is not a source of truth for the wire contract.

## Verification

From the repository root:

```text
cargo test --manifest-path research/metadata/rust-metadata-prototypes/Cargo.toml --locked --target-dir .tmp-metadata-target
cargo run --manifest-path research/metadata/rust-metadata-prototypes/Cargo.toml --locked --target-dir .tmp-metadata-target
```

The test run passed **2 tests**. The executable run completed and produced the
JSON artifact above. During development, Specta rejected `u64` because its
TypeScript exporter forbids bigint-style types by default; the fixture now uses
`u32`, and that concrete compatibility constraint is captured by the pinned
working run rather than hidden behind an untested example.
