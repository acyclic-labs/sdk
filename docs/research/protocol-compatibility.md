# Rust-owned wire contract bootstrap

This note records the first compatibility gate for moving SDK and documentation
generation to Rust-owned metadata. The prototype is
`rust/crates/sdk-contract-wire`. It models the Actors v1 file with typed Rust
definitions for messages, fields, oneofs, enums, services, methods, explicit
protobuf tags, protobuf scalar and reference types, JSON names, stream
directions, and the `go_package` file option.

## What the prototype proves

The crate does not use generated Rust wire modules or protobuf text as its
authority. `actors_descriptor()` and `actors_proto()` emit a descriptor set and
protobuf source directly from the structured Rust model. The old
`proto/actors/v1/actors.proto` is retained only as a temporary compatibility
fixture for an independent descriptor compile. The tests compare the model's
bytes and semantics with a committed source-info-free golden descriptor,
including:

- file name, package, syntax, dependencies, and `go_package`;
- all 24 message names and field counts;
- every field name, number, label, JSON name, scalar/reference type, and type name;
- explicit `SubscriptionStart.start` and synthetic proto3-optional oneofs;
- all enum values and numbers; and
- the `ActorsService` method identities, request/response types, and streaming flags.

Run the gate from the SDK worktree with:

```text
cargo test --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --locked
cargo run --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --bin sdk-contract-wire -- generate --out target/sdk-contract
cargo run --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --bin sdk-contract-wire -- check --out target/sdk-contract
```

The command is intentionally independent of the root Cargo workspace. It is a
real descriptor emission and drift check, so a field tag, presence bit, JSON
mapping, message reference, enum number, RPC identity, or stream direction
changed in the Rust model causes a test failure. The generator emits
`actors/v1/actors.proto` and `actors/v1/actors.fds.bin` under the requested
output directory; the `check` command fails on missing or stale files.

SDK and OpenAPI consumers should call `acyclic_sdk_contract_wire::actors_descriptor()`
for the source-info-free `FileDescriptorSet` bytes and
`acyclic_sdk_contract_wire::actors_proto()` for a generated protobuf source
view. They should consume those outputs as inputs and must not import the
existing generated Rust or TypeScript wire modules. The descriptor filename is
`actors/v1/actors.fds.bin`; the source filename is `actors/v1/actors.proto`.

Stream v2 is exposed through the same model API under
`acyclic_sdk_contract_wire::stream`: `stream::STREAM` is the typed
`ContractSpec`, while `stream::stream_descriptor()` and
`stream::stream_proto()` emit `stream/v2/stream.fds.bin` and
`stream/v2/stream.proto`. The Stream model preserves all 39 messages, the
`StreamLimit` values, 10 RPC identities, server-streaming flags, proto3
optional synthetic oneofs, and reserved numbers/names (`trim`, `delete`,
`trim_point`, and `retired`). Stream has no custom validation options or
imports; those remain an explicit future model extension rather than being
silently dropped.

The model also exports `ContractSpec::routes`, a transport projection linked to
the exact RPC identity and request/response messages. `ACTORS_ROUTES` preserves
the existing `/v1/actors/...` POST paths, and `stream::STREAM_ROUTES` preserves
the Stream adapter's `/v1/stream/...` suffixes, including
`idempotency/inspect`, `children/page`, and `commits/read`. Every route carries
an operation identifier and Rust-owned operation prose; OpenAPI and website
generators should consume this table instead of maintaining a second route map.
The route projection does not alter the protobuf descriptor or runtime
handshake identity.

The committed source-info-free Actors golden descriptor is independently
reproduced by `protoc` with SHA-256
`0515dc7e3f38a5648f85ee52f5bda7e639cc31cf83179a208bb5abbd398a04bf`; the
Rust emitter matches it byte for byte. The archived/generated descriptor still
has SHA-256
`70720491f34232b4b7e424a17f8383ad5a69b1018460e8fff7a62600fb6ec16c` because it
contains compiler-specific source information. Both identities must remain
distinct from runtime handshake digests.

For Stream v2, the Rust model and the generated `.proto` were independently
compiled with the pinned vendored `protoc`; their descriptor bytes are identical
at SHA-256
`1d311dd12a56de4f04923e4144071c507c6b59b1789955fd8629d09c990abd0c` (5,878
bytes). This is a canonical schema-descriptor hash. It must remain separate
from any deployed descriptor or handshake digest, which may include source
information or historical framing.

## Bootstrap and authority boundary

During migration, the checked-in `.proto` is an immutable baseline for
comparison. The Rust model is the candidate authority and must be reviewed
against that baseline before any generated wire files or SDK outputs are
replaced. After the migration gate passes, retain the old descriptor and
conformance vectors as an archived compatibility fixture and stop reading
generated wire modules from generation code. The intended dependency direction
is:

```text
typed Rust contract model
  -> descriptor / protobuf / OpenAPI / SDK metadata / docs inputs
  -> language generators and website presentation
```

The reverse direction is prohibited: generated Rust, TypeScript, or a generated
descriptor cannot be imported by the model or used to discover the contract.
This avoids a bootstrap cycle where regeneration silently accepts its own stale
output. The `.proto` fixture also cannot be copied into a new source tree as a
second authored contract; it is only a byte- and descriptor-level regression
oracle until the archive is frozen.

## Options and future families

Actors v1 currently has no custom field or method options and no imports. That
makes it a useful first family, but it does not cover the repository's complete
metadata surface. Before migrating families with validation annotations, the
model must represent extension identity, declaring file, wire number, scalar
kind, repeatability, and option value exactly. In particular, preserve the
`proto/validation/v1/options.proto` extensions (51001 through 51012) rather than
flattening them into ordinary documentation attributes. The same rule applies
to proto2 presence, reserved names and ranges, map entries, extension ranges,
custom JSON names, and source-level deprecation metadata.

Streaming methods and archived protocols are equally compatibility-sensitive:
the model must retain client/server streaming direction and the complete method
name (`/acyclic.actors.v1.ActorsService/CreateActor`, for example). Archived
protocols such as Objects v1 remain immutable and get a fixture-only model or
descriptor diff; they are not silently regenerated from a newer family.

## Verification gates before broad migration

1. Add descriptor semantic and, where required, deterministic-byte comparisons
   for each active family. Keep the current SHA-256 descriptor digest and the
   runtime handshake digest in separate fields; they are different identities.
2. Add golden serialization and handshake tests for Actors, then repeat for
   streaming, Filesystem, and Harness families. A descriptor match alone does
   not prove recovery, cancellation, or envelope compatibility.
3. Generate one remote client and one embedded binding from the model, compile
   both, and compare their descriptors and package metadata to the archived
   outputs before removing TypeScript contract inputs.
4. Make drift checking fail on modified generated files and record the source
   revision, generator version, and artifact hash in every bundle.
5. Only after all consumers read the Rust model should the old `.proto` files
   move to an explicitly immutable compatibility location. The website and SDK
   documentation then consume the same model and examples rather than another
   hand-maintained contract.

The prototype intentionally stops at Actors v1. Expanding this API to Workers,
Objects, Stream, Filesystem, Harness, Machines, or Inference should happen only
after the model shape is reviewed for custom options, streaming, archived
schemas, and handshake requirements in that family.
