# Filesystem and Harness Rust contract model

`sdk-contract-wire` now exposes `filesystem` and `harness` modules alongside
the existing Actors, Stream, Objects, and Workers families. Their public
descriptor functions preserve the full Filesystem v2 and Harness v2 file
surfaces: qualified file and package identities, imported protocol handshake
messages, field tags and JSON names, proto3 optional synthetic oneofs, real
oneofs, nested map entries, enum numbers, reserved identities, and unary,
client-streaming, and server-streaming RPC directions.

The checked-in descriptors under `rust/crates/sdk-contract-wire/tests/fixtures`
are immutable one-time migration oracles. Normal exporters consume the Rust
module APIs and do not parse active `.proto` files. The canonical schema
descriptor digest is kept separate from the archived runtime handshake digest;
the latter remains pinned so compatibility checks do not accidentally adopt a
new handshake identity during model migration.

Validation metadata is represented through the shared
`sdk-contract-options` identity table. The model retains all twelve proto2
extension declarations, including their extendee, field number, scalar kind,
and target (`FieldOptions`, `EnumValueOptions`, `OneofOptions`, or
`MethodOptions`). Raw typed option bytes therefore remain an explicit model
input instead of being inferred from `prost-types`, which drops unknown
extensions during an options round trip.

The model tests cover the descriptor inventory, presence and map-entry
markers, reserved metadata, streaming methods, option identities 51001 through
51012, and the archived handshake identity. A generated language adapter may
consume `filesystem_descriptor()` or `harness_descriptor()` and the rendered
source view, but it must preserve the descriptor and handshake roles as
separate compatibility claims.
