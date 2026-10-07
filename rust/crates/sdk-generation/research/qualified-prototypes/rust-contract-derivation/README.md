# Rust contract derivation prototype

Primary research copy provenance: this prototype was developed in `Q:\\sdk\\work\\sdkgen-main-port-final` and copied into the primary SDK generation research tree; keep the Q prototype as the source provenance for generator experiments.

This isolated crate tests an opt-in fallible proxy conversion for Protify 0.1.4.

The public Rust model is the only authored semantic contract. `proto_message(proxied)` derives the protobuf shadow and rendered schema from that model; explicit tags and field wire types stay beside the executable fields. `ts-rs` exports branded aliases for `ActorId`, `PositiveU64`, and `CodeSha256`, so the generated TypeScript keeps string, bigint, and Uint8Array identities.

`proto_message(proxied, fallible = IngressError)` selects the patched conversion generator. It emits `TryFrom<WireShadow>` for ingress and keeps `From<Domain> for WireShadow` for egress. The fallible path does not implement Protify's infallible proxy traits, so malformed wire values cannot reach a panic based on a semantic newtype conversion. Reusable parser functions provide typed errors for empty IDs, zero limits, short hashes, and an invalid nested message.

The patch is source-only under `vendor/` and is also captured in `protify-fallible-proxy.patch`: it adds opt-in `fallible = ErrorType` attributes to messages and oneofs, threads them through generation, and emits generic fallible field conversion using `TryInto` plus field-level conversion hooks. Optional oneofs and nested messages preserve absent versus present wire values. The vendor copy removes Protify's unavailable Diesel dev dependency only to make this isolated offline prototype buildable; production sources are untouched.

Run `cargo test --offline`. The seventeen tests cover schema rendering, Prost binary roundtrip, generated descriptor consistency, ts-rs exports, branded aliases, optional oneof and nested presence, and typed rejection of invalid top-level and nested fields.
