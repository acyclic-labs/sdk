# ts-rs-macros 12.0.1

Source: the crates.io `ts-rs-macros` 12.0.1 release, archive SHA-256
`38d90eea51bc7988ef9e674bf80a85ba6804739e535e9cab48e4bb34a8b652aa`,
upstream commit `7182ad8289596097235406b715fa04506443b4ad`, directory `macros`.
The MIT license is retained in LICENSE.

The shared attribute parser recognizes Serde's `try_from` type path and `other`
unit marker. These control deserialization; the generated TypeScript continues
to describe serialization. The original Serde derives, validation, naming and
catch-all behavior remain authoritative. Unknown attributes still report the
upstream diagnostic; `no-serde-warnings` is not enabled.

The parser control compares the generated serialization against the same
declaration without ingress metadata and rejects malformed metadata. Actors'
existing metadata properties, additive-event properties and generated authoring
contract check cover the actual public declarations.
