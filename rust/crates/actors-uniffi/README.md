# Actors UniFFI facade

This crate is the maintained foreign-language boundary over
`acyclic-actors`. It owns only UniFFI error conversion, cancellation handles,
the opaque connected client, and the eight async operation exports. Requests,
responses, enums, validation, wire conversion, transport, and cancellation
semantics remain in `acyclic-actors::domain` and `acyclic_actors::client`.

The Actors crate is expected to expose the existing domain declarations with
an opt-in `uniffi` feature. The feature-gated derives belong on those existing
types in the peer-owned domain crate; this facade deliberately does not mirror
the 19 semantic records/enums or parse handwritten generator metadata.

The root workspace owner may register this crate once that feature contract is
accepted. Until then the local `[workspace]` stanza keeps the crate buildable
as a standalone source checkout. The `bindgen` feature exposes only the
pinned UniFFI 0.31.0 CLI wrapper for local qualification.
