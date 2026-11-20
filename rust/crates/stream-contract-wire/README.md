# acyclic-stream-contract-wire

Canonical Stream v1 wire codecs and retained-fact persistence.

The domain types, provider traits and request admission live in
`acyclic-stream-contract-core`. This crate holds the one spelling of every
domain/wire conversion and the bounded canonical protobuf codecs for retained
facts, so a change to a codec recompiles only the codec and its dependents.
