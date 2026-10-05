# Python and Go Machines remote qualification

These two consumer entrypoints invoke all 19 generated Machines RPC scenarios
against the Rust-owned mTLS fixture. The unary receipts record decoded protobuf
summaries and assertions for identities, enums, counters, and echoed request
fields. The WatchOperation receipt consumes both server-stream items and checks
the Rust-defined pending then succeeded sequence.

The input and expected-value inventory is
`rust-input-inventory.json`. Its values are derived from
`rust/crates/sdk-examples/src/tls_fixture.rs`; a consumer run must use the same
fixture source revision and package archive metadata. A run is a qualification
only when every semantic check passes. Transport invocation alone does not
qualify a method.

The corresponding Inference consumers use the same receipt shape and consume
both RunEvent items from the Rust fixture's Watch stream.
