# acyclic-stream-contract-engine

The engine half of the hierarchical append-only Streams contract: provider
implementations, native custody, tracing, synchronization, runtime adapters and
the shared test fixtures.

`acyclic-stream-contract` is the public facade. It re-exports every item of
this crate and of `acyclic-stream-contract-core`, and adds the wire codecs,
HTTP codecs, paging, search and journal adapters. Depend on the facade unless
you are implementing a primitive that must not see the wire format.
