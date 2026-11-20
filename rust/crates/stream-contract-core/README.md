# acyclic-stream-contract-core

The domain half of the hierarchical append-only Streams contract: canonical
paths, records, heads, revisions, commit requests and receipts, provider
traits, native custody, tracing, synchronization and the shared test meters.

`acyclic-stream-contract` is the public facade. It re-exports every item of
this crate and adds the wire codecs, HTTP codecs, paging, search and journal
adapters. Depend on the facade unless you are implementing a primitive that
must not see the wire format.
