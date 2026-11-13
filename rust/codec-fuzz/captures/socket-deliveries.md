# Runtime socket input captures

Production format baseline: `1d12ee2d6b94da996278524c2a55d27767f372ed`.
Producer: `node capture-actors.mjs`, which runs
`cargo test --locked -p acyclic-actors --lib local::sockets::tests::socket_conformance_passes_on_local_runtime -- --exact --nocapture`.

The original `SocketPackage::on_record` boundary retained the Runtime-created
Delivery with the current `guest::wire::encode_delivery` and its socket connection/event
projection. The shared suite independently sent and checked Open, text, binary,
peer Close, actor Close and restart Close through actual LocalStream storage,
Runtime and the in-process session. The workflow restores the exact source bytes
after observing that boundary, including on failure; it adds no decoder or event producer.

`socket-deliveries.capture` retains those original envelopes and projections.
The six current `corpus/actor_codecs/socket-input-*-cuts-v1-*` files retain the full
SHA-256 of each original packet in their names. Clock and commit fields are not
normalized. Their original hashes and producer coordinates are in
`corpus/SOURCES.tsv` and shared golden frames; older socket captures remain unchanged.
The same `actor_codecs` target requires positive admission and exact event fields
before arbitrary input. The original negative control showed that conditional
admission misses a production `Delivery::socket` returning `Ok(None)`; the same
positive-writer requirement remains. Retired invocation markers now produce a
typed unsupported-format refusal rather than becoming ordinary input. The
historical `invocation`, `invocation-v2` and `invocation-committed-v2` packets
exercise that refusal before either valid or malformed payload decoding.

Error has no captured shipped producer. Malformed metadata, foreign members and
upgrades remain separate validation obligations. No alternate socket runtime,
decoder, protobuf schema or corpus target was introduced.
