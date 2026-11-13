# Runtime socket input captures

Source: `506ccbe437bcaa94f93e2cb21868df8cb8a42688`, recaptured after the invocation
record moved to the `acyclic.actor.invoke` v1 attribute (1d12ee2), whose earlier generation
stores refuse as a whole.
Producer: `cargo test --locked -p acyclic-actors --lib local::sockets::tests::socket_conformance_passes_on_local_runtime -- --exact --nocapture` (Linux x86_64, 1/1 passed, 0.16 s).

The original `SocketPackage::on_record` boundary retained the Runtime-created
Delivery with `guest::wire::encode_delivery` and its socket connection/event
projection. The shared suite independently sent and checked Open, text, binary,
peer Close, actor Close and restart Close through actual LocalStream storage,
Runtime and the in-process session. Capture observation was removed afterwards;
the decoder and event producers were unchanged.

`socket-deliveries.capture` retains those original envelopes and projections.
The six matching `corpus/actor_codecs/socket-input-*` files have original hashes
and producer coordinates in `corpus/SOURCES.tsv` and shared golden frames.
The same `actor_codecs` target requires positive admission and exact event fields
before arbitrary input. `actor-negative-control.py --socket-inputs` showed that
conditional admission misses an actual production `Delivery::socket` returning
`Ok(None)`, and the unchanged strengthened oracle rejects it. Restored production
replayed all 59 Actors captures. The bounded production-configuration instrumented
target ran 64 units (seed 1); this is not complete branch coverage.

Error has no captured shipped producer. Malformed metadata, foreign members and
upgrades remain separate validation obligations. No alternate socket runtime,
decoder, protobuf schema or corpus target was introduced.
