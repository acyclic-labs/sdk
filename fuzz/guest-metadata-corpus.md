The five `corpus/guest_json/caller*.json` seeds are captured from the public `AdmittedCaller`
constructor and serializer by `fuzz/examples/guest_metadata_corpus.rs`:

```sh
cargo run --manifest-path fuzz/Cargo.toml --example guest_metadata_corpus -- fuzz/corpus/guest_json
node fuzz/guest-negative-control.mjs
```

The capture retains named and positional admission, an unknown field, and three
individual refusals: empty account, noncanonical expiry, and zero capability
version. It checks each expected admission before writing the serialized bytes.
