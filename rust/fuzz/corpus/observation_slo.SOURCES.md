The eight `observation_slo/` seeds are byte-for-byte captures of the public
`acyclic_observability::Slo` JSON writer from source
`ef1f294bd4f064d47d0a37c5c4ac5650fb07490a`, produced by
`persisted_slo_corpus_and_mutations` on Linux x86_64. The corpus generator remains
in `support/observation_slo.rs`; no JSON fixture was written by hand.

`zero`, `threshold`, `maximum` and `rpc` are admitted v1 files. `version`,
`fraction`, `empty_rpc` and `nul_operation` serialize explicit semantic corruptions through
the same writer. These are this library's persisted format, not external service
responses. The replay checks all truncations and single-bit mutations through the
same oracle as the fuzzer.

Run the replay with:

```sh
cargo test --locked -p acyclic-sdk-fuzz --no-default-features --features observation --test observation_slo
```

From `rust/fuzz`, run the instrumented target with:

```sh
cargo +nightly fuzz run --fuzz-dir . --no-default-features --features observation observation_slo
```

Set `OBSERVATION_SLO_CORPUS=corpus/observation_slo` for the replay to capture the
writer output again. Relative output paths are relative to the Cargo package.
The original replay passed; changing production evaluation to admit an empty
cohort made the unchanged oracle fail on `Some(true)` versus `None`. Parser
mutation and instrumented coverage are separate obligations.

The original capture named `nul` is retained as `nul_operation` because `NUL` is
a reserved Windows device name. Its captured bytes are unchanged.
