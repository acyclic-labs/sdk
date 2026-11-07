Both invocation headers and failure codes require positive production admission
on every fuzz input. An arbitrary malformed packet must not let an implementation
which rejects every packet satisfy a conditional round-trip assertion.

`invocation_headers` reuses the original public-constructor/prost writer captures
in `rust/codec-fuzz/corpus/protobuf/acyclic.actors.v1.InvokeActorRequest-semantic`
and `acyclic.actors.v1.InvokeWorkerRequest-semantic`. Their original hashes and
producer are recorded in `rust/codec-fuzz/corpus/SOURCES.tsv`; the matching seeds
in `fuzz/corpus/invocation_headers` remove only the eight-byte corpus selector.
Their payload SHA-256 hashes are respectively
`efc9b79b8f7fa8d19f2dee384d9484501b8dc6f58529b83f92b9dca152a8a845`
and `39bbe221f82ba975295c27dd90b9460c602683d170c8e2921947ccc84ec6c304`.
The same assertions check admission, body refusal, and metadata preservation for
the retained writer output and arbitrary inputs.

`stream_error` selects a public `StreamError::ALL` value independently of
`from_code`, serializes it through the production serializer, and requires its
original value back. Raw-input differential checks remain. Capture the failure
corpus with the same serializer:

```sh
cargo run --locked -p acyclic-guest-fuzz --example stream_error_corpus -- fuzz/corpus/stream_error
```

The shared qualifier first applies each retained production mutation, requires
the intended assertion to fail inside libFuzzer, restores the exact production
source, and runs 1,024 instrumented inputs with the retained corpus:

```sh
node rust/proofs/qualify.mjs invocation-header-admission
node rust/proofs/qualify.mjs stream-error-admission
```

The invocation mutation rejects every deployment envelope in the shared
production admission; the failure mutation removes all canonical code matches.
Neither mutation changes the oracle. These are codec checks, with no external
producer or scheduling assumption. Work is bounded by input bytes and the two
fixed invocation captures or one fixed failure value, with no I/O per input.
They do not establish transport, storage, or executor progress.
