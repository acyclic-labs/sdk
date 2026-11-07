The `reader_snapshot` target calls the public `RecordCache::restore` boundary.
One oracle is shared with its property test. Rejection must preserve the
original cache, charged backing and borrowed payload; accepted snapshots must
stay within the admitted budget and produce a stable, restorable encoding.
Both entry points require the actual writer corpus to be admitted with its
original records, inherited rows, commit anchors and stale restored listings.
The property also checks every truncation of that corpus. Reject-all and
destructive-refusal defects must fail the same oracle.

Capture corpus bytes from the actual public writer, then run the property and
an instrumented campaign:

```sh
cargo run --locked -p acyclic-harness-fuzz --no-default-features --features reader --example reader_snapshot_corpus
cargo test --locked -p acyclic-harness-fuzz --no-default-features --features reader --lib reader_snapshot::snapshot_contract_and_destructive_refusal_control -- --exact
cargo +nightly fuzz run --no-default-features --features reader --sanitizer none --fuzz-dir rust/crates/harness-fuzz reader_snapshot -- -runs=1000 -seed=1 -max_len=4096 -print_final_stats=1
```

`corpus/reader_snapshot-SOURCES.tsv` records each original writer output and
its digest. These are SDK-generated persisted bytes, not a captured external
response. This target uses the public local-access cache; it does not qualify
credential renewal, expiry, trust loss, remote topology or crash durability.
A finite campaign's coverage counters describe that campaign, not complete
branch coverage of the Reader or its parser.
