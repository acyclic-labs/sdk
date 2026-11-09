# Signing reference custody

`native_signing_reference` calls the shipped versioned reference decoder and
read-only custody-binding check. It checks round trips, unknown fields/versions
and refusal to admit one reference to different private stores. The corpus
contains explicitly synthetic internal codec shapes; it contains no credentials
or output attributed to external tools. The process-level `signing_custody`
conformance also checks an actual generated reference through this same oracle,
reopens its private store and requires typed refusal of a different file store
and the native backend without creating the selected alternative directory.

Run the corpus with an instrumented libFuzzer build:

```sh
cargo fuzz run native_signing_reference --no-default-features --features owner-record -- -runs=0
```

`negative-controls/signing-reference-unbound-store.patch` removes the production
binding comparison. The unchanged oracle must report
`one signing reference admitted different custody stores`. Coverage counters
and the original failing input belong to the qualification receipt.

Selection hashes only the chosen directory's lexical components; it performs
no filesystem probe. References are versioned from v1. Old unbound shapes and
unknown versions are refused; no migration or fallback selects a different store.
