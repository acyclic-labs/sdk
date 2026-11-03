# Decoded-cache histories

`decoded_cache` calls the production `AsyncObjectStore::decoded_cache_lookup`
and consumes its original `DecodedCacheMiss::admit` token. Values come from
`encode_file_metadata` and `decode_file_metadata`, with authenticated object IDs.
An independent bounded LRU over input identities checks values, residency and
whether each original fill becomes shared or remains uncached.

Input bytes select entry capacity (one through four), byte capacity, then pairs
of operation and identity. Operations modulo five are lookup-and-fill, lookup,
clear, begin a delayed fill, and finish that fill. Identities modulo six include
different decoder limits for the same metadata object. Replacing a pending fill
drops the original token. A final incomplete pair is ignored.

The corpus contains operation schedules, not invented wire replies. `identity`
and `eviction` retain the previous property test's schedules. `stale-fill` and
`overlapping-fill` exercise the shipped miss-token protocol across clear and
another fill; `oversized` forces uncached delivery. Each replay produces metadata
bytes with the real writer. No recorded external-tool input is used.

One shared oracle runs under proptest and libFuzzer. The retained corpus must
reach all eight semantic branches: hit, miss, delayed fill, clear, stale fill,
oversized fill, duplicate fill and eviction. Negative controls inject identity
aliasing, omitted eviction and completion-time epoch replacement through the
real cache implementation; each must fail the unchanged oracle.

Run the deterministic property, corpus coverage and negative controls with
`cargo test -p acyclic-fs --lib cache::decoded_properties`. Run the fuzzer from
this directory with
`cargo fuzz run --features fuzz-targets decoded_cache corpus/decoded_cache`.
Semantic corpus coverage does not establish compiler branch coverage or native,
distributed, restart, decoder-grammar or physical final-alias coverage. Those
remain separate checks.
