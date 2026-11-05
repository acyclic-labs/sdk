# Canonical names in lineage records

Lineage registration and recovery require the stored child and parent names
to equal their canonical spelling. Validation runs the same workspace-name
parser as construction, with a comparison in place of retaining a new name.
ASCII validation allocates and copies nothing. Unicode validation keeps the
existing normalization scratch and omits the final String and Arc allocations.

The existing `workspace_name` fuzz target compares this predicate with the
independent parser's actual canonical output. Its `decomposed` and `hangul`
corpus entries exercise normalization; the generated parser property calls the
same fuzz entry. Registration and recovery use the real memory adapter in
`lineage::tests::malformed_lineage_shape_never_enters_the_graph`.

`mutations/lineage_noncanonical_name.patch` plants acceptance of any successfully
parsed spelling. The existing decomposed-name corpus and the deterministic
`workspace::identity::name_tests::redundant_normalization_and_missing_unicode_normalization_fail_the_same_oracle`
control reject it at `stored workspace names must already have exact canonical
bytes`. The lineage control also rejects the original constructor-success-only
validation at `noncanonical lineage registration was accepted`.

From `rust`:

```sh
cargo test -p acyclic-fs --lib malformed_lineage_shape_never_enters_the_graph
cargo test -p acyclic-fs --lib independent_parser_matches_outputs_errors_and_every_observed_phase
(cd crates/filesystem && cargo fuzz run --features fuzz-targets workspace_name)
```

This change does not establish caller-work propagation through lineage stores;
that contract also requires preserving the original read frame across the local
store's asynchronous physical ownership boundary.
