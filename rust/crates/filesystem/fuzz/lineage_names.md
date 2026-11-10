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
`lineage::tests::persisted_lineage_names_require_exact_canonical_bytes`.

`mutations/lineage_noncanonical_name.patch` plants acceptance of any successfully
parsed spelling. The existing decomposed-name corpus and the deterministic
`workspace::identity::name_tests::redundant_normalization_and_missing_unicode_normalization_fail_the_same_oracle`
control reject it at `stored workspace names must already have exact canonical
bytes`. The lineage control also rejects the original constructor-success-only
validation at `noncanonical lineage registration was accepted`.

```sh
node rust/proofs/qualify.mjs filesystem-lineage-canonical
cargo test --locked -p acyclic-fs --lib --features local,memory,test-support,native-watch independent_parser_matches_outputs_errors_and_every_observed_phase
cargo fuzz run --fuzz-dir rust/crates/filesystem/fuzz workspace_name --features fuzz-targets -- rust/crates/filesystem/fuzz/corpus/workspace_name
```

The lineage operation keeps main's caller accounting and original read frame
across asynchronous store ownership. These controls check canonical stored
names; the native ownership schedule controls remain separate obligations.
