# Ordered child selection

The `agent_wait_selection` target and the retained property call the actual wait
resolver with borrowed admission arguments and owned dispatch arguments. They
check the same selected children, original first references, complete parent
containment, quorum refusals and timeout. The identity counter observes actual
resolver calls; it does not model them or bound hash-table collision probes.

Run the retained property with:

```sh
cargo test --locked -p acyclic-harness-core --features filesystem,test-support --lib tools::agents::selection_checks::tests::recorded_waits_and_derived_selections_preserve_identity_order_and_work -- --exact
```

Apply each patch separately, run that command, then reverse the patch:

| Mutation | Required diagnostic |
| --- | --- |
| `keep-duplicates.patch` | `wait selected a child twice` |
| `scan-known-identities.patch` | `wait selection repeatedly decoded a known child identity` |
| `admit-foreign-parent.patch` | `wait selected a foreign child` |
| `admit-zero-quorum.patch` | `wait admitted an impossible quorum` |

The scan mutation restores growing-prefix comparisons of actual decoded child
identities. The production path decodes one fixed 16-byte identity per supplied
reference and inserts it once. Hash-table operations are expected amortized O(1),
not worst-case O(1). Whole selection is expected O(N), plus parsing the supplied
path bytes. Admission and dispatch still resolve independently. Selection is
synchronous, terminates on finite input and introduces no waits or I/O.

The fuzz target consumes the same oracle:

```sh
cargo +nightly fuzz run --dev --fuzz-dir rust/crates/harness-fuzz --no-default-features --features core agent_wait_selection -- -runs=256 -seed=1 -max_len=4096 -print_final_stats=1
```

With `admit-zero-quorum.patch` applied, pass the retained
`corpus/agent_wait_selection/derived-zero.json` as the input file. The fuzzer
must find the same impossible-quorum failure. Reverse the patch before retaining
the source. Finite campaigns report reached coverage, not complete branch coverage.

## Corpus provenance

`recorded-path.json` and `recorded-name.json` project the original wait
`tool_input` and the parent of the original spawn `tool_response.agent` from
`scripts/graphcoder-e2e/fixtures/live-child-results.json`, samples 2 and 3.
That captured fixture's SHA-256 is
`e9cfcd2ea6c3ceb79e21d25e6c7757a78a342b4063296d2cd81aee65037352bf`.
Its recorded compiled source is `065154f442c1f195813dc8b315e957ace5f32aa6`,
consumer revision `c90f7519cc8a904a13af9559a9e4e584d3985364` and matrix SHA-256
`24cf6224c4086c6c0b9fcc5f368378ed080e22beafc8ba9e5810280b22cca444`.
The two-element outer array is only the fuzz container; no captured field value
was rewritten.

Every `derived-*` seed is an explicit mutation of those captures: empty
selection, duplicate name/path order, foreign parent with the same child ID,
zero or excessive quorum, invalid name, unknown field, or 32 distinct names
formed by appending numeric suffixes to the captured spawn name. The retained
property also exhausts alias orderings through length six and checks quorum
values below, within and above the distinct selection. These are derived inputs,
not additional external captures.
