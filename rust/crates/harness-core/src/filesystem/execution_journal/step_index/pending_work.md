# Pending-tail storage work control

The existing recovered-tail control calls the production `Pending::push` and
`Pending::collect` paths with both original preparation owners. Every growth
crossing must copy zero bytes of prior backing. Ordinary single-entry
publication allocates no index nodes. Interleaved chains, combined selection
limits, clear and original-custody retirement remain checked.

```sh
cargo nextest run --locked -p acyclic-harness-core --features 'filesystem test-support' --lib -E 'test(=filesystem::execution_journal::step_index::pending_tests::recovered_tail_step_lookup_is_bounded_through_one_thousand_unrelated_steps)'
git apply rust/crates/harness-core/src/filesystem/execution_journal/step_index/pending_relocating.patch
cargo nextest run --locked -p acyclic-harness-core --features 'filesystem test-support' --lib -E 'test(=filesystem::execution_journal::step_index::pending_tests::recovered_tail_step_lookup_is_bounded_through_one_thousand_unrelated_steps)'
git apply -R rust/crates/harness-core/src/filesystem/execution_journal/step_index/pending_relocating.patch
```

The retained mutation restores the actual former vector and per-step head
implementation. Its second insertion copies 136 bytes of the previous entry
and must fail `pending insertion relocated prior entries`. A compiler failure
does not qualify. The mutation changes no wire decoder, output assertion or
counter implementation. Cold append recovery and allocation-cut admission
also exercise the shared journal's actual publication path.
