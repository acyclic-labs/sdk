# Same-turn subtree stop control

Run the original real-runtime regression from the repository root:

```sh
cargo nextest run --locked -p acyclic-harness-core --test main --features test-support \
  -E 'test(=agent_model::runtime::a_same_turn_stop_archives_newly_admitted_children)'
```

The test admits a child and stops in the same turn, and includes an ordinary
spawn without a stop as its healthy control. From the repository root, apply:

```sh
git apply rust/fuzz/mutations/harness-supervision/same-turn-stop.patch
```

The production mutant restores late spawn admission after the archive cascade
has frozen its child set. Run that unchanged test, and require its failure at either
`same-turn child was not admitted and archived` or
`same-turn spawn escaped the stop`. Compilation failure is not a successful
negative control. Restore the source, then require the original test to pass again:

```sh
git apply -R rust/fuzz/mutations/harness-supervision/same-turn-stop.patch
```

The original journal regression also covers a dormant grandchild: an archive of
its root, parent or itself refuses new model/tool dispatches before any cascade
delivery. An archived sibling leaves dispatch open, and previously accepted
dispatches still replay. Run it unchanged before and after this production mutant:

```sh
cargo nextest run --locked -p acyclic-harness-core --lib --features test-support \
  -E 'test(=filesystem::execution_journal::inline_tests::committed_archive_fences_dispatch_before_its_actor_consumes_the_control)'
git apply rust/fuzz/mutations/harness-supervision/ancestor-stop.patch
```

The mutant retains only the current agent's fence. Require the unchanged test to
fail at `ancestor archive admitted a fresh model dispatch` or
`ancestor archive admitted a fresh tool dispatch`, rather than at compilation.
Restore the source and require the same test to pass:

```sh
git apply -R rust/fuzz/mutations/harness-supervision/ancestor-stop.patch
```
