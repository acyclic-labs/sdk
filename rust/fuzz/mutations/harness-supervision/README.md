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

Run
that unchanged test, and require its failure at either
`same-turn child was not admitted and archived` or
`same-turn spawn escaped the stop`. Compilation failure is not a successful
negative control. Restore the source, then require the original test to pass again:

```sh
git apply -R rust/fuzz/mutations/harness-supervision/same-turn-stop.patch
```
