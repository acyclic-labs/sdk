# Same-turn subtree stop control

Run the original real-runtime regression from the repository root:

```sh
cargo nextest run --locked -p acyclic-harness-core --test main --features test-support \
  -E 'test(=agent_model::runtime::a_same_turn_stop_archives_newly_admitted_children)'
```

The test admits a child and stops in the same turn, and includes an ordinary
spawn without a stop as its healthy control. Apply `same-turn-stop.patch`, run
that unchanged test, and require its failure at either
`same-turn child was not admitted and archived` or
`same-turn spawn escaped the stop`. Compilation failure is not a successful
negative control. Reverse the patch and require the original test to pass again.
