# Canonical control interruption

`committed_controls_interrupt_a_held_turn_without_interrupting_ordinary_input`
uses the actual agent writer, actor subscription follow and cancellation token.
It holds a turn, commits ordinary input without cancelling it, then commits
cancel and archive in separate executions. One delivery poll must signal the
original token before the handler can finish. No deadline is needed to detect a
missed control. The durable phase and archive closure are checked afterward.

Run the property with:

```sh
cargo test -p acyclic-harness-core --test main agent_model::runtime::committed_controls_interrupt_a_held_turn_without_interrupting_ordinary_input -- --exact
```

Apply `stale-prefix.patch` to restore the obsolete production filters. The same
property must fail at `committed control missed the held turn`; reverse the
patch and it must pass. This is the safety and progress negative control.

Both discriminants are derived once when constructing the handler from the
canonical versioned writer. Removing the exact encoded control payload leaves
only the record tag, independent of sender and intent. There are two bounded
prefix comparisons per observed inbox record: no new reads, round trips,
threads, durability barriers or per-record allocations. Filter initialization
uses three bounded encodings; delivery costs are unchanged. The retained
negative also detects dropping this initialization in favor of stale filters.
