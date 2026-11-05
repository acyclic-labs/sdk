# Native binding initialization control

Apply `prepare-before-reservation.patch`, then run:

```sh
cargo nextest run --locked -p acyclic-harness-core --lib --features filesystem,test-support -E 'test(tools::native::tests::family_shell_binding_keeps_one_selection_and_the_original_capability)'
```

The mutation restores preparation before the once-cell reservation. Concurrent
first callers may each read and decode the same immutable native profile. The
control starts four callers together over each original embedded family, checks
one profile read and one shared binding, then binds the real shell capability
and retains its original grants and result contracts. A typed initialization
refusal must leave the cell available; a cached absent profile must stay absent.
The `native profile was read more than once` assertion detects duplicate work.

This is a concrete concurrency control, not an exhaustive scheduling model.
The production reservation and wakeup are the existing `once_cell` primitive;
no separate synchronization algorithm or provider response fixture is added.
Undo the patch before retaining a candidate.
