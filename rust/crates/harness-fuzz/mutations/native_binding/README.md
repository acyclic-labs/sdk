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

The same control covers replaceable declarations with a canonical name and an
alias, in both orders. Each alias is an edit of the original embedded family
declaration and retains its real argument template. Both names must stay
visible under the original capability grant and retain the original executor
owner; unrelated grants expose neither. A funded registry copy retains both
selections.

- `aliases-before-admission.patch` restores the production binding and selection
  code from commit `6766b845a2c0565fb143c7998849df6dedbbbc0d`. The new alias
  assertions detect the canonical declaration becoming hidden.
- `aliases-reselect-capability.patch` restores lookup during registration. The
  owner-identity assertion detects an alias capturing an earlier adapter.

Apply each patch separately, run the command above, and undo it before running
the fixed control. Neither mutation changes a decoder or adds a provider fixture.
