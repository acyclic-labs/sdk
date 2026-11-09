# Shared-completion error identity

Run `node rust/proofs/qualify.mjs native-custody` from the repository
root. The shared qualifier applies the retained production mutation, requires
the exact semantic failure, restores the original bytes and reruns the same test.

The existing `original_tasks_conform_to_scoped_and_shared_completion_custody`
test calls the native owned-task API and `TaskCompletion` on the original
coordination runtime. It cancels an already-polled waiter and drops its observer.
Dropping the original release producer then completes the retained task with
`BrokenPipe`; both remaining observers must retain pointer-identical errors.
The mutation reconstructs their shared error independently while preserving its
kind and completion, so the original identity assertion must reject it.

This adds no test entry point, production operation, syscall, durability barrier,
round trip, allocation, copy or wait. The only test change names the existing
identity assertion's failure. No decoder or persisted format is changed.
The qualification covers this native completion path and its cause identity;
it does not establish complete scheduler progress, population-independent cost,
other operating systems or deployment topology.
