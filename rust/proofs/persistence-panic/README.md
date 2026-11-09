# Original persistence producer failure

Run `node rust/proofs/qualify.mjs native-custody` from the repository root.
The qualifier mutates the shared task join-error projection to mislabel an
original panic as cancellation. It retains the original cause and leaves native
submission, worker execution, resource retirement, terminal publication and wake
unchanged. The existing physical persistence test must reject `Interrupted`
where it requires `Other`; exact source restoration must make it pass.

`persistence_worker_panic_terminates_its_original_wait` submits through the shipped
`spawn_persistence`, makes the original accepted closure panic, awaits its real
result and checks that its original wait report is empty. The mutation therefore
challenges the typed failure on an actual worker path, without removing a wake
and relying on a timeout to detect a hang. No new test or parallel task model is
introduced. The shared projection also serves ordinary and seeded tasks; their
separate custody conformance retains the original cause and cancellation checks.

The original same-service refusal and accepted-caller-drop controls remain:
they cover different dependencies and physical custody. File callback fail-stop
is a separate contract and does not establish a recoverable task error.
This control does not prove every possible producer failure, complete scheduler
cost, another operating system or a deployment topology. Production syscalls,
durability barriers, round trips, allocations, copies and waits are unchanged.
