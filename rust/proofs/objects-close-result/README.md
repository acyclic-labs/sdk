# Original close result and physical retirement

Run `node rust/proofs/qualify.mjs native-custody` from the repository root.
The qualifier changes only the production `LocalMedium::close` result handling:
it still submits and awaits the original worker, propagates worker unavailability
and retires the actual lock and ownership anchor, but discards the inner storage
result. The existing oracle must reject this false success. The qualifier then
restores the exact original source and requires the same oracle to pass.

`owned_close_reports_physical_completion_and_failure` uses actual local object
logs and native persistence services. It challenges both original durability
barriers with before-effect and after-effect failures, requires each exact
storage cause, reopens the original directory and checks retained bytes. Its
cancelled-waiter schedule separately holds the actual submitted native close:
the root remains owned until physical completion, and the original ownership
anchor retires before reopening. No extra test, simulated close implementation
or timer is introduced.

The mutation isolates result propagation from worker completion. It does not
establish every backend's progress or a complete operation-cost bound. The
existing barrier trace checks one data barrier and one directory barrier on
successful close, with no Drop retry after a failed barrier. Production work,
including syscalls, barriers, round trips, allocations and copies, is unchanged.
