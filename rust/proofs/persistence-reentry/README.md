# Accepted persistence dependency refusal

Run `python3 rust/proofs/qualify.py persistence-reentry` from the repository
root. The retained production mutation removes only `PersistenceTask::poll`'s
same-service guard. Eager submission refusal, physical job ownership, completion
publication and worker panic projection remain original.

The existing
`persistence_reports_the_original_job_and_refuses_same_service_dependencies`
test calls the shipped `spawn_persistence` entry point. One accepted worker
holds its storage mutex while receiving from its original release channel.
Another real worker checks direct and inline-adapter submission refusal, then
polls the already accepted observer exactly once. The guard must return typed
`WouldBlock` without cancelling the first worker or releasing its storage.
Removing it returns `Pending`; the unchanged assertion on `accepted.as_mut()`
must fail. Exact source restoration must make the same test pass.

Both phases retain the original producer of every wait. If the assertion
panics, the second worker publishes its original failure; the outer scope then
unwinds and drops the first worker's release sender. That channel loss ends its
receive. No timeout, readiness recheck, parallel service or new Rust test is
introduced. The original positive control also observes storage custody after
refusal and its release after the original sender disappears.

This qualifies the accepted-observer dependency guard on the real native
service. Its one thread-local observation and typed refusal add no dependency
wait. Production syscalls, durability barriers, round trips, allocations and
copies are unchanged. Complete admission, scheduler and diagnostic costs,
other operating systems and deployment topologies remain separate obligations.
