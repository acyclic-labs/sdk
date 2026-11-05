# Eager persistence submission refusal

Run `python3 rust/proofs/qualify.py persistence-submission` from the repository
root. The retained production mutation disables only the eager same-service
guard in `spawn_persistence`. The independent observer guard, real native worker
queue, physical custody and completion publication remain original.

The existing
`persistence_reports_the_original_job_and_refuses_same_service_dependencies`
test attempts nested submission directly and through the inline native adapter.
Both use one helper whose submitted closure owns a oneshot sender. The helper
executes within the original coordination runtime's entered handle, so a
missing-reactor panic cannot substitute for the submission refusal. It
checks the receiver exactly once immediately after submission, before polling
the observer. Only `Closed` is accepted: the rejected closure was dropped without
running. An admitted closure either retains the sender (`Empty`) or sends its
entry event (`Ok`); either result fails the named assertion. The observer's later
`WouldBlock` cannot hide admission. This replaces the detached body's panic,
which could be unobserved after the observer refused its same-service wait.

Qualification requires the intended assertion failure under the production
mutation and one passing original test after exact source restoration. The
existing accepted-observer refusal, held storage custody, original channel-loss
completion and wait-report checks stay in that same physical test. Failure
unwinds its original release senders; no new wait, readiness loop or timeout is
introduced. The independent [accepted-observer mutation](../persistence-reentry/README.md)
continues to target the later poll guard.

Production work is unchanged: no added syscalls, durability barriers, round
trips, allocations or copies. The test adds one oneshot channel and one bounded
state observation per attempted nested submission, without waiting for service
capacity. This qualifies immediate rejection and closure ownership on the native
service; complete admission, scheduler, diagnostic and operation costs, other
operating systems and deployment topologies remain separate obligations.
