# Persistence wait retirement

Run `python3 rust/proofs/qualify.py persistence-wait-retirement` from the
repository root. The retained production mutation forgets the original
`spawn_persistence` diagnostic wait instead of retiring it when its completion
task ends. Submission, physical work, panic projection, terminal publication
and wake remain unchanged.

The existing `persistence_worker_panic_terminates_its_original_wait` test submits
through the shipped entry point, awaits the original accepted worker's panic,
requires its typed `Other` error and checks its original `WaitReport`. The
mutation must fail with `a failed producer retained its wait`; exact source
restoration must make the same test pass. No new Rust test or parallel task
model is introduced.

This qualifies diagnostic ownership retirement after one real failed operation.
It does not prove the operation cannot hang: its original completion producer
still runs in both phases. The diagnostic report observes waits and never
decides readiness. The separate panic-kind negative, tracing-context and
retained-descendant controls keep their distinct assertions. Complete scheduler
cost, other operating systems and deployment topologies remain open. Production
syscalls, durability barriers, round trips, allocations, copies and waits are
unchanged.
