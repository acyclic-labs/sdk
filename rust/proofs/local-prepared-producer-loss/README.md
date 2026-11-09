# Original prepared-commit producer retirement

Run `node rust/proofs/qualify.mjs local-prepared-producer-loss`.

The oracle uses public LocalStream open, fork, native preparation and completion
with default storage configuration and a real journal. Two immutable forks carry
the target's inherited record into a grandchild. The actual conditional request
targets that grandchild. Its original current-thread executor returns preparation
without polling the newly spawned worker. The public completion registers its
consumer, then destruction of that executor must wake the original completion.
A manual poll on a live executor must return its original cancelled-task I/O
cause immediately. No timeout, readiness loop, replacement worker or private
receiver drives the result. The target stays unchanged, no write is charged,
funding retires, and reopening the same journal preserves the original tail.

The retained production mutation discards only the original worker's task error
and leaves that same public observation pending. The immediate result assertion
must reject it. The qualifier restores the real production source before running
the same oracle again. This covers executor loss before worker execution, not
arbitrarily noncompleting live storage or executor fairness.

Production operation cost is unchanged: zero added allocations, copies, syscalls,
durability barriers, round trips, tasks, events, locks or scans.
