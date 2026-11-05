# Persistence custody after caller drop

Run `python3 rust/proofs/qualify.py persistence-caller-custody` from the
repository root. The retained production mutation makes `PersistenceTask::drop`
abort its original task handle. Submission, worker completion and both
same-service refusal guards remain original.

The existing `persistence_retains_accepted_work_after_caller_drop` test calls
the shipped `spawn_persistence` entry point on the original coordination
runtime. It drops the returned observer without polling it, then awaits the
submitted body's original start event. The mutation cancels that body's owner
before entry: dropping its sender ends the await with `RecvError`, and the
existing test fails with the named abandonment diagnostic. Qualification
requires this exact failure and a passing test after exact source restoration.

With original custody restored, the body enters the actual persistence service
after caller drop and holds its original release receiver. One bounded
observation verifies it has not finished; dropping the release sender then ends
the body with the original typed `Interrupted` result. Every wait retains its
own producer; producer loss terminates it instead of requiring a timeout or
readiness recheck. No new Rust test, thread, runtime or channel is added.

This qualifies custody between submission and physical entry, plus original
release-producer loss after entry. It does not establish cancellation during
every native syscall or backend operation. The mutation performs one abort on
the original handle; the restored path adds no abort, scan or dependency wait.
Production before/after costs are unchanged: no added syscalls, durability
barriers, round trips, allocations or copies. Complete admission, scheduler and
diagnostic costs, other operating systems and deployment topologies remain
separate obligations.
