# Original task cancellation cause

Run `python3 rust/proofs/qualify.py task-cancellation-cause` from the repository
root. The shared qualifier applies the retained production mutation, requires
the existing native task custody test to reject it with
`task cancellation lost its original typed cause`, restores the exact source
bytes, and requires the same test to pass.

The existing control submits a real owned future, observes its start event,
aborts it through its original handle, and awaits the terminal. The original
future's destructor is the only producer of its release event. The error must
retain its typed join failure, including the original cancelled native join
error. The mutation retains `Interrupted`, `TaskCancelled` and finite completion
while replacing the original cause with a string. Panic mapping is unchanged.

The added assertion lives in the same custody routine used by the deterministic
executor. It accepts that executor's original typed channel cancellation cause;
this command qualifies the native entry point only. It does not qualify the
deterministic executor, every cancellation schedule, other operating systems,
deployment topology or population-dependent costs. No production operation or
additional test entry point is introduced.
