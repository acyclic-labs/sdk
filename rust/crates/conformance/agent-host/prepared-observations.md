# Original tool observations

The shared stock executor prepares a read-only tool after canonical authority,
target and policy admission, and before the original invocation claim. A ready
result passes through that claim once, without dispatching or executing again.
A deferred result hands the existing `Until` condition to the actor driver,
which commits it and releases delivery. Cancellation drops the original read.
No result cache, persisted format or separate consumer loop is introduced.

Run the real parent/child control:

```sh
cargo test --locked --offline -p acyclic-harness-core --features filesystem,test-support --test main agent_model::wired::a_child_inherits_context_but_retains_its_own_observed_answer_across_turns -- --exact
```

It extends the stock-executor spawn, inherited-context and reopen journey with
forwarding probes over its original child provider. The explicit Allow policy
exercises the path without an approval callback. The child advances after the
first observation; publication must retain the old coherent answer position and
perform exactly one status read. A committed pending child input forces real
actor deferral and subsequent delivery. Original runtime cancellation must end
the held preparation and drop its pending observation. A second real spawned
child proves independent answer reads start together while the first original
answer is held. The result retains the selected sequence, reference order and
actual bytes of both children. Dropping an answer producer ends the original
preparation with the actor failure instead of a successful replacement.
The wait argument is
derived from the recorded input under `harness-fuzz/corpus/agent_wait_selection`;
the actor schedule and canonical event wrapper are generated simulation inputs.

Apply each `mutations/preparation_*.patch` independently, run the same control,
and reverse the patch. They respectively restore a duplicate status read,
substitute a newer answer position, lose cancellation, bypass actor deferral,
serialize independent answer reads and mask an answer producer failure.
Each must fail the corresponding explicit invariant before publication passes.
The approval mutations remain applicable to the separate original-request
control in `approval-request.md`.

An already settled selected child previously needed a readiness observation and
an execution observation; preparation needs one. Exact answer reads remain one
per selected answer. Independent answer reads previously formed one serial
chain with one await per selected answer; they now share one join, with one
future per selected answer and no extra status reads or answer copies. The join
retains storage proportional to selected answers and ends on the first failure.
Preparation adds no durability barrier. Calls that already
crossed the approval owner's effects barrier retain that check. Explicit Allow
now crosses the same barrier too: its generation selection and directory read
buy ordering after earlier agent effects, instead of silently bypassing them.
Their physical request and allocation costs depend on the selected adapter.
Selection and result construction remain proportional to selected children and
answer bytes, with temporary observation storage proportional to the selection.
The per-turn effects barrier and configured approval traversal are separate
existing costs; this change does not claim a constant-time directory traversal.
Native agent observation producers still refuse unsupported custody explicitly.
