# Bounded swarm safety models

This is a finite TLA+ safety model of one stable activation operation with two
possible owners. It checks admission before dispatch, claim retention when the
journal is unavailable, durable results before success, and cancellation before
success. Crashes remove the live owner without removing durable admission.
Recovery enters reconciliation instead of starting a fresh provider attempt.

`ActivationRecovery.cfg` must finish without an invariant violation.
`ActivationRecoveryUnsafe.cfg` deliberately enables the rejected claim-release
rule and must fail `AdmittedClaimRetained`. The expected negative result guards
against an unreachable admission/failure path or an ineffective invariant.

Run with Java and the pinned [TLA+ v1.7.4 tools artifact](https://github.com/tlaplus/tlaplus/releases/tag/v1.7.4):

```powershell
./check-activation.ps1 -ToolsJar C:/tools/tla2tools.jar -EvidenceDirectory C:/evidence/activation
```

The runner verifies the artifact SHA256, uses one worker and a fixed fingerprint
index/seed, and records both checker outputs. State directories live in the
explicit evidence directory; it does not remove existing files.

## Connection to the implementation

- `claim_child_activation_on_stream`: durable single-winner claim.
- `ExecutionEvent::ModelStarted`: durable admission observed by `child_model_started`.
- `mark_activation_failed_if_safe`: unavailable journals retain the claim;
  an available journal without admission permits `ForkFailed`.
- The shared executor/provider reconciliation contract: an admitted operation
  is reconciled rather than blindly dispatched again.
- `activation_failure_requires_journal_proof_before_releasing_claim`: exercises
  unavailable storage, explicit uncertainty, actual local journaled execution,
  restart, and a proven pre-admission failure.
- `local_swarm_faults`: production recursive publication/restart, uncertain
  outcomes, live cross-handle retry and cancellation regressions.

These mappings are reviewed correspondences, not a machine-checked refinement
proof. The implementation tests use real local journals; this model does not
execute Rust or validate serialized model inputs.

## Deliberate bounds and assumptions

This model has one operation, two owner identities and abstract atomic durable
transitions. It assumes the storage CAS, authenticated journal, and provider
reconciliation contracts. It does not prove those assumptions. Cancellation
prevents future dispatch/success; stopping an existing OS process is outside the
model. It does not model messages, budgets, workspace generations, approvals,
recursive prefix bytes, multiple operations or cross-process lock semantics.

Only safety is checked. No fairness or eventual-completion theorem is asserted.
In particular, a crash after admission but before dispatch may remain uncertain
indefinitely. Deadlock checking is disabled because terminal and deliberately
uncertain states are permitted. TLC explores the reachable finite model with
fingerprints; this is bounded model-checking evidence, not an unbounded theorem
or whole-swarm verification. The locked acceptance matrix remains required.

## Completed fork boundary model

`ForkBoundary.tla` models one batch with two selected children and three parent
revisions. It checks that all children are bound and the ordered triggering
exchange is complete before any child dispatches, and that inherited captures
stay pinned while the parent changes. Two negative controls separately permit
early dispatch and mutable capture refresh; both must produce their expected
invariant violation. Run through the shared checker:

```powershell
./check-models.ps1 -Model ForkBoundary -ToolsJar C:/tools/tla2tools.jar -EvidenceDirectory C:/evidence/forks
```

The correspondence is `LocalModelForkPublisher::publish`'s completed-batch
barrier and the frozen declaration consumed by `inherited_task_bundle`.
This is a design model, not an implementation conformance proof. Captures are
abstract immutable revision identities: it does not check serialization bytes,
reference authorization, storage corruption, recursive depth, crashes, or
scheduler progress. The real provider-input and recursive-fork tests remain
necessary. In particular, the current publisher awaits child completion and
this safety model does not establish live parent-child communication.

## Verification direction

Keep the verified surface in Harness: a small set of authoritative transitions
for admission, fork publication, budget reservation, message delivery, workspace
publication and outcome recording. GraphCoder should only invoke these contracts.
Do not create a second orchestration implementation for the models or wrapper.

Extend finite models with explicit counterexample controls for these properties:

- Only the direct parent can integrate or discard a child; transcript inheritance
  does not grant authority. Root writeback requires an approval bound to the
  actual operation and workspace generation.
- Concurrent descendant reservations cannot exceed the session's remaining
  budget. Release and settlement must be idempotent under restart.
- A stable message identity cannot be delivered twice. Delivery order and
  admitted waits survive restart without importing another agent's history.
- Publication requires a current ownership fence and workspace generation.
  Completion records an outcome without implicitly integrating it.
- Dispatch follows durable admission, success follows durable outcome recording,
  and an uncertain effect cannot become a fresh attempt without a declared safe
  provider guarantee.

For each model action, document the corresponding production transition and
exercise it through real journals with concurrent callers and injected crashes.
Add trace-conformance checks against the transition relation; these are planned,
not current evidence. Exact inherited prefix bytes and provider request bytes
remain assertions at the actual serialization boundary: abstract revision
identity alone cannot establish byte equality or absence of hidden input.

Check progress separately from safety. Eventual completion needs explicit
fairness and availability assumptions; permanent uncertainty is an allowed
outcome. A parent servicing a live child, bounded cancellation/shutdown, and a
wait reaching delivery, cancellation or timeout need runtime scenarios as well
as any temporal model. The currently synchronous child publisher remains an
open implementation gap; the models must not assume it has already been fixed.

Bounded model checking does not establish an unbounded recursive theorem, a Rust
refinement proof, filesystem/process correctness, or model coding quality. Keep
those distinctions in qualification evidence and retain the full E2E matrix.
