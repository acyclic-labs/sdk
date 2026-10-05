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
The lightweight trace adapter now checks the event-level transition relation;
its fixture run is adapter evidence, not implementation conformance. Exact
inherited prefix bytes and provider request bytes
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

## Finite scheduler, authority, budget, publication and message models

The checker also runs four deliberately small finite models. They use numeric
identities and fixed operators inside the TLA modules, so the configuration
files do not depend on symbolic function expressions:

* `SwarmAuthority.tla` has four agents, three message identities and three wait
  identities. It checks that message and wait admission is restricted to direct
  parent/child pairs. `SwarmAuthorityUnsafe.cfg` enables one sibling admission
  path and must violate `DirectMessageAuthority`.
* `SwarmBudget.tla` has five finite agent identities, a depth bound of two, a
  session allocation budget of three, and a two-step per-agent budget. It
  separately checks aggregate allocation, per-agent steps, and recursive depth.
  The three unsafe configurations enable exactly one over-allocation, over-step,
  or over-depth transition and must violate the corresponding invariant.
* `SwarmPublication.tla` has one child and parent generations `0..1`. It
  captures the parent's generation at fork and checks that publication uses
  that captured generation. `SwarmPublicationUnsafeStale.cfg` enables a stale
  publication and must violate `PublicationAtCapturedGeneration`.
* `SwarmMessage.tla` has one durable message identity and delivery count `0..2`.
  It checks admission before delivery and at-most-once delivery. Duplicate and
  orphan-delivery configurations each have their own expected counterexample.

Run an individual family through the same pinned runner, for example:

```powershell
./check-models.ps1 -Model SwarmBudget -ToolsJar C:/tools/tla2tools.jar -EvidenceDirectory C:/evidence/swarm-budget
```

The negative cases are part of qualification: a green safe case without its
reachable expected counterexample is incomplete evidence. All four models are
bounded safety models. They do not establish unbounded recursion, fairness,
eventual completion, deadlock freedom, storage correctness, or Rust refinement.

## Trace conformance adapter

`check-trace.ps1` is a strict finite event adapter for future Harness event
traces. It retains the directed parent map, allocated agent set, operation to
agent bindings, captured generations, current generations, durable completions,
message deliveries and wait targets. It rejects swapped parent/child roles,
missing or mistyped fields, forged generations, operation/agent mismatches,
orphan and duplicate delivery, stale publication, and session overspend. The
communication relation is bidirectional only for messages and waits; fork and
publication remain parent-to-child operations.

`check-trace-fixtures.ps1` accepts one valid trace and rejects nine negative
fixtures covering swapped parent, missing identity, wrong agent, forged
capture, orphan delivery, double delivery, invalid boolean, overspend and
stale publication. These fixtures prove the adapter's own behavior; no
production Harness event trace is claimed as evidence. To claim implementation
conformance, a production test must export the same event fields from the real
journal and run this adapter against that trace.

### Real Harness trace export

`real_harness_trace.rs` is a test-only exporter included beneath the local
swarm tests. It runs the real Filesystem-backed `PersistentLocalSwarm` with a
deterministic mock provider, then joins the durable swarm registry's
`ForkAdmitted`/`ForkCompleted` records with the child's authenticated
execution-journal `ModelStarted` record. It writes a normalized four-event
trace and a provenance manifest containing source stream sequences, operation
identities, and the raw opaque project generation. Task identities map to
finite agent labels and the first observed project generation maps to ordinal
zero only at this adapter boundary.

The ignored test requires an explicit output path so ordinary test runs do not
write artifacts:

```powershell
$env:GRAPHCODER_REAL_TRACE_PATH = 'D:/evidence/real-harness-trace.json'
cargo test -p acyclic-harness --features filesystem-local exports_real_harness_trace_for_canonical_checker -- --ignored
./check-real-trace.ps1 -TracePath D:/evidence/real-harness-trace.json -ManifestPath D:/evidence/real-harness-trace.manifest.json
```

The result is runtime conformance evidence for this bounded trace only. The
export does not claim Rust refinement, unbounded recursion, liveness, or OS
confinement.
