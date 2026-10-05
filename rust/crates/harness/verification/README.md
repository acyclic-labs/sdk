# Bounded swarm safety models

This is a finite TLA+ safety model of one stable activation operation with two
possible owners. It checks admission before dispatch, claim retention when the
journal is unavailable, durable results before success, and cancellation before
success. A crash after dispatch marks the effect indeterminate and retains the
claim for reconciliation; a proven pre-dispatch fatal result closes the turn
with a failed terminal outcome. Recovery enters reconciliation instead of
starting a fresh provider attempt.

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
necessary. In the current implementation (`ebd74c762`), the publisher first
completes the durable admission barrier and then enqueues an owned child worker;
the parent publication does not wait for child model completion. This safety
model still does not establish live parent-child communication.

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
outcome. The owned worker and cancellation paths in `ebd74c762` still need
runtime scenarios for parent servicing, bounded shutdown, and waits reaching
delivery, cancellation or timeout; the models must not treat worker enqueue as
a liveness proof.

Bounded model checking does not establish an unbounded recursive theorem, a Rust
refinement proof, filesystem/process correctness, or model coding quality. Keep
those distinctions in qualification evidence and retain the full E2E matrix.

## Finite scheduler, authority, budget, publication and message models

The checker also runs four deliberately small finite models. They use numeric
identities and fixed operators inside the TLA modules, so the configuration
files do not depend on symbolic function expressions:

* `SwarmAuthority.tla` has four agents, four message identities and three wait
  identities. It checks that message and wait admission is restricted to
  distinct direct parent/child pairs, even after a child inherits a transcript
  prefix. The root's `Parent(1) = 1` mapping is therefore harmless.
  `SwarmAuthorityUnsafe.cfg` enables an unauthorized sibling/self admission
  path and must violate `DirectMessageAuthority`; the separate
  `SwarmAuthorityUnsafeSelf.cfg` negative control must violate
  `SelfMessageAuthority` for the explicit root self-target message; and
  `SwarmAuthorityUnsafeTranscript.cfg` must violate
  `TranscriptInheritanceDoesNotGrantAuthority` after an inherited prefix is
  present.
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

### Qualified property inventory

The source-bound receipt is
`checkpoint-formal-swarm-2026-10-05.json`. Its TLC logs are under
`D:/graphcoder-builds/formal-evidence/formal-integration-revised4-20261005`
and use the pinned `tla2tools-1.7.4.jar`, one worker, fingerprint index `0`,
seed `1`, and a 512 MB heap. The qualified finite cases are:

| Property family | Finite bound | Safe evidence | Required negative controls |
| --- | --- | --- | --- |
| Authority | four agents; directed parent map; four message and three wait identities; transcript inheritance flags | `SwarmAuthority` | sibling/unauthorized, root self-target, and inherited-prefix authority violations cover `DirectMessageAuthority` / `SelfMessageAuthority` / `TranscriptInheritanceDoesNotGrantAuthority` |
| Budget | five agents; depth two; total allocation three; two steps per agent | `SwarmBudget` | over-allocation, over-step, and over-depth violate their conservation/bound invariants |
| Publication | one child; generations `0..1` | `SwarmPublication` | stale publication violates `PublicationAtCapturedGeneration` |
| Message delivery | one durable message; delivery count `0..2` | `SwarmMessage` | duplicate and orphan delivery violate `AtMostOnce` / `DeliveredRequiresAdmission` |
| Activation recovery | one operation; two owner identities | `ActivationRecovery` | dropping an admitted claim violates `AdmittedClaimRetained` |
| Fork boundary | two selected children; three parent revisions | `ForkBoundary` | early dispatch and mutable capture violate their boundary invariants |
| Integration and approval | four agents; root `1`; children `2,4`; grandchild `3`; generations `0..1`; explicit integrate/discard | `SwarmIntegration` | sibling integration, grandchild writeback, stale approval, and mismatched approval each reach an effect and violate the scoped invariant |

These results establish finite transition safety for the model states and
counterexamples. The real runtime bridge below is the separate evidence needed
to show that one production journal execution emits an accepted trace for the
same event adapter.

## Trace conformance adapter

`check-trace.ps1` is a strict finite event adapter for future Harness event
traces. It retains the directed parent map, allocated agent set, separately
bound fork/publication/child operation identities, captured generations,
durable completions, message deliveries and wait targets. It rejects swapped
parent/child roles, missing or mistyped fields, forged generations,
operation/agent mismatches, orphan and duplicate delivery, stale publication,
and session overspend. The communication relation is bidirectional only for
messages and waits; fork and publication remain parent-to-child operations.

`check-trace-fixtures.ps1` accepts two valid traces and rejects thirteen negative
fixtures covering swapped parent, missing identity, wrong agent, forged
capture, orphan delivery, double delivery, invalid boolean, overspend, stale
publication, and missing fork, publication, model-start, or completion
observations. These fixtures prove the adapter's own behavior; no production
Harness event trace is claimed as evidence. To claim implementation
conformance, a production test must export the same event fields from the real
journal and run this adapter against that trace.

### Real Harness trace export

`real_harness_trace.rs` is a test-only exporter included beneath the local
swarm tests. It runs the real Filesystem-backed `PersistentLocalSwarm` with a
deterministic mock provider, then projects the current durable records:
`ForkPrepared` (with `ForkAdmitted` retained only for legacy read
compatibility), the parent conversation's `ForkPublished`, the child's
authenticated execution-journal `ModelStarted`, and `ForkCompleted`. It
writes a normalized four-event trace and a provenance manifest containing
source stream sequences, distinct fork/publication/child operation identities,
canonical source bytes and SHA-256 digests, and the raw opaque project
generation. Registry sequence, parent conversation revision, and child journal
sequence remain independent; the projection records source witnesses and does
not manufacture a cross-stream total order. Task identities map to finite
agent labels and the first observed project generation maps to ordinal zero
only at this adapter boundary. The trace omits current-generation claims when
the source run does not expose an independent retained-current witness.
The manifest also retains canonical typed bytes for the admission seed,
capture report, model publication, and inherited declaration. The checker
re-parses those values from the durable admission envelope and binds parent
step/task/prompt/agent fields, parent conversation operation/revision, parent
seed authority/revision, project generation, ModelStarted step/request digest,
completion output/digest, and the exact canonical provider-neutral request
bytes captured at the mock provider boundary. The request bytes are compared
with the child journal's staged `ModelInputPrepared` request, independently
hashed with BLAKE3, and checked against the durable `ModelStarted` digest;
their SHA-256 is retained as artifact evidence. It also requires the
normalized trace's captured generation to equal the source generation ordinal
before passing the finite event reducer.
The registry admission and completion retain raw envelope bytes; the current
conversation and execution-journal APIs expose typed events, so those two
source witnesses are recorded as canonical typed event bytes rather than
claimed raw storage envelopes.

The ignored exporter requires an explicit output path so ordinary test runs do
not write artifacts. Use the named qualification gate, which runs the
exporter with the repository's test features and then invokes the canonical
checker:

```powershell
./qualify-real-trace.ps1 -EvidenceDirectory D:/evidence/real-harness-trace
```

Running the ignored test directly is an export step only; it does not qualify
the implementation. A passing named gate is runtime conformance evidence for
this bounded trace only. The manifest binds the bytes read by the exporter and
checker to the current source, scripts, and test binary, but those hashes are
self-reported by the same test process. Until a separately trusted journal
reader or signed artifact supplies the source witness, this is self-reported
integrity evidence rather than external provenance. The export does not claim
Rust refinement, unbounded recursion, liveness, approval handling, aggregate
budget exhaustion, or OS confinement.
The named gate requires a clean tracked and untracked source worktree, binds
both the commit and tree object, and records the workspace manifest and lockfile
digests as the build closure. It also mutates representative source fields and
requires the semantic checker to reject every mutation, including corrupted
and reordered serialized model-request bytes. There is no production
causal publication negative case because the current APIs expose independent
registry, conversation, and child-journal orderings without an authenticated
cross-stream causal witness; the gate does not invent one.
The checker also requires the qualification environment's Python `blake3`
package to independently recompute the SDK's BLAKE3 seed digest over the exact
canonical seed bytes; SHA-256 remains the artifact-integrity digest.

## Direct-parent integration and root approval model

`SwarmIntegration.tla` is a bounded four-agent safety model with root `1`,
direct children `2` and `4`, and grandchild `3` whose direct parent is `2`.
Each child captures a project generation when it is forked. The safe behavior
permits only that direct parent to integrate or discard the child. Root
writeback is separately admitted only for a direct root child and binds the
exact writeback operation, `writeback` action, and captured generation. The
model includes a generation advance so publication at a captured generation is
kept distinct from a claim that the captured generation is still current.
Integrate and discard are explicit choices for every child. An approved
writeback remains durable when the workspace generation advances, but safe
execution rejects that stale approval; the approval invariant is evaluated on
an executed writeback, so malformed retained approvals do not become false
invariant failures before an effect is attempted.

The shared checker runs one safe case and four reachable negative controls:

* `unsafe-sibling` gives child `4`'s discard to agent `2`, and must violate
  `DirectIntegrationAuthority`.
* `unsafe-grandchild` lets root approve child `2` but writes back child `3`,
  and must violate `RootWritebackScope`.
* `unsafe-stale-approval` advances the workspace after approval, bypasses the
  safe stale-execution guard, and must violate `ApprovalBinding` on the written
  effect because its target generation differs from the approved generation.
* `unsafe-mismatched-approval` changes the operation and action, bypasses the
  safe execution guard, and must violate `ApprovalBinding` on the written
  effect.

The exact production transition matrix is:

| Model property | Harness/Filesystem transition | Evidence and remaining gate |
| --- | --- | --- |
| `DirectIntegrationAuthority` | `ProjectWorkspaceProvider::prepare_project_merge` creates a provider-owned `ProjectJoinPlan`; `ProjectJoinPlan::apply` receives the caller scope and child authority; `ProjectMergeReceipt::validate` checks that the receipt matches the direct-child fork seed. | The finite model and negative control are qualified. A real concurrent journal scenario still must show sibling and descendant authorization failures through the public Filesystem facade. |
| `RootWritebackScope` | The parent-bound `ProjectJoinLineage` and `ProjectMergeReceipt::validate` keep the source child and target parent bound to the fork; `ProjectJoinPlan::target_project`/`child_project` prevent a generic plan from claiming lineage. | The model proves the bounded root/child relation. The current real trace does not export a root writeback event, so Rust conformance for this property is open. |
| `ApprovalBinding` | `PersistentLocalSwarm::record_operator_approval` copies the ticket's immutable `ApprovalBinding`; `resolve_recorded_operator_approval` rechecks operation and action digest before `InteractionApprovalAuthorization` issues the exact resolver scope; `resolve_approval` commits the decision. | The model proves operation, action, and target-generation equality in its finite state space. Approval mutation and restart scenarios remain an explicit runtime gate. |
| `WritebackAtCapturedGeneration` | `ProjectJoinPlan::source_generation` and `expected_target_generation` are provider-owned compare-and-swap inputs; `ProjectJoinOutcome::StaleTarget`/`Fenced` refuse a stale publication, while `ProjectMergeReceipt` records the resulting generation. | The model proves the writeback records its captured generation. It does not prove that the capture is current at apply time; provider CAS and stale-target tests are required. |
| `TypeOK` and finite bounds | Typed Harness contracts validate authorities, scopes, generations, receipts, and approval tickets before durable publication. | TLC qualifies only the declared four-agent, two-generation state space. It is not an unbounded recursion, liveness, storage, or Rust-refinement proof. |

The model is design evidence tied to these existing Harness transitions; it
does not add a second integration or approval runtime. Its assumptions include
the provider's authenticated lineage and compare-and-swap behavior, durable
approval storage, and the caller's authority verifier. It does not establish
causal ordering across independent journal streams, OS confinement, or
fairness/eventual completion. The real production trace gate remains open for
integration, approvals, and root writeback.
