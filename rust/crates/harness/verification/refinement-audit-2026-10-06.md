# Bounded model refinement audit

This audit compares the finite verification models with the Harness and
Filesystem boundaries at root commit `4a780e289b871da10b21cb3f520e29c376c7231a`.
It is a design and test-gap report. It does not claim Rust refinement,
unbounded recursion, liveness, process confinement, or a model of every
provider schedule.

The findings below were recorded before the follow-up model correction on the
current branch. `SwarmMessage.tla` now permits completion of an already
admitted message after cancellation while retaining a pre-admission rejection
invariant. `ForkBoundary.tla` now separates immutable inherited capture from
publication revision, models proven rebind, qualifies a selected-child subset,
and advances selected children through explicit sequential preparation and
publication states. It also models idempotent retry after a lost publication
acknowledgement and negative controls for duplicate publication, early child
request, and stale publication ownership. The runtime tests and exporter still
need to establish those correspondences; the corrections are bounded model
changes, not a Rust refinement proof. The model does not claim journal replay,
rollback of a partially prepared batch, or activation-claim recovery.

## Native seed and activation failure boundaries

The native recursive failure at `1f5baa1c8` occurred after physical
preparations returned and during seed boundary verification. The later alias
recovery path establishes a separate durable edge: the parent seed can already
be committed while the Git compatibility alias is absent, and retrying the
same registration must repair that alias without redispatching the child.
`SwarmPublication.tla` now represents that state explicitly. It also requires
the child-budget reservation before seed commit and the alias before activation
reservation. `CancelResolver` models the pre-seed cancellation path that
releases the reservation; the unsafe case publishes after that cancellation
linearization.

`ActivationRecovery.tla` represents the post-admission uncertainty boundary:
`dispatched` is the actual effect count, while `dispatchEvidence` is the
durable observation available to recovery. `Crash` can therefore leave an
admitted operation indeterminate when the actual effect count is either zero or
one. `Recover` may reconcile without redispatching; `PersistNoDispatch` is the
separate path that records authoritative evidence that no effect occurred.
Negative controls cover false success without one effect and unsafe
redispatch. These are bounded transition checks only. They do not prove the
native overflow path, provider guarantees, alias storage implementation,
resolver cancellation timing, or Rust refinement.

## Findings

### Message cancellation has two linearization cases

The original `SwarmMessage.tla`'s `CancelledNeverDelivered` was sound only when cancellation
wins before `MessageAdmitted`. The production path first appends
`StoredEvent::MessageAdmitted` in `PersistentLocalSwarm::admit_message`
(`filesystem/swarm_local.rs:4813`), then publishes the recipient mailbox
record through `MailboxStore::send_admitted`
(`filesystem/swarm_communication.rs:231`). If the admission already exists,
the recovery path can finish that exact delivery after an endpoint has been
cancelled; this is the intended durable-admission recovery behavior. The
pre-correction model's `UnsafeCancelDelivery` case treated that allowed
recovery as unsafe behavior.

The current model distinguishes `cancel_before_admission` from
`cancel_after_admission` as `PreAdmissionCancellationNeverDelivered`, retaining
the invariant only for the first case.
The minimal runtime test is a controlled seam that admits the message, cancels
the recipient before mailbox publication, reopens the swarm, and retries the
same `(sender, recipient, message_id, payload)`. It must produce one mailbox
record; a changed body must remain a conflict. The existing local swarm test
mostly covers cancellation after the mailbox record already exists, so it does
not cover this boundary.

`SwarmMessage` also models a separate `Publish` count after `Deliver`. In the
runtime, the registry admission and mailbox append are the two durable
witnesses; there is no third message publication ledger. The checker should
interpret `Deliver` as the mailbox append or split the model into admission,
mailbox publication, and read observation. It must not claim that the current
`publicationCount` is independently observed.

### Fork boundary captures two different revisions

`ForkBoundary.tla` uses one `captured` revision and marks every refresh as an
unsafe mutable-history substitution. The production publisher does preserve
the inherited model boundary, but it may rebind the parent conversation
revision for a prepared child when another parent event advanced the stream.
`LocalModelForkPublisher::publish` does this at
`filesystem/swarm_local.rs:2129-2145` through an authenticated
`ForkRebindProof`, `rebind_report_history`, and `rebind_fork_seed`.

The original model therefore conflated two values:

* the immutable completed-model boundary whose bytes are inherited by the
  child; and
* the parent aggregate revision used by the later `ForkPublished` event.

The current model splits these values. A rebind may advance the publication
revision only when the proof binds the same boundary, seed resources, and
child operation. A focused runtime test should advance the parent stream
between preparation and publication, then assert that the child request bytes
are byte-identical while the seed records the authenticated rebind.

The model also requires every finite `Children` member to be captured before
the batch can complete. The implementation only requires all children
selected by one completed model publication: it prepares the plans in one loop
and enqueues them in a second loop (`swarm_local.rs:2129-2165`). Add a one-child
case alongside the two-child case before treating `DispatchRequiresCompleteBatch`
as a general scheduler property.

### Activation recovery is finer-grained than the model states

`ActivationRecovery.tla` has `claim`, `started`, and `dispatched`, but the
runtime has separate durable points: `ForkPrepared`,
`ForkPublicationCompleted`, `activate_child_budget`, the child execution
journal's `ModelStarted`, and `ForkCompleted`. The relevant production
boundaries are `swarm_local.rs:5733`, `swarm_local.rs:4199`,
`swarm_local.rs:6140`, and `swarm_local.rs:6252`.

The current model can establish the broad rule “an admitted operation is not
redispatched,” but it cannot identify which boundary was crossed when a crash
occurs. Add bounded scenarios for:

1. crash after `ForkPrepared` and before publication;
2. crash after `ForkPublicationCompleted` and before budget activation;
3. crash after budget activation and before `ModelStarted`; and
4. crash after provider dispatch and before usage/result persistence.

The third case must retain the dispatch identity without issuing a second
provider attempt. The fourth must remain indeterminate until the provider
guarantee permits reconciliation. `PersistResult` in the model currently
assumes a semantic provider observation that the finite state does not carry;
that is an explicit assumption, not a proof of provider recovery.

### Budget bounds are illustrative, not the production configuration

`SwarmBudget.tla` fixes five agents, total allocation three, depth two, and
two steps per agent. The production budget is configurable through
`SwarmBudgetLimits` (`swarm_budget.rs:32`), with defaults of eight active
agents, 64 total agents, depth eight, and 512 model steps
(`swarm_budget.rs:69-75`). The journal separately tracks active agents,
total agents, model steps, output bytes, execution time, owner epochs, and
remaining reservations (`swarm_budget_journal.rs:247`, `:338`, `:696`).

The bounded model is useful evidence for conservation in its declared state
space, but it is not a default-configuration proof. It also lacks release and
settlement transitions and stale-owner takeover. Add a parameterized small
model case for active-versus-total limits and a stale-owner case where an old
owner cannot reserve, cancel, or settle after takeover. Keep resource
dimensions separate from the agent-count model.

### Fork publication is not workspace integration

`SwarmPublication.tla` uses `parentGeneration` for a fork publication and
checks that `publicationGeneration` equals the captured value. The production
fork path publishes a completed model batch and parent conversation seed;
`ForkPublicationCompleted` is a publication receipt for that operation. The
workspace integration path is different: `ProjectMergeReceipt` carries
`source_generation`, `expected_target_generation`, and `result_generation`
(`merge.rs:263-275`), and the Git facade applies a provider CAS through
`apply_authenticated_project_merge_for_child`
(`filesystem/git_facade.rs:371`) before the parent
`ProjectMergePublished` event (`core.rs:724`).

Do not use the fork publication model as evidence for workspace writeback.
Maintain separate finite cases for parent conversation rebind and provider
workspace CAS. The latter needs a result generation and explicit stale-target
outcome; equality between a captured generation and the current generation is
not the whole integration contract.

### Integration model omits readiness and receipt state

`SwarmIntegration.tla` allows `IntegrateOrDiscard` as soon as a child is
`ready`. The production facade first prepares an authenticated child merge
plan (`git_facade.rs:327`), checks direct-parent lineage, validates the merge
notice/receipt, and only then applies the provider join. The result may be
`Applied`, `AlreadyApplied`, or `NoChanges`; the parent conversation receipt
is a separate publication.

The finite model should either define `ready` as “validated plan and child
completion are durable” or add those states explicitly. Minimal runtime cases
are sibling and grandchild rejection, duplicate receipt replay, merge conflict
continuation/abort, and a crash between provider publication and
`ProjectMergePublished`. The current real trace intentionally does not cover
this path.

The approval model also stores one global approval tuple. Runtime approvals are
durable interaction tickets with operation/action digests and resolver scopes.
The next bounded case should admit two concurrent tickets, restart, resolve one,
and prove the other cannot be substituted or reused for a different writeback.

### Authority and waits omit lifecycle and scope state

`SwarmAuthority.tla` checks a static parent map and transcript inheritance,
while `send_message` and `communication_scope` additionally require an
authenticated task admission, lifecycle phase, grants, budget reservation,
and direct-parent relation (`swarm_local.rs:4719` and
`swarm_communication.rs:68`). The model cannot establish behavior for an
unknown task, cancelled endpoint, stale owner, or exhausted child reservation.

Similarly, the model's wait transition has no durable request identity,
timeout, cancellation, or restart cursor. Add runtime cases for an invalid
target, cancelled waiter, timeout, restart/resume, and duplicate observation.
These are separate from the static authority invariant.

## Qualification boundary

The current production trace exporter covers a bounded fork admission,
parent conversation publication, publication replay, child model start,
completion, and an active parent-to-child message admission plus mailbox
delivery. It does not cover project integration, approval/writeback, stale
owner takeover, or the cancellation-after-admission message race. The exact
trace remains a bounded runtime correspondence sample; it is not a Rust
refinement proof. All finite model claims remain limited to their declared
agent, generation, depth, step, and operation bounds.
