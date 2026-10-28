# Committed actor operations (G59, G62)

`Store::invoke(&Invocation)` appends an operation to an existing actor's bound
subscription and observes its committed response. Ordinary delivery and
invocation use the same `Handler::on_record`. `Delivery::invocation()` exposes
the exact request while `Delivery::record` retains the original committed input.
The handler supplies `Transition::response`; the host publishes it with the
checkpoint, consumed cursor, alarm and effects in the original transaction.
Missing responses and responses on ordinary records or alarms are refused.
Shared input paths remain supported. A marked call for another actor or
subscription can be consumed without a response; only the addressed delivery
can publish its reply. `Invocation::decode` still exposes the original marker
to handlers that observe shared input, while `Delivery::invocation` returns
only a call addressed to that delivery. Malformed marked records still fail.

An operation identity belongs to one actor under one Store root. Its request
binds the subscription and exact bytes. Repeating the operation returns the
original input and response publication; changing its request produces
`IdempotencyMismatch`. A lost transport response is reconciled by calling the
same operation again. `enqueue_invoke` and `wait_invocation` separate admission
from observation when a caller needs the original input ticket. Dropping a wait
does not retract committed input. Paused actors retain queued operations.

The result includes response bytes and an immutable receipt containing the
original input, co-committed state and response records, committed revision,
publication identity and publication time. Recovery validates actual retained
records rather than constructing a response from caller bytes. Response records
live beneath the actor's state path, addressed by the operation identity; there
is no scan of earlier responses or second decision journal. Retention of those
original facts is required for reconciliation.
Admission appends the actor's activation in that same commit. Compact write
receipts are joined with `read_commit` and the canonical request binding to
retain the original authenticated input, including after a lost acknowledgement.
The ticket exposes the provider-issued `storage_revision` as a read floor.
Physical reopen can issue a new incarnation floor for the same original ticket.

`Store::wait_applied` observes a separately specified `AppliedCut`: subscription,
bound input path, exclusive cursor and original preceding input commit. It
verifies that original history even after consumption, and follows the actor's
state at its exact committed cut. Neither this observation nor an invocation
receipt authorizes subsequent external execution. The existing pre-handler
pause and placement observation remains in effect.
`AppliedCut::at_least(RevisionCut)` forwards the canonical Streams storage floor
to the original follows. The shared provider parks until that floor is applied;
the actor's consumed subscription cut remains a separate required guarantee.
Both record and alarm deliveries carry `Delivery::subscriptions.state_path()`
and the original state anchor from that same loaded state. The strict guest
wire retains the path, including the injected Store root, rather than deriving
it from the actor reference.

`reservation::Pool` provides `reserve(units, request_id, owner, deadline)` and
`release(&Grant)` over committed invocation. `reservation::create` returns the
companion state, discovery and inbox-initialization effects for a pool with
explicit positive capacity. Compose them into the original provisioning commit.
Run `reservation::Package` through the same native or strict-wire host, supplying
the artifact identity and explicit execution/checkpoint limits. An artifact can
instead embed the shared `Book` in its own checkpoint, validate it on decode,
and publish `Book::step`'s response and alarm with that checkpoint.

Grants retain the original pool actor and state path (including its Store root), request identity, units, owner evidence,
monotonic generation and logical deadline. Changed reserve arguments conflict.
Release must match the exact active grant; an old generation or another owner
cannot release its replacement. Repeated releases return their original
decision. Refusals are also retained decisions: use a new request identity for
a new attempt after capacity changes. The pool owns its actor's alarm, set to
the earliest active expiry; unchanged deadlines preserve the alarm generation.
Cleanup commits even with no input, and a paused/reopened owner retains its due
cleanup until resumed.

Owner evidence comes from the admitting host and is retained unchanged. The
host verifies that evidence and enforces the grant generation where external
work starts. Logical expiry and a returned receipt do not establish physical
execution death. Pool state and original responses remain in the same actor
and Stream authority; no adapter owns reservation arithmetic or retry rules.

The unchanged shared Actors verifier adds invocation and reservation scenarios
to every Target, including original Package delivery, pause, restart, changed
arguments, wrong owner/pool, lost-response reconciliation and due cleanup.
Native controls also cover strict-wire production-package execution and an
invocation publication with participant custody through a lost acknowledgement
and retry refusal. Admission controls cover failure before apply and a lost
append acknowledgement, followed by a retry refusal, caller reopen, pause and
placement replacement. They retain the provider's original input and retry
digest, reject changed bytes, and check that dropping a response wait leaves
the input queued. Native library checks exercise these controls alongside the
shared verifier. Hosted workerd qualification and scale measurements are separate.
The existing DO adapter remains 247 physical lines.

Every committed invocation record includes its original Store state path.
Delivery checks that path against its immutable loaded subscription cut, and
publication checks the same path before accepting a response. Identical actor
and subscription identities under another root may share the input and advance
without executing the operation. A real two-root reservation Handler control
checks independent capacity, subsequent calls and original-response replay.

Runtime admission first serializes a particular actor, then acquires an active
delivery permit. Duplicate events waiting for one actor cannot occupy the
capacity needed by other actors. Ordinary subscription selection rotates after
the last selected input across cooperative batches. This scheduling position
never advances a consumed cursor or replaces a committed observation. Controls
cover queued duplicate drains with a cold actor and 64 ready subscriptions
behind a hot first input; the former permit-first/key-first order fails them.
The native library run covers both controls. The measurement plan compares hot duplicate
drains plus cold actors and 64 hot inputs, then 100k dormant/ready actors with
1/8/64 producers, recording admitted futures, RSS and completion order. No
throughput, zero-control or scale result is claimed.
