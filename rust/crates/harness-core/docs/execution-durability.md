# Execution publication and recovery

This is the required contract for consolidating execution publications. The
executor has not yet been qualified against its two-boundary cost target.
Changing a durability scope alone does not establish atomic publication.

## Observing the original boundaries

The shared `Operations` collector observes
`acyclic.harness.executor.publication` with four static `rpc` selections:

| Selection | Original work inside the observation |
|---|---|
| `model-dispatch` | Prepared request staging and the conditional `ModelStarted` claim, under the existing required-durability scope. |
| `tool-intent` | Invocation staging and its conditional `ToolStarted` claim. Reconciliation publishes no new intent observation. |
| `tool-settlement` | Result/projection staging, conditional completion record and the already-admitted phase durability completion. |
| `tool-failure-record` | Conditional terminal failure record. This can be nested inside a rejected payload's settlement and is not another full settlement. |

The static `data` field names the requested payload role; `control` names the
conditional journal append. These describe requested work, not measured request
counts. Operation and step are diagnostic fields, never metric labels. The
existing in-place future observer retains original errors and records abandoned
observation as `incomplete`. No output queue, additional future allocation or
physical work is introduced. The counters consumer needs no log exporter.

These are logical publication scopes. A successful scope does not certify one
atomic commit, one physical call, a provider append range or device-cache
durability. Native attempts retain their separate typed observations and
creation-time parent edges through the original persistence worker. Missing
parent/owner/range coverage stays unknown. Failure-record success can precede
an enclosing phase's durability completion; it cannot substitute for that
completion. Replaying a completed turn adds no dispatch or settlement scope.

## Accepted dispatch

Before a model request can reach its provider, one conditional publication must
bind the original operation and step to its admitted request, canonical request
digest, prepared context and `ModelStarted` claim. The claim and every referenced
payload must be recoverable together. Policy, request verification, allocation
admission and acquisition of completion custody precede publication.

The exact original request and retry identity survive an uncertain publication.
A conflict authorizes no dispatch. A lost acknowledgement authorizes neither a
new claim nor a replacement request. Recovery observes the original publication
and uses the original provider's reconciliation contract. An executor may
re-dispatch only when that provider explicitly guarantees idempotence for the
same admitted dispatch identity; `ModelProvider::generate` currently makes no
such promise. Otherwise an unresolved attempt remains `Indeterminate`.

Tool dispatch has the same accepted-intent requirement. Its original effect
identity, invocation and policy decision must survive a crash before physical
execution. A model's completed response cannot retrospectively supply a missing
tool intent. Moving an intent barrier to a tool adapter does not remove its cost.

## Accepted settlement

One conditional settlement publication must bind the original call identity to
its result or typed failure, model-visible projection, filesystem authority
change and authenticated execution index. Its receipt is the permission to
expose settlement or run an after-tool hook. Replaying that receipt settles the
same call and never executes the effect again. A different result under the same
settlement identity is a conflict.

Exactly-once settlement concerns this durable association. Physical effects
retain their producer's reconciliation and idempotency guarantees; settlement
does not manufacture exactly-once external I/O. If an effect may have run and its
producer cannot recover the outcome, recovery retains the original uncertainty.

## Crash cuts

| Crash cut | Required recovery |
|---|---|
| Before publication, including between buffered writes | No accepted dispatch or settlement is visible. No provider starts from an uncommitted claim. |
| During the atomic append | Recovery finds the complete old cut or the complete new cut. A missing referenced payload, authority change or index association invalidates the candidate cut. |
| After append, before required durability completion | No durable success is acknowledged. Recover the original publication outcome; preserve uncertainty if the producer cannot establish it. |
| After durable dispatch, before provider entry | Recover the same request and dispatch identity. Reconcile it; re-dispatch requires the provider's explicit idempotency contract. |
| After physical effect, before settlement | Join the original effect producer's result. Never rerun an uncertain effect to obtain a result. |
| After durable settlement, before acknowledgement or feedback | Replay the original settlement receipt. Do not publish a second settlement or repeat the effect. |
| Cancellation or loss of a publication producer | Preserve accepted completion custody and the exact uncertain request. Every observer receives the original typed failure or receipt. |

Filesystem publication holds remain live until the original commit has a known
outcome. An uncertain `FilesystemCommit` stays with its original owner and is
resubmitted with the same sealed request, key, deadline and holds. A replacement
owner cannot adopt that commit merely by reconstructing equal bytes.

## Required work and cost evidence

The target is one durable publication per accepted model dispatch and one per
tool settlement, shared with another boundary only when one atomic receipt
proves both. A publication includes its referenced closure, authority fences and
index. It cannot acknowledge durability while leaving one of those pending.
Closure durability, accepted tool intent, release crash ordering and the Actor
start fence remain mandatory.

Record original provider commits, physical durability calls, covered append
ranges and completion latency for each cause. Count each physical call once,
even when several logical publications share it. Report setup, data staging,
accepted dispatch, tool intent, settlement and release separately. Independently
selected stores cannot claim a shared atomic cut without a common publication
producer. Copies, hashing, encoding, allocations and control reads remain part
of each boundary's account.

Execution-index pages already share the observation's conditional Stream
commit. Payload staging currently publishes through filesystem transactions
before the referring observation. Consolidation must compose the original
transaction with the observation; an extra barrier after those separate
publications is insufficient.

## Proof obligations

Use the shipping executor and its original durable-I/O fault controller. Inject
crashes before and after every commit, between buffered writes, after physical
effect and before settlement acknowledgement. Reopen with no retained executor
state and verify the original operation, request digest, effect identity and
settlement association.

The same correctness oracle must reject a folded commit with any required
record omitted. A missing durability operation must fail crash recovery. A
restored durable projection and an extra flush must independently fail the cost
oracle while retaining their correct functional result. A repeated physical
effect must fail the settlement oracle even when its returned bytes are equal.

Each wait names its original producer: publication receipt, provider result or
effect reconciliation. Producer closure returns a typed failure to active and
late observers. No readiness probe or timer supplies progress. Fixed touched
records and original receipt joins must remain independent of dormant agent
population and unrelated execution history; growing input, returned output and
actual referenced closure bytes are accounted separately.
