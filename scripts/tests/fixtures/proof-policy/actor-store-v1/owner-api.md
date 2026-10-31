# A4 Actors: rb-actors contract sketch and hosted join

2026-10-26. Single Actors owner across Cloud and SDK; supersedes the retired b-runtime owner mapping, not its adopted contract. Source review:  and package-execution-seam.md. Cloud work starts on rb/actors-a4-20261026 from origin/main 01317365260add52cb8c76885ae9c5b1b10f1777; #6635 head remains unchanged. The api/b-runtime.md Handler/Transition sketch is now adopted; the SDK implementation handover is adopted at signed 4056b65d72 in  (branch codex/rb-actors-a4). This is a provisional logical contract, not a compiled SDK signature or a hosted success receipt.

## Public shape

- Handler::on_record receives Delivery and an optional immutable Checkpoint and returns Transition, including a new Checkpoint and AlarmChange::Keep/Set/Clear. Restore/checkpoint/package decoding must use the same handler contract on local and hosted. Pin exact Rust signatures to the handed-over SDK code; do not create a second trait or Cloud schema meanwhile.
- Manifest names immutable code/package identity, subscriptions, authority, resource bounds and alarm semantics. Actor address remains (account, namespace, id); a namespace is explicitly deployed and first invocation activates its actor. Package identity carries no execution permission.
- local::Runtime runs the same supported package over memory and persistent local stream/fs providers. One provider-generic conformance function and arbitrary endpoint runner execute the same assertions against local and hosted. An unsupported package is an explicit refusal, never substituted fixture code.
- Checkpoint transition logically publishes (retained state/code, consumed cursor, desired alarm/generation, owning actor/heap epoch) all-or-none. Exact retry binds the original capture and desired effects. Alarm generation comparisons reject old fire/finish after set/replace/clear. Handler execution may repeat; external effects use stable keys and exact retry. Durable effect intent does not imply atomic external IO.

## Production join

Reuse server::Actors::heap -> launch, the real ScopeKey/claimed Authority/RestorePlan, installed Objects restore/publisher, Journal, host semaphore, BrokerOnly executor, HeapAdmission/term monitor and per-call InvocationAdmission. #6626 Session at d17f074e6e8f3f8430d636d2d66c79104b88201b supplies one capture/checkpoint path and no admission. Production serving internals and fixture PackageEngine are crate-private; the missing join and supported BrokerOnly package loader/dispatch are explicit implementation work. Do not expose a grantless run(Authority), call harness::Cell, or execute arbitrary Rust package code inside the authority process.

The current installed require_operation checks account and operation family; signed caller Grant checks remain mandatory separately. Hold/recheck the grant through queueing, dispatch and response. Hold heap custody through confirmed physical exit. Revalidate the full restore selection at Restore CAS; immutable launch context matches storage scope, next heap epoch, code and configuration revision. Fence output on owner/term/heap loss.

## Durable implementation and proof order

1. Fixed schedule demonstrates current split checkpoint/alarm boundary; extend accepted preparation, canonical digest, publication and recovery together. Prepare does not publish visible state or alarm. Restore of lost heap resolves original accepted capture before selecting the next restore plan.
2. Version the actual positional Raft/storage encoding. Reuse all-voter rolling format gate (lib/authority/src/rolling.rs:63); retain preceding-release decoder/fixture and keep writes readable until all voters support the new shape. No unconditional extra positional fields or new command variant sent to old voters. Do not claim no customer data permits quorum-breaking rollout.
3. Integrate SDK package engine with existing admitted executor/session path and remove the fixture authority from production consumption. Keep request/heap admission and retained restore negative controls; add direct missing-grant, missing-custody, stale heap/config/term and late-output checks for the new join.
4. Route append wakes through current node-wide session, durable cursor replay and subscribe/read/recheck at park/hibernate/reconnect. Missing paths return NotFound. Root-aware inherited notifications come from rx-hostability, not another Actors follow implementation.
5. Atomic alarm generation and checkpoint recovery cuts, takeover, set/fire/replace/clear, pause/resume/update/hibernate, lost ack and retained restart run through one shared suite. Production registration concurrency primitive gets a concurrency check and omitted-recheck negative control. Scope new builds to changed crates on Linux with admission and <10 min jobs.

## Costs and deletions

O(1) dormant metadata per actor, shared scheduler over ready/due work, no resident task/future/timer/socket per dormant actor. Count live allocations and actual work, not wall-time slope alone. Critical path includes user/package work, authenticated retained checkpoint bytes and actor publication; metadata-only publication can be one actor proposal, not an arbitrary one-Streams-entry promise.

Delete superseded checkpoint-time separate alarm mutations/completion paths in the replacing change. Delete fixture-only package/authority wiring when production join replaces it. Audit actual Cloud coordinator/worker_tick/duplicate timer consumers and delete only those whose semantics move to A4; preserve private distributed participant coordinator, current bidi session, physical custody, fences and rolling decoder. Do not modify managed-agents. SDK wave-2 consumer owners delete their redundant schedulers/leases/daemons after A4 is usable; no parallel primitive or duplicated conformance suite.


## Adopted implementation checkpoint, 2026-10-26

SDK tested Store/Handler pin252c28c27f and unqualified ReadySet head4056b65d72 are adopted into the single owned SDK worktree. The legacy169c branch is not adopted. Local signed WIP corrects the verify Keep contract: preserve generation/deadline and atomically mark the fired generation consumed. It remains unqualified; no passing Runtime claim. Streams shared readiness is requested directly from rx-hostability in rb-actors-to-rx-hostability-20261026T023100Z.md. Frozen SDK1512 stays unchanged.
