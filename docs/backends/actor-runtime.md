# Shared actor runtime (G40, G41)

Compose `local::Runtime` with any Stream-backed `Store`, a guest `Handler` and
the host clock. `AgentHost` uses this runtime for native, browser and Durable
Objects delivery. No backend owns discovery, interruption or checkpoint rules.

G40 stores the dispatcher's next activation sequence in its own actor state,
under `<state-root>/_dispatcher`, separately for each claimed namespace. Reopen
loads the last state record (one tail observation and one cursor-record read),
then follows the saved sequence. The opening append has a stable key. Actor
slots use 64 independently locked shards; this is not an actor capacity limit.

A saved cursor does not recover resident subscriptions or pending alarms.
Restart therefore rebuilds registrations from paged **current state names**,
capturing the cursor before enumeration so concurrent creations remain on the
follow. This costs O(current actors) metadata, without activation-history replay
or a growing checkpoint containing every address. Total startup is not O(1).

G41 lets a manifest declare subscription/value-prefix interrupt filters. A
matching later record signals the delivery's `CancellationToken`; lookahead
neither advances the committed cursor nor buffers skipped records. The handler
settles through its normal fenced transition. Cleanup remains under the original
timeout; timeout drops the handler and leaves the input unconsumed.

Agent handlers declare canonical Cancel/Archive prefixes. Execution journals
wait on the runtime's token, replacing `TurnStop` and its second inbox follow.
The strict guest manifest carries the same filters. `onRecord` takes separate
abort and cooperative-interrupt signals, so a cancelled guest can settle while
a timed-out guest is aborted. The adapter retains only its existing primitives.
Multiple filtered subscriptions share a polling budget and rotate the starting
subscription after budget exhaustion (#1710). Lookahead keeps the original cut
and releases provider futures on cancellation. AgentHandler uses one inbox.

Validation commands on bb2: `scripts/check-actor-runtime.sh`, actor and
agent-model tests, and `scripts/qualify-host-serving.sh`. The shared actor suite
checks cancellation with intervening input and restart. The host suite checks
saved discovery cursors and in-flight interruption for each backend unchanged.
On bb2, both native host entries passed in 5.81 seconds and the browser entry
passed in 10.80 seconds. Actor tests, all 34 agent-model tests and strict typed
forwarding against the generated guest declarations also passed. Workerd runs
the unchanged Stream and host suites plus packaged-guest restart checks.
Exact source pins and backend qualification receipts are recorded in
[#1709](https://github.com/acyclic-labs/sdk/pull/1709).

G50 adds committed subscription cuts to every `Delivery`, including alarms
without a record. Native handlers and the strict guest wire receive the same
bounded projection of the state already admitted for that attempt, with no
additional storage observation. The read-only view retains the original state
commit anchor. Cut order is canonical; duplicate ids, invalid paths and missing
anchors are refused. The cuts grant no storage access and cannot change
subscriptions or bypass pause and placement admission.

The harness now uses Actors' inbox cut for alarm resume and state observations,
and stores only artifact state in its checkpoint. Protobuf tag 1 is retired;
the one existing decoder naturally ignores that field in earlier checkpoints.
The artifact state encoding and codec version remain the same. Both old and
new checkpoints take their authoritative inbox cut from Actors' state.
G50 adds wire refusal and record/alarm reopen controls. Its validation is a
bb2 compile-only check; the earlier execution receipts above apply to #1709,
not this ABI change. No throughput or zero-control claim follows from carrying
a loaded cut.

G55 adds `Runtime::with_limits(RuntimeLimits::new(active, work)?)`. Defaults are
64 admitted deliveries and 64 steps per batch. One admission semaphore is shared
by continuous dispatch and event-owned delivery. Continuous dispatch allocates
at most `active` execution futures; dormant and queued actors retain keys.
Every batch releases admission and the actor lock. A hot actor rejoins the ready
queue at the back; an event-owned drain continues across batches until quiescent.
Dispatcher event classes rotate, and immediate work yields to the executor
without an additional timer. Due-index lock work is bounded by `work`; unchanged
alarm/index tuples retain the waiter without rewriting the index. Popped retries
and suspended alarms still reinstall their committed generation.

The G55 controls cover gated active admission in both hosting modes, cancellation
of admitted futures followed by exact-input replay, hot-actor batch fairness,
alarm no-op/retry/resume behavior and a later interrupt behind hot first input.
Only compilation is requested for this milestone; these controls have not been
executed. Scale qualification remains 100k dormant/simultaneously ready actors,
1/8/64 producers, active futures/RSS, alarm lock hold, control sends, foreign-tenant
restart churn and last-subscription cancellation. Wake indexing and acknowledged
control freshness remain open; the real per-handler pause/placement observation
and original invocation oracles are unchanged.

An event-owned platform can now open `acyclic_actors::hosting::Host` with its
original `Store`, exact actor, arbitrary `Handler`, clock and runtime limits.
It creates no background task and runs no guest work at open. `wake_paths()`
projects the actor's state and subscription routing paths; `pause()` persists
the original fence while a delivery is running. The platform registers routing
before the final input recheck, retains event/recovery custody through `wake()`
and persists its returned alarm before completing the native event. Reopen
uses the same root/provider and takes a fresh placement through the same runtime.

The browser wasm artifact exports `WasmActorHost.open(stream, root, actorBytes,
manifestBytes, onRecord, active, work)`. It accepts any strict Actors wire
Handler, independently of the harness AgentGuest/AgentHost composition. Its
`pause`, `wakePaths` and `wake` calls delegate to the shared host. The existing
guest callback bridge provides cooperative interruption and abort-on-timeout.
The platform remains responsible for authenticating the exact actor, routing
committed changes and native alarms, and keeping the event/recovery alarm alive.
This factory supplies a production binding for the unchanged hosted Actors
Package suite; compilation of the binding or a native strict-wire control is
not a hosted DO conformance result. Workerd qualification remains with the
conformance owner and has not been run for this milestone.
