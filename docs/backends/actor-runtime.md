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
Multiple filtered subscriptions share a polling budget; sustained earlier input
can delay a later subscription's interrupt. Fair polling is tracked in
[#1710](https://github.com/acyclic-labs/sdk/issues/1710). AgentHandler uses one inbox.

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
