# Actors and Objects simulation properties

The simulation uses `acyclic_native_runtime::testing::Schedule` and
`seeded_runtime`. Operation choices draw from `fill_seeded`; there is no
subsystem clock, scheduler or random generator. Replay requires the same seed,
locked dependencies and compiler. Failures report their seed.

## Actors

The persisted checkpoint, subscription cursor, alarm generation and delivered
generation, output records, child creation and companion records describe one
atomic prefix. An unacknowledged delivery can have committed or not committed;
reopening that prefix must never duplicate its effects. A definite refusal must
leave its input available. A committed alarm generation fires once, including
after reopening. Composition preserves the original actor publication and can
append wakes for several children even when they share a scheduling shard.
Admission close rejects new work while preserving already accepted work.

`local::model::simulation` currently samples dropped submissions and lost
acknowledgements around the existing production Store script and checks its
tuple after each publication attempt. This is logical recovery over MemoryStream,
not physical restart proof. The forked-custody seeded scenario checks that raw
stream copies never inherit the source actor's settlement obligation.

`local::runtime::tests::simulation` drives three actors concurrently through
the production delivery loop. Its independent model tracks input sequence,
ordered outputs, paused admission, alarm generation, deadline and delivery.
Generated operations append input, set or clear alarms, advance the shared
clock, pause or resume admission, and discard/recreate the Runtime with new
placement commands. Every step joins the finite delivery operation and checks
all three persisted actors and their outputs. Those generated restarts retain
the provider. A separate physical scenario arms a Before, After or Torn fault
at the original next file operation during a real delivery publication, drops
all Runtime/provider owners, reopens LocalStream, and requires both exactly-once
input effects and the pending alarm to complete.
Each seed also closes real admission credit while handlers hold all admitted
slots. Already accepted deliveries must finish, the queued deliveries must
return the typed permanent refusal without consuming input, and a fresh Runtime
must recover the refused inputs without duplicating accepted outputs.

The seeded composition scenario finds two distinct actor identities with the
same scheduling shard. The parent handler contributes the first child's create,
input and activation; its composer contributes the second through the same
PreparedPublish merge path. Both child state anchors must equal the parent
publication anchor. The real Runtime then delivers both inputs exactly once.

## Objects

Every verified put names the digest of its input. Every batch has one result per
input in original order, including duplicate digests and references to earlier
members. Every verified object remains readable with exactly its original bytes
until deletion or collection. A valid half-open range returns precisely that
slice. Empty non-full ranges are refused. Failures must not reorder other
members. A put alone does not promise crash durability: only successfully
flushed data must survive a physical restart.

`tests::simulation` currently checks the acknowledged prefix after each generated
put batch over the real Store and MemoryMedium, mixing full reads, random ranges
and invalid ranges in the same ordered batch. Its physical scenario first
flushes a baseline object, then injects a Before, After or Torn fault in a new
put's original log operation. Reopening LocalMedium must preserve the baseline;
re-put repairs the interrupted object, and after flush both must survive another
physical root reopening.

## Execution and remaining proof

The default tests run four seeds with bounded histories. The separate ignored
`deep_` tests enumerate 4096 seeds and are intended only for idle workers.
Neither sweep is a substitute for historical negative controls.

Historical targets are the shared scheduling-shard composition fix
`fe83ab67b5c` (pre-fix `88ea479eabc9f73daaebc248cfdac5026b2a1720`) and copied
custody fix `829464b4df7` (pre-fix
`d269a4731a4b932e31d7256640dccb6b8d8d065d`). Both still need executed failing
seed/trace and matching passing fixed-revision evidence. These results have not
yet been established by these drivers; the added scenarios remain unvalidated.
After the source revision compiles, `bash scripts/qualify-actors-history.sh`
runs each fixed scenario, then overlays only its test module/helper on the
corresponding exact pre-fix source and requires an executed failing test.
Compiler failures and zero-test selections are rejected. The output names
both the product source revision and the revision supplying the test overlay.

The shared core owner is adding `testing::sweep` for seed selection/replay and
trace minimization. These drivers will consume it when available. Their small
boundary adapters only release Schedule's permits and join its owned work;
they add no executor, clock, RNG or fault mechanism.
