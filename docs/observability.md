# Observability

## Trace window files

`live/agents/self/trace/<from>/<count>.json` projects a caller-selected window
of the selected agent's typed trace stream through the ordinary authenticated
private-file query. `from` is an inclusive record cursor; `count` is a positive
32-bit page size. The file host reads only that window, clipped to its captured
source cut. Its existing file subscription follows the original trace stream,
including creation of a previously absent stream. No background collector or
additional publication is created by installing this view. Authored overrides
use the ordinary file rules.

The shared projection reports the covered cursor interval, observed tail,
per-mechanism inclusive time and bytes, unfinished waits, unsettled outcomes,
and missing guarantees. Outlier classification remains unknown without an
explicit mechanism policy. A finish whose start is outside the selected window
is unpaired; it contributes no invented duration. Catching the captured tail
does not prove settlement. Missing streams and empty windows yield no file.
Malformed records fail with their original publication and decoder cause.
Children start with their own trace subtree rather than inheriting their
parent's trace history. The incremental Reader adapter applies the same query
to bounded pages and live deliveries while retaining active joins across pages.

An outstanding authenticated file wait admits trace interest for its selected
agent. Overlapping waits share one producer clock and identity sequence. The
last wait's completion or cancellation retires admission; already-admitted
spans may still publish their own terminal. Snapshot reads do not admit a
follower. The host selects a recorder through the primitive's existing interest
registry using a borrowed agent identity. No per-agent entry exists without a
follower; with no trace followers, selection is one atomic check with no clock,
allocation, registry lock or IO. Interest is ephemeral subscription custody,
not stored trace history or authorization.

The shared filesystem turn host binds each admitted journal to this same
registry and original agent. Ordinary execution replay pages emit
`harness.execution_replay_page` spans, buying `authenticated_execution_prefix`.
The same decoder and native-work custody serve both observed and unobserved
reads. Byte counts remain unknown because decoded events do not establish wire
or copy bytes. Diagnostic failures preserve the original request result;
failed reads stay unfinished until their original cause is retained. The old
separate replay tracing span is removed. Complete request-path instrumentation,
content correlation and deterministic crash replay remain separate requirements.

Private journal staging and immutable content loading use the same demand
boundary as replay. Their guarantees are `authenticated_private_publication`
and `authenticated_immutable_content`; byte counts describe supplied/returned
payloads, not device or wire traffic. Canonical model-request admission emits
`attested_request_prefix`. The original native work and cancellation stay in
the admitted operation future. Separate journal stage/load tracing wrappers
are removed. Nested observed futures inherit the original span as their parent
on each poll; suspension restores the preceding context, so unrelated work on
the same worker cannot acquire that parent. Disabled observation installs no
context and allocates no propagation task or boxed future.

Journal appends, including the native accounted path, share this boundary:
`harness.journal_append` buys `idempotent_journal_publication`, while
`harness.journal_append_if_tail` buys `atomic_journal_precondition_result`.
A successful conditional request means its precondition result was returned;
it does not imply acceptance. Private range reads report only returned payload
bytes under `authenticated_immutable_range`. The separate append tracing
wrappers are removed; physical publication, retry identity and projection
results stay owned by the original journal operation.

Dispatch-claim publication uses `Recorder::handoff`: it admits the span before
the original work and performs no diagnostic await after that work returns.
A trace finish cannot strand an accepted model dispatch claim before delivery
to its executor. Successful journal claims transfer their terminal append to
`Recording::finish_submitted`, using the shared SDK owned-publication completion.
It retains the original append bytes, path, retry family and idempotency key;
dropping every observer cannot withdraw publication. There is no diagnostic
suspension after the accepted claim. Failures without retained cause evidence
remain unfinished rather than fabricating a terminal result.
Ordinary journal publication identity, summary provenance and selected-context
authentication emit `AuthCheck` spans through the same demand-selected recorder.
The checks preserve their original decision and cause; byte counts remain unknown
because these boundaries do not own a complete physical-read accounting result.
Tool approval verification, prior-effect barriers and terminal-settlement lookup
use the same journal demand boundary. A held effect barrier or absent terminal
returns its original value and records `Unsettled`; an authenticated terminal
records completion of the lookup, whose result retains the tool outcome. The
shared owned append publishes these terminals without suspending effect delivery.
No retained failure cause is fabricated for an unsuccessful lookup.
The executor's original tool invocation uses the journal's object-safe
`trace_request` admission. Unfollowed admission returns `None` without a future
allocation; followed admission transfers the same SDK recording through
`OwnedRecording`, which delegates to the original terminal producer and per-poll
parent scope. No provider or tool bytes are serialized for tracing. A successful
tool receipt records success, interruption records cancellation, and pending,
controlled or receipt-less results remain explicitly unsettled. An error without
retained cause evidence abandons the owned observation as `Unsettled`. Physical subprocess
and filesystem hops still require their own observations beneath this boundary.
Fresh model dispatch and provider reconciliation also use the original journal
admission and owned recording. `harness.provider_call` encloses the original claim,
provider generation or continuation and event settlement, propagating its parent
to journal writes. Its start is admitted before a fresh claim; no trace wait is
inserted between an accepted claim and provider delivery. Completion is submitted
without awaiting diagnostics, and only an authenticated model completion records
success. Forced model interventions do not fabricate provider-call spans.
Provider-event suspension uses `harness.provider_event_wait` with the public
`provider_event_delivery` guarantee. A delivered original event settles that
wait; stream exhaustion without delivery records `Unsettled`. Original errors
without retained cause evidence abandon their owned wait as `Unsettled`. The terminal append is submitted
without delaying delivery to the original model loop. These shared spans replace
the separate native-only `provider-event` wait observations.
Followed tool failures publish `harness.tool_failure` with a typed failed outcome
referencing the original committed `ToolFailed` observation. The optional journal
receipt capability uses the same conditional append and returns its original
commit, path and zero-based sequence; exact retries authenticate that original
record instead of deriving identity from the current tail. Conditional refusal
returns no record. Unfollowed failures keep ordinary append and incur no receipt
lookup. Original receipt confirmation is the extra read that buys durable cause
identity on an exact retry; there is no second journal or repeated error payload.
The per-poll causal context retains the admitted producer as well as its parent.
SDK mechanisms use shared `trace::observe` to inherit the owning agent stream
without another subscription or registry lookup. With no active owner it checks
only local context and invokes the original work. The atomic `Stream::head`
boundary now emits `stream.head` / `atomic_history_durability_origin` through this
path, preserving the original head and unknown byte count. Diagnostic publication
suppresses this context for every poll, including owned terminal appends, so a
backend's own SDK metadata reads cannot recursively trace diagnostics. Scope exit
restores the caller's original owner; unrelated futures never inherit that owner.
Bounded `Stream::read` and keyed `seek` share cursor admission and inherit the
same active owner through `trace::observe`. `stream.read_open` names the bounded
ordered-record cursor guarantee; `stream.seek_open` names bounded header-key
selection. Their successful terminals describe opening the original cursor,
without consuming records or estimating delivered bytes. Replay-page spans remain
the separate delivery boundary and parent these admission spans. Inherited starts
now submit their original append before polling work and expose their causal
identity immediately. A terminal captures the return timestamp before waiting
for that start in its owned append continuation, so ready work and ready records
return without diagnostic IO while their original start and terminal remain
owned. A failed start cannot publish a detached terminal. `Recording::start`
awaits the actual retained receipt rather than inventing publication identity.
`Stream::fork` and bounded `children` inherit this same producer. The commit span
names the immutable selected prefix and its origin; the child-page I/O span names
the bounded direct-child page at its namespace cut. Both return the original
receipt or page unchanged, report unknown bytes, and add no diagnostic wait after
the original result. Requests without an active owner use the original futures.
Single, conditional and batch SDK appends share `stream.append` at their original
batch boundary. Its guarantee is the atomic contiguous append decision: both an
original committed receipt and a linearizable tail conflict complete that
request, without confusing a refused write with an applied effect. Bytes remain
unknown, and the original retry key and outcome pass through unchanged.
SDK `follow` admits the original ordered history-and-live cursor through
`stream.follow_open`, then reuses the same demand-driven record delivery wrapper
as finite reads. Snapshot and selection stay attached to the original cursor;
there is no collector, prefetched trace queue or new wait per idle child. Following
the active diagnostic source bypasses both wrappers, as reading that source does.
A delivery wait returning EOF on an original live selection has not observed
settlement and records `Unsettled`. A finite selection can complete successfully.
This classification uses the cursor's original selected end, without another
head read, and leaves its snapshot, delivered records and EOF unchanged.
The stock executor's owned and borrowed journal writers share one lock entrance.
`harness.journal_writer_lock` names the serialization of each read-tail/commit
transition and observes the original mutex acquisition; it returns the original
guard without extending it through a diagnostic wait. The SDK value observation
helper shares the same admission and terminal implementation as result observation,
so infallible mechanisms need no invented error or parallel recorder.
A registry-bound producer also carries its own admitted lease count. The final
lease retires new inherited child observations even when the original request
still retains its recorder or another agent remains followed. This is an inline
atomic check without a registry lock, allocation or scan; overlapping leases keep
the same producer active. Already-admitted spans retain their terminal custody.
Standalone explicitly constructed recorders remain explicit producer capabilities.
Admitted read cursors retain that original owner for lazy record delivery after
opening. `stream.record_delivery` waits on the original next result, reports the
actual delivered value bytes (zero at normal finite exhaustion), and submits its
terminal without another suspension. The original snapshot/selection and cursor
are preserved through `Opened::map_cursor`; an unfollowed open adds no wrapper.
Each next operation checks the original owner's demand, so a retained cursor adds
no new spans after final unsubscribe. Reading the active trace source bypasses
self-observation to prevent diagnostic-query feedback. Owned observations abandoned without retained
cause evidence submit `Unsettled`; incomplete admission or publication remains explicit coverage.
`OwnedRecording` retains one private terminal guard. Explicit finish consumes it;
dropping an admitted handle without settlement submits `Unsettled` through the
same original append producer. It never invents a failed cause or physical
cancellation. Native recorders retain the original runtime handle so terminal
work can be submitted after leaving that runtime's scope. With no available
executor or unsuccessful publication, the retained start still identifies missing
coverage. Raw caller-owned `Recording` values continue to require explicit finish.
The shared Actors Store invoke entrance, input publication and terminal wait also
inherit the active SDK producer. Their spans name journaled actor results, stable
input identity and original terminal receipts; known bytes come from the original
input record or response. `trace::observe_outcome` classifies the original typed
result only when observed. A committed guest failure references the original call
journal terminal's commit/path/sequence, cancellation uses its committed outcome,
and other uncertainty remains unsettled. The original handler, ticket, receipt and
reconciliation paths are unchanged. These spans replace the three corresponding
Actors ad-hoc operation wrappers.
The shipping local-harness DST recovery regressions now persist a version-one
`replay-seed.json` before execution and retain its original checkout/state world
through crash, recovery, failure and unwind. The controller reads that file for
scheduler seeds, semantic crash point, prompts and provider response. The emitted
`DST_REPLAY_SEED` path can drive the existing regression through
`SDK_DST_REPLAY_SEED`; this recreates the same recovery case rather than treating
a receipt reference as executable input. The retained world is not automatically
deleted. Production-wide capture of arbitrary failed runs and their external
nondeterministic inputs remains outstanding.

For example, set `SDK_DST_REPLAY_SEED` to the emitted file, then run
`cargo test --locked -p acyclic-harness --features test-support --test main dst::recovery::live_f3_killed_head_accepts_a_new_prompt -- --exact`.
The seed selects either supported crash phase; no second test runner is required.
Harness Stream failures use one source-owning `StreamFailure` on ordinary,
native and WebAssembly paths. Request-boundary classification remains explicit;
native provenance and admitted allocation custody remain attached. Journal
reads no longer replace the original provider cause with its formatted text.
General Stream-to-harness conversion retains the original agent classification,
including its source-based retry and effect decisions. This closes the cause gap;
the remaining bare error constructors elsewhere still require migration.

Typed `Compared` deltas carry a request, output or cache-prefix comparison in
an explicit identity domain: the preceding original publication and its
canonical integrity digest, plus the current original digest. They contain no
payloads or credentials. The shared query computes `repeatedCalls`,
`repeatedOutputs` and `cachePrefixBreaks` by comparing those original digests.
Each domain reports both equal and differing observations and retains the last
original publication of each. Absence of comparisons establishes no correlation
coverage; zero observed repeats does not prove there were no repeats. The
preceding publication can lie outside the selected file window or producer
clock domain. Equality needs no retained completed-call index or earlier-history
scan. A comparison neither starts nor settles its owning span. Producers may
publish one comparison per domain per span using the original idempotency key.
Automatic correlation at every provider/tool boundary remains to be wired to
their original canonical descriptors; names and durations never imply identity.

## Original model recovery observations

Generation recovery emits `ModelEvent::Wait` through the original model stream.
A start contains the typed transport cause, exact selected delay and whether the
provider supplied that delay. A finish contains the transport clock's measured
elapsed duration. Neither event contains headers, credentials or remote error
text. The ordinary model journal records each event once, including waits before
response acceptance and after an interrupted accepted response.

The original timer alone controls readiness. Cancellation or timer failure leaves
an unfinished start rather than fabricating a successful wait. Recovery events
do not enter assistant context, reset output or accumulate in the active model
attempt. Generation waits no longer publish a duplicate text log or native wait
report. Token-count requests still use their existing future-based recovery
reporting.

Waits occupy typed original journal records, without an encoded event string or
payload object. `acyclic inspect` projects their starts, successful finishes and
measured elapsed nanoseconds through its existing bounded usage pages. This adds
no payload reads, earlier-history scans or per-call state. Unfinished starts are
explicit: when starts and finishes differ, the elapsed subtotal omits unfinished
timers. Durations outside the nanosecond counter's representation remain unknown.
Model call duration includes recovery waits; these values overlap and must not be
added together as disjoint elapsed time.

## Live session inspection

Execution observations expose `committed_at_micros` from their original Stream
record. Replay and indexed selection preserve that publication timestamp; they
never sample a new clock or add it to the stored execution payload. Journals
without an original publication clock return `null`. This measures publication,
not provider dispatch, tool execution or physical durability. The publication
clock inherits Streams' ordering guarantees; it is not a monotonic elapsed-time
sample and must not be mixed with a provider's monotonic durations.

`acyclic inspect SESSION` writes a version-one JSON snapshot from the selected
session's authenticated streams. SESSION is a canonical path, complete stable id
or exact alias. `--agent PATH` selects an agent within that session; it never
searches the tree. Inspection does not change the selected session or dispatch
model work. It requires the existing session service, selected through
`--workspace PATH` or `--endpoint ADDRESS`.

`--follow` emits changed snapshots from the original status/usage event waits,
without periodic reads. `--children` reads one direct-child page instead; use
`--cursor`, `--page-size` and `--page-bytes` to continue discovery explicitly.
Hidden descendants and their history are never visited. Follow and child-page
selection are separate operations.

The shared `Conversation::inspect` query exposes the last observed phase,
original usage totals, those same contributions grouped by admitted provider,
the sum of original completed-call monotonic durations, context occupancy and
the direct open-child count. Tool admissions, successful settlements and typed
failure counts come from those same journal pages, without retaining call
history or copying tool/error bodies. Counting these facts requires no additional
reads beyond the original journal page. Publication clocks are not tool execution
durations; tool timing requires an original execution measurement.
Unknown counters and durations retain their
coverage; partial known subtotals must not be displayed as complete totals.
Each completion replaces its matching admission. Status and usage retain their
individual source frontiers, rather than claiming an atomic cross-stream cut.
`synchronized` describes initial usage replay reaching the live frontier.

Trusted feedback completions contribute category counts under `tools.feedback`
in the same inspection snapshot. Agent-authored cost estimates remain reports;
they do not change measured failure, token or latency totals. The query loads
only results carrying the producer's trusted feedback marker. It retains five
category counters, not report bodies or call history, and writes no counter
stream. Ordinary tool results cause no payload or invocation read. Full reports
use the existing execution page and payload query: select the original marked
`ToolCompleted` record and its `result` field. Agent, operation, step, sequence
and publication time come from that original record, not authored JSON.

Provider token counters retain their original billing conventions. Cache rates
must account for inclusive/exclusive cache units; a raw division cannot establish
that contract. A running phase alone is not evidence that an agent is stuck.

Rust crates emit `tracing` spans and events; hosts and tests decide whether
anything records them. `acyclic-observability` owns result recording and the
operation lifecycle projection. Work accounting stays in `WorkCounters`, and
spans only summarise it.

## Model usage JSONL

`graphcoder --usage-jsonl <path>` and `Conversation::write_usage_jsonl` use the
same paged tree exporter. Each line is a version 3 `ModelUsageProjection`:
`version`, `usage` (a version 3 `ModelUsageRecord`), and `estimated_cost` (the
selected price provenance and derived estimate). Older formats are refused,
with no migration or inferred replacement values.

`usage.started_at` and `usage.completed_at` are optional unsigned Unix
nanoseconds, observed immediately before live provider dispatch and on arrival
of its terminal event, respectively. The shared wall clock has millisecond
resolution. The terminal commit follows the completion observation; a staged
artifact alone does not certify completion. `latency_ns` and `ttft_ns` retain
their monotonic elapsed-time meaning. Wall-clock corrections can place
`completed_at` before `started_at`; do not compute latency by subtracting them.
Missing or unrepresentable observations are `null`. An unfinished admission
and a completion recovered without its live dispatch clock retain unknown
times. Replaying a committed call preserves its original times.

`usage.agent_path` is the canonical `AgentPath`, not a separate parent ID:
`harness/agents/<root-id>/children/<child-id>` identifies a child of
`harness/agents/<root-id>`. Each deeper child appends `/children/<id>`; removing
that final pair yields its parent. Roots have no parent. An executor without
an agent binding records `null`. `dispatch` identifies the original call,
`purpose` distinguishes response and summary calls, and `completed` reports
whether a terminal observation was committed. Provider counters remain in
`tokens`, with unknown counters preserved as `null`.

## One operation lifecycle

Attach one `acyclic_observability::Operations` layer to the host's existing
registry. It derives one latency histogram sample and one outcome counter from
each `acyclic.*` span declaring `outcome`. Record completion through the shared
`finish` or `record_outcome` on the operation's owned span. Metrics stop at the
first explicit completion, even if descendants retain its handle. A closed span
without a result is `incomplete`; this does not assert that its accepted work
failed. RPC method names distinguish operations using the same span factory.
`Completion` owns unfinished work independently of tracing handles, so producer
drop publishes `incomplete` immediately. Caller-owned read/RPC cancellation may
retain its explicit `cancelled` error contract through the same guard.

The layer's `Sink` contract is the OpenTelemetry export boundary. Exporters
receive the original static metadata, method, elapsed duration and outcome;
they share the same lifecycle implementation. Sinks must not wait for I/O.
`Export::channel` is the shared adapter for credited output. The host supplies
record credit and drains its receiver in its existing export loop. Full credit
and receiver closure refuse samples immediately and increment separate loss
counters; neither changes the operation result or prevents cancellation.
Records retain credit through consumer processing. Exact retained-record high
water is updated at admission, so between-snapshot spikes remain visible.
Optional first `error.kind` detail survives later generic errors and is never a
metric label. This records the original declared cause; it does not diagnose a
provider from an operation ID or reconstruct a terminal result.
Native `WaitReport` remains the live-wait projection on this registry, preserving
its own producer, retirement and diagnostic request/acknowledgement guarantees.

Harness child waits export `StatusRecord.child_wakes` through the existing status
stream and SessionFrame projection read by GraphCoder and benchmark captures.
Each sample identifies a direct child relative to the status owner and retains
`settled_at_micros` from the original `ChildSettled` or `ChildDone` inbox commit,
and `woke_at_micros` from the parent delivery clock where the wait resumes, before
the driver runs. Each admitted completion stores one fact in that step's ordinary
status append. It shares the actor's atomic commit: no separate request, commit,
quorum read or fsync is added. The waiting checkpoint retains only the first
fact's status sequence. The returning status references the dense original range
and its return clock; partial completions emit no wait-return measurement.
Consumers page samples with the authenticated owner/status query and its original
status-sequence continuation. Each query reauthorizes the owner and resolves the
original status before reading the requested facts. Checkpoint and record sizes,
subscription count and wait-return work are independent of completion count. Both
timestamps are Unix microseconds (the delivery clock has millisecond resolution).
Clock skew remains visible in the raw pair; an absent sample is unknown. These
are wait-return measurements, not provider-call or tool-result-publication times.

Terminal samples preserve their original registry ID and immediate parent ID.
The parent is captured at creation and follows the record across worker handoff
and credited output without retaining either span. Native durability attempt
fields project into the same sample as `DurabilityAttempt`: requested guarantee,
actual syscall, attempt-local opaque descriptor and first native error code.
Unknown fields remain absent. Stage markers supply no attempt evidence, and this
projection supplies no provider/group publication range or device-drain claim.

With no subscriber there is no metric storage, timer, allocation, lock, task or
syscall. The operation macro evaluates no fields on wasm32. The shared crate
keeps tracing dependencies out of wasm32 builds.

## SLO files and live queries

An SLO is a version 1 JSON file in a volume, read and followed using the ordinary
authorized volume reader. It selects an operation and optional RPC method, a
success fraction in parts per million, and an optional latency objective. The
live view is a selected metrics query, woken by that operation's completion
watch. Losing the producer returns a typed error. There is no SLO endpoint or
separate settings service. See the [file format and query contract](../rust/crates/observability/README.md).

Empty cohorts and counter precision loss are unknown. A histogram bucket crossing a latency threshold is
unknown whenever that ambiguity can change the verdict. Evaluating a query for
another operation or an unsupported file version refuses by type.

## Dependencies

- Libraries never install a subscriber.
- A library declares `tracing.workspace = true` under
  `[target.'cfg(not(target_arch = "wasm32"))'.dependencies]`. The workspace
  dependency enables only `std` and `attributes`. Its span tests use
  `tracing-subscriber.workspace = true` under the matching `dev-dependencies`
  table.
- Own the operation span explicitly when recording completion; filesystem
  `obs::span!`/`in_span`/`scope` erase tracing on wasm32. Attribute instrumentation
  remains suitable for spans that do not record completion through current-span
  helpers.
  Each crate has a private `obs` module whose `obs_event!`/`obs_record!` macros
  expand to nothing on wasm32.
- wasm32 builds stay free of tracing. Nothing installs a subscriber there, and
  each callsite would add roughly 0.2–0.4 KB to the wasm artifact. Check with
  `cargo tree -e normal,build --target wasm32-unknown-unknown -p <crate> -i tracing`,
  which must print nothing.
- Do not use `tracing`'s `max_level_*` features. `--all-features` runs would
  unify them across the workspace.
- `tracing-subscriber` and `tracing-chrome` are for hosts (the plugin) and tests.
  `divan` is for `benches/`. The workspace leaves `tracing-subscriber`'s `ansi`
  feature off, but `tracing-chrome` turns its default features back on. A
  plugin `fmt` layer that writes to a file therefore calls `.with_ansi(false)`.

## Names and levels

- Span names are `acyclic.<family>.<op>`. The family is one of `fs`, `stream`,
  `objects`, `harness`, `runtime`, `machines`, `inference`, `actors`,
  `workers`, `auth` or `plugin`. Native-mount callbacks are `acyclic.fs.mount.<callback>`.
- `target` stays the default module path, so `ACYCLIC_LOG=acyclic_fs::kernel=debug`
  works.

| Level | Use |
| --- | --- |
| `info` | Public API entry points and RPCs |
| `debug` | Kernel and internal phases, fsync, recovery, compaction; mount `fsync`/`flush`/`rename`/`create`/`setattr`/`release` |
| `trace` | Per-item, per-page, per-frame and other per-mount-callback work |
| `warn`/`error` | Only failures no caller sees (background pumps, retries) |

## Fields

Declare fields as `tracing::field::Empty` and record them when the operation
ends.

- **Ids:** `workspace_id`, `volume_id`, `generation` (short hex), `stream_id`,
  `rev`, `task_id`, `machine_id`, `run_id`, and `ino`/`fh` for mounts.
- **Counts:** `items`, `bytes`, `fsyncs`, `frames`, `batch_len`.
- **Retries:** each physical `acyclic.stream.grpc.call` span records its singular
  `attempt` ordinal. The logical read or follow span stays open through terminal
  completion or consumer drop.
- **Result:** `outcome` is `"ok"`, `"err"` or `"incomplete"`. `error.kind` is a stable
  `&'static str` from the error enum's `fn kind(&self) -> &'static str`; add
  that method to any error enum that lacks one. RPCs record `rpc.code` only for
  received transport status (the tonic `Code` or HTTP status). A streaming
  semantic decode failure records `invalid_response` and leaves the code unset;
  unary semantic rejection retains a received `OK` code.

Record completion on the operation's own span handle. A filtered child span
must not record on its visible parent. Preserve span and subscriber context
when work crosses a worker boundary; enter spans only while polling or running
the work, never across an unrelated asynchronous wait.
Retain caller ancestry when an operation span is filtered, while recording
completion only on the operation handle. Destroy retained span/context handles
under their originating subscriber, including cancellation and unwinding.

Objects RPC success includes body consumption and decoding. Streamed download
success requires validated EOF and the selected length, rather than headers or
merely receiving the expected bytes. Terminal errors record `err` and their
semantic kind. Once an RPC span is created, dropping an unfinished RPC future
or returned body records `err`/`cancelled`, including an unpolled returned body.
A public async RPC future that is never polled creates no span.
This is the consumer's lifetime: submitted native work may continue after
consumer cancellation and retain its own span until actual completion. Native
filesystem receipt spans record an explicit result only when the operation
returns. Abandonment without an owned terminal result projects `incomplete`;
it does not reconstruct a result from cancellation.

Stream gRPC bodies also require EOF for success and record `cancelled` on
unfinished consumer drop. Detached provider work
records its actual terminal worker result even when its caller was cancelled.
Portable and native provider `read`/`follow` operations observe cursor opening,
as defined by their public contracts. Returned cursors retain their original
late error and native accounting mechanisms; opening is not evidence of EOF.

**Forbidden fields.** Users attach trace files to bug reports, so never record:

- secrets, tokens, `authorization` or other metadata values, or environment values
- request/response bodies, file contents, prompts/completions, tool arguments or outputs
- user paths or file names (record `path_depth`/`path_len` instead)
- `%error` or `?error`, since `Display` can embed paths (record `error.kind` only)

Program names are allowed. Program arguments are not. HTTP routes are static
constants, never the request path. The same rules apply to the TypeScript
`AcyclicObserver` hook.

## Cost

- Always use `skip_all` with explicit fields. Values are integers or
  `&'static str`. Never use `format!`, `%`/`?` or `to_string()`.
- Never open a span per item inside a loop. Use `trace!` events, which cost one
  atomic interest check when disabled.
- Put work summaries behind `if tracing::enabled!(..)`.
- With no subscriber installed, overhead must stay under 1% on the fs commit
  bench.

## WorkCounters bridge

Do not add a second set of counters.

- The returned `OperationReceipt` or `OperationFailure` owns the original work
  evidence. `obs::measured_on` and `measured_on_with_kind` borrow that evidence
  for success and failure alike, without another provider read or result copy.
- Receipt-returning fs spans declare three `Empty`
  summary fields, filled from the receipt:
  - `work.items`: `items_examined`
  - `work.bytes`: `object_bytes_read + object_bytes_written + authority_bytes_written`
  - `work.durability`: `durability_operations`
  These are diagnostic summaries. An overflowing byte sum replaces any earlier
  byte summary with unknown; use the original receipt's individual fields for
  exact accounting. Unrecorded fields are unknown. There is no separate
  `acyclic.work` event ledger.
- Kernel, native capture, materialization and other nested spans can summarize
  work already included in their caller's receipt. Do not add those summaries
  together or infer a physical syscall count from logical provider requests.
- `NativeReadMeter` feeds the original invocation's work owner at the actual
  instrumented attempts and completions. Operation and byte reservations follow
  accepted work through cancellation; owned allocation credit retires after the
  capacity is freed. Its production filesystem implementations update the same
  work ledger used by the receipt. They do not count uninstrumented OS retries,
  threads, locks or timer registrations.
- `AllocationInfo` measures the current thread. OS resource and descriptor
  readings are available on demand; fields can reflect different instants.
  Neither establishes worker coverage or a process high-water mark. A maximum
  of periodic snapshots can miss an entire resource lifetime. Exact peaks
  require updates at the original resource admission and release events. Use
  the shared collector's event-fed high-water counter for admitted export
  records. Physical quantities without their original accounting hooks remain
  unknown.
- Convenience entry points that delegate to an instrumented implementation
  share that implementation's span. Blocking workers retain the caller's span
  and scoped subscriber through the actual work, including after caller drop.
  A retained worker span is not evidence that its caller succeeded. Its
  original worker records the terminal result; lost producers project
  `incomplete` without asserting that accepted work failed.

The shared harness conformance observer defaults to request counters without
retaining commit, append or follow history. The finite dispatcher and publication
fault vectors explicitly select request capture to compare exact retry bytes and
keys. They consume the captured evidence rather than cloning its history at
assertion time. Scale measurements use the counter-only observer. These logical
counts remain separate from original physical work receipts.

## Enablement

With none of these variables set, nothing is installed and output is unchanged.
The plugin never writes tracing output to stdout, which carries JSON-RPC.

| Variable | Effect |
| --- | --- |
| `ACYCLIC_LOG` | `EnvFilter` directives for a `fmt` layer on stderr. In `__service` mode it writes to `<state dir>/logs/service.log` instead. |
| `ACYCLIC_LOG_FILE` | Overrides the `ACYCLIC_LOG` destination |
| `ACYCLIC_TRACE_FILE` | Writes a Perfetto-compatible Chrome trace. `{pid}` is replaced by the process id. |
| `ACYCLIC_TRACE_FILTER` | Filter for the trace file. The default is `acyclic_fs=debug,acyclic_stream=debug,acyclic_objects=debug,acyclic_native_runtime=debug,acyclic_plugin=debug`. |
| `ACYCLIC_PERF` | In TypeScript, `ACYCLIC_PERF=1` (or `globalThis.ACYCLIC_PERF === true`) records `performance.mark`/`measure` entries named `acyclic.<family>.<op>`. |

The plugin service inherits its environment when it is spawned. Drain it to
apply a change. `doctor` prints the active filter.

File logs are best-effort diagnostics: the active file and one `.1` archive are
each bounded to 4 MiB, with one bounded `.1.next` staging file during rollover.
Writers attempt the shared active-file lock once; contention rejects a record
without waiting, and a writer opened while busy remains usable afterward.
Oversized records are rejected. I/O failure can leave a partial record; accepted
writes and `flush()` provide no durability guarantee.

## Tests

- Each instrumented crate has one span test. It installs a capture layer of
  about 30 lines on a `tracing_subscriber::Registry` with
  `tracing::subscriber::with_default`. It then asserts that one representative
  operation emits its span with the required fields, and that no field is named
  `path`, `token`, `content`, `body` or `authorization`.
- Span tests go in the crate's existing test modules or existing `tests/*.rs`
  files. Each new `tests/*.rs` file is another linked test binary, which slows
  CI. There is no shared test-support crate.
- Gate span tests with `#[cfg(not(target_arch = "wasm32"))]`.

## Profiling a slow test

Each CI lane summary lists the slowest Rust tests with a `cargo nextest run`
command that reruns one of them alone. Test processes install no subscriber,
and there is no shared test-support crate to add one per test, so a test trace
is a local, uncommitted edit: add `tracing-chrome = "0.7.2"` to the
crate's `dev-dependencies` and wrap the test body:

```rust
use tracing_subscriber::prelude::*;
let (chrome, _flush) = tracing_chrome::ChromeLayerBuilder::new().file("slow.json").build();
let _trace = tracing::subscriber::set_default(tracing_subscriber::registry().with(chrome));
```

Open the file in [Perfetto](https://ui.perfetto.dev).

## Benchmarks

Filesystem and Objects own their [divan](https://docs.rs/divan) hot-path
benchmarks. The private conformance crate retains the agent and host diagnostics.
Run these benchmarks on demand; no schedule or per-merge execution is required.

```sh
cargo bench -p acyclic-objects --bench objects
cargo bench -p acyclic-fs --bench filesystem
cargo bench -p acyclic-fs --bench filesystem_alloc
cargo bench -p acyclic-conformance --features local-runner --bench write_validation
```

Pass divan options after `--`, for example `-- --sample-count 10 put_small`.
Name the bench with `--bench`: the library's test harness rejects those options.
The `bench-fs` and `bench-objects` examples in their owning crates measure whole
workloads (see its README).
