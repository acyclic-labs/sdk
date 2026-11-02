# Research metrics

`research` explicitly enables additional capture in the harness and provider
adapters. It is off by default. Provider adapters use the core's unified setting;
ordinary builds omit first-delta timing and HTTP identity capture. Sampling is a
normal request contract, independent of research capture: `sampling.temperature`,
`sampling.top_p`, `sampling.seed` and `max_output_tokens` participate in the
original request digest and survive request continuations and recovery. An absent
sampling value delegates to the provider. A seed is a hint, not a determinism
guarantee; protocols without a seed field refuse it.

The private per-call JSONL record is version 2, committed with the original
terminal model observation. Research records retain sampling, the requested
output ceiling, `provider_request_id`, `system_fingerprint` and `ttft_ns`.
The HTTP request identifier is distinct from the provider's generation `id`.
TTFT measures dispatch to the first observed nonempty text or reasoning delta,
including preceding journal backpressure. It remains unknown for a tool-only
response, recovery without the original clock, or a platform without that clock.
Completion never waits for HTTP transport closure. A partial response or an
unreferenced staged file cannot become a completed usage record.

`metrics-export` separately enables `acyclic_harness::metrics`, an offline
projection into Arrow IPC streams or uncompressed Parquet. It adds no work to
execution. `journal_page` replays 32 original records once, loads only referenced
terminal usage files, and returns independent table batches plus an exclusive
cursor. Model rows reconstruct one original admitted request for its replay key;
the page does not load conversation history or tool payloads.
`inbox`, `conversation` and `status` project one original agent record each.

Schema v2 has six tables. `metrics::VERSION`, the row `version` and Arrow
`acyclic.metrics.version` metadata identify this layout independently of the
usage JSONL and cache-observation formats. Version 2 includes scope ownership
and nullable agent ownership. Union shards only when their version and table
match. Earlier saved layouts must be reprojected from original sources before
combining them with v2; the library does not migrate shards or fill missing
ownership fields with null. Arrow, Parquet and PROV writers refuse a foreign
layout before emitting bytes.

| Table | Original observations |
| --- | --- |
| `agents` | Adoption, parent and immutable role pin |
| `turns` | Journal start and settled conversation entry |
| `model_calls` | Admission and committed completion, requested sampling, reported usage, timing and identity |
| `tool_calls` | Admission, completion or typed failure, with original artifact references |
| `edges` | Forks, messages, notices and child completion |
| `outcomes` | Settled turn outcome and terminal agent status |

Every row retains `version`, nullable `agent_path` and `scope_path`, `operation_id`, `source_stream`,
`sequence` and `kind`. Admissions retain identity with null measurements;
completion rows retain the original measurements. Calls join on agent,
operation, step, purpose and request digest; tool calls join on agent, operation,
step and call id. Source stream and sequence identify an observation. Consumers
must select the latest observation for a call before aggregating, rather than
counting its admission twice. Agent outcomes have a null operation identity;
turn outcomes retain the driver's original identity.

Unknown values remain null, including absent counters, timing and prices. Zero
is retained only when it was reported or derived from known values. An explicit
immutable `ModelPriceRecord` selects the existing usage cost projection; the
exporter does not infer a price schedule, currency, wall time or failure label.
JSONL usage, Arrow/Parquet and Inspect share one admission of the original
terminal model observation and its immutable usage artifact. Nonterminal
sources, foreign operations/agents and contradictory counters or reported
metadata refuse before completed rows are returned. Missing usage remains
unknown. An available indexed admission must also match the completed call's
request digest before supplying its replay key; an unsupported index leaves
that key unknown rather than scanning history.
Artifacts are original immutable `FileRef` values encoded as JSON strings.
Edge endpoints retain their original type (`agent`, `user`, `host` or an opaque
`producer`); consumers must not infer an agent from a notice source's spelling.
Waking notice producer payloads remain references for later provenance resolution.

Research usage also retains the original version-1 `ModelCacheObservation`.
The model-call columns preserve its credential scope, profile prefix, final wire
prefix and parent digest, requested TTL and wire-message count. Absence remains
unknown. Prefix edges are emitted only with a known credential scope; equal
prefixes from different accounts never join. A prefix edge proves projected
identity, not that the provider retained or hit that exact cut. Requested TTLs
do not prove residency, and per-call cache-read counters cannot identify a
shorter prefix that was hit. Original token counters remain authoritative.
`cache_savings_nano_units` uses the existing price calculation as an exact signed
Decimal128 value: cache creation premiums can make net savings negative.

Write each returned batch with `metrics::write(&batch, writer, format)` as its own
shard. This bounds Arrow and Parquet file indexes by one page. Union shards in
the analysis tool. Durability and atomic publication belong to the caller's
volume commit; the encoder performs no fsync, retry or remote call. At the current
tail, wait on the source stream's event rather than repeatedly requesting pages.
Journal and writer failures propagate to the caller; no recovery loop is hidden
inside the exporter.

Compile this optional surface with
`cargo check -p acyclic-harness-core --features metrics-export,research`.

`provenance-export` adds `metrics::provenance::write(&batch, writer)`, a streaming
[W3C PROV-O](https://www.w3.org/TR/prov-o/) projection in
[RDF N-Triples](https://www.w3.org/TR/n-triples/). It consumes the same metrics
batches, without another journal reader or capture mechanism. Each row produces
a fixed number of triples. Source-derived, domain-separated identifiers make
independent shards joinable; concatenate their triples and let RDF's graph set
semantics handle repeated assertions. No growing deduplication index is kept.

Turns and calls are activities associated with their agent. Original request
identities and artifact references are entities; observed messages, forks,
notices and child completion express influence. A notice's unknown producer
stays unknown. An artifact reference alone does not attest who generated that
file or that a model read it; this exporter makes neither inference. Observation
entities have one generating activity, even when a call belongs to a turn.
Preserve the original producer's agent path, operation, source stream and cursor
when exporting inherited history. Do not relabel inherited observations as
new observations of the child.

Only inline identifiers and scalar references are retained while writing a row.
The RDF codec handles literal escaping. The writer is neither flushed nor
synced; the caller publishes only after success with its existing volume commit.
This projection is independent of per-turn file-read coverage: completeness
requires observations from the original authenticated read producer, including
native reads satisfied from the OS cache.

With `research`, `executor::read_set::page(journal, agent, operation, after, cut)` projects original
successful builtin read-tool receipts. It retains the result's `FileReadSource`
and optional returned interval. An authored selection keeps its original volume,
path and generation; it does not invent a full-file digest. Absent intervals do
not imply a full-file read. The original indexed tool admission binds each
completion; adapters without this index refuse instead of rescanning history.
Each page visits up to 32 journal records and retains only that page's receipts.

Supply `cut` from the original settled execution journal sequence, distinct
from an agent inbox cursor. In research mode, the stock executor supplies
`TurnOutput.journal_cut`: version 1, original execution operation and the
accepted terminal model observation's sequence. The original journal append
returns this sequence on fresh publication and exact retry; replay retains the
same original record. It performs no tail lookup, extra append or durability
barrier. `ExecutionJournal::sequenced_appends` exposes a stable
`SequencedExecutionJournal` capability for the immutable binding. Its append
returns the original sequence or a typed error. A custom journal without this
capability leaves the cut unknown on both live completion and replay; a saved
completion's sequence alone cannot upgrade that coverage.
Pending, failed or cancelled execution without a successful output cannot
certify closure. Settlement must bind the cut's operation to its original turn.
Use one
`read_set::Writer` across the pages, then call `finish` and publish its bytes once
as the turn's immutable read-set `FileRef`. Its version-1 JSONL header declares
`committed_read_tool_results_only` coverage, agent, original operation and cut;
the footer certifies that every source position through that cut was visited.
Gaps, repeated pages, foreign bindings and failed writes prevent successful
finish. It performs no flush or publication itself. A short source page is an
incomplete pinned prefix, never evidence of completion.

`read_set::project` maps each receipt into the existing edges table, and PROV
exports the original turn's `prov:used` relation to that generation. Shell,
context, hook, search and failed-read coverage remains unknown. These functions
reuse committed results and add no work to execution or read operations.

`executor::read_set::source` exposes the same artifact as a pull-based
`ContentSource`, retaining one encoded page and source page. Cursor records
preserve progress across pages with no reads. EOF follows successful closure;
an incomplete prefix returns an error. Feed this source to the shared
`ContentPublisher::stage_stream_once` with an operation derived specifically
for the read-set artifact. Other outputs from the turn retain their own
publication identities. The filesystem implementation hashes the consumed
bytes during original immutable chunk staging and publishes them with metadata
in its original transaction. It never buffers the complete artifact, starts a
worker, or flushes per chunk. Source retirement or cancellation prevents a
successful publication. Original native allocation custody is required for
native publication; an ordinary stream cannot assert that custody and refuses.

Model and tool tables carry the existing canonical `model::replay_key` of the
step's admitted request. `ExecutionPayloadField::AdmittedRequest` reconstructs
request segments and verifies the original admission digest in one shared
reader. Hashing the stored segment manifest would give a different identity.
The key permits content comparisons across fresh executions and forks; the
dispatch `request_digest`, operation and step still identify the original call.
PROV annotates the call with this key without merging distinct activities.
Each admission reconstructs one request. Dependent rows select one original
admission through the authenticated step index; unsupported custom indexes
leave those keys null for an offline join to the original admission. Export
adds no provider request or execution-side capture work, retains one request
at a time, and performs work proportional to that request's canonical bytes.

Research capture is controlled by operator-only `sys/capture`. These offline
library functions consume original committed records and immutable artifacts;
they do not install an actor, plugin or background subscription. A scope is
the grouping root; independent root agents do not acquire a fabricated parent.

Future call integration uses the original caller-owned `calls/<id>` reply path
and its terminal record, rather than a separate capture settlement carrier.
Publish a read-set artifact only after its declared source cut closes, and bind
its reference to that original call. A missing terminal or failed/cancelled
producer leaves coverage unavailable; it never certifies an empty read set.
Live consumers use the original durable read and alarm rather than polling.
Status and counts remain the existing phase projection and `counts(range)`;
the exporter adds neither a status writer nor execution counters.

The following row kinds use the same six tables. Their original producers
remain separate from this offline codec:

| Table / kind | Original authority and coverage |
| --- | --- |
| `agents` / `placement` | `metrics::placement::project` consumes the original settled `MachineBinding` v2 and stream envelope. Coverage is logical provider placement; physical location and resource measurements stay unknown. Requires `machines`. |
| `edges` / `stream_record_shared` | `metrics::stream_records::project` preserves an authorized original record's account, origin, sequence and commit ID, separately from the selected destination. Coverage is logical record-value bytes; union original identities before summing. |
| `turns` / `operation_interval` | Original operation binding, parent and collector clock domain with checked relative start/end coordinates. Unknown Unix placement stays unknown. Actual producer edges must establish causality. |
| `outcomes` / `call_receipt` | `metrics::calls::project` borrows an original Actors `call::Journal` receipt and its authorized original terminal or fence record. Scope-owned calls need no agent. |

Interval integration remains reserved. The stream projection
accepts the original `Record`, selected `StreamPath` and authenticated
`AccountId`; it makes no read or provider request. Common nullable
`source_account` (URL-safe base64 without padding), `source_origin` and
`source_commit_id` retain exact original source fields. The common nullable
`source_committed_at_micros` preserves replicated stream commit time; it is not
an execution start or a provider latency. Stream sequences are
zero-based; journal completion sequences use their separate one-based contract.
The `source` edge endpoint is the JSON tuple `[account, origin, sequence, commit]`,
and `target` is `[account, selected_path]`. An observation at its original path
has kind `stream_record`; an inherited reference has kind `stream_record_shared`.
Equal-length or equal-valued independent records retain different identities.
`logical_value_bytes` excludes headers, framing and physical backing;
`physical_copied_bytes` and `physical_shared_bytes` remain null. Zero-length
record values remain known zero. RDF retains the same identity and coverage
without attesting a new content producer.

Live resource files and this offline edge use the same
`live::resources::Sharing` projection over the original record. The shared
`Source` carrier retains the authorized account, selected path and original
envelope before encoding. No payload is reparsed, and physical coverage is
neither derived from inheritance nor filled with zero.

The projection retains one row and does not copy, hash or decode record values.
It encodes account and commit once, formats two endpoint tuples and copies
metadata into the existing Arrow columns. Borrowing dynamic source paths removes
an intermediate owned path. Added execution reads, allocations, provider calls,
commits, barriers and flushes remain 0. An exact logical union is an offline
query, never a growing capture-side seen-content map.

Placement reads the fixed settled position at `AgentPath::machine(target)`
(`from=1, limit=1`), then projects that already-authorized record synchronously.
Position 0 is the provisioning plan. A foreign selected agent/target, wrong
position, malformed binding or unsupported version refuses instead of producing
an empty placement. The selected source path is separate from the original
record origin, sequence and commit, including a shared source record.

The `agents` row contains `binding_version`, original `target`, `machine` (the
complete typed `SandboxRef` as JSON), `provider` (its complete `ProviderRef` as
JSON), and nullable `forked_from` (the original logical Machine ID). These opaque
references are authority identities, not physical locations or credentials.
`coverage` is `logical_provider_binding`; `physical_host`, `region`, `core`,
`numa` and `memory_bytes` stay null. No current configuration, provider usage or
machine inspection is consulted. The RDF projection retains those original
facts without inferring a provisioning activity, clock interval or measurement.

Both stream projections share one original-envelope encoder: account and commit
are encoded once per row. Placement additionally decodes one binding, checks
its selected source, encodes two typed references and formats at most one parent
ID. It retains one row and one binding, with work proportional to their selected
bytes rather than history or agent count. It adds no execution reads, provider
requests, publication commits, durability barriers, flushes or waits.

Call outcomes reuse that same original-envelope encoder. Supply the existing
Actors call reader and `CallPath`, plus its original terminal record, or the
first session-fence record (sequence zero) when no terminal exists. The projection
checks the selected caller/path, terminal sequence/commit binding, and first
fence position. A later append to the same fence path cannot supply a new
settlement commit or timestamp. It does
not replay the protocol or read its journal again. `Journal::receipt_ref` is the
shared borrowed assembly boundary, also used by the owned receipt API; projection
does not copy outcome text or result bytes. The caller remains responsible
for source authorization and the original scope identity. Caller labels alone
do not certify provenance.

`scope_path` is independent of nullable `agent_path`. A scope-owned call has
no fabricated SoftwareAgent in RDF. `caller`, `call_log` and `outcome` retain
the original caller and folded disposition. A cancellation after acceptance is
`indeterminate`, even when the raw terminal says `cancelled`. Missing request
receipt remains null; request sequence/commit are known without claiming its
unknown original origin. A settling `fence` has no `terminal_sequence`, distinct
from a genuine terminal at sequence zero. Coverage is `original_call_terminal`
or `original_call_fence`; `logical_value_bytes` measures that source record,
including a fence marker when selected, rather than inferred callee output.
RDF observation identity also includes the original call log: one session-fence
record can settle many different calls, which remain distinct observations of
that shared source.

No model operation, content type, complete read set, physical work or clock is
inferred from opaque call IDs or result bytes. Projection retains one row,
borrows the original receipt and leaves its payload unread and uncopied. It
encodes original account/commit metadata, scope/caller/log paths and fixed
disposition fields, independently of chunk count, history and payload size.
It adds zero execution operations, waits, workers, commits or barriers. Existing
row constructors now share one base constructor and one envelope constructor;
scope-only and agent-owned facts use the same schema and RDF path.

Existing object identity or `AlreadyExists` alone never
proves zero physical writes, and overlapping sibling durations are not added
into a critical path. Phase counts come from the primitive's `counts(range)`;
no checkpoint resource accumulator or separate snapshot writer is used.
With both features enabled, `metrics::inspect::Call` projects one original
model admission into an Inspect JSON log, also accepted by Docent's Inspect
JSON loader. Read `ExecutionPayloadField::AdmittedRequest` to reconstruct the
request first; the stored `Request` in a later step can be a segment manifest.
`Call::new` checks the original operation, sequence and request digest and uses
the shared `model::replay_key`. Supply the original settled assistant content
and optional committed `ModelUsageRecord` to `Call::write`. Foreign usage is
refused. Scope, agent, stream, operation and sequence remain the observation
identity; the replay key groups equal content across observations.

Replay requests and recorded model events use the same complete-value decoder
as admitted requests, without serde's unrelated default nesting ceiling.
JSON embedded in harness-owned text first passes the saved-record codec's
existing depth admission. Encoded strings share that allowance; they cannot
restart it. Deeper or malformed embedded content remains opaque text, with UUID
normalization preserved, rather than rejecting the request or recursively
constructing an unbounded tree. Opaque tool JSON keeps its portable record
contract; malformed outer JSON and trailing data are refused.

Prefer `Call::settlement(journal, terminal_record)` when the original terminal
record is available. It checks the original operation, step, purpose and source
order before reading that event and its optional usage artifact. The shared
usage validator binds accounting to the terminal's original metadata. Its
`Settlement::write` also preserves `terminal_sequence` and raw
`terminal_metadata`. Nonterminal or foreign observations refuse; missing usage
stays unknown. This adds at most two original artifact loads, independent of
history, without replay or a tail lookup. The terminal model event contains
metadata rather than the complete assistant message: canonical response content
must still come from its original settlement owner. This codec does not attest
arbitrary caller-supplied response content or add a call-settlement writer.

Each shard contains one sample, with its original context and settled response.
The request context is written once, in `messages`; evaluation dataset input is
unknown and remains an empty list.
Readable text, reasoning and tool pairs use Inspect's native fields. Files,
notices, configuration, native continuity and structured projections remain in
message metadata; no media is fetched and no substitute text is invented.
Grouped tool results stream into separate Inspect messages in original order.
Their complete original group appears once, on the first result; each result
retains its original message and part index. A refused call with non-object
arguments has an empty Inspect display object and an explicit `parse_error`;
the unchanged original arguments remain in the assistant message metadata.
Usage remains the original record in sample metadata, with absent measurements
preserved. Inspect creation time is supplied by the exporting caller; execution
timestamps and normalized token totals remain unknown. No score is inferred.

The serializer borrows one decoded request and response, without a run-sized
buffer or file index. It performs no read, flush, commit, retry, provider call or
upload. A JSON tool result needs a temporary encoded display string; other
message serialization borrows its content. Discard the unpublished shard after
any output failure. Docent's external loader may materialize an entire shard,
so import shards separately rather than accumulating a whole swarm in one log.
