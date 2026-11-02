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
cursor. It does not load model requests, conversation history or tool payloads.
`inbox`, `conversation` and `status` project one original agent record each.

Schema v1 has six tables:

| Table | Original observations |
| --- | --- |
| `agents` | Adoption, parent and immutable role pin |
| `turns` | Journal start and settled conversation entry |
| `model_calls` | Admission and committed completion, requested sampling, reported usage, timing and identity |
| `tool_calls` | Admission, completion or typed failure, with original artifact references |
| `edges` | Forks, messages, notices and child completion |
| `outcomes` | Settled turn outcome and terminal agent status |

Every row retains `version`, `agent_path`, `operation_id`, `source_stream`,
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
from an agent inbox cursor. Use one
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

The research-only `conversation::read_set::TurnReadSet` carrier is available
without Arrow or RDF dependencies. It records version 1, the fixed original
agent, execution operation, optional journal cut, explicit producer coverage,
and either `Closed { file }` or `Unavailable { reason }`. An enclosing absent
observation, an unavailable observation and a closed empty artifact have
different meanings. Admit this association once in the original turn
settlement, bound to its original agent and operation. A failed turn without a
conversation entry does not imply an empty read set.

The following row kinds are reserved for the original research carriers in
the same six tables. Their producers remain separate from this offline codec:

| Table / kind | Original authority and coverage |
| --- | --- |
| `turns` / `resources` | Cumulative resource snapshot at its original resource stream, sequence and commit ID. Select latest; never sum snapshots. Original inbox cursor is distinct from the source sequence. Semantic step commits and newly encoded conversation bytes do not measure physical work. |
| `agents` / `placement` | Original immutable placement fact on initial observation or change, joined by its original digest. Unknown location stays unknown. |
| `edges` / `stream_record_shared` | Original record origin, sequence and commit ID, selected destination stream and bound provider/account. Coverage is logical record-value bytes; union original identities before summing. |
| `turns` / `operation_interval` | Original operation binding, parent and collector clock domain with checked relative start/end coordinates. Unknown Unix placement stays unknown. Actual producer edges must establish causality. |

These reservations do not claim that the outstanding producer integrations
already emit rows. Existing object identity or `AlreadyExists` alone never
proves zero physical writes, and overlapping sibling durations are not added
into a critical path.
