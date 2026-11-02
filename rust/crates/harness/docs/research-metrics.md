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
