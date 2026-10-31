# Operation observability

`acyclic-observability` derives operation metrics from the same spans used for
diagnostics. Libraries do not install subscribers. A host attaches one
`Operations` layer to its registry, alongside native wait reports and its trace
exporter. No subscriber means no metric storage, clock read, allocation, lock,
task or syscall on an operation's path.

An operation span declares `outcome` and `error.kind`. `finish` records its
explicit terminal result. The layer records one latency sample and one outcome
counter on the first completion. Retaining a span after completion does not
extend its latency or record another sample. Closing a span without a result
records `incomplete`, which does not imply that accepted work failed.

`Sink` is the export boundary. An OpenTelemetry adapter receives the original
static metadata, RPC method, elapsed duration and disposition. It does not time
or reconstruct completion. Sinks must not wait for I/O. A tuple of sinks shares
one lifecycle traversal.

`Metrics` stores counts and a base-two nanosecond histogram for each selected
operation and RPC method, without operation history or resource labels. Its byte radix lookup is
bounded by the selected operation name, independently of other series. A query
subscribes only to the selected series. Its `changed` future waits on the
completion watch and returns `QueryError` when its producer closes. Register the
query before the cohort begins. Concurrent queries share that cohort; dropping
the last query releases the series and its index path. A later query starts an
empty cohort. An unselected operation creates no series.

SLOs are ordinary, authorized files in a volume, for example
`observability/slos/append.json`:

```json
{
  "version": 1,
  "operation": "acyclic.stream.append",
  "rpc": null,
  "success_ppm": 999000,
  "latency": { "maximum_ns": 1048575, "target_ppm": 990000 }
}
```

Read and follow that file through the volume's existing reader and change event.
Decode it as `Slo` and select `Metrics::query_slo`, which validates the file and
selects its exact operation and optional method. `SloQuery::snapshot` and
`changed` return the serializable file, evidence and verdict together. There is no SLO
endpoint, separate settings store or readiness poll. Empty cohorts are unknown.
A latency threshold crossing a histogram bucket is unknown if that ambiguity
can change the verdict; the view never claims precision the histogram lacks.
Version mismatches and a query naming another operation are typed refusals.
Operations have no count or duration cap. If the fixed-width metric counters
lose precision, the snapshot marks `overflowed` and SLO verdicts stay unknown
for that cohort. The layer still calls its sink for each terminal sample;
output delivery remains subject to the host's credit.

`Export::channel` supplies the nonblocking output adapter for a host's existing
export loop. Its caller provides a `tokio::sync::Semaphore` containing admitted
record credit; there is no default capacity. The permit follows each record
through preparation, delivery and consumer processing. Dequeue does not release
credit. Recording never waits for credit or output I/O, and refuses without
copying when credit is exhausted or the receiver is closed. `Export::stats`
distinguishes those losses from operation outcomes. It reports the current and
exact high-water count of admitted records, updated on every admission rather
than sampled. These are exporter resources, not process-wide OS counters.

An exporter can opt into the first recorded static `error.kind`. Later generic
failures cannot replace it. It remains diagnostic evidence, never a metric
label, and does not change uncertainty or an operation's original result.
Every sample carries its original subscriber registry span ID and immediate
parent ID, so diagnostics can join the existing provider/key/stage/predecessor
observations. The parent is captured at creation, including explicit roots;
completion on another worker does not replace it with that worker's context.
These scalars retain no span or physical work. They are registry-local
diagnostic joins, rather than metric labels or globally unique identities.

For a join to an admitted operation, the original owner can use
`Binding::attach(&original_span, original_ids, caller_owned_permit)` before
creating children. The key is a fixed-size `Copy + Send + Sync + 'static` value
containing that owner's exact opaque authority, operation, generation and
physical-owner IDs. The adapter does not discover or authorize those IDs.
An opt-in detail sink carries the same token along each immediate registry
edge, including non-operation worker spans, without searching ancestors.
`Binding::matches(&sample)` compares the original token; `Binding::key::<K>()`
recovers its typed IDs. Equal scalar IDs from another admission do not match.
Missing or filtered parents leave provenance unknown. Attach after terminal
completion, duplicate attachment to the same root, and a disabled or unavailable
registry return typed errors. Counters-only sinks propagate no token.

One allocation at explicit binding admission holds the fixed key and its
caller-supplied credit. Clones share that allocation through diagnostic
retirement, retaining neither a span nor physical work. Each admitted export
record still owns its separate output credit through processing. Neither token
nor record retirement completes the operation or certifies a provider range,
physical coverage or report acknowledgement.

Detail sinks also receive `Sample::work`, the original producer's fixed
`work.items`, `work.bytes` and `work.durability` summary when declared. Producers
record these fields before their terminal outcome. Missing and unrepresentable
totals remain unknown. These semantic units do not count hidden copies, locks,
quorum requests, OS syscalls or device drains. No measurement can treat their
absence as zero physical work.

The original `acyclic.runtime.durability` producer's fields project into
`Sample::durability: Option<DurabilityAttempt>`. Its fixed fields preserve
requested Full/Barrier, the actual named syscall, an attempt-local opaque
descriptor ID and the first recorded native error code. Missing or unrecognized
fields remain `None`. This projection adds no dynamic map or evidence-string
allocation. It does not infer a device-cache drain, provider/group publication
coverage or OS-wide counts from a successful call or descriptor. Those guarantees
require their original producers. Ordinary stage markers carry no attempt data.
Metrics alone retain no failure string. Detail retains one failure category per
active operation, with one owned copy when first recorded. The collector calls
`Sink::record_owned` to transfer its original recorded fields into output;
compose `(metrics, export)` to borrow for metrics then transfer to export.
Generic sink tuples borrow for their first sink and transfer to their last.
Direct borrowed `Sink::record` calls copy only after acquiring record credit. No
payload, resource path, credential or formatted error belongs in those fields.
The host must retain `ExportRecord` until its own output completes, then drop it
to release credit. Dropping the receiver refuses later output immediately;
dropping all exporters ends the receiver's channel wait. There is no telemetry
worker or per-agent timer, and no observer can retain the original operation.
This delivery channel does not acknowledge a diagnostic report request. Hosts
retain the original ReportPort owner, generation and acknowledgement contract.

On a volume-query file replacement, construct the replacement `SloQuery` from
the newly authorized bytes. Select its completion event alongside the volume's
existing change event. Removal ends that file's view; malformed replacements
are errors. No per-file worker, directory scan or readiness probe is needed.

Run the independent operation lifecycle and allocation witnesses with:

```sh
cargo test --locked -p acyclic-observability --test operations one_terminal_sample_survives_retained_handles_and_producer_loss -- --exact
cargo test --locked -p acyclic-observability --test operations disabled_operation_allocates_nothing_with_enabled_negative_control -- --exact
cargo test --locked -p acyclic-observability --test operations credited_export_keeps_original_cause_and_cancellation_with_a_blocked_sink -- --exact
```

Measure the disabled, selected-metrics, and credited detail-export costs with
`cargo test --locked -p acyclic-observability --test operations trace_on_off_cost -- --ignored --exact --nocapture`.
The last case includes the host's dequeue and record retirement. Report these
costs separately; they provide no claim about unobserved physical I/O or a
different trace exporter's work.

On Linux with `strace`, count actual kernel crossings over the marked operation
interval. The same binary's real file open/close is the negative control:

```sh
cargo test --locked -p acyclic-observability --test operations disabled_operation_has_zero_syscalls_with_real_io_negative_control -- --ignored --exact
```
