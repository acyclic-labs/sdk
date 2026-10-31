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
for that cohort. Export sinks still receive every terminal sample.

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
Metrics alone retain no failure string. Detail retains one failure category per
active operation, with one owned copy when first recorded; delivery copies the
category and declared RPC method only after acquiring record credit. No
payload, resource path, credential or formatted error belongs in those fields.
The host must retain `ExportRecord` until its own output completes, then drop it
to release credit. Dropping the receiver refuses later output immediately;
dropping all exporters ends the receiver's channel wait. There is no telemetry
worker or per-agent timer, and no observer can retain the original operation.

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
