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

`Metrics` stores counts and a base-two nanosecond histogram per operation and RPC
method, without operation history or resource labels. Its byte radix lookup is
bounded by the selected operation name, independently of other series. A query
subscribes only to the selected series. Its `changed` future waits on the
completion watch and returns `QueryError` when its producer closes.

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
Decode it as `Slo`, validate it and select `Metrics::query` (or `query_rpc`). The
live view evaluates the latest snapshot against that file. There is no SLO
endpoint, separate settings store or readiness poll. Empty cohorts are unknown.
A latency threshold crossing a histogram bucket is unknown if that ambiguity
can change the verdict; the view never claims precision the histogram lacks.
Version mismatches and a query naming another operation are typed refusals.

Run the independent operation lifecycle and allocation witnesses with:

```sh
cargo test --locked -p acyclic-observability --test operations one_terminal_sample_survives_retained_handles_and_producer_loss -- --exact
cargo test --locked -p acyclic-observability --test operations disabled_operation_allocates_nothing_with_enabled_negative_control -- --exact
```

On Linux with `strace`, count actual kernel crossings over the marked operation
interval. The same binary's real file open/close is the negative control:

```sh
cargo test --locked -p acyclic-observability --test operations disabled_operation_has_zero_syscalls_with_real_io_negative_control -- --ignored --exact
```
