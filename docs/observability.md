# Observability

Rust crates emit `tracing` spans and events; hosts and tests decide whether
anything records them. Work accounting stays in `WorkCounters`, and spans only
summarise it.

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
  `workers` or `plugin`. Native-mount callbacks are `acyclic.fs.mount.<callback>`.
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
- **Result:** `outcome` is `"ok"` or `"err"`. `error.kind` is a stable
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
semantic kind; unfinished or unpolled RPC/body drop records `err`/`cancelled`.
This is the consumer's lifetime: submitted native work may continue after
consumer cancellation and retain its own span until actual completion. Native
filesystem receipt spans record a terminal result only when the operation
returns; an abandoned future leaves its outcome unset.

Stream gRPC bodies also require EOF for success and record `cancelled` on
unfinished consumer drop. Detached provider work
records its actual terminal worker result even when its caller was cancelled.

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

- `WorkCounters::emit` emits one
  `debug!` event with all `WorkCounters` fields, under their `WorkCounters`
  names. It does so only when
  `tracing::enabled!(target: "acyclic.work", Level::DEBUG)` is true.
- Each receipt-returning fs `info`/`debug` span also declares three `Empty`
  summary fields, filled from the receipt:
  - `work.items`: `items_examined`
  - `work.bytes`: `object_bytes_read + object_bytes_written + authority_bytes_written`
  - `work.durability`: `durability_operations`
  Plain `Result<OperationReceipt<_>, _>` APIs record these totals on success;
  failures without a receipt leave them unset. Measured failures carry their
  own work totals and record those too.
- `Observe::observe_on(self, span, op)` on `MeasuredResult` does both at the return
  point, for success and failure alike, only at receipt-returning facade
  operations. Observed facade operations do not call another observed facade
  operation. Sum `acyclic.work` events to account for that facade work once.
  Each event has an explicit operation span parent; if that span is filtered,
  the event has no parent. Its static `op` still identifies the receipt, and
  filtering spans does not suppress independently enabled work events.
- Kernel, native capture, materialization, and other nested operations use
  `obs::measured_on` to record summaries on their own spans without emitting
  `acyclic.work`. Their `work.*` fields overlap with caller receipts and must
  not be added together. Direct calls to those APIs are visible through spans,
  but are outside the facade event ledger.
- Convenience entry points that delegate to an instrumented implementation
  share that implementation's span. Blocking workers retain the caller's span
  and scoped subscriber through the actual work, including after caller drop.
  An abandoned operation has no terminal outcome unless it returns; a retained
  worker span is not evidence that its caller succeeded.

## Enablement

With none of these variables set, nothing is installed and output is unchanged.
The plugin never writes tracing output to stdout, which carries JSON-RPC.

| Variable | Effect |
| --- | --- |
| `ACYCLIC_LOG` | `EnvFilter` directives for a `fmt` layer on stderr. In `__service` mode it writes to `<state dir>/logs/service-{pid}.log` instead. |
| `ACYCLIC_LOG_FILE` | Overrides the `ACYCLIC_LOG` destination |
| `ACYCLIC_TRACE_FILE` | Writes a Perfetto-compatible Chrome trace. `{pid}` is replaced by the process id. |
| `ACYCLIC_TRACE_FILTER` | Filter for the trace file. The default is `acyclic_fs=debug,acyclic_stream=debug,acyclic_objects=debug,acyclic_native_runtime=debug,acyclic_plugin=debug`. |
| `ACYCLIC_PERF` | In TypeScript, `ACYCLIC_PERF=1` (or `globalThis.ACYCLIC_PERF === true`) records `performance.mark`/`measure` entries named `acyclic.<family>.<op>`. |

The plugin service inherits its environment when it is spawned. Drain it to
apply a change. `doctor` prints the active filter.

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
is a local, uncommitted edit: add `tracing-chrome.workspace = true` to the
crate's `dev-dependencies` and wrap the test body:

```rust
use tracing_subscriber::prelude::*;
let (chrome, _flush) = tracing_chrome::ChromeLayerBuilder::new().file("slow.json").build();
let _trace = tracing::subscriber::set_default(tracing_subscriber::registry().with(chrome));
```

Open the file in [Perfetto](https://ui.perfetto.dev).

## Benchmarks

`benches/` in objects, stream, harness, filesystem and native-runtime hold
[divan](https://docs.rs/divan) benchmarks of the local hot paths. They run on
demand only, never in CI, and `package.exclude` keeps them out of published
crates. `clippy --all-targets` keeps them compiling.

```sh
cargo bench -p acyclic-objects --features local --bench objects
cargo bench -p acyclic-stream --features local --bench stream
cargo bench -p acyclic-fs --bench filesystem
cargo bench -p acyclic-harness --bench harness
cargo bench -p acyclic-native-runtime --bench native_runtime
```

Pass divan options after `--`, for example `-- --sample-count 10 put_small`.
Name the bench with `--bench`: the library's test harness rejects those options.
`bench-fs` and `bench-objects` in the conformance crate measure whole
workloads (see its README).
