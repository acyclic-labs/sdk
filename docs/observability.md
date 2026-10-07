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
- Instrument with `#[cfg_attr(not(target_arch = "wasm32"), tracing::instrument(...))]`.
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
- **Result:** `outcome` is `"ok"` or `"err"`. `error.kind` is a stable
  `&'static str` from the error enum's `fn kind(&self) -> &'static str`; add
  that method to any error enum that lacks one. RPCs also record `rpc.code`
  (the tonic `Code` or the HTTP status).

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

- `WorkCounters::emit(&self, op: &'static str, outcome: &'static str)` emits one
  `debug!` event with all `WorkCounters` fields, under their `WorkCounters`
  names. It does so only when
  `tracing::enabled!(target: "acyclic.work", Level::DEBUG)` is true.
- Each fs `info`/`debug` span also declares three `Empty` summary fields, filled
  from the receipt:
  - `work.items`: `items_examined`
  - `work.bytes`: `object_bytes_read + object_bytes_written + authority_bytes_written`
  - `work.durability`: `durability_operations`
- `Observe::observe(self, op)` on `MeasuredResult` does both at the return
  point, for success and failure alike.

## Enablement

With none of these variables set, nothing is installed and output is unchanged.
The plugin never writes tracing output to stdout, which carries JSON-RPC.

| Variable | Effect |
| --- | --- |
| `ACYCLIC_LOG` | `EnvFilter` directives for a `fmt` layer on stderr. In `__service` mode it writes to `<state dir>/logs/service-{pid}.log` instead. |
| `ACYCLIC_LOG_FILE` | Overrides the `ACYCLIC_LOG` destination |
| `ACYCLIC_TRACE_FILE` | Writes a Perfetto-compatible Chrome trace. `{pid}` is replaced by the process id. |
| `ACYCLIC_TRACE_FILTER` | Filter for the trace file. The default is `acyclic_fs=debug,acyclic_stream=debug,acyclic_objects=debug,acyclic_native_runtime=debug,acyclic_plugin=debug`. |
| `ACYCLIC_TEST_TRACE_DIR` | In tests, writes a Chrome trace per test into this directory. Use it to profile a slow test. |
| `ACYCLIC_PERF` | In TypeScript, `ACYCLIC_PERF=1` (or `globalThis.ACYCLIC_PERF === true`) records `performance.mark`/`measure` entries named `acyclic.<family>.<op>`. |

The plugin service inherits its environment when it is spawned. Drain it to
apply a change. `doctor` prints the active filter.

## Tests

- Each instrumented crate has one span test. It installs a capture layer of
  about 30 lines on a `tracing_subscriber::Registry` with
  `tracing::subscriber::with_default`. It then asserts that one representative
  operation emits its span with the required fields, and that no field is named
  `path`, `token`, `content`, `body` or `authorization`.
- Span tests and the `ACYCLIC_TEST_TRACE_DIR` helper go in the crate's existing
  test modules or existing `tests/*.rs` files. Each new `tests/*.rs` file is
  another linked test binary, which slows CI. There is no shared
  test-support crate.
- Gate span tests with `#[cfg(not(target_arch = "wasm32"))]`.
