# Coordination executor and GraphCoder performance

C.2 step 16 / R25. Measured source: `64b447fc6b8227a3023103dfe68db1823d52577e`; containment base: `c302d2e0b98d2769827a18b1fe180924a045d836`. Main landed the executor and benchmark in `da4f54dd4d8a`, but omitted the CPU-discovery fallback and this report. The follow-up restores those two missing changes on main; its executor settings and benchmark match the measured version.

The shared runtime now uses Tokio's multi-thread executor with one worker per `available_parallelism()`, worker-local runnable queues, work stealing and the existing OS readiness driver. When CPU discovery is denied, it uses one worker, matching the existing native worker service. The root `block_on` future stays on the calling thread. The blocking slot, persistence service and task cancellation/completion contracts are unchanged. There is no per-agent thread or application readiness loop. This changes async scheduling; it does not claim to replace every native file backend with a per-core proactor.

## Method

`cargo run --locked --release -p acyclic-native-runtime --example executor-perf -- 10001 before` retains the exact former constructor in the experiment. Omit `before` for production. Both modes use the same binary, compiler and operations. Each path warms once, then records 10,001 individual monotonic-clock observations and reports the median in nanoseconds. Operations run inside an executor task so children can use its local queue. Each operation includes task submission, its body and joining its result. The body is respectively ready completion, a scheduler yield, a oneshot send/receive, or a stored Notify permit.

The calibration is raw Tokio submission/join of the identical operation on the same executor; the local measurement uses the public owned-task API. It measures scheduler/API overhead, including allocation and timing overhead. This is an executor floor, **not** a hardware I/O, storage API, loaded throughput, TUI paint or model TTFT claim. No latency assertion is imposed on shared CI hosts.

The construction observations below are historical measurements of the recorded source. Current main additionally calls `raise_descriptor_limit()` in the public `coordination_runtime()` factory before building the executor. The benchmark's construction timer includes that initialization when run on current main; the recorded startup observations predate this extra work and do not measure the current startup path.

## Linux x86_64

Rust 1.98.1; 76 available CPUs in this allocation. Scoped all-targets checks passed for Linux and x86_64-pc-windows-gnu. Focused task tests: 5 passed in 0.13 s; with test-support, 6 passed in 0.06 s.

| Operation | Before local p50 ns | After local p50 ns | After calibrated floor ns | After / floor |
| --- | ---: | ---: | ---: | ---: |
| ready | 250 | 260 | 260 | 1.000 |
| yield | 470 | 560 | 600 | 0.933 |
| oneshot | 290 | 300 | 290 | 1.034 |
| notify | 280 | 300 | 300 | 1.000 |

All four after measurements meet the requested <=1.5x executor-floor target. Uncontended latency increased slightly while independent work can now execute on all workers. Single observed runtime construction: before 53,410 ns; after 2,434,634 ns. An earlier allocation with 96 available CPUs on `321eaaffba4a3f7315ec8952749dff5413c7e1e6` measured 110,563,981 ns for construction. These are individual observations under shared-host conditions, not startup p50 or shipping-binary startup. Thread creation has an explicit startup cost proportional to available cores; no hidden CPU cap masks it.

## Windows x86_64

The GNU-target test executable and release benchmark were cross-compiled from the same source SHA, then executed natively on Windows 11 (build 26100), with 16 available CPUs. All 5 focused task tests passed in 0.42 s.

| Operation | Before local p50 ns | After local p50 ns | After calibrated floor ns | After / floor |
| --- | ---: | ---: | ---: | ---: |
| ready | 400 | 500 | 500 | 1.000 |
| yield | 2,100 | 1,600 | 1,600 | 1.000 |
| oneshot | 400 | 600 | 600 | 1.000 |
| notify | 400 | 600 | 600 | 1.000 |

All four after measurements meet <=1.5x the executor floor. Single construction observations: before 108,100 ns; after 566,000 ns.

## macOS aarch64

Native all-targets compilation passed on the same measured source SHA. With 10 available CPUs, all 5 default task tests passed in 0.01 s and all 6 test-support task tests passed in 0.20 s.

| Operation | Before local p50 ns | After local p50 ns | After calibrated floor ns | After / floor |
| --- | ---: | ---: | ---: | ---: |
| ready | 167 | 167 | 167 | 1.000 |
| yield | 12,000 | 292 | 292 | 1.000 |
| oneshot | 208 | 208 | 208 | 1.000 |
| notify | 167 | 208 | 208 | 1.000 |

All four after measurements meet <=1.5x the executor floor. Single construction observations: before 139,875 ns; after 269,250 ns. The yield measurement includes task submission and joining, as on the other platforms; it is not a bare yield instruction timing.

## Validation commands

- `cargo check --locked --all-targets -p acyclic-native-runtime`
- `cargo check --locked --all-targets -p acyclic-native-runtime --target x86_64-pc-windows-gnu`
- `cargo test --locked -p acyclic-native-runtime --lib task::tests`
- `cargo test --locked -p acyclic-native-runtime --features test-support --lib task::tests`
- Windows: `cargo test --locked -p acyclic-native-runtime --target x86_64-pc-windows-gnu --lib --no-run`, followed by native `task-tests.exe task::tests`.
- Windows: `cargo build --locked --release -p acyclic-native-runtime --example executor-perf --target x86_64-pc-windows-gnu`, followed by native `executor-perf.exe 10001 before` and `executor-perf.exe 10001`.

## Containment and cleanup

At the original containment base, main already had the shared constructor used by GraphCoder/embedded callers, task custody, seeded scheduling, persistence workers and dedicated stdin handling. The executor and benchmark are now on main at `da4f54dd4d8a`. The follow-up contains only the omitted CPU-discovery fallback and the performance report, completed with native macOS results. C.3 has no executor-specific deletion row; its other areas are outside this step. The former constructor exists only in the benchmark's before control.

## R25 render-work qualification

Source `704da84775e9865e1912d9f49ccb7cf9ff98b760` supplies the design's
`tui-render-work` acceptance case. It drives an ordinary editor key and a
streamed transcript replacement through the shared `PromptInput`,
`TranscriptRows` and `Screen` paint path. The terminal buffer must contain the
edited prompt and the updated answer. At both 80×50 and 120×40, increasing
retained history from 100 to 10,000 blocks must leave each event's row visits
and wrapped source-byte count exactly equal. Paint visits must also fit within
the viewport's height.

The same oracle runs against two negative controls: rewrapping historical
blocks before painting, and scanning retained rows before painting. Each
viewport must independently reject each mutation with the expected work
failure; an unrelated panic fails the control. The Linux run passed all three
controls in 0.58 s, and the native Windows acceptance case passed all three in
1.06 s. A subsequent Linux acceptance run passed the named case in 0.86 s
(the three controls took 0.82 s). The source also passed Linux and Windows GNU
all-targets compilation
with the `tui` feature, plus Windows MSVC test-executable compilation.
These counters establish the tested history-independent render work. They do
not establish a wall-clock p99 or certify the design's 16.6 ms frame and 2 ms
render targets on another machine.

Reproduce the focused controls with:

```sh
cargo test --locked -p acyclic-harness-tui --features tui --lib screen::tests::tui_render_work
```

The ordinary acceptance runner selects the same native test executable through
`E2E_TUI_TEST_BINARY` and requires a complete libtest summary including both
negative controls:

```sh
node scripts/graphcoder-e2e/run.mjs /absolute/graphcoder /absolute/harness-mock-model tui-render-work
```

## Product first-paint measurement

PTY captures now retain `first_paint_ms` and emit a `pty-startup` observation.
The interval begins immediately before the runner requests its terminal launch
and ends at the first completed, available frame containing GraphCoder. This
includes process supervision, terminal transport and negotiation, application
startup, and terminal-emulator decoding at 80×24. It is distinct from the
executor-construction observations above.

Startup measurements use GraphCoder's shipping `dist` profile. The ordinary
`pty-markdown` case must still finish successfully; observing an early frame
does not turn a failed case into a pass. The runner records the elapsed time
without asserting a shared-host latency ceiling. The design's warm first-paint
target is 50 ms; an individual launch observation is not a warm-start p50.

| Platform | Product source | Profile | First paint ms | Case result |
| --- | --- | --- | ---: | --- |
| Linux x86_64 | `3357174beef3bc29c3283b0618fd6e0035682d59` | `dist` | 343.29 | `pty-markdown` passed |
| Linux x86_64 | `704da84775e9865e1912d9f49ccb7cf9ff98b760` | `dist` | 3265.84 | `pty-markdown` passed |
| Windows x86_64 | `3357174beef3bc29c3283b0618fd6e0035682d59` | `dist` | unavailable | Startup refused current-user DPAPI before paint |
| macOS aarch64 | `704da84775e9865e1912d9f49ccb7cf9ff98b760` | `dist` attempted | unavailable | Shared build artifacts disappeared before completion |

Both Linux observations used fresh isolated settings directories on a shared
host, including the launch and emulator costs described above. They do not
establish the warm 50 ms target. Both runs passed `tui-render-work`; the second
includes the correction that checks each negative control at each viewport.
The intervening code changes affect tests only. These individual observations
are not a before/after performance comparison.

Windows' SSH key logon refused current-user DPAPI with access denied (OS error
5). A separate public-payload DPAPI round trip also failed under both the
normal and isolated profiles. Owner records require this protection before
publication. No plaintext or machine-wide protection fallback was used.
Windows product timing therefore needs a credentialed user logon; native
render controls do not need that store and passed separately. Product startup
qualification remains incomplete until Windows and macOS have successful
observations.

On macOS, native all-targets compilation passed in 10.26 s and test-executable
compilation passed in 49.23 s on the reviewed source. During the subsequent
shipping-profile build, dependencies that Cargo had just emitted disappeared,
causing missing-crate and missing-extern errors. The compiled test executable
was also absent afterward. Neither the native acceptance controls nor product
startup ran; these compilation results do not qualify their execution.
