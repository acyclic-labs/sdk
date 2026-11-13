# Coordination executor and GraphCoder performance

The shared coordination runtime uses Tokio work stealing with one worker per
available CPU. Its root `block_on` future remains on the calling thread. Native
I/O, host callbacks, process I/O, persistence and output use separate shared
services sized from the same `available_parallelism` discovery, without upper
clamps. Linux completion ownership also has one thread per available CPU.
Queue admission provides backpressure; thread creation and startup costs scale
with CPU count, and insufficient OS resources return an initialization error.
Per-file predecessor fences and accepted-operation custody retain ordering and
storage ownership independently of scheduling. The deterministic simulation
scheduler is unchanged.

## Current Linux qualification

Measured source: `c46f4e396b9871ab5790f0d54182d1852b2f2d26`, Rust 1.98.1,
192 available CPUs. The release benchmark took 10,001 observations per operation
using the method below. The before control is the historical current-thread
constructor in the same executable, not a second product implementation.

| Operation | Before local p50 ns | Current local p50 ns | Current calibrated floor ns | Current / floor |
| --- | ---: | ---: | ---: | ---: |
| ready | 290 | 290 | 290 | 1.000 |
| yield | 560 | 1150 | 1130 | 1.018 |
| oneshot | 340 | 340 | 350 | 0.971 |
| notify | 330 | 320 | 320 | 1.000 |

All four operations meet the 1.5× executor-floor target. Runtime construction
was 62,080 ns for the control and 29,887,433 ns for production. These are single
construction observations, not shipping-binary startup distributions.

On this exact source, all three stdin regressions passed in 0.04 s. The
positive open-input control uses the production runtime; the negative control
uses an explicit single blocking slot, proving why indefinitely open reads
must retain their dedicated owner. All three render-work cases passed in
1.06 s: input/delta work independence plus the historical-rewrapping and
historical-row-scanning negative controls. The three cases exercise each
viewport independently, as described below.

The historical cross-platform measurements below are retained with their
original source identities. They do not substitute for current product
startup or native render execution.

## Service-width observations

A paired 160-CPU measurement used 16 concurrent agent turns, synchronized at
entry, with identical deterministic CPU work interleaved with native reads,
host callbacks and persistence jobs. Before source:
`ee9b20600c603ada63abc76c80881b6178742bd3`; after source:
`d2e9f189512b75b8d4ee82a1b05377fa1a9302dc`.

| Observation | Before | After |
| --- | ---: | ---: |
| Coordination workers | 160 | 160 |
| Native / host / persistence workers | 4 / 16 / 4 | 160 / 160 / 160 |
| Linux completion owners | 4 | 160 |
| Event-observed queue peaks, same service order | 1 / 14 / 15 | 1 / 15 / 13 |
| Execution ms | 88.575333 | 183.621628 |
| Sampled process threads | 190 | 802 |

Both runs had 18 coordination threads with 3–5 CPU ticks each, read from
`/proc/self/task/*/stat` at workload boundaries with 100 ticks per second.
Other threads had zero whole ticks, except one tick on the after run's test
thread. The baseline was already multi-threaded; no single coordination
thread saturated. Queues occurred in both runs, but event sampling does not
establish their exact maxima or sustained saturation. The window includes
lazy completion-owner startup: wider ownership increased startup cost, and
these observations show no latency speedup. Full active-thread readings are
retained in [the change's measurement record](https://github.com/acyclic-labs/sdk/pull/3604).

A further integration measurement at
`9b047d3b175b95c5a4b12780aed187b2294228a7` passed with 192 available CPUs,
192 workers per measured service, queue peaks 1/14/14, 962 process threads and
138.907739 ms elapsed. Its machine had more CPUs, so this is not a direct
comparison with the paired observations. The external fixture omitted its
unused placeholder checkpoint/conversation output to meet current record
validation; its CPU loops and I/O submissions were unchanged.

## Method

Historical executor measurement source: `64b447fc6b8227a3023103dfe68db1823d52577e`.

`cargo run --locked --release -p acyclic-native-runtime --example executor-perf -- 10001 before` retains the exact former constructor in the experiment. Omit `before` for production. Both modes use the same binary, compiler and operations. Each path warms once, then records 10,001 individual monotonic-clock observations and reports the median in nanoseconds. Operations run inside an executor task so children can use its local queue. Each operation includes task submission, its body and joining its result. The body is respectively ready completion, a scheduler yield, a oneshot send/receive, or a stored Notify permit.

The calibration is raw Tokio submission/join of the identical operation on the same executor; the local measurement uses the public owned-task API. It measures scheduler/API overhead, including allocation and timing overhead. This is an executor floor, **not** a hardware I/O, storage API, loaded throughput, TUI paint or model TTFT claim. No latency assertion is imposed on shared CI hosts.

The historical construction observations below belong to their recorded source. The current benchmark times the coordination-runtime factory; native service initialization and lazy completion-owner startup occur separately and are not included. Constructor timing does not measure the complete shipping startup path.

## Historical Linux x86_64

Rust 1.98.1; 76 available CPUs in this allocation. Scoped all-targets checks passed for Linux and x86_64-pc-windows-gnu. Focused task tests: 5 passed in 0.13 s; with test-support, 6 passed in 0.06 s.

| Operation | Before local p50 ns | After local p50 ns | After calibrated floor ns | After / floor |
| --- | ---: | ---: | ---: | ---: |
| ready | 250 | 260 | 260 | 1.000 |
| yield | 470 | 560 | 600 | 0.933 |
| oneshot | 290 | 300 | 290 | 1.034 |
| notify | 280 | 300 | 300 | 1.000 |

All four after measurements meet the requested <=1.5x executor-floor target. Uncontended latency increased slightly while independent work can now execute on all workers. Single observed runtime construction: before 53,410 ns; after 2,434,634 ns. An earlier allocation with 96 available CPUs on `321eaaffba4a3f7315ec8952749dff5413c7e1e6` measured 110,563,981 ns for construction. These are individual observations under shared-host conditions, not startup p50 or shipping-binary startup. Thread creation has an explicit startup cost proportional to available cores; no hidden CPU cap masks it.

## Historical Windows x86_64

The GNU-target test executable and release benchmark were cross-compiled from the same source SHA, then executed natively on Windows 11 (build 26100), with 16 available CPUs. All 5 focused task tests passed in 0.42 s.

| Operation | Before local p50 ns | After local p50 ns | After calibrated floor ns | After / floor |
| --- | ---: | ---: | ---: | ---: |
| ready | 400 | 500 | 500 | 1.000 |
| yield | 2,100 | 1,600 | 1,600 | 1.000 |
| oneshot | 400 | 600 | 600 | 1.000 |
| notify | 400 | 600 | 600 | 1.000 |

All four after measurements meet <=1.5x the executor floor. Single construction observations: before 108,100 ns; after 566,000 ns.

## Historical macOS aarch64

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

## Render-work qualification

Source `704da84775e9865e1912d9f49ccb7cf9ff98b760` supplies the
`tui-render-work` acceptance case. It drives an ordinary editor key and a
streamed transcript replacement through the shared `PromptInput`,
`TranscriptRows` and `Screen` paint path. The terminal buffer must contain the
edited prompt and the updated answer. At both 80×50 and 120×40, increasing
retained history from 100 to 10,000 blocks must leave each event's row visits
and wrapped text-byte count exactly equal. Paint visits must also fit within
the viewport's height.

The counter records text bytes at the shared transcript layout entry point,
including cached Markdown presentation, so cached historical rewrapping is
also covered.

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
cargo test --locked -p acyclic-harness-tui --lib screen::tests::tui_render_work
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
| Linux x86_64 | `c46f4e396b9871ab5790f0d54182d1852b2f2d26` | `dist` | 2018.40 | `pty-markdown` passed with controller `f72301007bb770197aa1f230a21e1a940d39c0f8` |
| Linux x86_64 | `3357174beef3bc29c3283b0618fd6e0035682d59` | `dist` | 343.29 | `pty-markdown` passed |
| Linux x86_64 | `704da84775e9865e1912d9f49ccb7cf9ff98b760` | `dist` | 3265.84 | `pty-markdown` passed |
| Windows x86_64 | `3357174beef3bc29c3283b0618fd6e0035682d59` | `dist` | unavailable | Startup refused current-user DPAPI before paint |
| macOS aarch64 | `704da84775e9865e1912d9f49ccb7cf9ff98b760` | `dist` attempted | unavailable | Shared build artifacts disappeared before completion |

The current Linux observation uses the shipping executables built from
`c46f4e396b9871ab5790f0d54182d1852b2f2d26`, reused by a controller whose only
source change scripts the default feedback retrospective and waits for its
request event before detaching. Both model generations returned HTTP 200,
both scripted steps were consumed once, and the original bold-markdown and
literal-marker assertions passed. Case duration was 6.72 s. Binary SHA-256:
GraphCoder `2c1b21cef2a3e274566d2ce2b65146a51b268a925a72dc12f1c9fe3b0a4e4165`;
mock server `08ba125783828522d7a056c079745c74624555d55f3e22ec8668a791824ecc00`.
An earlier launch observed 464.54 ms but failed on an unscripted retrospective
request; that timing is not a qualifying case result. The successful run used
an isolated profile on a shared host, not a measured warm-start distribution.

The two historical Linux observations used fresh isolated settings directories on a shared
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
