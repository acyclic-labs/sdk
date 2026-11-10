# Shared coordination executor performance

C.2 step 16 / R25. Production code and matched measurements: `64b447fc6b8227a3023103dfe68db1823d52577e`; containment base: `c302d2e0b98d2769827a18b1fe180924a045d836`.

The shared runtime now uses Tokio's multi-thread executor with one worker per `available_parallelism()`, worker-local runnable queues, work stealing and the existing OS readiness driver. When CPU discovery is denied, it uses one worker, matching the existing native worker service. The root `block_on` future stays on the calling thread. The blocking slot, persistence service and task cancellation/completion contracts are unchanged. There is no per-agent thread or application readiness loop. This changes async scheduling; it does not claim to replace every native file backend with a per-core proactor.

## Method

`cargo run --locked --release -p acyclic-native-runtime --example executor-perf -- 10001 before` retains the exact former constructor in the experiment. Omit `before` for production. Both modes use the same binary, compiler and operations. Each path warms once, then records 10,001 individual monotonic-clock observations and reports the median in nanoseconds. Operations run inside an executor task so children can use its local queue. Each operation includes task submission, its body and joining its result. The body is respectively ready completion, a scheduler yield, a oneshot send/receive, or a stored Notify permit.

The calibration is raw Tokio submission/join of the identical operation on the same executor; the local measurement uses the public owned-task API. It measures scheduler/API overhead, including allocation and timing overhead. This is an executor floor, **not** a hardware I/O, storage API, loaded throughput, TUI paint or model TTFT claim. No latency assertion is imposed on shared CI hosts.

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

The revised source's native check and matched measurements are pending on the shared build target. The earlier before-only binary (`6803564a7a857be17d59b918fc08331341ed56ca`) passed its all-targets check and measured local ready/yield/oneshot/notify p50 of 416 / 48,667 / 458 / 417 ns, respectively. A matched same-binary comparison must supersede these preliminary values before macOS qualification is claimed.

## Validation commands

- `cargo check --locked --all-targets -p acyclic-native-runtime`
- `cargo check --locked --all-targets -p acyclic-native-runtime --target x86_64-pc-windows-gnu`
- `cargo test --locked -p acyclic-native-runtime --lib task::tests`
- `cargo test --locked -p acyclic-native-runtime --features test-support --lib task::tests`
- Windows: `cargo test --locked -p acyclic-native-runtime --target x86_64-pc-windows-gnu --lib --no-run`, followed by native `task-tests.exe task::tests`.
- Windows: `cargo build --locked --release -p acyclic-native-runtime --example executor-perf --target x86_64-pc-windows-gnu`, followed by native `executor-perf.exe 10001 before` and `executor-perf.exe 10001`.

## Containment and cleanup

Main already had the shared constructor used by GraphCoder/embedded callers, task custody, seeded scheduling, persistence workers and dedicated stdin handling. Only the constructor's single-thread scheduler is replaced. C.3 has no executor-specific deletion row; its other areas are outside this step. The former constructor exists only in the benchmark's before control.
