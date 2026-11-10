# Shared coordination executor performance

C.2 step 16 / R25. Production code and Linux measurements: `321eaaffba4a3f7315ec8952749dff5413c7e1e6`; containment base: `c302d2e0b98d2769827a18b1fe180924a045d836`.

The shared runtime now uses Tokio's multi-thread executor with one worker per `available_parallelism()`, worker-local runnable queues, work stealing and the existing OS readiness driver. The root `block_on` future stays on the calling thread. The blocking slot, persistence service and task cancellation/completion contracts are unchanged. There is no per-agent thread or application readiness loop. This changes async scheduling; it does not claim to replace every native file backend with a per-core proactor.

## Method

`cargo run --locked --release -p acyclic-native-runtime --example executor-perf -- 10001 before` retains the exact former constructor in the experiment. Omit `before` for production. Both modes use the same binary, compiler and operations. Each path warms once, then records 10,001 individual monotonic-clock observations and reports the median in nanoseconds. Operations run inside an executor task so children can use its local queue. Each operation includes task submission, its body and joining its result. The body is respectively ready completion, a scheduler yield, a oneshot send/receive, or a stored Notify permit.

The calibration is raw Tokio submission/join of the identical operation on the same executor; the local measurement uses the public owned-task API. It measures scheduler/API overhead, including allocation and timing overhead. This is an executor floor, **not** a hardware I/O, storage API, loaded throughput, TUI paint or model TTFT claim. No latency assertion is imposed on shared CI hosts.

## Linux x86_64

96 available CPUs. Scoped all-targets checks passed for Linux and x86_64-pc-windows-gnu. Focused task tests: 5 passed in 0.06 s; with test-support, 6 passed in 0.33 s.

| Operation | Before local p50 ns | After local p50 ns | After calibrated floor ns | After / floor |
| --- | ---: | ---: | ---: | ---: |
| ready | 280 | 290 | 290 | 1.000 |
| yield | 530 | 700 | 680 | 1.029 |
| oneshot | 330 | 340 | 340 | 1.000 |
| notify | 310 | 330 | 330 | 1.000 |

All four after measurements meet the requested <=1.5x executor-floor target. Uncontended latency increased slightly while independent work can now execute on all 96 workers. Single observed runtime construction: before 57,060 ns; after 110,563,981 ns. These are single cold-construction observations, not startup p50 or shipping-binary startup. Thread creation has an explicit startup cost proportional to available cores; no hidden CPU cap masks it.

## macOS aarch64 and Windows x86_64

Native measurements and focused test results are pending. The earlier macOS before-only binary (`6803564a7a857be17d59b918fc08331341ed56ca`) measured local ready/yield/oneshot/notify p50 of 416 / 48,667 / 458 / 417 ns, respectively. A matched same-binary comparison will supersede these preliminary values.

## Containment and cleanup

Main already had the shared constructor used by GraphCoder/embedded callers, task custody, seeded scheduling, persistence workers and dedicated stdin handling. Only the constructor's single-thread scheduler is replaced. C.3 has no executor-specific deletion row; its other areas are outside this step. The former constructor exists only in the benchmark's before control.
