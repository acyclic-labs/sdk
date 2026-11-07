# Persisted model usage

`model_usage` calls the same `contract::json_from_slice<ModelUsageRecord>`
used by the journal completion projector. Accepted inputs must preserve the
original timestamp and elapsed-time fields through typed decode and encode.
Absent observations remain null. Version and journal identity admission remain
in the existing projector; this target does not replace that authentication.

The seed in `corpus/model_usage/committed-call.json` is the original committed
artifact emitted by `committed_completion_does_not_wait_for_transport_close`,
not a handwritten record. Its source and SHA-256 are in `corpus/SOURCES.tsv`.

From the repository root on Linux x86_64, collect edge coverage over the target
and its production decoder, excluding build scripts and proc macros:

```sh
RUSTFLAGS="--cfg tokio_unstable -Cpasses=sancov-module -Cllvm-args=-sanitizer-coverage-level=4 -Cllvm-args=-sanitizer-coverage-inline-8bit-counters -Cllvm-args=-sanitizer-coverage-pc-table -Cllvm-args=-sanitizer-coverage-trace-compares" cargo run --locked --target x86_64-unknown-linux-gnu -p acyclic-harness-fuzz --no-default-features --features core --bin model_usage -- rust/crates/harness-fuzz/corpus/model_usage -runs=1024 -seed=71 -max_len=4096 -print_final_stats=1
```

Apply `usage_decode_times.patch` and use that same target with the unchanged
captured seed. The production decoder discards the original start observation;
the target must fail `usage timing observation changed: started_at`. Compilation
errors and unrelated crashes do not qualify. Reverse the patch before collecting
positive coverage. Edge counts do not establish complete source branch coverage.

The original producer is independently guarded by the existing real executor
completion control. Apply `usage_call_times.patch` and run:

```sh
cargo nextest run --locked -p acyclic-harness-core --lib --all-features -E 'test(committed_completion_does_not_wait_for_transport_close)'
```

It must fail `live dispatch time was lost`. The patch removes only the borrowed
writer fields and their initializers, retaining the reader and original
observation assertion. Reverse the patch before positive validation.

`usage_completion_eligibility.patch` restores the incorrect gate that treats
an unrepresentable start timestamp as a recovered call. The existing
`usage_times_keep_dispatch_boundaries_across_wall_jumps_and_replay` control must
reject its lost completion timestamp after a wall-clock correction. The restored
control includes forward and backward jumps, overflow at either boundary, exact
committed replay and an independent monotonic elapsed interval. The original
recovery control checks that reconciliation never samples recovery time.
