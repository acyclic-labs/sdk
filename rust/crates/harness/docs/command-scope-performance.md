# Command-scope performance comparison

The canonical workload is [command_scope_benchmark.rs](../fixtures/command_scope_benchmark.rs).
It exercises the real `native_scope` on a current-thread executor, with zero and
500 seeded files. It measures coordination for a no-op command body, including
view admission and completion; it does not measure shell process execution.
The driver/session is warmed before measurement. Single-command output contains
ten samples and their upper median; concurrent8 is one batch of eight commands.
The contended case enters a holder and keeps it open until the contender ends,
using channels rather than timing or readiness polling.

The fixture is an injection module because this workload must compile against
both the candidate and main's private `native_scope`, without changing either
implementation or publishing a benchmark-only product API. It is not an
automatically discovered test or a substitute mount implementation.

## Exact-head comparison

Run only on the authorized build host, in disposable checkouts. Export the
fixture once from the candidate commit and append those same bytes to
`rust/crates/harness/src/local/workspace/volumes.rs` in each checkout. Record
the original checkout SHA and the fixture digest before compiling either tree:

```sh
git rev-parse HEAD
sha256sum /path/to/command_scope_benchmark.rs
cat /path/to/command_scope_benchmark.rs >> rust/crates/harness/src/local/workspace/volumes.rs
cargo test --locked -p acyclic-harness --lib -- \
  --exact local::workspace::volumes::command_scope_benchmark::command_scope_workload_benchmark \
  --nocapture --test-threads=1
```

Use the same host, toolchain, profile and fixture for both heads. Preserve every
`COMMAND_SCOPE_BENCH` row, including file and native-volume counts, alongside
both source SHAs and the workload digest. Compare matching rows. The targets
are candidate single-command median at most 0.25 times main, and candidate
concurrent8 and contended times at most main. A run against an older baseline
does not prove a current-main result; a source cleanup does not prove a speedup.
No current-head result is recorded here.

The recovered historical workload had SHA-256
`e89caa222601c075e010b584bd21faf0d9eb2634beee94a095b32e4de4074f31`.
The checked-in fixture has been formatted; record its actual digest for the next
comparison rather than treating the historical digest as a current artifact.

## Admission work and its guarantees

| Work | Guarantee |
|---|---|
| Deduplicate selected mounted volumes | One command view per selected volume/destination, including aliases. |
| Retain the shared native instance | A view has callback custody through retirement without another physical session. |
| Verify the callback's original owner | Current identity, workload revocation and original allowance still authorize admission. |
| Read the authorized checkout head | Reads start from the selected immutable generation; no-write commands create no fork. |
| Hold the projection policy lease | A policy publication cannot race root disclosure during admission. |
| Reselect through the retained public verifier | Bind the new generation without a new signer, preserving identity and capability ceiling; verification surrounds asynchronous policy compilation. |
| Recheck persisted policy revision | A stale or substituted selection owner cannot admit a view against another current policy. |
| Check List and selected-root identity | Admission discloses no missing/file/directory distinction to a denied caller and mounts only the authorized directory. |
| Drain callbacks before completion | A retired view cannot continue mutating the candidate while it is discarded or handed back. |

The checkout path consumes the canonical funded namespace path. Its formerly
eager Unicode string was unused and has been removed; lazy-source admission
alone builds the text that its lookup and returned root consume. Do not remove
the owner or persisted-policy checks based only on equal generation IDs: policy
and workload revocation are independent of the data selection. Quantify remaining
cost before adding caches, layers or special entry paths.
