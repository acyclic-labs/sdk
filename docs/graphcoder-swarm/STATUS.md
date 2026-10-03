# Local swarm implementation status

Goal active; qualification incomplete. All changes stay in the managed worktree on `codex/graphcoder-sdk`, based on `31b9ff52d63c91f2b9bf87e16b78ad682d26546f`. No merge occurred.

## Ownership

GraphCoder is a terminal composition wrapper. Harness owns model requests, recursive forks, sessions, communication, budgets, approvals, effects, and recovery. Filesystem owns workspace lifecycle and direct-parent integration. CLI approval bookkeeping and manual host composition still need consolidation into Harness; their presence is not accepted as final architecture.

## Latest native evidence

The latest run used source `a9ca8ff4`, handle 66153, and exited 1 during compilation: integration-test providers referenced the library through an invalid crate-relative path. No tests executed in that run. The mandatory prepared-request provider contract compiled in the library; this does not prove runtime behavior. See [checkpoint-native-repair-2026-10-03.json](checkpoint-native-repair-2026-10-03.json) for source and suite hashes.

The earlier source `c3a98adc`, handle 35128, produced these scoped results:

| Suite | Observed result |
|---|---|
| Local recovery | 7 passed, 0 failed |
| Recursive workspace | 1 passed, 1 failed |
| Budget production boundaries | 1 passed, 1 failed |
| Budget scheduler | 0 passed, 2 failed |
| Harness library | Incomplete: two observed failures and two stalled tests; exact test process stopped, no aggregate pass claim |

The recursive workspace descriptor assertion was corrected in `813d2523` after tracing its rejection through `FileDescriptor::verify`. A focused rerun exposed a private-volume grant mismatch; `c1e167a1` aligns that fixture with its child scope. Execution approval resolution and bounded fixture cleanup were repaired in `bcf94efc`, with unreachable approval handling removed in `6c3fdae0`. These repairs still require fresh native verification. Budget tests expose missing authenticated admission and concurrent capacity accounting. Provider metering is under review and not yet fully wired into the persistent runtime.

Native CLI source `0e15465d`, handle 86472, compiled and ran eight tests: six passed and two failed because fixture model options lacked a registered provider policy. Installed-artifact and interactive PTY qualification remain outstanding.

## Other scoped evidence

[checkpoint-fresh-wasm-terminal-2026-10-03.json](checkpoint-fresh-wasm-terminal-2026-10-03.json) records the earlier fresh WASM build, 100 passing Harness TypeScript tests, one native/WASM canonical equivalence test, and 49 passing GraphCoder TypeScript tests. Those artifacts predate the latest native input-policy changes and must be rebuilt. Native CLI qualification remains pending.

[checkpoint-native-2026-10-03-followup.json](checkpoint-native-2026-10-03-followup.json) records the earlier complete native library run (300 passed, 3 failed) and other source-bound results. Historical checkpoints retain their original scope; they do not prove the current source passes.

## Completion gates

The locked [requirements.json](requirements.json) remains authoritative: 68 requirements, unchanged digest `4d723c6391a234d8cf18c149960c3d45eb19459a642ff326cb8dc439dc1605ef`.

Remaining work includes exact prepared-request dispatch through every provider wrapper and WASM binding, frozen policy identity, recursive fork/restart qualification, hard runtime resource ceilings, execution and publication fault recovery, thin terminal composition, generated public contracts, fresh distributable artifacts, installed interactive/headless and Windows PTY tests, package consumption, regression and dependency checks, and a complete source/suite/artifact evidence audit. Required failures, skips, flakes, and missing evidence prevent completion.

Models are mocked. Host execution requires exact approval. Workspace routing provides no process confinement; no sandbox is implemented. The original checkout must remain untouched. Root writeback requires approval and reconciliation with concurrent user edits.
