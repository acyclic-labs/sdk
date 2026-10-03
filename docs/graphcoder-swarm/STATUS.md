# Local swarm implementation status

Goal active; qualification incomplete. All changes stay in the managed worktree on `codex/graphcoder-sdk`, based on `31b9ff52d63c91f2b9bf87e16b78ad682d26546f`. No merge occurred.

## Ownership

GraphCoder is a terminal composition wrapper. Harness owns model requests, recursive forks, sessions, communication, budgets, approvals, effects, and recovery. Filesystem owns workspace lifecycle and direct-parent integration. CLI approval bookkeeping and manual host composition still need consolidation into Harness; their presence is not accepted as final architecture.

## Latest native evidence

Source `c3a98adc`, handle 35128, exited 1. See [checkpoint-native-repair-2026-10-03.json](checkpoint-native-repair-2026-10-03.json) for suite and artifact hashes.

| Suite | Observed result |
|---|---|
| Local recovery | 7 passed, 0 failed |
| Recursive workspace | 1 passed, 1 failed |
| Budget production boundaries | 1 passed, 1 failed |
| Budget scheduler | 0 passed, 2 failed |
| Harness library | Incomplete: two observed failures and two stalled tests; exact test process stopped, no aggregate pass claim |

The recursive workspace descriptor assertion was corrected in `813d2523` after tracing its rejection through `FileDescriptor::verify`. A focused rerun exposed a later missing volume-operation grant; it remains open. Execution fixtures need reliable runner cleanup and exact production approval grants. Budget tests expose missing authenticated admission and concurrent capacity accounting. Provider metering is under review and not yet fully wired into the persistent runtime.

## Other scoped evidence

[checkpoint-fresh-wasm-terminal-2026-10-03.json](checkpoint-fresh-wasm-terminal-2026-10-03.json) records the earlier fresh WASM build, 100 passing Harness TypeScript tests, one native/WASM canonical equivalence test, and 49 passing GraphCoder TypeScript tests. Those artifacts predate the latest native input-policy changes and must be rebuilt. Native CLI qualification remains pending.

[checkpoint-native-2026-10-03-followup.json](checkpoint-native-2026-10-03-followup.json) records the earlier complete native library run (300 passed, 3 failed) and other source-bound results. Historical checkpoints retain their original scope; they do not prove the current source passes.

## Completion gates

The locked [requirements.json](requirements.json) remains authoritative: 68 requirements, unchanged digest `4d723c6391a234d8cf18c149960c3d45eb19459a642ff326cb8dc439dc1605ef`.

Remaining work includes exact prepared-request dispatch through every provider wrapper and WASM binding, frozen policy identity, recursive fork/restart qualification, hard runtime resource ceilings, execution and publication fault recovery, thin terminal composition, generated public contracts, fresh distributable artifacts, installed interactive/headless and Windows PTY tests, package consumption, regression and dependency checks, and a complete source/suite/artifact evidence audit. Required failures, skips, flakes, and missing evidence prevent completion.

Models are mocked. Host execution requires exact approval. Workspace routing provides no process confinement; no sandbox is implemented. The original checkout must remain untouched. Root writeback requires approval and reconciliation with concurrent user edits.
