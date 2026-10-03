# Local swarm implementation status

Goal active; qualification incomplete. All changes stay in the managed worktree on `codex/graphcoder-sdk`, based on `31b9ff52d63c91f2b9bf87e16b78ad682d26546f`. No merge occurred.

## Ownership

GraphCoder is a terminal composition wrapper. Harness owns model requests, recursive forks, sessions, communication, budgets, approvals, effects, and recovery. Filesystem owns workspace lifecycle and direct-parent integration. CLI approval bookkeeping and recursive host composition moved into Harness in `fbfedbda`, `1f4ee16b`, and `dd023c42`; the composition reuses the durable session key. Runtime and package qualification are still required.

## Latest native evidence

Focused local composition on source `e60b913e` (runtime `1373cf6d`), handle 10138, passed all four tests: stable project/session key across reopening, no model dispatch during inspection, exact operator decision, and fork refusals. See [checkpoint-local-composition-2026-10-03.json](checkpoint-local-composition-2026-10-03.json). This does not prove positive recursive model forks.

The recursive restart followup on source `265f1d1f`, handle 74198, passed one case and failed one. The live-stream handle error is repaired; the scenario now reaches a later stale-target assertion and fails there. See [checkpoint-recursive-restart-2026-10-03.json](checkpoint-recursive-restart-2026-10-03.json).

Source `c44432dc`, handle 93437, compiled and completed all five selected integration suites: recovery 7/7, execution journal 5/6, recursive workspace 1/2, model-fork boundary 1/3, and model-selected swarm 0/1. Open failures concern exact approval fixture grants, command/revision binding, invalid attestation allocating a workspace, positive fork attestation, and a live child stream handle preventing restart. See [checkpoint-integration-2026-10-03.json](checkpoint-integration-2026-10-03.json).

The focused execution run on source `91214d3a`, handle 50667, completed: 18 passed and one failed. Both previously stalled cases passed. The remaining failure is the hidden descendant-process fixture's missing startup marker. See [checkpoint-host-execution-2026-10-03.json](checkpoint-host-execution-2026-10-03.json); the full matrix remains unqualified.

An earlier broader run used source `a9ca8ff4`, handle 66153, and exited 1 during compilation: integration-test providers referenced the library through an invalid crate-relative path. No tests executed in that run. The subsequent `c44432dc` run compiled successfully. See [checkpoint-native-repair-2026-10-03.json](checkpoint-native-repair-2026-10-03.json) for the earlier source and suite hashes.

The earlier source `c3a98adc`, handle 35128, produced these scoped results:

| Suite | Observed result |
|---|---|
| Local recovery | 7 passed, 0 failed |
| Recursive workspace | 1 passed, 1 failed |
| Budget production boundaries | 1 passed, 1 failed |
| Budget scheduler | 0 passed, 2 failed |
| Harness library | Incomplete: two observed failures and two stalled tests; exact test process stopped, no aggregate pass claim |

The recursive workspace descriptor assertion was corrected in `813d2523` after tracing its rejection through `FileDescriptor::verify`. A focused rerun exposed a private-volume grant mismatch; `c1e167a1` aligns that fixture with its child scope. Execution approval resolution and bounded fixture cleanup were repaired in `bcf94efc`, with unreachable approval handling removed in `6c3fdae0`. These repairs still require fresh native verification. Budget tests expose missing authenticated admission and concurrent capacity accounting. Provider metering is under review and not yet fully wired into the persistent runtime.

Native CLI source `3ebc129d` (runtime source `c44432dc`), handle 66011, compiled and ran seven tests: six passed and the stage retry failed because its reopened path omitted the registered provider policy. Echo now passes. See [checkpoint-cli-composition-2026-10-03.json](checkpoint-cli-composition-2026-10-03.json). Installed-artifact and interactive PTY qualification remain outstanding.

## Other scoped evidence

The fresh WASM build from `9fa97138` succeeded. Current TypeScript source checks pass, test contract compilation fails, and the runtime suite passes 223/239: sixteen transport cases fail event-version validation. GraphCoder transport and terminal tests pass 49/49. See [checkpoint-harness-wasm-typescript-2026-10-03.json](checkpoint-harness-wasm-typescript-2026-10-03.json) and [checkpoint-graphcoder-typescript-2026-10-03.json](checkpoint-graphcoder-typescript-2026-10-03.json). Process cleanup remains unqualified; native lifecycle and bridge fixes are being reviewed for detached readers, bounded reaping, and hidden launches.

[checkpoint-fresh-wasm-terminal-2026-10-03.json](checkpoint-fresh-wasm-terminal-2026-10-03.json) records the earlier fresh WASM build, 100 passing Harness TypeScript tests, one native/WASM canonical equivalence test, and 49 passing GraphCoder TypeScript tests. Those artifacts predate the latest native input-policy changes and must be rebuilt. Native CLI qualification remains pending.

[checkpoint-native-2026-10-03-followup.json](checkpoint-native-2026-10-03-followup.json) records the earlier complete native library run (300 passed, 3 failed) and other source-bound results. Historical checkpoints retain their original scope; they do not prove the current source passes.

## Completion gates

The locked [requirements.json](requirements.json) remains authoritative: 68 requirements, unchanged digest `4d723c6391a234d8cf18c149960c3d45eb19459a642ff326cb8dc439dc1605ef`.

Remaining work includes exact prepared-request dispatch through every provider wrapper and WASM binding, frozen policy identity, recursive fork/restart qualification, hard runtime resource ceilings, execution and publication fault recovery, thin terminal composition, generated public contracts, fresh distributable artifacts, installed interactive/headless and Windows PTY tests, package consumption, regression and dependency checks, and a complete source/suite/artifact evidence audit. Required failures, skips, flakes, and missing evidence prevent completion.

Models are mocked. Host execution requires exact approval. Workspace routing provides no process confinement; no sandbox is implemented. The original checkout must remain untouched. Root writeback requires approval and reconciliation with concurrent user edits.
