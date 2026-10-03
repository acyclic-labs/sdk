# Locked matrix gap audit

This audit is deliberately conservative. A `checkpoint` means that a focused
test or source audit exists in the checkpoint evidence, but it is not a final
source-bound receipt. `partial` means that one part of the contract is tested
while a required runtime, platform, recursive, recovery, or provenance lane is
still absent. `unmet` means that no qualifying run is recorded. Only a final
receipt with every row passed can close the matrix.

The authoritative row definitions and required execution kinds remain in
`requirements.json`. The rows below enumerate every locked ID exactly once.

| Current state | Matrix IDs | Missing gate that keeps the row open |
|---|---|---|
| checkpoint | SCOPE-01, SCOPE-02, SCOPE-04, SCOPE-05, INPUT-01, INPUT-02, INPUT-04, INPUT-05, INPUT-06, INPUT-07, FORK-02, FORK-03, FORK-04, FORK-05, EFFECT-01, EFFECT-02, EFFECT-03, EFFECT-04, WORK-01, HOST-01, HOST-02, QUAL-03 | Re-run against the final source in the isolated branch and attach descriptor, transcript, and artifact provenance to the final receipt. |
| partial | SCOPE-03, SCOPE-06, INPUT-03, FORK-01, FORK-06, FORK-07, FORK-08, FORK-09, EFFECT-05, EFFECT-06, EFFECT-07, EFFECT-08, WORK-02, WORK-03, WORK-04, WORK-05, WORK-06, API-01 | Production task activation, WASM execution/parity, scoped grants, fork publication faults, approved process recovery, git-facade integration, user-edit reconciliation, and complete model-facing tool evidence remain open. |
| unmet | API-02, API-03, API-04, API-05, API-06, API-07, LIMIT-01, LIMIT-02, CLI-01, CLI-02, CLI-03, CLI-04, CLI-05, LOAD-01, LOAD-02, BIND-01, BIND-02, E2E-01, E2E-02, E2E-03, E2E-04, E2E-05, E2E-06, FAULT-01, FAULT-02, QUAL-01, QUAL-02, QUAL-04 | No qualifying evidence has yet been recorded for the full recursive control surface, limits, real local-runtime GraphCoder bridge, installed artifact, PTY, generated binding parity, fault matrix, fresh package artifacts, or final provenance audit. |

The GraphCoder terminal worktree has a deterministic mock fixture, a generic
UI state machine, a bridge transport, and a Windows PTY smoke driver. Those
are useful implementation and mock evidence, but they do not close the
terminal and lazy-loading rows until the bridge is connected to
PersistentLocalSwarm and the packaged artifact is captured by the suite
provenance runner. The real-backend command sequence is locked in
`graphcoder-real-backend-scenarios.json`.

The official installed-Harness run remains a required failure: its TypeScript
consumer and native/WASM event equivalence passed, while the isolated Rust
package suite stopped at the Windows `std`/`test` metadata-stub error during
host contention. Provider conformance therefore remains unrun. No row above
is promoted because compilation or fixture output succeeded.
