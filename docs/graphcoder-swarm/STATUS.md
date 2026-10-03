# Local swarm implementation status

Goal active; qualification incomplete. Implementation stays in the managed worktree on `codex/graphcoder-sdk`, based on `31b9ff52d63c91f2b9bf87e16b78ad682d26546f`. No merge occurred.

## Ownership

Harness owns exact model requests, recursive forks, communication, budgets, approvals, effects, and recovery. Filesystem owns workspace lifecycle and direct-parent integration. GraphCoder is a terminal composition wrapper. Platform adapters own process execution; workspace routing is not confinement.

## Verified checkpoints

- Fresh WASM, Harness source/test type checks, and all 242 TypeScript tests pass (1,218 assertions). GraphCoder source types and 49 adapter tests pass. See [checkpoint-fresh-input-wasm-2026-10-03.json](checkpoint-fresh-input-wasm-2026-10-03.json).
- Native executor tests pass 24/24, including output admission before staging and fresh-provider exact request reconciliation. The journal in these tests is in memory. See [checkpoint-input-facade-followup-2026-10-03.json](checkpoint-input-facade-followup-2026-10-03.json).
- The real recursive facade grandchild-to-child-to-root integration fixture passes. Cold provider conflict/replay tests pass 2/2. See the input/facade checkpoint and [checkpoint-cold-conflict-pass-2026-10-03.json](checkpoint-cold-conflict-pass-2026-10-03.json).
- Completed-turn cold replay and two real local communication cases pass. The model-invoked wait checkpoint proves task identity, with its limited host/store scope recorded. See [checkpoint-cold-replay-communication-2026-10-03.json](checkpoint-cold-replay-communication-2026-10-03.json) and [checkpoint-model-wait-cold-conflict-2026-10-03.json](checkpoint-model-wait-cold-conflict-2026-10-03.json).

Each checkpoint is source-bound and scoped. Historical runs remain in [EVIDENCE.md](EVIDENCE.md) and their original receipts; they do not qualify newer changes.

## Current verification and unmet gates

The latest broader native run (handle 43700, runtime source `9497e03db`) passes all nine git-facade cases and the grandchild integration case, but the 32-sibling case failed. The 1,024-level recursive case is still running; no aggregate result is claimed. The workspace worker has proposed a notice-before-final-plan repair for the sibling fixture, awaiting verification.

Held fork stack `0cd58c9b4` failed no-run compilation (handle 78431, E0382: consuming invocation arguments before borrowing invocation for replay). It is outside the root branch. Further fork work must prove historical capture before allocation, terminal-state consistency, cancellation at durable model admission, and recursive serialized prefixes through restart.

Budget changes remain under review for durable concurrent usage accounting, live-owner takeover, default publisher binding, and authenticated host secrets. Process cleanup changes remain under review for live-claim-bound uncertainty recording, required durable-store capability, forced descendant cleanup, and real subprocess fault windows. Communication still needs production model-selected messaging, genuine wait effects, cold replay without observation, cancellation identity binding, and provider-byte assertions against hidden history.

Terminal root writeback must use durable exact operator admission and the existing Filesystem facade/recovery path. Installed artifacts, Windows PTY, package consumption, generated contracts, applicable platform lanes, regressions, and the full fault matrix remain required. Qualification fixtures are being extended; source/mock adapter passes do not replace them.

## Completion requirements

All 68 entries in [requirements.json](requirements.json) remain required; its locked digest is `4d723c6391a234d8cf18c149960c3d45eb19459a642ff326cb8dc439dc1605ef`. Required failures, skips, flakes, and missing evidence prevent completion. Fresh final artifacts and source/suite/descriptor/artifact digests are still required after the final source change.

Models are mocked. No sandbox or cloud is implemented. Host execution requires exact approval and excludes inherited credentials. Applying changes to the user's checkout requires approval and reconciliation with concurrent user edits. The original checkout must remain untouched.
