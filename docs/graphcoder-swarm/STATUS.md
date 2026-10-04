# Local swarm implementation status

Fresh isolated native results exist for Rust source `454797822`: exact frozen request/manifest conformance passes 2/2, read projections pass 2/2, and real local swarm faults pass 1/5. The four failures include manifest command/revision binding and missing child dispatch. These are blocking failures with source-qualified artifacts. See [checkpoint-isolated-native-2026-10-04.json](checkpoint-isolated-native-2026-10-04.json). Final qualification remains incomplete.

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

Native artifact provenance needs revalidation in separate per-worktree build directories. A root conformance build reported newly added public methods missing even though they exist in its source; the shared build directory contains an unqualified `libacyclic_harness.rlib` overwritten by another worktree. Results from that shared directory remain observations of the recorded artifacts, but cannot establish exact runtime-source qualification until repeated with isolated outputs. No final native gate is claimed passed on that basis.

Latest actual held-stack runs: fork source `6c6ae5444` compiles and passes nine local recovery cases, but fails two fork-boundary allocation assertions and the real swarm seed-publication check. Cleanup source `a5188016a` compiles, but its full Windows host execution run passes 24/28; operator uncertainty resolution, the descendant fixture, and two intermittent output-limit cases remain under repair. See [checkpoint-fork-native-2026-10-04.json](checkpoint-fork-native-2026-10-04.json) and [checkpoint-cleanup-followup-native-2026-10-04.json](checkpoint-cleanup-followup-native-2026-10-04.json).

Focused request scope pinning passes 84 TypeScript cases with 478 assertions and test type checks, using the existing WASM artifact. Fresh installed GraphCoder package consumption passes 21 host calls, including authenticated operator approval. These checks do not qualify the unfinished native runtime, terminal PTY, or native/WASM equivalence. See [checkpoint-scope-pinning-2026-10-04.json](checkpoint-scope-pinning-2026-10-04.json) and [checkpoint-installed-graphcoder-build-2026-10-04.json](checkpoint-installed-graphcoder-build-2026-10-04.json).

The broader native run (handle 43700, runtime source `9497e03db`) finished: nine git-facade cases pass; recursive cases pass 2/3, including all 1,024 levels and grandchild integration. Its 32-sibling failure was a fixture child reusing its parent's identity. The corrected focused scenario passes on `8423ca7a0` (handle 79605), retaining authority checks and pinning approval after notice staging. See [checkpoint-recursive-native-2026-10-04.json](checkpoint-recursive-native-2026-10-04.json) and [checkpoint-sibling-and-transport-2026-10-04.json](checkpoint-sibling-and-transport-2026-10-04.json).

Held fork stack `0cd58c9b4` failed no-run compilation (handle 78431, E0382: consuming invocation arguments before borrowing invocation for replay). That borrow was repaired in `53a07c797`; its subsequent uncommitted centralization proposal failed compilation at a different moved-event use (handle 3886). These changes remain outside the root branch. Further fork work must prove historical policy and context capture before allocation, terminal-state consistency, cancellation at durable model admission, and recursive serialized prefixes through restart.

Budget changes remain under review for durable concurrent usage accounting, live-owner takeover, default publisher binding, and authenticated host secrets. Process cleanup changes remain under review for live-claim-bound uncertainty recording, required durable-store capability, forced descendant cleanup, and real subprocess fault windows. Communication still needs production model-selected messaging, genuine wait effects, cold replay without observation, cancellation identity binding, and provider-byte assertions against hidden history.

Terminal root writeback must use durable exact operator admission and the existing Filesystem facade/recovery path. Installed artifacts, Windows PTY, package consumption, generated contracts, applicable platform lanes, regressions, and the full fault matrix remain required. Qualification fixtures are being extended; source/mock adapter passes do not replace them.

## Completion requirements

All 68 entries in [requirements.json](requirements.json) remain required; its locked digest is `4d723c6391a234d8cf18c149960c3d45eb19459a642ff326cb8dc439dc1605ef`. Required failures, skips, flakes, and missing evidence prevent completion. Fresh final artifacts and source/suite/descriptor/artifact digests are still required after the final source change.

Models are mocked. No sandbox or cloud is implemented. Host execution requires exact approval and excludes inherited credentials. Applying changes to the user's checkout requires approval and reconciliation with concurrent user edits. The original checkout must remain untouched.
