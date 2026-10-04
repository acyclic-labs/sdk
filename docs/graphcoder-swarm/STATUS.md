# Local swarm implementation status

The goal remains active. All 68 entries in [requirements.json](requirements.json) remain required. Its locked SHA256 is `4d723c6391a234d8cf18c149960c3d45eb19459a642ff326cb8dc439dc1605ef`. Missing, ignored, failing, or flaky required verification prevents completion.

## Current integrated evidence

At clean source `e1c23133c`, all 15 Windows native local coordinator regressions pass with no ignored cases. Pinned registry reads reject an incomplete range instead of publishing a partial projection. Host-only observations expose lazy activity without adding durable state or model content; the cross-handle scenario proves metadata reads do not open a cold child or fetch its workspace and that stale workspace publication remains denied. The initial observation port failed compilation due to a missing test-only type name; that failure is preserved alongside the repaired run. See [source, log and executable evidence](checkpoint-coordinator-observations-native-2026-10-04.json). The production recursive swarm and installed runtime remain unqualified.

At clean source `40e2f854d`, the Windows native GraphCoder CLI suite passes 7/7, the TypeScript dispatcher and UI boundary suites pass 42/42 (139 assertions), and GraphCoder type checking passes. Metadata-only snapshots retain an unknown workspace generation; generation-bearing reads explicitly obtain the pinned Filesystem generation. Unknown activity and approval metadata remains null rather than fabricated values. The qualification provenance suite passes 8/8, including a real receipt producer/consumer flow and altered-source, artifact, environment and missing-evidence rejection. These are focused checks, not installed recursive-runtime qualification. Logs, suite sources and the native test executable are archived with verified SHA-256 digests on D: in [metadata and tooling evidence](checkpoint-metadata-tooling-native-2026-10-04.json). The initial C: archive attempt failed due to disk exhaustion and does not count as evidence.

At clean source `b3dff5345`, the 12 Windows native exact-input tests pass after pinning the explicit stage-file v2 definitions, schema digests and model binding digest. The manifest assertions independently include the declared immutable tool-result reference. Executables, suite sources and the complete log are archived in [stage-file v2 evidence](checkpoint-exact-input-stage-v2-native-2026-10-04.json). The initial v1 golden failures and the subsequent run invalidated by concurrent manifest edits do not qualify this source. Full recursive swarm, WASM and installed execution remain separate gates.

At source `ba66f9a12`, the production recursive `local_model_swarm` run fails with Windows `STATUS_STACK_OVERFLOW` (`0xc00000fd`) after the schema-declared inherited-file grant repair. The test runs with normal stack settings; this is a required runtime failure, not a qualified swarm. Runtime diagnosis must preserve recursive depth and real effects rather than increase fixture stack size or narrow the scenario.

At source `99ae53078`, 13 Windows native local coordinator regressions pass with no ignored cases, including concurrent empty-registry openers, cancellation/completion preservation across late failure and restart, and direct-parent project selection. The production entrypoint's three Node contract tests also pass after removing its implicit working-directory fallback. This remains focused coordinator evidence, not full recursive swarm qualification. The native suite executable SHA256 is `4f0b43c6b832318be7b25aea06223981baecc0a35faa66dfe88e24e8011c0d32`.

At source `5214f51ea`, `cargo check --workspace --all-targets` passes after the fork model-boundary bindings and async coordinator closure repair. This is native compilation evidence, not WASM execution or installed-artifact qualification.

At source `bc110358c`, 21 Windows native fork regressions pass after reports and seeds share one history-capture validator. Wire formats, proof digests and strict/rebound scopes remain unchanged. See [focused validation evidence](checkpoint-fork-validation-cut-native-2026-10-04.json).

Independent review still holds duplicate child activation: concurrent retries can dispatch a child more than once. The startup and late-failure defects identified in that review have focused repairs and regression evidence above. Durable cross-handle activation claims and full production qualification remain required.

At source `d595f3ba4`, 10 Windows native lazy-projection regressions pass with no ignored cases after integration. Registry refresh reads an unseen suffix, metadata inspection avoids cold child activation, and cancellation releases the refresh fence before host propagation. See [focused source and artifact evidence](checkpoint-integrated-lazy-native-2026-10-04.json). Independent review, installed counters and full coordinator qualification remain required.

At source `173cb1b85`, 12 Windows native exact-input tests pass with no ignored cases, including the physical corruption/deletion test enabled by `test-support`. These capture serialized provider requests and exercise generation-pinned content, explicit limits, schema denials and restart replay. See [source, suite and artifact evidence](checkpoint-exact-input-native-2026-10-04.json). Full recursive coordinator, WASM and installed qualification remain separate gates.

At source `1953894ae`, the two Windows native owned/shared crash-atomic regressions pass with no ignored cases after hidden subprocess launch and intentional crash termination changes. Test execution takes 14.99 seconds. See [focused source, log and executable evidence](checkpoint-hidden-crash-atomic-native-2026-10-04.json). This does not qualify the full Filesystem matrix or the Harness swarm.

At source `4ba8293a9`, all 353 Windows native Harness library tests pass with no ignored cases. The recursive production swarm fails on a missing exact read capability for an inherited staged file. The separate fault suite passes 2/5: two cases observe three provider dispatches instead of two, and cold fork-intent recovery duplicates completion text. See [source, suite, log and executable evidence](checkpoint-integrated-fork-native-2026-10-04.json). These failures remain acceptance blockers; library results do not qualify production orchestration.

At source `239c00bb8`, the Filesystem default-feature native library run finishes with 1112 passed, zero failed and 36 ignored. Both owned/shared crash-atomic cases pass. Ignored cases remain unqualified. See [archived Filesystem evidence](checkpoint-filesystem-native-2026-10-04.json).

The cold recursive model boundary at source `9dfc54cb5` passes 3/3 after every original provider handle is released. Each cold request preserves inherited prefix bytes, serialization, binding and durable manifest. See [cold-prefix evidence](checkpoint-cold-prefix-native-pass-2026-10-04.json). This focused boundary suite does not qualify the production coordinator.

Earlier passing and failing checkpoints are retained in [EVIDENCE.md](EVIDENCE.md). A receipt proves only its source and stated scope. The earlier fork-port compilation failures are repaired in the current source; their evidence remains historical.

At source `3d0a42bff`, the integrated Windows native Harness library and fault suites pass 365/365 and 5/5. See [recovery regression evidence](checkpoint-recovery-foundation-native-2026-10-04.json).

At source `97d11a74a`, recursive child turns run as independently scheduled, abort-on-drop owned tasks, and GraphCoder snapshot assembly uses one lazy Harness projection. The library passes 365/365 and the real recursive durable swarm passes 1/1 on the ordinary Windows stack. The fault suite passes 4/5; `cancelled_child_after_publication_cannot_be_reactivated` times out after release. This failed gate prevents qualification. See [integrated recursive evidence](checkpoint-recursive-integrated-native-2026-10-04.json).

The existing public terminal tests pass 7/7 at source `f07b958d8`. Frozen dependency installation passes at `fd4e73c29` after adding the missing GraphCoder workspace lock entries; no dependency versions changed. See [terminal and lock evidence](checkpoint-cli-lock-native-2026-10-04.json). These tests do not cover installed recursive commands, native approval or user-checkout writeback.

## Remaining integration and qualification

1. Repair the cancellation fault timeout exposed after independently scheduling recursive child turns. The ordinary Windows production recursive scenario now passes without a larger stack; concurrent activation, cancellation and full recovery still require qualification.
2. Integrate and qualify scoped communication admission, durable resource budgeting, lazy metadata projection, pinned operator authority and approved root-writeback recovery. Worker commits remain candidates until integrated and tested.
3. Complete recoverable process ownership, including launch-initialization failures, cancellation, overflow, descendant cleanup and installed native transport. Uncertain effects must remain uncertain.
4. Complete the thin terminal routes and verify public inspection, exact approvals, concurrent user edits/deletions, conflicts, continuation, abort and cold recovery through installed artifacts.
5. Build fresh distributables after final source changes. Execute every locked matrix gate, including Windows PTY, package consumption, generated bindings, provider conformance, Filesystem/plugin regressions and actual supported platform lanes.
6. Audit library ownership and dependency boundaries, record final digests, and confirm all intended changes committed on the isolated branch without a merge.

## Boundaries

SCOPE-06 is not fully qualified: the original `Q:\sdk` checkout remains on pinned `main` commit `31b9ff52d63c91f2b9bf87e16b78ad682d26546f` with clean tracked and staged diffs, but no pre-task untracked-file inventory was found. A current clean tracked tree does not prove preservation of the original untracked set. Do not claim the required before/after untracked digest audit passed.

Harness owns model inputs, orchestration, forks, communication, admission, effects and recovery. Filesystem owns workspace semantics and its host adapter. GraphCoder stays a composition and terminal wrapper.

Models are mocked. There is no sandbox or cloud implementation. Workspace routing is not process confinement. Host commands require exact approval and exclude inherited credentials. Root writeback requires approval and reconciliation with concurrent user changes. The original checkout remains untouched; work stays on `codex/graphcoder-sdk` without merging.
