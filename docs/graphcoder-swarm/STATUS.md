# Local swarm implementation status

The goal remains active. All 68 entries in [requirements.json](requirements.json) remain required. Its locked SHA256 is `4d723c6391a234d8cf18c149960c3d45eb19459a642ff326cb8dc439dc1605ef`. Missing, ignored, failing, or flaky required verification prevents completion.

## Current integrated evidence

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

## Remaining integration and qualification

1. Repair exact reference inheritance in the production recursive swarm, without granting mutable parent-private access. Repair duplicated dispatch/output in fault recovery.
2. Integrate and qualify scoped communication admission, durable resource budgeting, lazy metadata projection, pinned operator authority and approved root-writeback recovery. Worker commits remain candidates until integrated and tested.
3. Complete recoverable process ownership, including launch-initialization failures, cancellation, overflow, descendant cleanup and installed native transport. Uncertain effects must remain uncertain.
4. Complete the thin terminal routes and verify public inspection, exact approvals, concurrent user edits/deletions, conflicts, continuation, abort and cold recovery through installed artifacts.
5. Build fresh distributables after final source changes. Execute every locked matrix gate, including Windows PTY, package consumption, generated bindings, provider conformance, Filesystem/plugin regressions and actual supported platform lanes.
6. Audit library ownership and dependency boundaries, record final digests, and confirm all intended changes committed on the isolated branch without a merge.

## Boundaries

SCOPE-06 is not fully qualified: the original `Q:\sdk` checkout remains on pinned `main` commit `31b9ff52d63c91f2b9bf87e16b78ad682d26546f` with clean tracked and staged diffs, but no pre-task untracked-file inventory was found. A current clean tracked tree does not prove preservation of the original untracked set. Do not claim the required before/after untracked digest audit passed.

Harness owns model inputs, orchestration, forks, communication, admission, effects and recovery. Filesystem owns workspace semantics and its host adapter. GraphCoder stays a composition and terminal wrapper.

Models are mocked. There is no sandbox or cloud implementation. Workspace routing is not process confinement. Host commands require exact approval and exclude inherited credentials. Root writeback requires approval and reconciliation with concurrent user changes. The original checkout remains untouched; work stays on `codex/graphcoder-sdk` without merging.
