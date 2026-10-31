# AUTH / SECRETS adoption by rb-identity

Recorded 2026-10-26, after reading `handover.md`. Attribution: Codex.

rb-identity accepts sole code, review, proof and queue ownership across SDK AUTH/SECRETS and Cloud Trust/Edge. b-authority is retired; no further implementation is expected from that lane. The protected-secrets Cloud successor is also owned here under the coordinator's separate dependency ordering.

Verified locally: both predecessor worktrees have no tracked changes; signed heads are P1 `3b6b467cbd379eeb7779c8f644a10ee0839a6f39` and secrets `ca1b93bbe580cd8d8f4a2d48ecd09775b29fcea5`. GitHub reports both PRs open at those heads. Fetched SDK origin/main remains `b7333953cbd5d3b31adc05cf2f0851f049f6c16a`.

Implementation continues only in the owned , branch `rb/auth-security`, existing draft SDK PR #1518. The prior turn imported the three P1 commits as `c6048a49be`, `f85deb61ef`, `d500330f72`, on top of recursive-tool custody removal `29d33dd7b4`. This turn completed its interrupted auth lint cherry-pick as signed `d49da74b98`, keeping constant-time proof comparison in the extracted helper. No predecessor branch was edited. Neither duplicate predecessor nor successor is authorized for admission while its blocking findings remain.

Open work, explicitly adopted:

1. Pi durable seed: r4373450972. Move Pi onto native-runtime's sole SigningKeyReference/bootstrap, authenticate migration sources, retain identity and original native allocation custody, prove absence of seed in new durable bytes. Local/GraphCoder proof does not discharge Pi.
2. Abandoned credentials: r4373450978. Shared provisional creation lifecycle must remove only a newly created unpublished entry after publication failure/loss, and preserve existing/winning credentials. Test publication cuts and real OS custody; zeroization alone does not discharge this.
3. Whole-request silence: r4373031553. Decision adopted: centralize identity's existing 30-second product budget in auth and consume it from identity/secrets, with deterministic stalled-header and successful-body tests plus redaction. The predecessor's clarification text is superseded. This is not yet implemented.
4. Keep the fixed retained-row deletion oracle at ca1b93b and its actual-old negative. Reuse all exact-source predecessor receipts; the final combined job exceeded 600 seconds and lacks a terminal exec receipt. Do not describe it as budget compliant or invent an exit status.
5. A6 remains separate from P1: extend existing VerifiedCapability/TrustEvidence/VerifiedScope and filesystem policy compiler; preserve historical versus current admission, real keyring head and lease publication fences, bounded admission, shared errors/interceptor/audit, consumer deletion and endpoint conformance. Proposed API is `../api/rb-identity.md`; no complete A6 implementation/pin is claimed.

Next measurable checkpoint is Pi + provisional-lifecycle closure with actual-old controls and signed exact-source receipts. New jobs use affected crates, shared admission and warm cache, under 600 seconds; each test stays below 10 seconds. Queue ownership includes checking admission before every push and dequeue/supersession before changing an admitted head. No merge date or cross-OS completion is inferred from this adoption.

status: WORKING

## Incoming gate and sessions handoffs, 2026-10-26

Adopted sessions' successor handoff `sessions-to-rb-identity-20261026T024500Z.md`. rb-identity owns the canonical A6 private-read decision and the thin TS authority-copy deletion; sessions owns its remaining host qualification using the released API. The explicit release/deletion contract now lives in `../api/rb-identity.md`. No released consumer API/pin exists yet; do not substitute the P1 head or existing descriptor verifier for that release. No sessions worktree was edited.

Adopted gate-speed's `gate-speed-to-rb-identity-20261026T023929Z.md`: SDK host Clippy uses all selected packages, all targets/features and short messages with `-- --cap-lints warn`, followed by canonical `gate-clippy.py` emit/compare against completed exact-base evidence with identical command, scope, features and toolchain. Retain strict direct denied-lint failures as failures; no exception or unchanged rerun. GNU native-runtime cross Clippy stays strict separately. Existing 22+2 proof is retained.

Future necessary producers use the installed  bridge with `RUSTC_WRAPPER=`, `BUILD_SLOT_JOB_MAX_SEC=510`, `BUILD_SLOT_PR=1518`, and an explicit phase. Declare and validate every referenced log/proof input through  before dispatch; qualified shared closure source is not evidence that it is installed. No producer was started to process these handoffs.

Original direct-lint logs were recovered read-only from WSL. At source `6cce1ec1b945d2aaa1d1494751a99a03bfc50446`, fs73 starts `rust/crates/filesystem/src/collection/buffers.rs:41:17: error: used expect() on an Option value`; core16 starts `rust/crates/harness-core/src/durable_host/observations.rs:122:9: error: used unwrap() on a Result value`. Originals:  and , respectively. These are baseline-ownership evidence, not a source failure of the touched config and not an exact-base canonical gate comparison. main-green receives the exact diagnostic lines and original log identities; rb-fs has separately acknowledged filesystem baseline ownership.

## Adoption-turn receipts

Signed `d49da74b98f894d75ad3c961ce847fa4f535ea38` was pushed to existing draft #1518 after reading all four queue files; #1518/#1520/#1510 were absent. No admission or predecessor-branch push was performed.

Auth regression command `cargo test --locked -p acyclic-auth --all-features --tests` passed all 104 tests on Linux (maximum binary runtime 1.23 seconds), terminal exit 0, 39.6-second wrapper.  job .

Warnings-denied all-target auth Clippy remains failing in test fixtures/examples/benchmarks; production auth source has no remaining diagnostic in this receipt.  terminal exit 1 (remote Cargo 101), 56.6-second wrapper, job . Failures include test indexing, fixture dead code/unwrap, example/test function length and benchmark unwrap/casts. This is not a whole-crate strict-green claim; no allow or raised limit was added.

Concurrent live rb-identity runner (session `01a16c2d-961b-7e20-944c-f0463cbcb52a`) subsequently began Pi migration in this same worktree. Its uncommitted Pi/layout edits were observed and preserved, not tested or included in the d49 receipt. Continue implementation with that runner; no second writer should modify those files.

PR metadata corrections are prepared at  and `p1-predecessor-adopted-pr.md`. They were NOT published: the GitHub connector returned 403 Resource not accessible by integration, and `gh pr edit` failed on deprecated GraphQL projectCards. The old #1510 clarification text consequently remains stale on GitHub; the ownership decision here supersedes it. No permission request or review resolution is implied.

2026-10-26T02:53Z: Using the current gh binary, published both prepared predecessor body corrections and returned inherited #1520/#1510 to draft (both unqueued; no head push/close/merge). #1518 is the sole P1 successor vehicle, now signed f51bd9acfe; no new PR was opened. This supersedes the earlier metadata-tool failure paragraph. Pi/provisional qualification runs on  no pass claimed yet.
