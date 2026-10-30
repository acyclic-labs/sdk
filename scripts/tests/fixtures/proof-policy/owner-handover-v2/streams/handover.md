# Streams A1-A3 handover to rx-hostability

2026-10-26, prepared immediately on delivery of streams.handover.md. This is a signed WIP transfer, not a qualified production pin. Implementation has stopped. No new PR or gate was opened.

## Source and ownership

- Branch: `redesign/streams-wave1`, pushed to `origin/redesign/streams-wave1` without force.
- Head: **41d8b46a5e3e18786d601b125b586b5b7f413506**. `git log -1 --format='%H %G? %GS'` reports G; SSH signing identity 66573927+Var1377@users.noreply.github.com.
- Commit: https://github.com/acyclic-labs/sdk/commit/41d8b46a5e3e18786d601b125b586b5b7f413506
- Worktree: `[private-metadata-removed]`, clean after commit. Original dirty `Q:/sdk` was not modified.
- Base: `b7333953cbd5d3b31adc05cf2f0851f049f6c16a` (origin/main at the last fetch). Rebase/adopt current main before the eventual domain PR; never amend another queued head.
- New owner: Cloud **rx-hostability**, both SDK and hosted implementation.
- API sketch: . Read `common.md`, `streams.review1.md`, `streams.review2.md`, `cloud-review/streams.md` and `streams-cloud-contract.md` with it.
- Open SDK PRs for this branch: **none**, verified `gh pr list --head redesign/streams-wave1 --json number,url,state` => []. No queue entry or GitHub review threads. Existing predecessor/frozen PRs were untouched. Cloud review mentions its separate draft #6645; that is not this branch.

## Implemented WIP

1. Persistent segment trie with persistent AVL sibling indices in `stream/src/memory/namespace.rs`, immutable Arc histories/payloads, shared subtree nodes and metadata aggregates. Root replacement under the real provider lock establishes one pre-commit cut. Fork is constant in descendant cardinality plus path/index and exclusion input work. Descendant enumeration remains only in recovery/snapshot traversal, not fork publication.
2. Existing ForkRequest/CommitMutation::Fork/CommittedFork extended with canonical segment-relative `exclude`. Nested destination inside source uses the pre-commit root; excluded siblings do not leak, textual prefix siblings survive; disjoint destination mutations compose with source appends. Existing sole Fork mechanism, no ForkSubtree variant/shim. Proto field numbers: ForkRequest.exclude=5, ForkMutation.exclude=5, CommittedFork.exclude=6.
3. Conditional conflicts retain neither key binding nor local journal frame. Successful bindings retained for authority lifetime. Tail(0) matches absence. Rust/TS high-level helpers require caller keys; raw provider request keys remain optional. Prepared native settlement prepays persistent namespace nodes/AVL spines.
4. Unpaged/live children removed from domain/provider/proto, native/WASM bindings, TS, and obsolete tests/models. `children_page` is the listing. Browser notification implementation renamed discovery.rs -> notifications.rs. Push follows reuse the canonical bidi/WebSocket session and bounded credit; no new JS session state machine.
5. Public generic `conformance::verify<P: StreamProvider + ?Sized>(&P)` plus `examples/provider-conformance.rs` using ACYCLIC_CONFORMANCE_ENDPOINT, ACYCLIC_CONFORMANCE_TOKEN, optional ACYCLIC_CONFORMANCE_CA. Includes identical key retries, absent Tail0, subtree/nested/exclusion/private divergence, and a spawn-shaped envelope (subtree Fork + two volume authority Forks + ForkAdopted).
6. Verify's five-case suite integrated from signed df031ac762773611d634b97d331e924c3b75f1bb, plus new subtree proptest and real-lock Loom controls. `wave1_recovery` exercises local journal and forced snapshot reopening.
7. A7 page-tail projection was assigned in the sketch while working. **Partially implemented/unqualified**: ChildrenPageRequest.tail_stream / TS tailStream, optional relative path, proto field5; Child.tail_record / TS tailRecord, proto field2. Memory captures membership and tails under the same namespace read lock, enforces aggregate protobuf byte budget including headers/framing. Generic conformance, byte-bound test, and two real-lock Loom cases added but final qualification remains pending.
8. New canonical InvalidResponse error/code + grpc DataLoss mapping makes malformed peer response terminal instead of retryable Unavailable. Final run exposed an old assertion expecting Unavailable; see failures.

## Retained proof (all built on Linux)

Logs copied alongside this file; do not represent older snapshots as final-head qualification.

- Snapshot **1a7ae0d2e73652a46ef59b030ce6b52f16b36307**: 375 Rust lib tests passed (9.45s suite total), ten wave1_keys memory/local tests passed (0.06s), wave1_recovery passed (0.24s). `streams-tests.log`. Includes 48-case subtree proptest, constant allocation vs 2/1024 descendants and prepared native prepaid tests. This predates page-tail/volume-case additions and InvalidResponse.
- Snapshot **d0e2ff7b4fe360e5c03b3488d5886c009be21d0c**: four Loom tests passed in 0.00s; real namespace lock, atomic two-descendant fork cut and nested exclusions, with root-only and ignore-exclusions negative controls. Command: `RUSTFLAGS="--cfg acyclic_loom" cargo test --locked -p acyclic-stream --features local --test wave1_loom -- --test-threads=1`. `streams-loom.log`. Two page projection Loom cases added later have NOT run.
- Snapshot **ec793ef7f3fc134e0962d174ada8bc3011f4be48**: `node scripts/generate.mjs stream`; `ACYCLIC_WASM_PROFILE=dev node scripts/build-wasm.mjs stream`; `node node_modules/typescript/bin/tsc -b typescript/packages/stream` all passed. Logs streams-generate.log, streams-wasm.log, streams-tsc.log. Stream Rust/WASM plus native bindings declaration generation compiled, but production native addon not built/staged.
- Verify old-main negative controls:  and  All ten five-case tests failed the intended old b733 invariants in 0.15s. Build/setup failures were not counted as controls.

## Exact current failures / unrun checks

The final already-started job completed after retirement arrived. Snapshot **6e3310f979d50a02482b09cef7b4b63c494143ed** corresponds to final WIP code before whitespace cleanup. Command:

`cargo test --locked -p acyclic-stream --features local,http,test-support --all-targets`

Result: **374 passed, 6 failed**, 9.77s for lib suite; stopped before other targets. Full `streams-rust-all.log`. Failures:

- conformance::tests::memory_provider_passes_the_public_suite => LimitExceeded
- grpc::tests::grpc_transport_passes_the_public_suite => LimitExceeded
- local::tests::local_provider_reopens_and_passes_public_conformance => LimitExceeded
- local::tests::a_half_full_journal_is_compacted_into_a_snapshot => LimitExceeded
- local::tests::a_journal_torn_before_its_header_completed_is_created_again => LimitExceeded
- session::client::tests::isolation::malformed_discovery_closes_only_its_node_and_keeps_later_opens_live: actual Some(Err(InvalidResponse)), old expected Some(Err(Unavailable)).

Likely direct cause of the five LimitExceeded conformance failures: newest `verify_page_tails` creates `CommitRequest { conditions: vec![], ... }`, while validate_commit_shape rejects empty conditions. Add meaningful conditions for its two status writes. This is an inference from source, not yet verified. The page-tail Loom writer also has empty conditions and needs the same correction.

Other explicit unfinished details:

- grpc/client.rs and wasm/remote.rs still construct Child { tail_record: None, ... }; must decode and validate returned projected records using wire_codec::record. Canonical memory->wire conversion already emits records. These two response adapters were missed by a text replacement, so projection is NOT complete across providers.
- Page tails need HTTP fixture/endpoint, browser, type-level and authorization conformance. Memory byte test passed in the final native run; page cut Loom cases unrun.
- Missing-path follow contract conflict: newest delivered Verify steer requests prefix watches before creation, while updated Cloud review #7 (01:56Z) and API sketch explicitly accept NotFound/no side effect and say Verify corrected its oracle. Production follows NotFound. New owner should reconcile the delivered requirements; do not silently claim pending missing-path subscriptions are implemented.
- `bun test typescript/packages/stream/test` on ec793 snapshot: missing generated/native/binding.cjs causes native credential/session tests to fail; stale record/HTTP codec expectations; timer count expected1 but actual2 (cleared Welcome + registration deadline). Shared session transport tests passed (64 native follows + LATEST on one HTTP2 session; 64 browser WebSocket follows, no fetch polling). The run hung on malformed hosted frames and was stopped (only its Bun PID killed). `streams-ts-tests.log`.
- WIP fixes those stale expectations and adds InvalidResponse terminal classification but has NOT rerun TS tests after them. Check WASM `decode_frame` errors too: only JS adapter code and non-Uint8Array errors were changed; malformed protobuf may still surface Unavailable/reconnect. Do not assume the hang is fixed merely because code compiles.
- Production native addon is not built/staged. Use the canonical producer, not an ad-hoc .so rename: `node scripts/build-stream-native.mjs build --target x86_64-unknown-linux-gnu --output ${TMPDIR:-/tmp}/cx/tmp/streams-native-bundle --target-dir `, then `node scripts/build-stream-native.mjs stage --bundle ${TMPDIR:-/tmp}/cx/tmp/streams-native-bundle`.
- BrowserStream real browser conformance has NOT run. `browser_wave1_conformance` exists and invokes both generic suites.
- cargo fmt check was nonzero before final changes (`streams-fmt.log`); final source is not rustfmt-clean. Only trailing whitespace was removed before WIP commit. No final Clippy or protocol contract gate. No final full-head proof receipt.
- A7 never-requested vs retired/generation-fenced channel distinction remains unimplemented: pool.rs::receive currently refunds/accepts missing route. Client-perf requested a bounded exact ownership seam without unbounded tombstones; see steer below. Do not add a parallel credit loop.
- A5 retained namespace-root cut/lease/query API is NOT exposed. Existing atomic caller CommitRequest/Fork seam is implemented, but filesystem still needs public retained-cut custody for multi-volume page windows. Avoid claiming per-volume latest reads satisfy it.
- Admission checked_subtree currently builds a temporary excluded spine with an uncharged builder for metadata validation, then publication rebuilds charged nodes. Cost bounded by exclusions, but native admission/custody should be reviewed.
- Some stale test names mention live children/discovery even though their implementation now exercises record follows only; cosmetic cleanup pending. No compatibility children API remains.

## Next 5 steps (rx-hostability)

1. Adopt signed SDK branch into the Cloud-owned lane, reconcile latest missing-path follow decision, read exact schema fields and retained-cut needs, and fix empty test commit conditions plus final malformed-frame terminal classification. Complete grpc/WASM tail projection decoding.
2. Format only affected files; run scoped Rust tests/wave1 recovery and six-case Loom suite, including per-test <10s and existing negative controls. Finish missing inherited-root wake proof and crash/lost-ack spawn history. Preserve successful-key lifetime and pending-attempt fences in hosted recovery.
3. Generate bindings/WASM, build canonical native addon, run TypeScript suite, Rust HTTP fixture conformance and actual BrowserStream/WebSocket browser conformance. Then run same public generic endpoint runner against hosted; do not fork the suite.
4. Finish shared retained cut/holds with A5, bounded issued/retired channel ownership with A7, and coverage + policy-root fence with A6/Cloud. These are not independently implemented here. Publish one exact signed qualified SDK/Cloud source pin to consumers.
5. Rebase current main, open the one complete domain PR only after qualification, answer reviews, and queue through submission.sh under the active owner. This retired lane opens no PR and queues nothing.

## Linux and coordination gotchas

- All builds were on Linux. Workspace `${TMPDIR:-/tmp}/cx/wt/cx-streams`; target ; relay `${TMPDIR:-/tmp}/cx/relay.git`. Last checkout 6e3310f979d50a02482b09cef7b4b63c494143ed. All jobs completed/stopped; no running build is handed over.
- `source ${TMPDIR:-/tmp}/cx/env.sh`; `PATH=`; Node24.4.1, Bun1.4.2. RUSTC_WRAPPER= No broad workspace build; jobs <10min; reuse receipts, not identical reruns.
- Windows snapshot helper  fixes stdin bundle stalls with SCP and relay ref recovery. It snapshots worktree to signed? synthetic commit-tree objects (do not treat synthetic snapshots as delivered signed WIP), resets remote checkout BEFORE target lock, startup typically1-2min. Never launch it over an in-flight lane job.
- Bun deps installed frozen lock. Generated WASM/proto/native declaration files are ignored; generation left no tracked diff. Final page/error schema changes require regeneration.
- Chrome for Testing installed on Linux: `${TMPDIR:-/tmp}/cx/opt/chrome-streams/chrome-linux64/chrome`, driver `${TMPDIR:-/tmp}/cx/opt/chrome-streams/chromedriver-linux64/chromedriver`, version156.0.8078.47. wasm-bindgen-test-runner installed. Set browser binary explicitly and --no-sandbox; browser tests remain unrun. scripts/browser-test.mjs provides existing pushed CDP pipe harness with9s deadline.
- Steers sent and logged: streams-to-b-authority-20261026T0138Z.md, streams-to-fs-concurrency-20261026T0138Z.md, streams-to-verify-20261026T0138Z.md, streams-to-efficiency-20261026T0138Z.md (). Earlier claimed02:15 proof/02:30 queue targets are superseded by retirement; no qualified ETA asserted.
- Pending inbound:  (canonical session/credit ownership); fs-concurrency-to-streams-20261026T0133Z.md (retained domain cut/lease and caller-owned fork_many composition). Relay Cloud through coordinator. No edits to other lanes' branches.

status: RETIRED handover
