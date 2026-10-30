# Actors A4 handover to Cloud lane rb-actors

Codex b-runtime,2026-10-26. User-directed retirement. Ownership of SDK and hosted A4 is now **rb-actors**. No new PR was opened. No build, implementation or qualification work was started after the P0 handover steer. Existing queued PR1512 was left untouched and is Cloud's to finish.

## Branch, source and command state

- Worktree: ** Current branch **codex/b-runtime-actors-wave1**, pushed to origin at signed **4056b65d72ed901caef674a3ec01d93d630800fe**. Base **b7333953cbd5d3b31adc05cf2f0851f049f6c16a**. Remote exact head verified with git ls-remote. Tracked worktree is clean.
- First commit **252c28c27fd490186f7e17e1d55b47ffe82a9e9a** is the tested transaction/handler checkpoint. Second commit **4056b65d72** explicitly preserves **uncompiled, unformatted, unqualified** readiness work and its unfinished tests. Do not treat the branch head as a passing Runtime candidate.
- Legacy predecessor was also pushed for preservation: **codex/b-runtime-localhost**, signed head **169c02ee53266edf0f76bded3b70b81d8fa58baf**. This contains the prior large harness/localhost/fork implementation and incomplete parent authority edits. It is **not A4**; do not adopt the whole branch into the redesign. Main-green owns reconciling the retained authority source references.
- Last actual producer was Linux job ****, completed rc0 at02:14Z in100.8s. Its local command session has finished; no b-runtime build remains running. The push command completed rc0 too. Only untracked .bb-* historical helper scripts remain in this worktree; never commit them.
- No local compilation or local product test ran. No Cloud repository or credentials were accessed.

## Scope and API

Canonical scope:  and common.md; design audit ; API sketch **; verification specification ** The API file distinguishes implemented storage from planned Runtime signatures. At handover there is no b-runtime Cloud-review file under redesign/cloud-review.

Implemented at252c28c27f:

- `rust/crates/actors/src/guest/handler.rs`: Handler/Manifest/Delivery/Trigger/Transition/AlarmChange/Effect, reusing canonical guest Checkpoint/CodecIdentity/GuestEvent, domain code/limits/subscription/binding types and canonical account/namespace/id ActorReference. No second guest wire ABI.
- `rust/crates/actors/src/local.rs`: Store<P:StreamProvider> with load/claim/set_paused/publish. One final-record read restores state without scanning earlier checkpoints. One exact Head CAS publishes epoch, checkpoint, consumed cursor, alarm generation/deadline, pause state and outbox appends through the canonical provider commit.
- Outbox `Effect` retains its canonical CommitCondition. Effects to one destination coalesce only if their conditions agree. Destination conditions plus actor condition are committed together. Replaying an exact request with its original key/snapshot returns the same committed effects; no generated/salted key.
- `rust/crates/actors/src/local/tests.rs`: common storage histories exercised against MemoryStream and LocalStream, local reopen and16 bounded generated alarm histories. Actors native dev-dependency enables stream/local; Cargo.lock needed no change.

WIP at4056b65d72, **never compiled**:

- `local/ready.rs`: one shared FIFO ready index, per-registration metadata only; initial replay, notifications coalesced while queued and retained while work runs; Drop of Work parks/requeues; Drop of Registration unlinks queued entries and makes old weak wakers inert. Queue links are in metadata, so cancellation does not scan or retain tombstones. One dispatcher waiter installs under the same lock as the empty check. Register IDs have checked overflow; no per-entry ticket counter wraps. Uses stream::query_sync::SyncMutex so acyclic_loom instruments the real production primitive.
- `local/ready/tests.rs`: two ordinary tests including100000 dormant registrations, plus three cfg(acyclic_loom) schedules for append/replay/park, cancelled registration vs stale wake, and dispatcher empty/register handoff. These are source-only, **zero proof receipts**. No actual readiness mutant has been run.

Still absent: **local::Runtime**, shared due/alarm index, provider readiness integration, filesystem bindings, native/WASM package execution adapter, runnable localhost service, public ActorProvider conformance and arbitrary-endpoint runner. Manifest limits/codec/bindings are declarations only so far; Runtime must enforce them, validate source records and pin the deployed package identity. Store is a host-internal transaction primitive, not an authorization or package-admission boundary. No Windows/macOS, Clippy or full Runtime qualification exists for A4.

## Actual proof and failures

Producer command, from the tested source worktree:

`bash .bb-a4-store.sh`

Driver retained at **[private-metadata-removed] (copy to the new owner's own worktree as an untracked .bb-* helper before using; do not run it unchanged against an adopted head and assume an identical receipt).

Actual package command:

`cargo test --locked -p acyclic-actors --all-features --lib local::tests -- --nocapture`

Six positives passed,0.05s libtest: memory/local atomic_history, memory/local epoch_alarm_pause_fences, reopen_preserves_the_complete_committed_tuple, bounded_alarm_histories. Three actual production mutations compiled and failed unchanged assertions in0.00s each:

1. Remove owned epoch comparison; memory_epoch_alarm_pause_fences fails **epoch fencing**.
2. Remove alarm generation comparison; same history fails **alarm generation fencing**.
3. Publish outbox and state in separate provider commits; memory_atomic_history fails **one checkpoint/effect transaction**.

All controls were restored. Whole uploaded input inventory, all six formatter outputs and post-control restoration were admitted before applying the Linux formatted files locally. Evidence archive **[private-metadata-removed] SHA256 **af96f5e9459b56961f5775c1352f4a7501457945a67f3ba4a073387755fe876d**; dispatch/build log **a4-store-conditioned-producer.log** in the same directory. The archive contains source hashes, Cargo/test logs, mutation receipts and formatter/restoration hashes. This producer does not retain a separately hashed test executable; do not present it as the complete cross-OS EXE provenance join requested by verify.

First attempt, same Cargo command, failed3/6 with Stream(InvalidArgument): my outbox mutations omitted mandatory per-destination conditions. Fixed by retaining each effect's canonical condition and coalescing consistently. First archive retained as **a4-store-first-evidence.tar.gz**, SHA256 **0874d852d0392431d04de1e7a7f6d34e6e2a4b2cc1c73bdd7a122a9505350180**. Do not describe that failure as environmental.

**Current full WIP head has not been built**. Suggested next scoped commands after source review/format/input admission, not completed results:

- `cargo test --locked -p acyclic-actors --all-features --lib local::ready::tests -- --nocapture`
- `RUSTFLAGS='--cfg acyclic_loom' cargo test --locked -p acyclic-actors --all-features --lib local::ready::tests::schedules -- --nocapture`

Dispatch both only through the shell from the new owner's worktree, all jobs<600s. Do not locally compile to format or discover errors.

## Contract decisions and gotchas

- **Newer verify review needs reconciliation:** latest steer says Keep preserves alarm generation. Current Store consumes an Alarm+Keep by clearing the deadline **and incrementing** generation; current fence test expects generation3 after firing generation2. Decide/align to the canonical newer rule before publishing final conformance. Set/Clear always increment with checked overflow. Do not call the current passing test proof of the newer Keep rule.
- Atomicity is committed checkpoint/cursor/alarm/outbox effects. A physical handler can execute again after a crash; external HTTPS/process effects before commit are not exactly once.
- Store snapshots and explicit effect conditions must be retained for exact-key replay. Re-reading outbox tails on retry would change the request digest. Do not add an automatic retry loop or manufacture a new key after uncertain acknowledgement.
- State stream embeds the complete canonical ActorReference encoding. Runtime still needs the admitted provider/account/manifest boundary; Store currently rejects effects targeting its own state stream only. Binding enforcement must prevent access to other actors' internal state too. Store alone does not admit package identity/codec/limits or prove that a supplied Trigger names a real source record.
- Current Streams follow/follow_updates returns a pending per-path stream state machine. A4 must not park one future/task per idle actor, and an Actors-only append wrapper would miss external appends. **Concrete request: , for one cancellable Waker registration through the canonical shared Streams session/publication index. A1-A3 owner is **rx-hostability** (SDK+Cloud). That file awaits owner/coordinator relay; no callback API was added to Streams here. Do not implement a polling substitute or second notifier path.
- A5 owner is now **rb-fs**, SDK+Cloud. A6 is shared auth owner. Cloud native/WASM execution should reuse its package ABI; no new interpreter/dependency was chosen in this SDK work.
- Shared ReadySet has O(1) metadata per registration, O(log population) map work and a single shared dispatcher waiter; no per-actor future/timer. The100000-entry source test is unrun and proves no allocation bound yet. Actual Runtime must count registrations, ready work, due nodes and live heaps/futures, and bind the real Stream append seam to its Loom/history proof.

## Open PRs and gate state

Only open PR under codex/b-runtime* is **https://github.com/acyclic-labs/sdk/pull/1512**, branch **codex/b-runtime-wire-selections**, frozen signed head **559e642d96b2f24793521917e1e7080485f486e0**. It contains RBSDK-092 and both reference-only secrets paths. Those items were parked/transferred to Cloud by the latest steer; **Cloud owns completing this PR**. No edits or push were made to it during handover.

At02:23Z API read: OPEN,ready(not draft),mergeCommit null,reviewDecision REVIEW_REQUIRED. GraphQL reviewThreads returned **zero threads**, so no unresolved review body is outstanding.  last says fails[],threads0,closing[],READY. Batch log ** has target failures and no final causal verdict observed: harness DST sources::mount_callbacks_cannot_read_host_clocks_directly, harness-core memory-content native_host_original_key_recovers_immutable_upload_after_head_edit, GraphCoder presentation wrapped-answer oracle.1508 touches those areas, but no causal isolation proof is claimed. Follow the gate/bisector rather than label unrelated/flaky. Last check left frozen head unchanged; no merge SHA yet.

PR1512's retained wire proof predates A4: real wire absence/status table3, explicit secret selection3, manifest21, predecessor descriptor12; actual old status/lowering controls; paired reverse21-package all-feature Clippy+0. Evidence/body pointers: [private-metadata-removed] and wire-exact-final/wire-generated-final/wire-static-final archives. These prove the urgent wire candidate, **not Actors Runtime**. No new PR should be opened by the retired SDK lane.

## Legacy debt and owner handoffs

Main-green owns legacy retained parent/content authority, ready queued1515 frozen13d9e15ffddb92a385dff23a6f4ae0beea8ab9d1. Read ** It explicitly retains native None-IO public-entrypoint audit and will reconcile selected-policy/original-publication behavior from signed legacy **4ba222474ce4916ff6e8c094056ebbc4a97ca41e**. Do not duplicate that producer graph in A4.169c preserves later **unqualified** parent/Arc changes; its last producer command `--env PARENT_STAGE=authority bash .bb-parent-publication.sh` failed E0609 at seven stale raw-scope/verifier field references in summary_fork.rs. That failed branch is semantic reference, not a passing dependency.

Earlier interrupted legacy producer hit the old30min ceiling; recovery-only job retained75 positives and2 actual planning controls, but one mutant source was not restored when killed. Archive **[private-metadata-removed] SHAb7e462395c9ca27e450bf9a89c158c4abe8ee75c20fe6539abeb49d2a3e2f6aa. No claim of complete controls or full restoration follows. Do not rerun the obsolete broad producer; all new jobs<600s.

E2e-gate's original P5 debt request remains **unclosed evidence debt**, not fixed by redesign retirement. Full rows and exact source/negative/three-OS receipt requirements: ** and  Missing current production/EXE/timing joins must be explicitly retained/owned; no ignored Unix fixture qualifies another OS.

## Next five actions for rb-actors

1. Fetch the pushed A4 branch into your own worktree; review tested252c vs uncompiled4056 separately, resolve the newer Keep rule, and read any Cloud A4 review. Do not adopt the legacy branch wholesale.
2. Arrange the one Streams shared-session readiness registration with rx-hostability; consume its actual SDK pin and rb-fs's actual A5 pin. Keep the concrete wake guarantees in the linked request; no per-actor polling/follow future.
3. Qualify the production ready primitive on Linux with real Loom schedules, actual omitted-dirty/registration controls and100000-idle ownership counts; retain complete source/Cargo/EXE receipts and formatter outputs. Correct any WIP compile/layout mistakes before claiming a pin.
4. Finish one Runtime, shared due index, package/native-WASM execution and localhost APIs over canonical stream/fs, with atomic epoch/cursor/checkpoint/alarm/outbox and pause/recovery custody. Add no harness migration in wave1.
5. Publish the **single** public provider-generic ActorProvider conformance and env-driven endpoint runner, run identical memory/local/hosted histories and actual semantic controls under10s, then one signed ready PR for the whole A4 scope. Separately finish frozen1512's Cloud-owned gate and report its real merge SHA.

Attribution: Codex <noreply@openai.com>.
