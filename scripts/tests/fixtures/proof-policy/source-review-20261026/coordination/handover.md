# AUTH / SECRETS handover: b-authority to rb-identity

Recorded 2026-10-26 02:23Z. Sole SDK/Cloud owner is now **rb-identity**, per the user-directed topology change at 01:12:38Z. This supersedes status paragraphs in  and . No new implementation, build, PR, queue action or push started after processing retirement. All production work is signed and pushed. New owner worktree: , branch `rb/auth-security`, starting at b7333953cb.

## Branches and PRs

| Work | Worktree / branch | Signed pushed head | State observed 02:20Z |
| --- | --- | --- | --- |
| P1 predecessor [#1520](https://github.com/acyclic-labs/sdk/pull/1520) | ; `codex/b-authority-p1-security` | `3b6b467cbd379eeb7779c8f644a10ee0839a6f39` | OPEN, ready, **not queued**, unfrozen; two new unresolved findings |
| Secrets predecessor [#1510](https://github.com/acyclic-labs/sdk/pull/1510) | ; `codex/b-authority-secrets-6547` | `ca1b93bbe580cd8d8f4a2d48ecd09775b29fcea5` | OPEN, ready, **not queued**, unfrozen; one unresolved transport finding |
| A6 redesign | No b-authority implementation branch, PR or API sketch | None | **Not started**; #1520 is P1, not A6 |

Both tracked worktrees are clean, upstream equals HEAD, and signature status is G. Untracked `.bb-p1-*` / `.bb-secrets-*` are scripts/artifacts, not missing production changes; do not bulk-add them. P1 commits: `65e31baeaf47814cd1e91c7fb9518f5aa9bacdf4` implementation; `b7844dbe9caf152665d3e62700821ffecf409f7d` independent locks; `3b6b467cbd379eeb7779c8f644a10ee0839a6f39` precise denial assertion and fixture diagnostics. Base last inspected: `b7333953cbd5d3b31adc05cf2f0851f049f6c16a`. Recheck origin/main before integration.

Queue observation: pending1508,1515; active1495,1504,1508,1512; held-pending and baseline-pending empty. Neither authority PR is admitted. Older notes describing frozen #1510 are historical. **Once admitted, never push without dequeue or supersession.** Leave other lane heads alone.

#1520 SDK Static, aarch64 Linux and latest readme-guard succeed; an older duplicate readme check is cancelled. Cubic neutral, Greptile SUCCESS02:16:10Z, but two blocking findings below. #1510 checks also succeed; review remains blocking. Check success does not close a review finding.

## Open review findings

1. **P1 Pi seeds still on disk:** [r4373450972](https://github.com/acyclic-labs/sdk/pull/1520#discussion_r4373450972), thread `PRRT_kwDOUMoiI86wHeKO`, unresolved. `plugin/src/pi_owner.rs` retains `PiOwnerConfig.key: [u8;32]` around118–120 and constructs `AuthorityRoot::new` from it around306–316. Unix owner framing/digest is not encryption. Local/GraphCoder migration does **not** close Pi. Move Pi to shared `SigningKeyReference`/bootstrap, migrate authenticated records preserving identity, prove no seed in new durable bytes. No fix/control yet for this new finding.
2. **P2 abandoned creation leaks credentials:** [r4373450978](https://github.com/acyclic-labs/sdk/pull/1520#discussion_r4373450978), thread `PRRT_kwDOUMoiI86wHeKR`, unresolved. `SigningKeyReference::create` persists before owner publication; failure or losing publication leaves a persistent entry. Add one shared provisional lifecycle: commit ownership after durable publication; delete only the newly created entry on abandonment, preserving winner/existing credentials. No implementation/control yet. Zeroization is not persistent cleanup.
3. **Secrets transport silence:** [r4373031553](https://github.com/acyclic-labs/sdk/pull/1510#discussion_r4373031553), thread `PRRT_kwDOUMoiI86wGeOt`, unresolved, valid. Native `HttpTransport` in `rust/crates/auth/src/secrets.rs` has no whole-request bound for headers and successful body. See Decisions: earlier coordinator-clarification block is superseded. No transport fix yet.
4. **Deletion receipt oracle FIXED:** [r4373031559](https://github.com/acyclic-labs/sdk/pull/1510#discussion_r4373031559), thread `PRRT_kwDOUMoiI86wGeOy`, resolved at ca1b93b. Public provider conformance lists after deletes and requires empty scope at final acknowledged generation. `secrets_conformance_rejects_successful_deletion_receipts_that_retain_rows` rejects valid advancing receipts with retained rows. Actual old receipt-only oracle accepts that provider and fails regression. Nine secrets tests pass0.00s.

## P1 implementation present in #1520

- Parent-only issuance: trusted `AuthorityRoot` replaces old root issuer throughout composition; `AuthorityIssuer` holds public trust and issues from a verified parent via existing narrowing. No alias. `delegated_issuance_requires_a_parent_and_refuses_every_widening` accepts read narrowing, rejects write/read+write widening and revoked parent. Trusted root widening is the control.
- Mandatory common Git authority: `tools::git::Workspace` requires `WorkspaceAccess` with scope, ceiling, logical VolumeRef. Admission before tree/patch reads and around effects. Existing shared FS policy compiler; raw exposure requires unrestricted valid policy. Sole `tools/git/access.rs` walker includes branch, stash, retired, pending and reconciliation sources; checkout duplicate deleted. Unknown recovery actions deny. Plugin integration now `plugin/src/control_plane/git.rs`.
- Foreign workspace ancestry: `WorkspaceGraph::authorize_descendant` and `ancestors` share one walker; host copy removed. Trusted anchor need not be registered; cycle/foreign denies. Common local owner adapter in `harness-core/src/filesystem/native_execution/local_owner.rs` consumes existing host/mount compiler.
- Bash/fs and mount ceilings: `NativeMountAuthority` preserves signed caller path ceiling. Legacy delegated raw mounts need logical volume read AND write, never directory-only expansion. Raw Bash/fs refuses restricted policies/ceilings. Real former-path Bash writes `echo compromised > secrets/key`; product denies without changing the restored file. Typed callback uses existing policy compiler.
- Shared crypto: auth `authentication_eq` uses subtle; strict Ed25519 `verify_signature` replaces harness metadata copy. Scope proof/workload, Pi key equality, owner digest and certificate/key comparisons use common authentication comparison. Public IDs/content hashes remain normal equality. Strict controls mutate every byte, truncate every prefix, change key/payload; bad prefix-only comparator accepts planted truncation.
- Local/GraphCoder custody: `native-runtime/src/signing_key.rs` owns opaque content-bound `SigningKeyReference`, service `acyclic.authority.signing-seed.v1`, OS keyring adapters, zeroizing entropy/seed, missing/replaced refusal. Local `harness/src/local/owner.rs` writes reference only, authenticates private source descriptor, migrates without identity change, bootstraps through reference. Explicit legacy bare-JSON migration test; framed records share path but no additional new explicit framed fixture. **Pi remains open.** Explicit memory/WASM/test `AuthorityRoot::new` still exists; A6 sole bootstrap API incomplete.
- Real headless fixture: `scripts/with-owner-keychain.sh` shared by PR/main tests starts isolated HOME/XDG/private DBus and real Secret Service. Fetches real Ubuntu gnome-keyring dependencies privately if absent; no mock/developer collection. Unlock and start registration both needed. Missing-command control refuses before service start; failures name step and exact recovery.
- Dependency closure: Cargo-generated13 independent locks retain custody dependencies, preserving existing package/version pairs. Actual old auth-fuzz lock fails locked admission.

## Decisions and known limits

- **Secrets request budget:**01:01:54 ownership steer directs choosing the existing consistent product design. Adopt identity's existing30s whole-request deadline (`plugin/identity/src/lib.rs:421`) in the lowest shared auth request-budget/transport layer, consumed by identity and secrets. Cover headers and success body, preserve redaction; deterministic stalled-peer proofs under controlled clock, not a watchdog/retry/skip/raised limit as acceptance. **Decided here, not implemented.** #1510 thread still says awaiting clarification and is stale; owner updates on fix.
- Missing/replaced custody must never regenerate identity. Preserve source descriptor/durability guards; Pi consumes same module, not another key store.
- #1520 ~237 files are mostly mechanical root rename; large additional diff is13 Cargo locks. It is a legacy P1 predecessor, not A6. Current main already has Ed25519 chains, `VerifiedScope`, `AccessView`, `NativeMountAuthority`; audit8438 site counts are historical, not current inventory.
- #1520 body source `[private-metadata-removed]` still labels final checks in-progress and overstates complete P1 closure in its lead. Update around two open findings/final receipts before queue. No macOS execution, hosted qualification, full-workspace runtime or three-OS closure claimed.
- A6 sketch  and Cloud review  absent when inspected. No signed A6 source/proof PR. No supportable merge date; next measurable checkpoint is adoption plus closure of two P1 review findings with exact-source receipts.

## Proof and build receipts

All Cargo compilation through `...` on Linux/job-owned targets; no local Cargo/direct SSH Cargo/root targets. Windows runs Linux-built MSVC via canonical runner. Named runtime groups under10s. **Some combined jobs exceed the subsequently delivered10min rule; receipts do not prove compliance. Future affected-crate, pre-admitted warm-cache jobs must finish under600s.** No identical reruns or broad builds for scheduling.

Evidence: `[private-metadata-removed]`, `p1-record-qualification.py`, `p1-clippy-comparison.json`, `p1-lock-inventory.json`. WSL Ubuntu log root .

| Job / log basename | Evidence |
| --- | --- |
|  / `.log` | keychain1/0.19s; signatures2/0s; Git22/2.22s; authority21/0.01s; Git views6/0.72s; lineage4/0s; host family1/0s; mount1/0.01s; local owner18/3.65s. Then FAILED overly narrow checkout assertion; whole job not green. |
|  / `.log` | final Git22/1.00s; reducer properties8/4.46s; harness Clippy |
|  / `.log` | nine-package all-feature/all-target Linux Clippy, strict Windows GNU native-runtime Clippy |
|  / `.log` | same-selection b733 lint baseline: candidate4331 vs4330; only new doc backticks corrected/final delta checked. Not a clean -D warnings claim. |
|  / `.log` | actual wasm32 compilation auth-wasm/core/harness/providers, not browser execution |
|  / `.log` | MSVC build and actual Windows keychain1/0.01s, runner0.438s |
|  / `.log` | standalone auth fuzz all-feature/all-target Clippy, generated13 locks |
|  / `.log` |13 locked offline admissions; old-lock negative0.089s; providers standalone fuzz Clippy |
|  / `.log` | final log complete: Linux keychain1/0.08s; corrected Bash1/0.01s; plugin current-policy/exact-workspace1/0.05s; downstream final Finished dev, no compile error. Start02:03:26Z, final log02:18:37Z ~15m12, OVER new job budget. Exec session63839 gone, terminal return object unavailable; do not invent exit receipt. |

Windows artifacts `[private-metadata-removed]`, EXE SHA256 `fc7d5e45d5186f465028ace801e1c77008e1a78cab54141b59ec13adf957ac67`.

3b6 corrected test precision, not product policy: directory-only legacy task first refuses child selection with `Unauthorized("child workspace has no original owner")`. Test now asserts precise denial and independently proves raw legacy mount refusal. Typed cases still demand policy-boundary denial; arbitrary error cannot satisfy.

Commands below are retained evidence selections, not requests for identical reruns. Execute new changed-source selections only via the shell:

```powershell
bash scripts/with-owner-keychain.sh cargo test --locked -p acyclic-harness --all-features --lib raw_checkout_commands_reject_restrictive_policy_and_caller_ceiling
bash scripts/with-owner-keychain.sh cargo test --locked -p acyclic-plugin --all-features --bin acyclic host_managed_routes_bind_current_policy_and_exact_workspace
cargo test --locked -p acyclic-harness-core --features filesystem-local,native-execution,test-support --test git_executor
cargo test --locked -p acyclic-harness-core --features filesystem-local,native-execution,test-support --lib core::tests::authority_reducer_properties
cargo check --locked -p acyclic-harness -p acyclic-harness-providers -p acyclic-harness-fuzz -p acyclic-plugin -p acyclic-conformance --all-features --all-targets
bash  acyclic-native-runtime --all-features --lib signing_key::tests
```

Linux lint command: `cargo clippy --locked --keep-going -p acyclic-auth -p acyclic-native-runtime -p acyclic-fs -p acyclic-harness-core -p acyclic-harness -p acyclic-harness-providers -p acyclic-harness-fuzz -p acyclic-plugin -p acyclic-conformance --all-targets --all-features --message-format=short -- --cap-lints warn`. Windows GNU: `RUSTFLAGS= CARGO_ENCODED_RUSTFLAGS= cargo clippy --locked --target x86_64-pc-windows-gnu -p acyclic-native-runtime --all-targets --all-features --message-format=short`.

Lock archive recovered read-only from successful producer after base64 interleaving corrupted first transport, no identical rerun: `${TMPDIR:-/tmp}/wt-sdk/b-authority-p1-security-7299a50e.2-`; local `[private-metadata-removed]`, SHA256 `272cdb8e02dd3cb76a8ecc04716d066cf1ea4e16e889e9ce12dbc3a59163c719`. All members validated before import.

#1510 prior auth109/identity50/WASM-TS-descriptor/downstream proofs in . Final deletion positive/actual-old negative log`; terminal script failed log grep because rg absent, changed to grep. Successful rustfmt/auth lint follow-up `.log`. No test closes transport silence yet.

## A6 remaining scope and coordination

One grant/trie decision; verified parent chain with path/ops/expiry/revision; verify-once scope; sole keychain issuer bootstrap; exhaustive shared typed errors; one sensitive refresh interceptor; audit stream; commit/mount/facade/S3/tools enforcement and revision/lease cache fences. Delete actual current duplicates, historical36 checks/43 verifications; per-event harness MAC removal is user-directed. None of complete A6 integration is delivered by P1. Public provider-generic conformance plus arbitrary endpoint/env credentials runner required. Read `redesign/common.md`, `design-audit/REDESIGN.md`, `auth.md` and current Cloud objections.

- **main-green:** existing VerifiedScope/compact TrustEvidence reused in signed unpublished517b809cb2/ec236e1d5c, no PR at dispatch. Owns core FS content_verifier, parent/*, project_workspaces, summary_fork, conversation volumes adapter. Preserve fresh trust/time/cancel/original allocation. Recursive1024 forks10.028s exceeds10s. Profile7.213s/100 samples27 leaf Curve/Ed25519; inclusive verify_record18/verify_trusted11/record7 overlap. Per-record signer/cert construction at core/authority/workload/admission.rs:23, workload.rs:108; receipt/event/reducer/fork verification repeated. Centralize via A6 preserving historical admission vs live time/revocation. Receipts , `[private-metadata-removed]`.
- **sessions:** delete TS grant/identity/byte authorization copy `typescript/packages/harness/src/task-context.ts:98–119` through canonical Rust runtime/content_read.rs:431 and conversation/private_read.rs:5. Keep contracts.verifyFileBytes descriptor verifier. Different internal/.system handling. Prove grants, owner/provider, retained generation, internal/escape paths, exact bounds through Rust/native/browser/installed TS plus old/bypass controls. ; no extra bridge/reducer/TaskVolume dependency.
- **streams:** Fork includes entire subtree except exclusions: ForkRequest.exclude5, ForkMutation.exclude5, CommittedFork.exclude6; RelativePath canonical nonempty StreamPath. Cover all included descendants AND destination/protected/excluded paths; exact-root grant cannot authorize subtree, no descendant enumeration or capability inheritance. Real commit fence checks revision/lease movement. .
- **verify:** : canonical Scope/Grant/AccessError, precedence and expiry (`now < expires` unless contract says otherwise), pure production hooks, signed source. Kani bounded production subset/decision; trie vs independent segment/grant properties; real fence loom/DST. Controls unchecked parent, textual prefix, wider ops/expiry, dropped revision/stale scope. Exhaustive mappings and explicit audit failure semantics. Historical39 auth -D warnings diagnostics are not green.
- **e2e row11:** `agents::a_composition_wide_reader_cannot_read_a_childs_task`, harness/tests/e2e/agents.rs; fixes ab27375288595ea1d490055d71bf655b69c91539 /8f15cf3fa837bb4021698b601716086831d4c721; control agents::reader_control_rejects_unrelated_failures. Historical Linux5.21s, b733 wide line7446 positive exist; current production/Cargo/EXE all3OS, actual-old adapter negatives and per-row<10s joins still absent. /.md. Full main pass not closure; no build commissioned for handover.
- **efficiency:** report source/proof/merged outcome, dependencies and measurable checkpoint; new owner owns schedule, no speculative A6 merge date.

## Next five actions for rb-identity

1. Adopt signed heads/receipts; reconcile current main, reviews and queues. Reuse custody/authority mechanisms rather than implementing copies.
2. Close Pi seed and abandoned-credential findings using sole native-runtime custody, actual-old negatives, migration/publication-cut controls. Update body/proof and resolve on evidence; changed-crate jobs<600s, tests<10s, then queue per topology.
3. Finish secrets shared product request budget and deterministic header/body/redaction controls; retain CAS/permission/protected-value/retained-row proofs and coordinate predecessor order.
4. Publish canonical A6 API/Cloud review resolution; coordinate main-green scope, sessions TS deletion, Streams subtree commit fence, verify production proof hooks; identify one signed A6 PR distinct from predecessors.
5. Deliver generic/endpoint conformance with signed source and actual positive/mutant provenance, cross-OS closure; remove duplicate consumers/MAC under agreed semantics, queue and own merge.

No tracked WIP remains. b-authority retires here; rb-identity owns subsequent code, review, queue and proof closure.
