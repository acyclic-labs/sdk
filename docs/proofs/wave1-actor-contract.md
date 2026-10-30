# Actors wave-1 proof contract

2026-10-26. SDK Actors continuation belongs to Claude cl-a4; rb-actors owns the matching Cloud implementation. The [captured predecessor handover](../../scripts/tests/fixtures/proof-policy/actor-store-v1/handover.md) records b-runtime's earlier retirement and the inherited evidence. The eight A4-01..08 histories in the [captured predecessor contract](../../scripts/tests/fixtures/proof-policy/owner-handover-v2/actors/verify-contract.md) remain the shared oracle, with the following refinement. No build is requested solely for this review. Cloud-owned requests go through the coordinator.

## Decisions

`Keep` preserves alarm generation **and deadline**, including an alarm-triggered transition. A separate durable consumed-generation marker prevents repeat settlement of that generation. The marker, epoch, checkpoint/codec, input cursor, alarm generation/deadline, pause and effect intent must publish under the same exact state fence. It is an internal state field, not a new scheduler, alarm generation convention or public conformance model. Set/Clear reset consumption while advancing the generation with checked arithmetic, including equal-deadline Set and Clear-empty. Canonical rolling storage compatibility must cover the new field on hosted voters.

Extend A4-03's same-provider history: set G/D, trigger G with Keep, crash at pre-publication, durable-publication and genuinely lost-ack cuts, recover by the original key, then deliver duplicate G before/after restart. Assert G/D unchanged, consumed G durable and one committed effect. Set the same D again: generation must advance and the new firing is eligible. Replace/clear while the old handler is prepared: it must fail its state/version or generation fence. Pause/takeover races cannot bypass this. Old production252c must fail the unchanged G/D assertion; removing the consumed-generation comparison must fail the duplicate-effect assertion. Split-marker/checkpoint publication must fail the crash-cut tuple oracle. Keep generic StreamProvider tests separate from actual Runtime/package/authority qualification.

## Shared histories retained

| History | Same-provider oracle | Production control |
|---|---|---|
| A4-01 commit/crash | At every pre-commit, durable-commit and lost-ack cut, tuple is wholly before or after; restart with the original intent, one effect and receipt | Split checkpoint/cursor/alarm/outbox publication |
| A4-02 takeover | Old owner paused at preparation cannot settle after a new epoch, including after restart | Remove epoch/state-version fence |
| A4-03 alarm | Setâ†’dueâ†’replace/clear, duplicate wakes, equal-deadline Set, Clear-empty, Keep and overflow preserve generation semantics | Remove generation comparison; wrap the counter |
| A4-04 pause | Enumerate pause/resume against prepared handler, due alarm, input append and lost acknowledgement; no stale settlement or lost pending work | Settle with a pre-pause state version |
| A4-05 wake | Production Loom register/replay/recheck/hibernate races append and cancellation; remaining input is ready or has a live shared registration | Drop register/recheck or unregister after last tail check |
| A4-06 outbox custody | Cancel at each preparation boundary, then recover Unknown by original key; no leaked/lost hold or duplicate committed effect | Release holds before terminal reconciliation |
| A4-07 population | Fixed ready/due/visible work at1k/10k/100k dormant actors; bounded queue/index duplicates, registrations and payload; count actual work visits | Restore per-idle future/subscription or population scan |
| A4-08 codec/resume | Same real handler package over native/WASM; malformed/version-mismatched checkpoint and stale cursor fail typed without consuming input | Default-on-decode-error or reset-to-latest |

## Admitted inherited scope

Exact Store pin **252c28c27fd490186f7e17e1d55b47ffe82a9e9a**, signature G verified. Six formatted archive files byte-match that commit, including unchanged tests; restored six-file hashes match. Archive SHA256 **af96f5e9459b56961f5775c1352f4a7501457945a67f3ba4a073387755fe876d**. [Mirrored admission](../../scripts/tests/fixtures/proof-policy/actor-store-v1/receipt.json).

| Entry / invariant | Captured source at252c | Existing proof / limitation |
|---|---|---|
| load: exact final state/cut, typed decode/provider refusal | actors/src/local.rs:106 | memory/local history and reopen example; decoder fuzz and malformed retained state controls absent |
| claim: epoch fencing | local.rs:149 | local/tests.rs:150; remove epoch comparison fails `epoch fencing` on memory |
| set_paused: no stale settlement | local.rs:185 | local/tests.rs:291 and305; no concurrent pause/crash schedule or Runtime delivery proof |
| publish: one checkpoint/cursor/alarm/effect transaction | local.rs:198 | local/tests.rs:111; split effect/state commits fails `one checkpoint/effect transaction` on memory |
| alarm generation fence | local.rs:230 | local/tests.rs:195; remove generation comparison fails `alarm generation fencing` on memory |
| Keep and bounded alarm histories | local.rs:241 and local/tests.rs:262,336 | tests expect generation3 after firing2; this proves the old, incompatible convention. Sixteen generated cases have no retained fixed seed |
| Handler/Transition/Effect | guest/handler.rs:67,86,96 | declared shared boundary, not actual package/native/WASM execution or authority admission |

The six positive tests report0.05s total; each of three actual mutation runs selects one unchanged semantic assertion and reports0.00s. Positive examples cover memory/local/reopen; controls are memory only. Actual input manifest and producer are retained, but **test executable hashes and compiled mutant input manifests were not retained**. Do not count these observations as complete executable provenance, DST, Runtime, hosted or full A4 qualification. The initial rejected archive (0874d852d0392431d04de1e7a7f6d34e6e2a4b2cc1c73bdd7a122a9505350180) exposed real missing effect destination conditions, not an environment failure; handover preserves it.

Source audit of the new local.rs and guest/handler.rs at252c found no unsafe, unwrap/expect/panic, or catch-all typed status mapper; public items have doc comments. Invalid(&'static str) remains a coarse transition error, and no end-to-end UI error conversion proof is implied. Miri/Kani and strict current-source Clippy are absent.

## Current unqualified work and done criteria

An accepted but unresolved publication must retain its exact prepared request, digest, key and original custody. Reloading an unchanged visible state is insufficient to prove that the first attempt was never accepted. Extend A4-01/A4-06 at the private accepted-before-publication fault cut: lose the response, advance injected time and supply a handler whose next call produces different effect bytes. Drive Runtime retry, cancellation and restart; assert byte-identical unresolved retries and exactly one committed tuple/effect. The production control drops/recomputes the retained request and must fail that identity assertion. Discarding an already-successful reply does not cover this cut. Source review of the unqualified `ae2f73875a761e90aef7be84eb35e16385e192da` Runtime finds held records/follows but no retained prepared request before its error return; this is an inference requiring execution, not a demonstrated failure.

Adopted readiness transfer **4056b65d72ed901caef674a3ec01d93d630800fe** is signed but uncompiled; test source is not a Loom receipt. Read-only inspection of the new owner worktree observes signed local **3f0eb97a509b40e06b27f580ce34577ebc947ecb** adding `delivered_generation` and preserving Keep. This observation is not qualification or a tested pin. Keep the corrected oracle above when qualifying it; retain exact positive/control compiled manifests, unchanged test hashes, executable hashes, selected cases, fixed seeds and runtimes.

Finish the production ready primitive through the canonical Streams shared readiness seam, Runtime/shared due index, admitted real package/native-WASM execution and one public ActorProvider conformance/endpoint runner. A4-05 Loom must exercise the actual production lock/register/recheck/cancel path with omitted-recheck/dirty controls; one per-actor follow future or actor-only write wrapper cannot satisfy external append wake. A4-07 counts actual dormant registrations, queue duplicates, timer/socket/future allocations and visits at1k/10k/100k with fixed ready work. All suites <10s, changed-crate preflight Linux jobs<600s. Hosted auth/custody/term/heap/rolling-decoder fences remain required. Preserve the legacy P5 original-authority/provenance/three-OS debt under main-green/e2e-gate; retirement does not close it. Frozen1512 remains untouched.
