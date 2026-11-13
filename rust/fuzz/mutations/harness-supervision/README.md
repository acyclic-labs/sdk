# Same-turn subtree stop control

Run the original real-runtime regression from the repository root:

```sh
cargo nextest run --locked -p acyclic-harness-core --test main --features test-support \
  -E 'test(=agent_model::runtime::a_same_turn_stop_archives_newly_admitted_children)'
```

The test admits a child and stops in the same turn, and includes an ordinary
spawn without a stop as its healthy control. From the repository root, apply:

```sh
git apply rust/fuzz/mutations/harness-supervision/same-turn-stop.patch
```

The production mutant restores late spawn admission after the archive cascade
has frozen its child set. Run that unchanged test, and require its failure at either
`same-turn child was not admitted and archived` or
`same-turn spawn escaped the stop`. Compilation failure is not a successful
negative control. Restore the source, then require the original test to pass again:

```sh
git apply -R rust/fuzz/mutations/harness-supervision/same-turn-stop.patch
```

The original journal regression also covers a dormant grandchild: an archive of
its root, parent or itself refuses new model/tool dispatches before any cascade
delivery. An archived sibling leaves dispatch open, and previously accepted
dispatches still replay. Run it unchanged before and after this production mutant:

```sh
cargo nextest run --locked -p acyclic-harness-core --lib --features test-support \
  -E 'test(=filesystem::execution_journal::inline_tests::committed_archive_fences_dispatch_before_its_actor_consumes_the_control)'
git apply rust/fuzz/mutations/harness-supervision/ancestor-stop.patch
```

The mutant retains only the current agent's fence. Require the unchanged test to
fail at `ancestor archive admitted a fresh model dispatch` or
`ancestor archive admitted a fresh tool dispatch`, rather than at compilation.
Restore the source and require the same test to pass:

```sh
git apply -R rust/fuzz/mutations/harness-supervision/ancestor-stop.patch
```

Background shell supervision has two independent completion boundaries. The
model receives an immutable start result; only the physical capture owner can
publish `completed.json`, after it reaps the process tree. Its publication is
followed and fenced by the same quiet actor that follows output activity.

The normal tests include both negative controls: a start result must retain the
original job, and a writable private-volume lookalike must not retire its alarm.
Physical completion and an ordinary foreground terminal must retire it.

| Mutation | Unchanged regression | Required failure |
| --- | --- | --- |
| `background-start-finish.patch` restores retirement on the logical start result. | `filesystem::execution_journal::supervision::tests::selected_check_repetitions_ignore_capture_options_but_stay_within_the_original_turn` | `background start receipt retired quiet supervision` |
| `physical-completion.patch` discards the authoritative physical completion. | `agent_model::hosting::quiet_tool_alarm_rearms_on_output_and_refuses_completed_jobs` | `physical completion did not retire quiet supervision` |
| `completion-unkeyed.patch` removes completion-key routing from the shared handler. | `agent_model::hosting::quiet_tool_alarm_rearms_on_output_and_refuses_completed_jobs` | `unrelated journal output woke the quiet actor` |

Run each regression before applying its mutation, require an actual assertion
failure (not a compiler error), reverse the patch, and require the same test to
pass. The library test uses `-p acyclic-harness-core --lib`; the actor test uses
`-p acyclic-harness-core --test main`. Both use
`--features test-support,acyclic-fs/test-support` and an exact nextest selector.
`filesystem::hosting::journal::tests::call_output_uses_the_journal_without_granting_tools_its_writer`
also proves the physical completion publication's original identity and replay
through the shared output publisher.
The same real-actor test publishes output and completion for a different call
into the shared journal and requires the original actor's state stream to remain
unchanged. Its own output and physical completion still deliver normally.

The native-cut point comparison is exercised by
`native_mount::lazy::tests::publication::original_publication_retains_both_logical_cuts_after_cleanup_and_later_writes`
in `acyclic-fs --lib`. It distinguishes equal-length edits, creation and removal
from an unchanged source promotion, then repeats the original comparison after
source removal and a later write. `native-content-generation.patch` substitutes
authored-generation equality for logical content equality; the unchanged test
must fail at `source promotion with unchanged bytes counted as a content change`.
Restore the patch and require the same regression to pass. This point proof
does not claim a complete source-alias diff. An unavailable comparison never
counts as positive progress or as a proved absence of content changes.

The original native capture producer is covered by the shipping
`supervision::headless_native_edits_count_progress_but_unchanged_rewrites_do_not`
(`acyclic-plugin --test cli`). Three independent real-binary scenarios make an
equal-length edit, rewrite identical bytes after the initial import, and make
an edit followed by a read-only command. Only the actual edit may admit another
paid goal turn; clean captures must not borrow progress from the import or the
earlier edit. Every native command must return success and exit code zero.
`native-progress-publication.patch` restores capture without its original
content measurement. The identical regression must fail at
`native changed=true`, then pass after restoration.

`native-capture-parent.patch` restores the use of a generation's DAG parent in
place of the capture receipt's actual serial predecessor. The same shipping
regression must fail at `native changed=false earlier_edit=false` or
`native changed=true earlier_edit=true`, then pass after restoration. The SDK
regression `workspace::publication::tests::original_publication_evidence_survives_later_heads_without_workspace_files`
also recovers a clean receipt after a later edit, requires its actual predecessor
to have the same generation, and distinguishes that unchanged comparison from
the genuine later content change. It exercises both synchronous and asynchronous
authority stores. These are executable proof recipes; a checked patch or a
registered test alone is not a claim that the mutant was killed.

`native_capture_receipts_keep_original_identity_and_refuse_unavailable_progress`
uses real journal and filesystem publications to reject an unavailable positive
verdict before immutable staging, retain original replay exactly once, refuse
another call's receipt and reject a foreign volume. The existing real Streams
`original_turn_spending_is_atomic_and_does_not_borrow_later_turn_usage` includes
unavailable, positive and unchanged native measurements: unavailable evidence
must not create a positive fact, and subsequent known unchanged spending turns
must still reach the configured stop.
`native_pending_measurements_settle_per_call_without_bypassing_no_progress`
(`acyclic-harness-core --lib`) commits original pending and completed captures,
then settles real spending. It must count unchanged completed calls, retain
other calls' pending credit, refuse delayed-start downgrades, keep replay
idempotent and distinguish unavailable evidence from positive progress.
`native-pending-completion.patch` leaves a completed call's credit outstanding;
the same regression must fail at
`completed native measurements did not settle their own pending calls`.
Reverse it and require the identical regression to pass again.

The shared computed-file demand boundary awaits only the requested path's
provider snapshot. Run the exact SDK regression:

```sh
cargo nextest run --locked -p acyclic-fs --lib --features test-support \
  -E 'test(=demand::computed_tests::computed_snapshot_wait_keeps_the_requested_path_cut_and_cancellation)'
```

It waits on channels, advances the source while its original snapshot is
pending, and distinguishes a healthy retained cut from cancellation and source
replacement. `computed-cancel-after-wait.patch` removes the post-wait cancellation
check and must fail at `cancelled snapshot projected data after its provider wait`.
`computed-source-after-wait.patch` removes the source-identity recheck and must
fail at `snapshot rebound to another provider after its wait`. Apply each patch
independently, require the unchanged regression's actual assertion failure,
reverse the patch, and require that regression to pass again. Existing computed
overlay/fork, opened-file version, bounded page/range, idle-cost and mounted
override-removal regressions use the same asynchronous provider boundary.
The harness native signal-file adapter remains a separate integration obligation;
this SDK primitive proof alone does not establish that mounted integration.

Original-cut native point reads share the portable Streams selector and never
fall back to portable IO. In `acyclic-stream --lib`, run
`replay_tests::native_latest_before_keeps_original_cuts_and_budget`,
`replay_tests::portable_success_cannot_substitute_for_native_admission`, and
`replay_tests::latest_before_requires_the_original_single_record_cursor_to_close`.
The real memory provider distinguishes old keyed/unkeyed cuts from later writes,
missing keys and zero cuts, denies original native allocation, and rejects a
cancelled zero-cut read. The real-provider cursor wrapper injects an extra record
or a late error, with the healthy single-record cursor as its control.
`point-cursor-closure.patch` removes the portable cursor-close check; the cursor
regression must fail at `point read ignored an extra record or a late cursor error`.
`point-zero-cancellation.patch` removes the original native cancellation check;
the native regression must fail at `cancelled native point read admitted absence`.
Apply each independently, require that exact assertion, reverse it and require
the identical regression to pass. These new tests and mutant executions are
pending; patch applicability is not a runtime kill or native mounted-file proof.

`supervision::streams::tests::signal_files_follow_a_continuing_runs_pause_at_the_captured_cut`
in `acyclic-harness-core --lib` also reads both original run cuts through
`native_signal_file`. The returned JSON must equal the portable original-cut
projection and retain the native output allocation until its bytes retire.
Zero and missing-signal cuts remain absent; decoder denial, cancellation,
another agent's source and a payload indexed under the wrong kind are refused.
`native-signal-original-cut.patch` substitutes the latest source cut and must
fail at `native signal borrowed a later run or signal cut`.
`native-signal-output-custody.patch` substitutes an ordinary output buffer and
must fail at `native signal output lost its original allocation owner`.
Run each mutation independently with that same regression, require the named
assertion, reverse it and require the unchanged regression to pass again.
`supervision::events::tests::supervision_event_decode_custody_covers_native_storage`
measures real decoder allocations for every lifecycle variant, including long
owned strings, and requires the original peak reservation and retained custody
to cover the actual allocations and retire completely. These new executions
remain pending. This proves neither a mounted adapter nor a live PR producer.

The same `replay_tests::native_latest_before_keeps_original_cuts_and_budget`
regression measures allocations while a nonempty original revision cut is moved
into a denied native read. It must allocate nothing before the provider's
refusal. A unique ordinary key likewise cannot allocate its lazy shared header
before admission. `point-request-clone.patch` restores the unmetered request
copy and must fail at `native point read copied its revision vector before admission`;
`point-key-promotion.patch` removes key admission and must fail at
`native point read promoted a unique key before admission`.
Run these independently, require the exact assertion, reverse, then require
the same regression to pass. The native signal query's necessary revision-vector
copies use `RevisionCut::native_clone`, sharing the existing admitted vector
copy implementation and retaining its guard through the original request.
The regression also checks healthy cut-copy custody/refund and denied cut-copy
admission. New test/mutant execution remains pending.

The real Streams schedule regression
`supervision::streams::tests::late_native_completions_keep_original_turns_and_latched_controls`
settles a spending turn while its native measurement remains pending, then
settles a later spending turn without a pending producer of its own. An older
pending call must not exempt that later turn from no-progress. Four schedules
place changed/unchanged physical completion before or after the later settlement.
They retain original progress identity, exact stop cause, idempotent completion
and delayed-start refusal. Physical completion does not retroactively recount
the earlier logical settlement or clear the later stop.
`native-pending-turn.patch` shares the pending counter across turns and must
fail at `another turn's pending measurement bypassed no-progress settlement`.
`native-late-stop.patch` forgets the prior action and must fail at
`late native completion cleared a latched stop`. Apply independently, require
the unchanged regression's exact assertion, reverse and require that regression
to pass again. Execution remains pending. This query-boundary proof is separate
from qualification of native process completion through the shipping binary.

The shipping `acyclic-plugin --test cli` regression
`supervision::headless_configured_morph_chat_preserves_usage_and_process_credentials`
uses a configured Morph catalog entry and a real local Chat Completions HTTP
endpoint. Complete trailer usage must stop a spending, unchanged turn through
`no_progress`; missing trailer usage must instead preserve unknown spending and
stop at the owner's token reservation. Each authenticated case admits exactly
one model request with the Chat wire shape and process-only credential. The
missing-credential control admits no request. The same persisted-artifact and
output credential scan is shared with the root/worker configuration regression.
These are real binary/input controls, not a claim of live service qualification.
New execution remains pending.

Stream-only computed snapshots have no authored generation; their input domain
must remain distinct from every explicitly authored generation. In
`acyclic-fs --lib --features test-support`, run
`demand::computed_tests::stream_only_computed_cuts_do_not_alias_authored_inputs`.
It keeps the same bytes, source identity and stream position, changes only the
authored-input domain, and checks distinct versions, directory/lookup agreement,
original opened bytes and rejection of the old version. The production mutation
`computed-generation-domain.patch` normalizes absent generations to an authored
zero digest and must fail at `stream-only cut aliased an authored generation`.
Require that unchanged regression to fail, reverse the patch and require its
restored pass. Execution remains pending; this source-cut proof is not mounted
signal-file or native-watch qualification.
