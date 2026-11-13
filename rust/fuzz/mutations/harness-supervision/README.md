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
(`acyclic-plugin --test cli`). Its two real commands make an equal-length edit
and rewrite identical bytes. Only the edit may admit a second paid goal turn;
both commands must have returned success and exit code zero.
`native-progress-publication.patch` restores capture without its original
content measurement. The identical regression must fail at
`native changed=true`, then pass after restoration.

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