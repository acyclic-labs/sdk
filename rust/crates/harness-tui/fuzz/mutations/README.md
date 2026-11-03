# Production mutation controls

Console guard ownership has one shared control:
`term::console_owner::tests::metadata_and_io_follow_the_original_guard_handoff`.
It observes the original owner from the callback submitted by `Owner::run`,
while the production worker owns its guard, then checks the acknowledged result.
It also checks the original generation before and after that worker's write.
No guard state is manually removed and no negative overwrites an observation.
Apply each retained mutation separately, run the control below and require its
exact assertion; reverse the patch before the next control.

| Mutation | Required assertion |
| --- | --- |
| `console-profile-without-guard.patch` | `borrowed profile must be unknown` |
| `console-counter-without-guard.patch` | `borrowed guard must be unknown` |
| `console-empty-without-guard.patch` | `borrowed guard acknowledged an empty frame` |
| `console-io-holds-owner.patch` | `console I/O retained owner state` |

```sh
RUSTC_WORKSPACE_WRAPPER="$PWD/scripts/loom-rustc.sh" cargo test -p acyclic-harness-tui --features 'tui model-checking' --lib term::console_owner::tests::metadata_and_io_follow_the_original_guard_handoff -- --exact
```

The device is a supplied writer. These production ownership checks do not
establish physical console input, cancellation or OS completion guarantees.


## Frame model mutation controls

The frame models call the production renderer and lifecycle mutex, and interpret
its output with Alacritty. The adapter passes those bytes unchanged. Run the
four positive controls from the repository root:

```sh
RUSTC_WORKSPACE_WRAPPER="$PWD/scripts/loom-rustc.sh" cargo test --locked \
  -p acyclic-harness-tui --features tui,model-checking --lib \
  term::frame::frame_model::
```

Apply each patch independently with `git apply`, then run the named test with
`-- --exact`. It must fail at the assertion below; a compiler error does not
qualify the mutation. Reverse the patch before applying another, and rerun the
positive controls after restoring production source. The model and its assertion
stay unchanged throughout.

| Production defect | Test in `term::frame::frame_model` | Required assertion |
|---|---|---|
| `frame-empty-commit.patch`: skip the lifecycle boundary for an empty frame | `resume_after_the_render_snapshot_is_repaired` | resume before commit must not leave the cursor shown |
| `frame-drop-recovery-attributes.patch`: omit the recovery attribute reset | `checked_presentation_recovers_attributes_under_the_real_lock` | checked recovery must reset attributes before cells |
| `frame-drop-recovery-contents.patch`: omit encoding the retained contents during recovery | `retained_cells_recover_under_the_real_lifecycle_lock` | retained contents must follow lifecycle invalidation |

These controls check production renderer transitions under modeled interleavings.
They do not establish native terminal input provenance or OS cancellation behavior.

## Lifecycle model mutation controls

The lifecycle models call the production `attach`, `detach`, `active`, `restore`
and `resume` functions with the production state mutex. Their writers represent
OS state; they do not establish native OS completion or input provenance. Run
the eight positive controls from the repository root:

```sh
RUSTC_WORKSPACE_WRAPPER="$PWD/scripts/loom-rustc.sh" cargo test --locked \
  -p acyclic-harness-tui --features tui,model-checking --lib \
  term::lifecycle::terminal_lifecycle_model::
```

Apply each patch separately, run its exact positive control with `-- --exact`,
and require the assertion below. A compiler error does not qualify a negative
control. Restore production source between mutations; neither the model nor its
assertion changes during a control. Finally run all eight restored models.

| Production defect | Test in `term::lifecycle::terminal_lifecycle_model` | Required assertion |
|---|---|---|
| `lifecycle-early-owner-release.patch`: release ownership before restoration | `detach_restores_before_default_signal_and_allows_reopen` | default signal action before restoration |
| `lifecycle-failed-restore-releases-owner.patch`: release ownership despite a failed restoration | `failed_restoration_keeps_owner_until_successful_retry` | failed restoration released the owner |
| `lifecycle-split-close-fence.patch`: separate admission from the guarded mode write | `close_fences_every_mode_write` | mode enabled after terminal close |
| `lifecycle-unfenced-owner-identity.patch`: remove both identity checks from delayed detach | `delayed_split_half_cannot_detach_a_new_terminal` | delayed split-half close detached the new owner |
| `lifecycle-repeat-os-restoration.patch`: remove the successful-restoration fence | `completed_restore_cannot_mutate_a_reopened_terminal` | old terminal repeated OS restoration after handoff |

The delayed-destructor/reopen and suspend/resume controls remain alongside these
targets. No test writer invents a destructor or overwrites state after the
production transition. Native destructor behavior needs its native control;
the modeled restoration fence alone does not qualify it.

## Console cancellation handoff

`console-cancel-replacement-generation.patch` removes the production generation
check after a cancellation wake. With the wrapper above, run the exact test
`term::console_owner::tests::cancellation_wakeup_cannot_target_replacement_generation`.
It must fail with `cancel reached replacement worker`; restore the patch and run
the same test again. Both generations are submitted by production `Owner::submit`,
and the original worker publishes its own acknowledgment. Channels constrain only
the observed action/cancellation boundary; the model does not seed worker state
or qualify native `CancelSynchronousIo` completion.
