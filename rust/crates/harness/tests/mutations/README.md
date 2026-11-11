`view-retirement-admission.patch` restores replacement admission before the
original physical view has finished retiring. Apply it from the repository
root, then run from `rust`:

```sh
cargo test -p acyclic-harness --features filesystem-local,native-execution,test-support --lib local::host_owner::tests::replacement_view_waits_for_original_retirement_and_reports_producer_loss -- --exact
```

The actual native owner must reject the mutation with `replacement attachment
was admitted while its original view remained live`. Reverse the patch and
run the same control. It checks cancellation of the original caller, shared
replacement attachment, and typed failure for both waiters when the original
retirement producer disappears. The existing idle-view lifecycle control
retains settled contents and active-session custody.

The historical-child scenario also exercises the native `VolumeResources` adapter
opened by the real local harness. It retains a cut, advances the parent with
an actual write, reuses the original operation and refuses mismatched selected
generations on both retained and fresh destinations.

From the repository root, apply either `history-retained-cut.patch` or
`history-fresh-cut.patch` from this directory. Then run from `rust`:

```sh
cargo test --locked -p acyclic-harness --all-features --lib local::tests::agents::historical_child_settles_after_its_parent_has_later_context_segments -- --exact
```

Each patch must reach `selected generation mismatch must refuse both retained
and fresh cuts`. A compiler or setup failure does not qualify the control.
Reverse the patch with `git apply -R` before applying another. Restored
production must pass the same scenario.

`history-retry.patch` restores the original generation-independent retention
identity. Run the unchanged native preparation regression from `rust`:

```sh
cargo test --locked -p acyclic-harness --all-features --lib local::tests::history_retry::a_refused_history_preparation_can_select_a_new_project_generation -- --exact
```

It must reach `a refused history preparation must admit the new selected
generation`. The test uses checkpoints and conversation bytes from actual
local agent turns, the production composer and native lazy cut adapter, and
an actual source-head conflict. It checks reopening preparation and stable
same-generation retention; the original conversation has already committed,
so it does not claim a second actor publication. Reverse the patch afterward;
restored production must pass the same regression.

`history-preparation-retry.patch` restores the original conversion of a
changed-source cut into a handler failure. Run the same native preparation
regression above. It must reach `a source changed during preparation must
request an actor retry`. The regression writes the actual project between
source selection and the native cut, and keeps the generation guard enabled.
Reverse the patch afterward; restored production must pass both this early
refusal and the later commit-conflict retry in the same regression.

Run `node rust/proofs/qualify.mjs conversation-observation` from the repository
root to qualify conversation observation startup. It restores the original
two-settlement default path in the production client and requires the existing
native parent-and-child scenario to reach `default observation repeated its
settlement query`. After exact source restoration, the same scenario must pass.
The scenario counts actual native stream head reads, checks the root observer's
settled cursor, and retains the original parent/child volume, context and
authority assertions. Explicit replay cursors keep their original semantics.

`settings-generation-refresh.patch` restores settings admission's independent
home-generation lookup. Apply it from the repository root, then run from `rust`:

```sh
cargo test --locked -p acyclic-harness --lib --features filesystem-local,hooks,test-support local::agent_settings::tests::a_settings_tree_keeps_one_generation_across_concurrent_home_edits -- --exact
```

It must reach `settings admission refreshed its pinned home generation`.
The scenario edits real home modules and imports after pinning their tree,
then checks both plugin and agent admission through their production entry
points. A fresh admission must observe the edits. Reverse the patch with
`git apply -R`; the same scenario must pass with the shared snapshot restored.

# Live workspace diff authority control

From the repository root, apply `diff-retained-authority.patch` and run:

```sh
cargo nextest run --locked -p acyclic-harness --features hosting,test-support --test main -E 'test(live_diff_queries_fence_frontiers_and_cached_patches_against_policy_changes)'
```

The unchanged test must fail at `revoked policy must stop the original query`.
The mutation removes the current-policy disclosure fence from already compared
patch fragments. The native frontier remains fenced, so the cached-file case
must distinguish this defect. No cursor names an undisclosed following file.
A compilation failure does not qualify the mutation. Reverse the production
patch and require the same test to pass.

Apply `diff-utf8-midpoint.patch` and run:

```sh
cargo nextest run --locked -p acyclic-harness --features hosting,test-support --test main -E 'test(workspace_diff_pages_exact_changes_since_the_fork_base)'
```

It must fail at `a fitting scalar must preserve its accepted prefix and continuation`.
This plants the former raw byte midpoint in the shared fragment pager, leaving
the projected filesystem input and exact wire-credit oracle unchanged. Reverse
the patch and require the same test to pass.

`diff-missing-baseline.patch` restores the former production fallback that
treated an incomplete source import as an empty diff. Apply it and run the
existing native capture control through the served client:

```sh
cargo test --locked -p acyclic-harness --features filesystem-local,native-execution,test-support --lib local::checkout_tools::capture::tests::unreadable_before_images_remain_optional_until_continuity_is_lost -- --exact --nocapture
```

The test must fail at `uncertified source was reported as an exact diff`,
after the real source refuses its before-image and before continuity is
recovered. Reverse the production patch and require the same test to pass,
including the exact empty diff after a complete baseline is retained.
A compilation or setup failure does not qualify the mutation.

`diff-eager-content-comparison.patch` restores the former unbounded content
comparison before the consumer's input allowance. Apply it and run:

```sh
cargo nextest run --locked -p acyclic-harness --features hosting,test-support --test main -E 'test(workspace_diff_admits_range_restoration_before_reading_content)'
```

The unchanged test must fail at `content equality bypassed the page allowance`.
The original filesystem range-write entry point restores equal bytes with a
different payload layout. Insufficient credit must retain an explicit omission;
sufficient credit establishes equality only below the file read cap. Reverse
the production patch and require the same test to pass. A compilation or setup
failure does not qualify the mutation.

`instruction-presence.patch` makes instruction discovery continue through the
history after finding the instruction prefix. Apply it from the repository
root and run:

```sh
cargo nextest run --locked -p acyclic-harness-core --features filesystem,test-support --lib -E 'test(=model::instruction_review_tests::read_projection_has_exact_linear_index_work_and_quadratic_negative)' --no-tests=fail
```

The retained property must fail at `history traversal must visit only the
presence prefix and each message/part once`. It also compares authenticated
projection output and checks original-ledger retirement and linear work.
A compilation or setup failure does not qualify the mutation. Reverse the
patch afterward; restored production must pass the same property.

`physical-view-release.patch` omits physical release from the actual closing
composition. Run the unchanged public-host control:

```sh
cargo test --locked -p acyclic-harness --features filesystem-local,native-execution,test-support --lib local::tests::agents::physical_archive_releases_only_the_closed_binding_and_retains_history -- --exact
```

It must fail `archived physical binding survived its committed close`.
The restored control opens two real physical child views, waits on original
actor settlement, archives one child and checks its exact directory is gone.
The idle sibling and root retain their bytes, and the archived child's actual
conversation content remains readable. Repeating release is idempotent.
Reverse the patch afterward; restored production must pass this same control.

`input-preview-unadmitted-follow-up.patch` substitutes an inbox-prefix test
for the committed conversation's message-admission proof. Run:

```sh
cargo nextest run --locked -p acyclic-harness --lib --features serve,test-support,native-execution -E 'test(=local::tests::agents::historical_input_previews_do_not_admit_reserved_follow_ups_inside_a_steering_cut)' --no-tests=fail --success-output immediate
```

The real actor accepts an initial message, steering message and reserved
follow-up before a two-step model turn. The selected turn's boundary passes
the reserved follow-up without admitting it. A real historical session fork
must preview the admitted steering message; the originating session can still
preview its own accepted follow-up. The mutation must fail `an inherited
preview must not disclose an unadmitted follow-up`. A compilation or setup
failure does not qualify. Reverse the patch and rerun the same control.
