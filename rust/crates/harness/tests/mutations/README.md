The historical-child scenario also exercises the native `ForkCuts` adapter
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

# Retained workspace diff authority control

From the repository root, apply `diff-retained-authority.patch` and run:

```sh
cargo nextest run --locked -p acyclic-harness --features hosting,test-support --test main -E 'test(workspace_diff_pages_exact_changes_since_the_fork_base)'
```

The unchanged test must fail at `retained diff pages require the requesting
reader's authority`. A compilation failure does not qualify the mutation.
Reverse the production patch and require the same test to pass.

The same command with `diff-utf8-midpoint.patch` must fail at `a fitting
four-byte scalar must survive page shrinking`. This plants the former raw
byte midpoint in the actual collector, while leaving the projected filesystem
input, cursor and exact canonical wire-credit oracle unchanged. Reverse it
and require the same test to pass.

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
