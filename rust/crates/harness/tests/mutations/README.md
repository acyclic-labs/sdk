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

Run `python3 rust/proofs/conversation-observation/run.py` from the repository
root to qualify conversation observation startup. It restores the original
two-settlement default path in the production client and requires the existing
native parent-and-child scenario to reach `default observation repeated its
settlement query`. After exact source restoration, the same scenario must pass.
The scenario counts actual native stream head reads, checks the root observer's
settled cursor, and retains the original parent/child volume, context and
authority assertions. Explicit replay cursors keep their original semantics.

`instruction-presence.patch` makes instruction discovery continue through the
history after finding the instruction prefix. Apply it from the repository
root and run:

```sh
cargo nextest run --locked -p acyclic-harness-core --features filesystem,test-support --lib -E 'test(=model::instruction_review_tests::read_projection_has_exact_linear_index_work_and_quadratic_negative)' --no-tests=fail
```

The retained property must fail at `history traversal must visit only the
presence prefix and each message/part once`. It also compares authenticated
projection output and checks original-ledger retirement and linear work.
Reverse the patch afterward; restored production must pass the same property.
