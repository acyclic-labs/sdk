# Production record concurrency controls

The two Git compatibility Loom controls execute the production repository and
operation-window coordinator over their actual memory adapters. `CountedStore`
only observes conditional-write attempts; `MemoryRecords` owns every state
transition, including test setup. Its backing is private.

Run the controls from the repository root on Linux:

```sh
export RUSTC_WORKSPACE_WRAPPER="$PWD/scripts/loom-rustc.sh"
cargo nextest run --locked -p acyclic-fs --no-default-features \
  --features memory,model-checking --lib --profile ci \
  -E 'test(=git_compat::tests::model::concurrent_branch_registration_publishes_only_accepted_updates) | test(=git_compat::tests::model::concurrent_renewal_fences_the_superseded_lease)' \
  --no-tests=fail --no-fail-fast
```

Apply `fuzz/mutations/record_revision_fence.patch` relative to this crate to remove
the occupied-record revision check in the production transition. Repeating the
command must fail both controls with `lost branch update` and `stale lease
renewed`, respectively. Reverse the patch before checking the original source.
A compiler error or an unrelated failure does not qualify the negative control.

Each model explores two concurrent writers, checks accepted state and forward
progress, and counts conditional writes. The adapter future must finish in one
poll; a pending future fails immediately. These controls do not establish native
or browser IO progress, or constant-time lookup: the current map is a B-tree.
