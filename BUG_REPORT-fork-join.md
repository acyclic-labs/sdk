# Fork/join engine bugs found porting to Pydantic AI

Branch: `pydantic-fork-join` (uncommitted). Platform verified: macOS (darwinfuse NFS mount). Linux: not verified.

Test status at time of writing:

| Suite | Result |
|---|---|
| `cargo test -p acyclic-fs --features native-mount --lib` | 897 passed, 0 failed |
| `cargo test -p acyclic-labs-plugin` | 61 + 5 passed |
| `plugin/packaging/pypi` (`ACYCLIC_LIVE_BIN=target/debug/acyclic pytest`) | 13 passed (10 protocol, 3 live on real mounts) |

## Fixed

### 1. Files invisible in their own fork; `mkdir` returns ENOENT
- **Symptom:** a file written in a fork is not listed or re-openable in that fork. `mkdir` reports ENOENT even though the directory was created.
- **Root cause:** `LazyMountSource` answered lookup, readdir and read from the published head plus the lazy source. The mount's unpublished writes live in the `authored` checkout and were never consulted.
- **Fix:** a composed live view in `native_mount/lazy.rs` (`resolve`, `merged_listing`).
  - Directory listings are buffered, because per-entry getattr invalidated the lazy continuation (ESTALE on page 2).
  - Promotion uses `adopt_materialization_async`.
  - `rebase_head_over_materialization` in `facade.rs` waives observation-only conflicts.
  - NotFound is now classified correctly in `adapter.rs` and `lazy_workspace.rs`.
- **Tests:** `native_mount/customer.rs::live_view_tests` (2); live `test_a_fork_can_create_directories`.

### 2. Parallel siblings adding files to the same directory always conflict
- **Symptom:** a sibling merge conflicts, or fails with `file link count is incorrect`.
- **Root causes:**
  1. `kernel/merge.rs` compared directory metadata (mtime) three-way *before* merging entries. Both siblings bump the mtime, so it always conflicted.
  2. The driver resolved that "metadata" conflict by substituting the whole record, which orphaned the other sibling's file.
  3. The materializer rewrote unchanged root files, giving them new inodes and therefore new source identities in other forks.
  4. A fork saw a sibling's merged files through the shared physical root and re-merged them under fresh ids.
  5. Two forks converging on the same file with different timestamps conflicted.
- **Fix:**
  - `merge_metadata_async`/`merge_metadata_fields`: timestamps take the later value; authored fields (mode, uid, gid, flags, ACLs, …) must resolve three-way.
  - A directory resolution supplies metadata only; the entry merge always runs.
  - Convergent additions reconcile their metadata.
  - `materializer.rs` skips rewriting host files that already hold the exact bytes.
  - `plugin/src/main.rs::paths_inherited_since_fork` tombstones inherited paths before exactify.
- **Tests:** kernel `metadata_timestamps_reconcile_and_authored_fields_conflict` and `sibling_additions_merge_entries_and_reconcile_directory_times`; workspace `sibling_forks_adding_distinct_files_join_in_sequence` and `sibling_directory_times_reconcile_but_authored_metadata_conflicts`.

### 3. A child merge into root fails after a sibling of the child merged first
- **Symptom:** root → A and H; H → G. A merges into root, G merges into H, then H into root fails: `merge candidate validation failed: file link count is incorrect`.
- **Root cause:** the inherited-path filter only compared against the *direct* parent. G's view read A's merged files from the physical root, and they were merged into H as G's own work.
- **Fix:** `agent_merge_as` walks the whole ancestor chain and filters inherited paths at every level.
- **Test:** covered by live `test_decompose_and_speculate_against_the_real_engine`. Scratch repro `repro10.py` passes in both orders.

### 4. ESTALE creating a new file in a fork after two merges into root
- **Symptom:** `open("retries.py", "w")` fails with `OSError(70, 'Stale NFS file handle')` in forks spawned after other children merged into the root. This made speculative attempts fail silently, so the wrong attempt won.
- **Root cause:**
  - All forks of a root share one physical-root `DemandSource`.
  - `refresh_native_root` observes the root watcher, bumps the source epoch (`invalidate()`), then calls `rebind_source()` on the root's lazy workspace only.
  - Every fork's lazy state stays on the old epoch. `LazyWorkspace::state_measured` then returns `StaleSource` for any path the fork hasn't observed yet, and the mount maps that to ESTALE.
- **Fix:** `refresh_native_root` also rebinds every route on that root (`plugin/src/main.rs`).
- **Tests:** plugin `forks_follow_the_shared_root_source_across_a_refresh` (fails with `StaleSource` without the fix); live end-to-end test.

### 4b. `merge --abort` after a conflict leaves the parent stuck
- **Symptom:** abort fails with `path lookup batch is empty`. A later abort gives `conflict abort overlaps later workspace changes`, discard is refused, and every later merge into the parent is blocked.
- **Root cause:** when a conflict projection changes no path in the target (every conflicting record is held back as "ours"), `abort_conflict` in `multi_root.rs` called `apply_paths_from` with an empty path list.
- **Fix:** an empty projection has nothing to undo; abort returns the current head.

### 4c. An add/add name conflict publishes an invalid candidate (pre-existing on `HEAD`)
- **Symptom:** both sides add `/x` with different bytes and no merge driver. Publishing fails with `file link count is incorrect` instead of reporting a conflict.
- **Root cause:** the conflict projection resolved the name to "ours" but still carried the other side's new file record, leaving it with no names and a link count of 1.
- **Fix:** `adjust_link_counts` in `kernel/merge.rs` drops a record only the other side added when it ends up with no bindings.
- **Test (covers 4b and 4c):** `multi_root::tests::aborting_a_conflict_that_changed_no_target_path_succeeds`. Also verified live: after conflict + abort, the parent is restored, the loser discards, and the next merge applies.

### 5. `._*` AppleDouble files merged as agent work (macOS)
- **Symptom:** `._retries.py` lands in the repo. `changedPaths` includes `._*` files, which skews speculation's "fewest changes" choice.
- **Root cause:** the macOS NFS client stores xattrs it can't send to the server as `._name` files. Adding the `namedattr` mount option (`darwinfuse.c`) didn't change that.
- **Fix:** handled in the engine, not the Python package. `subagent_stop` removes the `._name` companions the agent created, through its own mount, before the final sync (`remove_appledouble_companions`, macOS only). A first attempt that ignored them at capture broke the post-merge child rebase, so it was reverted.
- **Test:** live suite asserts the exact final tree.

### 6. Python binding bugs
`result.usage` is a property in pydantic-ai 2.48, not a method. A speculation check could merge its own artifacts; checks now run in a throwaway grandchild fork.

## Open

### B. Linux: `acyclic __service` exits immediately (high)
- **Environment:** Docker `rust:1.94.0-bookworm`, `--privileged --device /dev/fuse`, as root and as a normal user.
- **Detail:** the control mailbox is under `/tmp/acyclic-<uid>`. The service's stderr is sent to `Stdio::null()` (`native-runtime/src/lib.rs:262,277`), so there is no diagnostic. None of the fixes above have been run on Linux.
- **Next step:** run `__service` in the foreground and route its stderr to a log.

### C. Forks aren't isolated from later root changes (medium, design)
- A fork reads the live physical root, so files merged into the root after the fork was created show up in it (repro: sibling `c.py` listed in fork `b`).
- Merge-time filtering (fixes 2 and 3) keeps *added* files out of a fork's merge. It doesn't cover a sibling *modifying* an existing file that the fork never touched: the fork would see the new content and could re-merge it under a different identity. There's no test for this case yet.
- Fix 4 (rebinding forks to the new epoch) makes forks follow the live root by design. Proper snapshot isolation would pin unresolved lookups to the fork generation.

### D. AppleDouble files still exist *while* a fork runs (low)
They're removed at stop, so agents can see `._*` in listings mid-run. The package's `list_files` tool hides them. The real fix is NFSv4 named-attribute (OPENATTR) support in darwinfuse, so the client stops writing them.

### E. Missing coverage (medium)
- A black-box conformance test in `plugin/tests/` that drives only `__hook sdk`, `acyclic git merge/discard` and `agents --json` on a real mount. It should cover create/mkdir/list/rename/remove, sibling merges, grandchild merges, discard of a subtree, conflict + abort, no `._` files, and create-after-root-refresh (bug 4).

### F. Housekeeping
`cargo fmt` and clippy are clean for `acyclic-fs` and `acyclic-labs-plugin`. The pitch artifact still cites Arena for speculation; the source is now `speculate()` in `acyclic-pydantic-ai`.
