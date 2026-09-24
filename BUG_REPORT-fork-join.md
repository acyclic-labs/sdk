# Fork/join engine bugs found porting to Pydantic AI

Branch: `hotfix/fork-join-merge-semantics` (PR #124). Verified on macOS (darwinfuse NFS mount) and Linux (FUSE, Docker `rust:1.94.0-bookworm`, privileged).

Test status at time of writing:

| Suite | macOS | Linux |
|---|---|---|
| `cargo test -p acyclic-fs --features native-mount --lib` | 900 passed | 904 passed |
| `cargo test -p acyclic-labs-plugin` | 61 + 5 passed | 60 + 5 passed |
| `plugin/packaging/pypi`: protocol, live and black-box conformance tests on real mounts | 25 passed | 25 passed |

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
- **Fix:** `refresh_native_root` rebinds every route on that root (`plugin/src/main.rs`). That only narrowed a race: mount callbacks run concurrently with the refresh. The complete fix is in `LazyWorkspace` (`follow_source_epoch`): a view bound to an older epoch of the same source adopts the current one on access instead of failing.
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

### 4d. Linux: the first command after start fails ("service did not become ready")
- **Symptom:** in Docker, `SessionStart` failed with `service is not running through either Linux control transport`; the next call worked.
- **Root cause:** not a crash. Every invocation SHA-256-hashes its own executable to name the service. The Linux debug binary is 335 MB, and unoptimized `sha2` took ~22 s, longer than the client's readiness wait.
- **Fix:** `sha2` and `blake3` compile with `opt-level = 3` in dev builds (workspace `Cargo.toml`). A cold start takes 2 s.

### 4e. Linux: creating a file in a directory made inside a fork returns EIO
- **Root cause:** the lazy source observes a path's parent with an inotify watch. A directory that exists only in the fork is absent from the physical root, and `watch_directory` turned that ENOENT into `native watcher I/O failed`.
- **Fix:** `watch.rs` watches the deepest existing ancestor instead; the directory's later creation is reported there.
- **Test:** `watch::tests::linux_demand_watch_accepts_directories_absent_from_the_source` (fails with the original ENOENT without the fix).

### 4f. Linux: stopping a fork wedged the whole service
- **Symptom:** the service stopped answering. Later calls failed with `service lock is held without a reachable endpoint`, and the fork's mount stayed attached.
- **Root cause:** `FuseSession::stop` relied on `fuser` unmounting on drop. `fuser` swallows unmount failures, and `join` then waits forever for a request loop that ends only on unmount. The mount was busy because the binding sent `SubagentStop` with its cwd inside the fork's own mount.
- **Fix:**
  - `fuse.rs` unmounts explicitly before joining: it retries on EBUSY, then detaches lazily (or uses `fusermount3 -u`/`-uz` when unprivileged), then aborts the FUSE connection via `/sys/fs/fuse/connections/<id>/abort`, so a held mount can't block `join`.
  - The binding sends `SubagentStop` from the parent's directory.
- **Result:** unmount went from 8.6 s (after a client timeout) to 0 ms.

### 4g. Observability
- **Service log:** the service's stderr goes to `/dev/null`, so every failure above reached users as a bare errno. The service now writes JSON lines to `<state>/service.log` (`acyclic_fs::diagnostics`, bounded at 16 MiB with one rotation, `ACYCLIC_LOG_LEVEL=error|warn|info|debug`).
- **What it records:**
  - every control request (hook or CLI command, cwd, lock wait, duration, outcome)
  - every mount callback error with its errno, on both drivers
  - stale-source and checkout-rebase conflicts behind ESTALE
  - mount and unmount timings
  - aborted busy mounts
  - root refreshes (epochs, forks rebound)
  - inherited paths filtered at merge
  - removed AppleDouble files
- **Client errors:** these now name the log file.
- **Test:** `diagnostics::tests::events_are_json_lines_with_escaped_fields`.

### 4h. Found by the conformance suite
`plugin/packaging/pypi/tests/test_conformance.py` drives the engine only through its public surface: `__hook sdk`, `acyclic git merge/discard`, `acyclic agents --json`, and POSIX calls on the mounts. On its first run it found:

- **Siblings adding files to an existing subdirectory failed** (`file link count is incorrect`). My earlier sibling tests only added at the repo root.
  - Cause: the same source directory ended up with different identities across forks and the root. Directory promotion didn't keep the source id, and the materializer replaced a lazily known host directory wholesale (new inodes for it and every file under it).
  - Fixes:
    - Kernel merge semantics: directories merge by path. When both sides add a directory under one name with different ids, theirs' entries fold into ours (recursively) and theirs' unbound record is dropped (`fold_directories`, `kernel/merge.rs`).
    - Promoted directories keep their source id (`Reidentify` allows a directory while no other record holds the id).
    - The materializer keeps an existing host directory instead of replacing it.
    - The inherited-path filter tombstones any path the fork only reads from the source.
  - Test: `workspace::tests::sibling_directories_created_independently_merge_by_path` (fails without the fold).
- **Linux: a metadata edit on a directory tripped the "external mutation" guard** once a file was installed into it. The fingerprint covered the whole subtree. Metadata edits are now checked with an attribute-only fingerprint (kind, mode, owner, identity).
- **macOS: `rmdir` after renaming a child out of a directory failed with EIO.** The rename was pending in the live checkout, while the removal was checked against the published head. The mount now publishes and retries in that case. Non-empty directories report `ENOTEMPTY` (both drivers), not `EIO`.
- **macOS: `._*` files were visible inside a running fork** (open item D). Directory listings on the macOS driver now leave out AppleDouble companions, and `rmdir` removes hidden companions first.
- **macOS: `._*` files could still reach the root.** Stop-time cleanup unlinked through the service's own NFS mount (the service is that mount's server) and silently skipped failures. It now removes companions straight from the fork's workspace after unmount.

### 4i. A fork's deletion or rename of a file it only read from disk was lost
- **Symptom:** `rm README.md` in a fork left `README.md` in the root after the merge. `mv README.md GUIDE.md` produced a copy, and `README.md` also reappeared inside the fork once its checkout published.
- **Cause:** a file the fork only read lazily from the physical root was never part of any generation. Deleting it records only a tombstone in the fork's lazy overlay, and a three-way merge can't delete what its base doesn't hold. A rename in the live checkout didn't tombstone the old source path at all.
- **Fix:**
  - `LazyWorkspace::source_tombstones` lists a view's source deletions.
  - The merge applies them to the parent once the child has advanced onto the result: the physical file for the root, a removal in the parent's view otherwise. A path the parent changed since the fork is kept and logged as `deletion_skipped_changed_in_parent`.
  - Renames publish and tombstone the old source path.
- **Tests:**
  - conformance: `test_posix_operations_inside_a_fork`, `test_renaming_a_source_file_moves_it_in_the_parent`, `test_a_deletion_travels_up_one_parent_at_a_time`, `test_a_deletion_does_not_discard_a_siblings_newer_edit`
  - Rust: `lazy_workspace::tests::source_tombstones_list_only_removed_source_paths`; the live-view rename test now also asserts that publishing doesn't resurrect the old name.

### 5. `._*` AppleDouble files merged as agent work (macOS)
- **Symptom:** `._retries.py` lands in the repo. `changedPaths` includes `._*` files, which skews speculation's "fewest changes" choice.
- **Root cause:** the macOS NFS client stores xattrs it can't send to the server as `._name` files. Adding the `namedattr` mount option (`darwinfuse.c`) didn't change that.
- **Fix:** handled in the engine, not the Python package. `subagent_stop` removes the `._name` companions the agent created, through its own mount, before the final sync (`remove_appledouble_companions`, macOS only). A first attempt that ignored them at capture broke the post-merge child rebase, so it was reverted.
- **Test:** live suite asserts the exact final tree.

### 6. Python binding bugs
`result.usage` is a property in pydantic-ai 2.48, not a method. A speculation check could merge its own artifacts; checks now run in a throwaway grandchild fork.

## Open

### C. Forks see later root changes while running (low, design)
- A fork reads the live physical root, so files merged into the root after the fork was created are visible inside it.
- Merges are unaffected: added paths are filtered, and a sibling's edit to an untouched file is not reverted (`test_a_sibling_edit_is_not_reverted_by_a_fork_that_never_touched_the_file` passes).
- Proper snapshot isolation would pin unresolved lookups to the fork's generation.

### F. Housekeeping
`cargo fmt` and clippy are clean for `acyclic-fs` and `acyclic-labs-plugin`. The pitch artifact still cites Arena for speculation; the source is now `speculate()` in `acyclic-pydantic-ai`.
