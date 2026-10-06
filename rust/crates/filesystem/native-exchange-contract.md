# Native carry rollback contract

Scope: recovery of `NativeExchangePhase::Carrying`, using the existing exchange journal and native-runtime rename. This does not qualify host restore, root approval, generation publication, Git merge, mount reconciliation, or whole-programme writeback.

The trusted host owns the journal, serializes exchange/recovery for each root pair, and keeps roots and intermediate directories stable during an invocation. Intermediate directories contain no symlink/reparse redirection. Prepared carried entries belong exclusively to this operation. Root identities are stable and distinct for live objects. A user may create or edit leaf entries while rollback runs. Durable rename and journal persistence rely on the filesystem's documented crash semantics. Root checks detect replacements before recovery; they are not an atomic fence against parent replacement during recovery.

| Invariant | Production mechanism | Assumptions | Verification and test map |
| --- | --- | --- | --- |
| Rollback never replaces a destination binding that exists at the rename boundary. | `durable_rename(NoReplace)`; collision returns `CarriedPathConflict`; journal remains. | Stable trusted parents; native filesystem supports no-replace rename. | Implementation inspection of native-runtime Linux/macOS/Windows operations; `carrying_recovery_keeps_a_concurrently_recreated_live_path` orders a real user write after observed absence and immediately before rename, checks both entries and journal, then resolves and retries. |
| Carry recovery does not mutate roots whose identities already differ from the persisted request. | Compare both root identities before rollback. | Stable identities; no root replacement after the comparison. | `carrying_recovery_rejects_replaced_roots_without_mutation` replaces each root separately, checks retained bytes and journal. |
| Empty paths or non-normal path components cannot trigger preparation or journal replay effects. | One carried-path validator at preparation and journal read boundaries. | Host-native `Path::components` interpretation; normalized interior dot components do not escape roots. | `invalid_carried_paths_fail_before_preparation_or_recovery_effects` covers empty, dot, parent, absolute, and Windows drive-prefix/rooted paths; places a valid path first to detect partial effects. |
| A returned carried entry is not replayed when rollback resumes. | Missing prepared source is skipped; remaining entries use durable no-replace rename. | Exclusive operation ownership of prepared carried entries; stable parents. | `carrying_recovery_replays_a_partially_completed_rollback` persists the request, returns one entry, edits it, resumes remaining entries, and checks both contents. This simulates a crash edge; it is not a power-loss test. |

Run on each supported native OS, with a private target directory and bounded build jobs:

```sh
cargo test --locked -p acyclic-fs --lib native_exchange::tests --jobs 2 --target-dir <private-target>
cargo test --locked -p acyclic-native-runtime --lib no_replace_preserves_both_entries --jobs 2 --target-dir <private-target>
```

These are implementation verification and finite real-filesystem test evidence, not a machine-checked or unrestricted correctness proof. Execution logs and platform results belong in worktree artifacts and the PR validation report.
