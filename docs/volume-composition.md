# Pinned volume composition

Filesystem forks accept an optional path selection. An absent selection inherits
the complete pinned generation; an empty selection inherits only its root. A
filtered fork retains the real source generation as its direct parent. Its
initial root carries the filtered-fork feature bit, and later generations clear
that bit. Merge and rebase use the selected initial tree as the effective delta
baseline, so excluded source paths are not interpreted as authored deletions.
Inherited bytes are the baseline even when the source already contains edits
from earlier forks. Integrating that selected child publishes its subsequent
edits; it does not implicitly import earlier source edits. A destination that
differs from the selected baseline still participates in conflict detection.
Hardlink counts are rebuilt for the retained aliases. Selection traversal obeys
the existing mutation and work budgets and never reads file bodies.

Fork retry uses the existing allocation record and checks the exact selected
builder. It does not replace the allocation journal with a process-local cache.
Harness records the pinned source and initial child scratch capture separately
from the child generation containing inherited context. The scratch source must
belong to the current direct parent agent, including when the selection is empty.

Volume owner and consumer role are independent identities. Exact signed grants
authorize reads and destination writes. Every agent-owned volume requires its
owning agent for writes, regardless of its role. Role changes produce a different
volume identity and cannot reuse an existing capability. Construction allocates
no content and grants no authority.

`FilesystemHost::fork_volume` and `prepare_volume_import` compose existing
Filesystem forks and pinned join plans. Imports require a source read grant and
destination write grant; they expose no source workspace handle. Source and
target generations are pinned, and publication retains the existing stale-target
and idempotency checks. The project provider uses the same mechanism without an
ancestry registry gate. Different provider deployments are rejected explicitly.
Three-way imports retain the Filesystem planner's compatible-semantics and
authenticated common-history requirements, including its configured lineage
bound. Unrelated volume roots produce an explicit semantic error.

Editable session skills can be forked into their own volume while built-ins
remain a separate read-only grant. Project integration never invokes skills
integration. Root writeback requires a separate explicit destination grant from
the caller's approval workflow; no fork, completion or teardown automatically
imports a volume. An import does not append conversation messages.

Custom conflict drivers use the existing registry and deterministic publisher.
`VolumeImportPlan` exposes exact conflict descriptions and retained candidate
publication for an admitted caller workflow. Effectful policy observations and
recovery belong to that workflow's journal; an in-memory resolution cache is not
durable evidence.

## Browser recursive preparation

`BrowserTaskRegistry.registerForkPreparation()` registers the ordinary Rust
machine; it does not start workers. `prepareRecursiveForkSource` opens the
authenticated conversation on the runtime's own BrowserStream. Supply the
current caller's real lease fence, owner, signed scope, and pinned volume
bindings. A root supplies its explicit bindings; a descendant's agent and
volumes must match its retained admission, never the root's bindings.

Retain the source handle's `snapshot()` and `request()` before admission.
`preparationRequest` builds the exact ordinary machine input; stage its fork
payload with the ordinary runtime, then admit the registered preparation task.
`resumeForkPreparation` and `reconcileForkPreparation` accept only that caller's
matching original preparation lease and bind its actual Filesystem preparer.
The existing prepared-admission command separately admits the child under the
original caller; preparation alone does not publish, merge, or write back.

After restart, restore the retained authenticated snapshot with
`BrowserTaskAuthority.restore`, then use `restoreRecursiveForkSource`. Restore
checks its exact head event against canonical browser history and never uses a
newer conversation tail. An explicit foreign `source` selects only project
data. Trusted RootService policy may authorize an operation-specific read across
the actual same-session root, ancestor, sibling or descendant graph without
changing the original caller's grants. The owning coordinator verifies retained
creator evidence; a mandatory trusted provider verifies the pinned generation,
genuine native capture and protected bootstrap allocation when selecting a root.
The original issuer MAC binds the fixed `harness.project-source.read.v1` purpose,
complete caller admission, issuing fence, fork operation and exact selection.
Immutable admitted evidence
survives owner recovery; current preparation authority remains separately fenced.
The serialized policy scope attests only an opaque descriptor label, not a
Filesystem read capability. Only the owning kernel can consume verified original
owner, graph and snapshot evidence into the private exact-selection read handle.
Neither this record nor its sealed read handle imports another agent's transcript,
private data or signed scope. Foreign workspace forks inherit no conversation.
History capture retains the ordinary bounded HistoryReader and trusted fork witness.

For ordinary model-driven defaults, pass `BrowserTaskOptions.recursive_tools`.
Its explicit signed caller, child allocation and merge-notice bindings configure
the same generic `ConfiguredRecursiveToolSource` used by native consumers.
Opening installs the real file tools and both matching recursive tool registries
on the existing runtime; it adds neither a browser source registry nor workers.
Known descendants bind their actual admitted agent and volumes. An unlisted new
child receives a typed authority denial, never a guessed root identity.
For browser foreign reads, `project_policy_graph_root` must identify the actual
coordinator graph root. Opening binds the production `FsRootProjectSourceProvider`
to that root and the supplied service owner, separately from conversation keys.
This creator-only policy verifies the selected immutable Filesystem generation;
it rejects raw native descriptors and unproved bootstrap sources. Native hosts
install the same provider with an independently authenticated native source.
Direct source consumers supply `selected_project` and finite
`source_policy_limits`; these references are verified evidence, never grants.
Logical scratch/output namespaces remain distinct from protected service journals.

The ordinary recursive workflow commits its bounded selected-source intent
before staging a source checkpoint or admitting preparation. Recovery loads that
exact authenticated canonical cut, and the operation-only publication receipt
recovers the original immutable checkpoint instead of selecting a newer source.
`content_budget` bounds measured receipt observation, checkpoint reads and
publication with their actual remaining work. Explicit `input_files` authorize
only the exact launch files; referenced child messages use the configured outer
and inner JSON pointers, not recursive descriptor guessing. Source checkpoints
are recovery data, not automatically delegated launch-file authority.


## Validation scope

| Invariant | Production mechanism | Assumptions | Verification and evidence |
|---|---|---|---|
| Selection is a real fork; omissions are not deletions | Initial feature bit and selected delta baseline | Authenticated immutable roots; bounded selection | `filtered_fork_subset_model_preserves_unselected_paths`, including the authored-deletion negative control; filtered rebase regression |
| Scratch belongs to the direct parent and survives retry | Pinned private capture and durable creation-generation lookup | Same Filesystem provider; owner read grant | Preparation retry, local reopen, 32-sibling and 1,024-level recursive consumer tests |
| Agent-owned destination writes require the owner | `VolumeRef::require_writer` shared by live/replay checks and `ContentGrant` | Trusted scope issuer; full volume identity in each capability | Copied-capability rejection and skills consumer with a different volume role |
| Imports preserve source and concurrent destination edits | Sealed pinned plan and existing CAS publisher | Explicit source read and destination write grants; one deployment; compatible semantics and bounded common Filesystem history | Shared native/browser selection fixture; project and skills consumers; source-advance and stale-target tests |
| Skills integration is separately authorized | Separate volume fork/import and built-in read grant | Approval workflow supplies the exact root destination grant | `skills_are_explicitly_forked_and_imported_with_read_only_builtins` |
| Recovery cannot infer success from a process cache | Existing allocation journal and exact builder verification | Provider acknowledgements implement their durability contract | Every-provider-cut empty/selected fork tests, durable preparation and local reopen |
| Discovery stays bounded by the selected query | Pinned authenticated directory page | Retained task-to-volume mapping remains owned by PR4/PR7 | 10,000 published-workspace test with nonincreasing page/backend reads and zero source traversal/materialization |

The bounded subset model enumerates all eight subsets of three files across two
descendant shapes, and includes an authored-deletion negative control. Property
tests cover retained hardlink aliases and arbitrary bytes. Provider-cut tests
exercise every operation boundary before calls and after commits for both empty
and selected forks, with and without a concurrent source advance. These are
bounded executable checks, not a proof over arbitrary provider implementations.

The discovery consumer keeps 10,000 published workspace generations present and
checks that a pinned one-entry query does not increase backend reads or page
reads, traverse source entries, or materialize a workspace. The heterogeneous
MountedView consumer uses memory and local object storage through a test-owned
implementation of the existing object-store trait. Its authorities remain in
memory; it demonstrates logical routing and cross-mount rename rejection, not an
OS mount or a cross-provider transaction.

Harness consumer tests exercise durable preparation retries, local reopen,
32 siblings, a 1,024-level recursive lineage, separate skills grants and explicit
pinned writeback. Native and browser adapters execute a shared selection/import
fixture; Chromium qualification includes reload, abrupt-tab recovery and
concurrent workers. Required platform checks must pass on the final source before
the change is qualified for landing.
