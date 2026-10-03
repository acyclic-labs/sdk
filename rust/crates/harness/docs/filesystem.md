# Filesystem adapter

Enabled by the `filesystem` feature (`filesystem-local` adds the durable local stores).

Explicit bridge from Harness resource references to versioned Filesystem workspaces. It keeps generation identities visible rather than pretending mutable paths are durable references.

The local preset's file tools require both their tool-call grant and the caller's exact volume read or write grant, including on completed replay. Its ephemeral directory-listing tool retains the first page observed for each operation ID, so reconciliation cannot silently return a newer generation.

Choose an `acyclic-fs` authority and object store for the required durability boundary, then wire the adapter into Harness. See the [adapter API](https://docs.rs/acyclic-harness/latest/acyclic_harness/filesystem/), [Filesystem guide](https://docs.rs/acyclic-fs/latest/acyclic_fs/), and [Harness guide](https://docs.rs/acyclic-harness/latest/acyclic_harness/).

For an infrastructure-free local run, `LocalHarness::new(model, provider).await` creates a fresh agent, its private volume and bound conversation, and a runnable stock loop. `local.run(prompt)` is the short text path; stage a file through `local.storage().stage(...)` and pass its `Attachment` to `local.run_with_attachments(prompt, attachments)` for the same canonical ref-only path. `local.storage()` and `local.bundle()` expose the full typed conversation API. `LocalHarness::with_limits` accepts an explicit agent identity and checked limits. The default registry contains `acyclic.list_files` for bounded, generation-pinned lazy discovery, `acyclic.read_file` for authorized immutable UTF-8 content, and `acyclic.stage_file` for bounded UTF-8 text in the agent-private volume with a pinned `FileRef` result. The staging tool uses its operation ID as a single-file upload retry identity, so reconciliation checks the provider's atomic receipt before any write. These tools have no shell or parent project merge authority. `LocalHarness::with_tools` replaces that registry and requires explicit grants for the supplied tools. This preset is deliberately ephemeral; substitute durable providers for resumable deployments.

For customized local composition, `MemoryHarnessStorage::new(agent, file_limit).await` creates the agent-private volume, bound conversation, Stream journal, and exact content verifier. `storage.builder()` starts a `HarnessBuilder` with that journal and an owner-bound content publisher; `storage.default_tools(limits)` provides the same owner-bound read and stage registry for custom builders without reconstructing it. The caller selects its model or custom executor and grants. Typed tasks and tools can call `TaskContext::stage_file` and `read_file` under their narrowed grants. `storage.stage` uploads primary text or an attachment before admission and returns its pinned `FileRef`; `storage.run_conversation` appends the canonical user record, commits and verifies an exact model-context selection, executes the typed turn, and appends the assistant result as ref-only history. `storage.run_prompt` is the text convenience over that path. Retry by operation ID recovers the same selection even if later turns exist.

Harness volumes have three distinct lifecycles: project workspaces fork from an exact generation and merge only through an inspected Filesystem join; agent-private workspaces are created afresh for each agent and never merge; session-shared workspaces are read through explicit grants. The default `ParentProjectController` binds the parent conversation reducer, verifies the acting scope belongs to its immutable agent, and selects its exact project volume. Only that parent's scope can fork (`fork:publish` plus project read), prepare promotion, or publish the inspected `ParentMergePlan` (`project:merge` plus project write). The raw Filesystem `JoinPlan` is not returned; conflict inspection, selected-side resolution, driver resolution, and expected-generation CAS publication all remain on the parent controller. Children may report their changes, but cannot authorize a merge into the parent project. A `FileRef` carries a generation-pinned path, SHA-256 digest, byte length, media type, and display name, but does not grant access. `ContentGrant` verifies the caller's signed scope for one exact volume operation.

`FilesystemProjectWorkspaces` adapts that controller to Harness's provider-neutral `ProjectWorkspaceProvider` binding. An inspected join plan retains the original parent authority and Filesystem CAS heads, accepts only its signed scope on application, and returns a typed outcome; opaque conflict keys preserve native name bytes for exact selected-side resolution. Publication of its successful `ProjectMergeReceipt` and conversation notice remains a separate parent Stream operation.

`FilesystemForkPreparer` is the parent-controlled, retryable path from an exact `ForkRequest` to a ref-only `ForkReport`. The request fixes child volume identities and inherited-context bounds before work begins. A separate provider-owned journal claims the entire request under its operation ID and stores the final report, so `reconcile` reads durable state without starting a second fork. Provider-global allocation claims prevent another preparation, even under a different parent, from adopting either child volume; an exact seed digest binds each allocation before `FilesystemForkVerifier` permits publication. Custom parent-controlled preparation paths must call `FilesystemHost::claim_fork_seed` with the exact parent reducer and authenticated parent scope; it compares inherited bytes with the selected parent prefix before claiming volumes. Project and child-private work use distinct operation-derived Filesystem retry keys; a changed request under the same ID conflicts. The adapter captures the bound Stream parent revision and Filesystem project/shared selections. Other selected provider resources are reported as unsupported until their capture adapter is installed; an unavailable required capture cannot be published. These journals are not stored in the child private or project volume, preserving their publication and merge invariants. This preparer does not claim a common Stream/Filesystem boundary or accept an attestation it cannot verify.

Install other exact resource owners with `with_capture_provider`. The preparer journals each capture start and result. Once started, another attempt calls the provider's `reconcile` method by the stable fork operation ID and never blindly repeats `capture`; an unresolved observation remains explicitly indeterminate rather than committing a misleading report. Read-only Machines and Objects captures can safely re-read their immutable revision. Publication still requires the corresponding provider verifier.

An owner can share one pinned private file or a segment-bounded private directory with an unrelated attached agent, without inventing fork lineage. A reader-bound `FilesystemContentVerifier` exposes `list_private_directory` and `read_private_path` through that signed, read-only scope, so durable adapters can open the owner's workspace lazily without mounting or copying it; pagination and subsequent reads can require the same observed generation, so a changing head yields a conflict instead of a mixed view. Staged files carry an atomic, versioned metadata index so a discovered path retains its actual media type, display name, and digest rather than being guessed from its name. Internal upload receipts and metadata are excluded from delegated discovery and cannot be read or overwritten through agent file APIs. The local `MemoryHarnessStorage` exposes the same owner-issued delegation and lazy list/read flow.

Use `put_content` to stage message text and attachments before appending a conversation event. Bind `FilesystemContentVerifier` to the Stream aggregate so admission checks every exact file version and refuses dangling references. Large ordered attachment lists can be staged with `put_attachment_manifest`; admission resolves the complete manifest, validates its count and canonical JSON, and verifies each member. The parent controller's `materialize_inherited_conversation` writes the selected bounded prefix into a reserved path of an empty child-private generation and returns exact read grants and attachment manifests for every child/attached reader; ordinary agent uploads cannot write there. Its canonical bytes bind both the authoritative parent messages and the attached-reader set, so changing either under the same staging key fails. The `ForkSeed` pins that private generation, and `FilesystemForkVerifier` checks that no file or directory outside the selected prefix was adopted before the fork event becomes visible. After a successful inspected join, use `merge_receipt` and bind `FilesystemProjectMergeVerifier` to the parent Stream aggregate: publication verifies the provider-owned join witness against the durable operation fingerprint, exact source/target generations, retry identity, and join ancestry, then appends the project-merge receipt and parent conversation notice atomically. The notice may be published after later project edits because it names immutable generations. No private volume or child history is merged.

The raw Filesystem host and the `AuthorityIssuer`/verifier pair are trusted provider wiring, not reader inputs. Resolve an attached agent's signed scope against the owner's retained verifier; accepting a reader-supplied verifier would let that reader invent its own grants. The ref and its volume identity remain non-bearer even when the owner makes a directory lazily discoverable.

`FilesystemExecutionJournal` keeps model observations and tool inputs/results in pinned agent-private files. `FilesystemInteractionHost` stages interaction prompts, answers, and optional approval explanations there, then commits ref-only records to the owning conversation Stream aggregate. The executor journal delegates interactions to that same aggregate; it has no second interaction history. Resolution requires an authenticated grant for the exact interaction plus `interaction:resolve`, validates staged bytes against the original request, and reconciles identical retries.

`FilesystemWorkflowJournal` applies the same rule to resumable state-machine transitions: checkpoint state and input live in descriptor-verified private files, while its Stream contains only exact `FileRef` records. Emitted `WorkflowCommand`s contain stable operation IDs, kinds, and immutable argument refs—not inline command bodies. The journal verifies each command payload's owner-private residency before commit and on public replay. Normal commits reuse a verified in-process checkpoint/identity summary; cold recovery and uncertain appends rescan authoritative record envelopes before proceeding. A workflow is bounded to 4,096 transitions and 65,536 total transition/command identities so recovery cannot accumulate an unbounded identity set. A lost append acknowledgement is reconciled by operation identity instead of blindly dispatching the transition again.

`FilesystemSchedulerPayloadStore` stages join, quorum, and reducer JSON results in an explicitly granted agent-private volume. The coordinator verifies each exact staged result and its declared schema before publishing its `FileRef`, and repeats that verification on replay.

## Exact completed model history

Shared `HarnessStorage` publishes completed batches from their immutable model
request and completed boundary records. This includes assistant text and tool
rejection feedback, rather than inferring history only from successful effects.
The versioned application/vnd.acyclic.model-text.v1+json artifact is a canonical
JSON string; projection restores the exact text message and refuses corrupt,
noncanonical or oversized content.

`completed_conversation(operation, step, limits)` publishes the complete exchange
before fork preparation. It checks the authoritative boundary before writing
publication artifacts and refuses later conversation messages. Existing Stream
and Filesystem fork preparation/publication still own recursive fork semantics.

`inherited_builder` binds a child's private storage to the parent's pinned model
and prefix with a declared suffix. Tool registration and grants remain explicit;
the inherited transcript does not add mutable workspace permissions. Applications
still need durable fork/task admission; the constructor does not create a child.

## Model tool admission provenance

`StockExecutor` passes `ModelToolContext` (owning turn and zero-based step) to model
tools through `execute_in_model_batch` and `reconcile_in_model_batch`. It validates
the deterministic `ToolInvocation` identity before dispatch or reconciliation.
Default adapters retain their existing execute/reconcile implementation;
context-dependent adapters can refuse calls without model provenance.

This metadata is not injected into tool arguments or model conversation content.
`ModelToolContext.publication_operation` supplies a stable, separate identity for
the completed-batch publication. It is a prospective activation dependency, not
proof of completed publication or a permission grant. The admission provider
must still persist bounded child admission and gate activation on publication.

The executor admission identity is now acyclic.stock.v4. Earlier executor
journals are refused before dispatch; this change does not migrate them.
`FilesystemGitFacade` is the small model-facing bridge for Git-shaped workspace
commands. It binds one authenticated project scope and delegates parsing,
generation transitions, merge/rebase sequencers, conflict continuation, and
recovery to `acyclic-fs::GitCompatRepository`; it never invokes a system Git
process or copies directories. Read commands require the exact project read
capability. Mutating commands require project write, and merge/rebase
transitions additionally require `project:merge`. `resume` rechecks the
pending transition kind, requiring `fork:publish` for a retained branch fork
or workspace switch and `project:merge` for a retained merge or rebase, then uses the same durable
transition and executor identity after restart.

The same facade exposes typed project lifecycle methods for fork, merge-plan
inspection, conflict description, side or driver resolution, publication, and
merge-receipt construction. Each method creates a `ParentProjectController`
against the caller's reducer and exact project volume, so direct-parent
lineage, signed scope ownership, generation pinning, and provider CAS checks
remain in Filesystem. Applications do not need a second model-facing schema
for these operations. When a publication will carry a merge notice, use the
notice-bound child publication method; it validates the child and notice at the
same boundary immediately before the provider join. When the caller also
needs the authenticated receipt, the receipt variant consumes the exact child
and notice values used for that publication.
Native root writeback has a corresponding notice-bound method for the same
pre-join validation requirement.

Use the `*_for_child` variants when a model-facing request carries a child
conversation identity. They revalidate that the current parent reducer has a
published direct fork whose project resource matches the requested child
volume, and that the inspected plan captures that same child volume; possessing
a project capability alone cannot authorize sibling or grandchild promotion.
Native approved writeback uses the same lineage fence,
operation identity, and immutable source/target generation approval, and a
retry of a completed provider operation reconciles to its durable result.

Root writeback is a separate explicit boundary. `RootWritebackApproval` binds
one operation ID to the inspected child generation and expected root target,
and requires the parent scope's `project:writeback` grant. The approval is
checked again immediately before `ProjectJoinPlan::apply`, so a plan from a
different child, generation, operation, or scope cannot publish into the root.
The underlying provider continues to own parent-only authorization, conflict
resolution, CAS publication, continuation, abort, rebase, and recovery.
