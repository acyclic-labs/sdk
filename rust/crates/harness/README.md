# acyclic-harness

Rust-first, provider-neutral agent runtime. Harness owns typed conversation, task, effect, interaction, and fork semantics; providers own byte storage, history, execution checkpoints, and model-specific projection. A Rust future is not automatically a durable operation.

Declared instructions and skill frontmatter use the ordinary context pipeline.
An authorized caller captures a `ContextDiscovery` revision with finite limits,
then binds it through `HarnessBuilder::declared_context`. Skill bodies stay lazy
and `PinnedContextPath::read` uses the existing owner-authorized retained path
read. Explicit refresh is the default; next-request refresh is opt-in at the
caller's admitted boundary. See [discovery and reload](../../../docs/instruction-skill-composition.md).

`HarnessBuilder::limits` centralizes file size, path, attachment, direct-rendering, selected-history, model-step, per-step stream-event, and tool-call ceilings; `StreamAggregate::with_limits` applies content bounds before publishing a conversation message. Physical providers may impose tighter upload and rendering bounds.

At event admission, the content resolver must return the exact bytes of every primary file, inline attachment, extension, and manifest member. Harness checks each pinned descriptor itself and decodes manifests from their canonical pinned bytes; a provider's metadata-only `verify` or pre-parsed `load_manifest` response cannot authorize publication. Uploads may remain staged after a failed admission, but no event is published with a dangling or substituted ref.

Fork publication verifies the manifest file itself before decoding its canonical bytes, checks every listed member at its owning provider, and counts unique referenced descriptor bytes against each provider's aggregate limit. The protocol also caps total manifest membership and grant vectors before walking them; attached readers need volume authority or exact owner-issued file grants, including for the manifest.

Bounded producers share `canonical_json_bytes_bounded(value, JsonBounds, charge)` instead of constructing an unchecked serde `Value` first. It preserves sorted raw UTF-8 keys, last-duplicate-key wins, full-width integers and the existing serde-Value float identities (including `f32` widening and nonfinite values becoming `null`). Bounds count every emitted duplicate entry before it is discarded, not only final output bytes. Depth is zero-based with a bounded-codec implementation ceiling of 256. `json_bytes_bounded_trusted` preserves ordinary declaration-order record bytes through two bounded passes and requires a deterministic trusted serializer.

`json_value_bounded_trusted(value, JsonBounds, charge)` returns that same encoder's actual owned canonical `Value` directly: there is no encoded-buffer/parsing roundtrip, second serializer, or unchecked `to_value` prelude. Its returned logical string/Vec/map-entry custody remains live until the value is freed. Already-freed temporary claims are not transferred; failure drops nodes before unconditional release and retains the original callback error. Custom serde internals and opaque allocator/node/RSS effects are not observed or sandboxed. The existing `RawValue` branch still requires separate schema/parser `Admission` before parsing; its next `Retain` observes the parsed tree, so a native adapter adopts the prior quota with zero invented allocation operations instead of double-counting a future reservation and actual tree ownership.

`JsonCodecCharge` supplies encoded-byte/item work, explicit copies, lexical input consumption, before-growth logical retention and after-free release. Retained output custody must live until its buffer is freed. Both encoder paths release every admitted codec retention after their buffers drop on any error; spent work/highwater is preserved and the first failure is not replaced by cleanup. `Release` is unconditional cleanup and callbacks must accept it after cancellation/fencing without another authority checkpoint. Separate caller-owned schema/adoption guards remain the caller's responsibility. Logical storage covers requested string/Vec payload and map entries, not opaque allocator rounding, B-tree nodes or process RSS. Borrowed input and credentials remain caller-owned. Lexical consumption is not another physical provider read. Serde custom code remains trusted: callbacks cannot sandbox allocations or loops performed before that code calls serde.

`json_from_slice_bounded_trusted(bytes, bounds, maximum_owned_bytes, charge)` admits complete input size/items/depth before typed serde decoding. Its `Admission` event is a conservative preflight of an **audited concrete schema** plus parser scratch, not an actual allocation receipt or an arbitrary `DeserializeOwned` heap proof. Concrete type owners justify that witness from finite fields and account actual retained capacities/copies separately. Raw JSON values use the same existing serde-Value interpretation under analogous lexical/schema preflight. Trusted legacy canonical helpers use the same encoder engine without imposing producer admission policy.

Canonical event reads preflight the complete closed Protobuf carrier, then audit the **concrete `EventPayload` graph** before its existing serde decoder runs. Its logical quota includes the four reachable tagged-enum `Content` buffering layers, initial/geometric Vec slots sized from actual declared element types, typed map-entry payloads, heap-owned root boxes, the custom interaction-outcome `Value`/`from_value` overlap, decoded string data, parser scratch and source-bounded diagnostic interpolation. File-reference paths are strings; native paths/process bodies are not part of this schema. The audit anchors serde 1.0.229's actual hidden `Content` type, so a dependency upgrade requires source review. This caller-owned logical quota/highwater reservation spans decode and error formatting, then is replaced by observed retained payload custody without double counting. Guards release after owned buffers are dropped, including cancellation failures. Canonical re-encoding is the original wire validation, never an extra encoding just to manufacture receipt work. This is not an arbitrary serde guarantee and invents no allocator attempts, opaque node footprint or global RSS bound.

Checkpoint decoding uses the same lexer and original typed serde decoder with a separate **closed `Snapshot` schema** admission. It reuses the core snapshot clone inventory for collection/map-entry slots and the event audit for tagged buffers, strings, transient `Value`, diagnostics and per-event boxes. The quota additionally covers `ConversationState`'s omitted UUID/position/digest caches and its original per-message canonical history-hash tree/output overlap. It preserves the caller's finite multi-chunk checkpoint byte allowance rather than imposing a single-event cap. One `Admission` must create real logical RAII custody before decoding; failures release after payloads drop, and success adopts actual retained capacities without double counting through snapshot/restore ownership. This admission neither authenticates a snapshot nor replaces authoritative event-anchor/MAC checks. Cancellation is cooperative at codec callbacks; the audited synchronous serde converter is not an arbitrary user-code sandbox. Opaque allocator/node overhead and process RSS remain outside the bound.

The same optional observer covers original coordinator replay, declaration, append/retry recovery and lease release. Scheduler reads admit all seventeen concrete event variants before the original single Serde parse; protobuf, raw JSON and actual decoded-event custody overlap on one ledger. A measured quiescent release uses the original coordinator, execution and protected workflow tails in one atomic commit, including the current pending/suspended release witness rather than an initial workflow tail.

Fenced commit producers construct a private `PreparedCommitRequest` on that original observer. Known condition, mutation and nested-record Vec growth is admitted before allocation; owned request payloads drop before their quota guard. The same commit/recovery engine consumes its borrowed custody without a release/reacquire gap or double retention, and retains a generated retry-key payload through acknowledgment-loss inspection. This witness conveys storage custody, never publication authority.

Checkpoint reads likewise admit the closed `Snapshot` schema, including concrete conversation caches and canonical-digest scratch. One original Serde parse moves a private conversation wire carrier; the original reconstruction then observes its actual message walks, canonical encoder and digest inputs on the same ledger. Its future cache/scratch allowance is released before actual cache construction, whose retained Vec capacities and logical map payloads transfer into snapshot custody. No extra re-encoding manufactures work, and no provider/allocator/RSS footprint is inferred.

Versioned extension events stage JSON in an owner-controlled content provider and append only a verified `FileRef` plus pinned schema, implementation digest, and fork policy. Stream admission checks digest, length, JSON syntax, and the registered schema before publication; replay requires the exact registered implementation and never embeds extension payload bytes. A parent may inherit an extension revision only under `inherit`, capture a distinct provider-owned child revision only under `reset`, and cannot select it under `reject`.

`SchemaRegistry::register_with_dependencies` pins exact extension-version edges before activation. Its deterministic `activation_order(selected)` resolves only the chosen roots and transitive dependencies, rejecting missing versions, version conflicts, and cycles without recursive traversal; installation may be out of order, but an incomplete selection cannot be activated. `register_configured` additionally pins a JSON configuration schema. An agent aggregate accepts `ConfigureExtension` only at a pending/waiting boundary, after provider admission has verified the staged JSON bytes, digest, length, and schema. `SelectExtensions` then requires configuration for every selected version that declares it and records the exact configuration refs alongside its dependency-ordered selection. `Reducer::extension_admission` captures the committed activation event and exact selected configuration refs; scoped tasks cannot retarget that snapshot, and the durable host stores it in its ref-only admission file for recovery. A later configuration or selection does not rewrite earlier admissions. An extension state version switch requires `MigrateExtensionState` from the exact latest retained event; a plain append cannot bypass it. `StreamAggregate::migrate_extension_state` authenticates the source and exact destination write grant before reading, executes a pure, exact-digest `NativeExtensionMigrations` implementation over verified prior bytes, stages the target JSON through an owner-bound volume (which may differ from the inherited source volume), and publishes only its ref. Direct provider admission independently re-executes and compares the migration before Stream CAS, rejecting absent or substituted implementations. The host lifecycle is complete at composition level: `ExtensionRuntime` pins exact executable leases, links native tasks/tools and provider slots through `NativeExtensionBundle`, gates new admissions after `disable`, and retains existing leases for reconciliation.

Effects likewise pin request, successful result, and JSON Schema bytes by `FileRef`. Planning validates the staged schema before admission; resolution re-reads that exact schema and result under the owning provider grant before publication. Stream events retain only refs, operation identities, and digests.

Interactions have one ref-only aggregate history for open requests and versioned outcomes. Prompt/schema and answer JSON are staged as private files and validated by the content provider before admission. Presentation through `InteractionRouter` is separate from the owner-mediated `InteractionResolver`; `HarnessBuilder::interaction_resolver` binds the latter. `AgentHarness::inspect_interaction`, `resolve_answer`, and `resolve_approval` use an authenticated responder scope, exact version, and stable operation identity, returning a typed `ResolutionReceipt` including whether the operation replayed. The Filesystem/Stream adapter validates the signed responder grant before staging bytes, checks the response against the admitted request, and reconstructs the original aggregate CAS revision on a lost-ack retry. The Stream never contains prompt or answer bodies. The execution journal can retain its own effect/wait boundary without copying those bodies.

Scoped tool installation asks a `ToolApprovalVerifier` to read a resolved journal approval for the exact installation operation and tool-definition digest. A caller-constructed approval object is not accepted.

Durable scheduler declarations pin initial process state by `FileRef`, not inline JSON. Opening the Stream-backed coordinator requires an authority-scoped content verifier; it checks declared state and successful result bytes on replay and before new publication. New operations must enter through `declare_operation` with a signed, matching owner scope and `operation:declare`; generic event submission cannot publish a declaration or an orchestration decision. Successful worker outcomes are JSON `FileRef`s. Joins, quorums, and reducers stage their result through an owner-bound `SchedulerPayloadStore` before publishing a ref-only event. Malformed or missing bytes leave no new event and prevent reopening an affected history, while an exact in-process retry reconciles by idempotency key. Placement is bounded exact-match worker label metadata, enforced when leasing, and durable inbox items carry `FileRef` payloads.

```sh
cargo add acyclic-harness
```

The [`custom_executor`](examples/custom_executor.rs) example uses public
`StockExecutor::model_step` independently of the stock control loop. It reuses
turn/step validation, exact request admission, durable observations and
reconciliation. A started request loads its retained canonical artifact before
recovery; it never reloads sources or transforms. New projections require new
admissions. A custom loop must finish each tool exchange before requesting the
next model step or publishing a child prefix.

Stock output-token budgets are caller-controlled: Rust exposes
`StockExecutor::with_max_output_tokens`, and TypeScript exposes
`HarnessBuilder.modelOutputTokens`. Neither adds an output-token ceiling when
the caller omits the budget; explicit budgets accept any positive value
representable by the model request's `u32` field. Rust binds the
budget into the durable execution identity; admitted requests retain it on replay.

Default admission and projection limits use representable values, with counts
fitting the 32-bit WASM runtime and byte lengths fitting exact JavaScript integers.
Callers may choose narrower budgets. The local runtime's default concurrency uses
the semaphore's representable permit count; explicit concurrency still controls
scheduling. Replay page and cache sizes control processing and retention, without
limiting the total admitted history.

`ContextPipeline` composes ordered sources and transformations. `SelectionStage`
registers a pinned file/span or schema-validated custom attribute with a typed
`ContextRenderer`; full, summary and reference representations are explicit
renderer policies. Summaries must come from separately admitted operations and
pin their dependencies in the renderer contract. Prompt rebuilds and appended
updates use the same immutable attribute state. `reload` validates replacement
stages and returns a new pipeline; failures leave the inspectable previous
contracts intact. Existing clones retain their implementations. Compiled changes
require rebuild/restart unless the host supplies replacement implementations.
`run_bounded` bounds each intermediate projection without truncation. Browser
hosts use generated selection types and `NativeContracts.applyContextProjection`
for the same Rust placement/bounds; they supply authorized file reads and custom
rendering. No constructor grants authority or starts effects.

`HarnessBuilder` composes the stock journaled turn executor from an immutable model binding, context pipeline, and versioned tool registry, or accepts a replacement executor. `agent_loop` separately binds a typed live `AgentLoop` over the public `TaskContext`; it can recursively spawn and join ordinary tasks without a turn journal, while its Rust future is explicitly not resumable. The same builder can bind model and context stages for live task code independently of the turn executor. Conversation records are typed and ref-only: primary text, attachments, tool artifacts, and extension content are `FileRef`s pinned to an exact provider, owner, volume, generation, SHA-256 digest, length, and media type. Possessing a ref does not grant access; resolution requires a signed scope with either the matching volume read grant or an exact-version file read grant. Private-volume writes additionally require a host-signed acting-agent identity matching that volume's original owner; scoped descendants inherit that identity without being able to change it. An owner can delegate a pinned private-file read or a segment-bounded, read-only private-directory grant to any agent without creating a fork. The directory grant supports lazy discovery but cannot expose internal storage paths or grant writes.

`CompositeContentVerifier` routes refs by exact owner volume. A `ContentMountResolver` resolves an unseen exact `FileRef` on demand and returns both its owner reader and a verified `ContentGrant`; the composite checks that grant against the ref on every use. Attached agents therefore need neither an eager copy nor registration of every private directory. Explicit volume bindings take precedence; both routes still verify pinned bytes at the owning provider before admission or model projection.

A replacement executor can coexist with the same pinned tool registry used by typed tasks. Only stock-loop model, provider, and context-stage bindings are exclusive with that replacement; tool registration is not silently discarded.

Durable task admission has an operation-only reconciliation path: `HarnessBundle::reconcile_admission` observes a committed task ID and pinned implementation without resending the input. A confirmed absence returns `None`; missing content for a committed admission is an error, not an absent operation. `HarnessBuilder::state` and `spawner` may bind retained observation and admission independently; `durable_host` supplies both as a combined local convenience. An independent spawner requires the state provider to attest the entire owner-retained `TaskAdmissionRecord` before the runtime exposes an accepted handle; an operation ID need not equal its task ID. Unobserved batch members stay individually indeterminate. The typed `attach` call checks the registered definition before exposing the task result.

Task and batch admissions retain explicit `resource_proofs`, empty when no
creation-derived authority is requested. A configured `ResourceCreationProvider`
verifies the existing preparation task, original allocation generation and
protected provider records. The coordinator authenticates its original parent,
creation fence and intended child before sealing exact grants and content
bindings into that child's scope. Caller-built proof refs alone grant nothing.
`FilesystemResourceCreationProvider` uses the ordinary workspace, content verifier
and publisher; `open_with_creation_providers` composes it into the same filesystem
task runtime. Child writes and new child admission require their current retained
publication lease, while inherited access preserves the original proof identity.
Workflow command and model inputs resolve through those same owner-authenticated
bindings. Protected workflow and execution records remain in their original
owner-controlled storage; ordinary content refs cannot substitute for them.
`FilesystemForkPreparer::project_creation_operation` exposes the exact derived
Filesystem creation identity for binding an already-created child to its native
overlay. It does not authorize creation or start another fork.

An optional explicit child result volume uses the same original fork, protected
allocation claims and admission proofs. Its mandatory `Result` role creates an
empty, unmounted private volume with READ/WRITE custody; it is never inferred
from private-volume order and never replaces the unique ordinary `Scratch`
writer. `stage_artifact` selects this result binding when present.
`child_scratch_creation_proof` instead retains only the original scratch identity,
sharing the existing provider handle without copying a lease. Its measured
settlement authorization checks that original task, agent and allocation plus
current-fenced WRITE; a native physical leaf receipt remains separately required.

The filesystem task runtime enforces the originally admitted absolute deadline
through the host's current clock and native or browser-worker timers. Expiry
records cancellation but does not claim an uncertain command stopped.
`reconcile_task` only observes already retained command intents under their
original lease with caller-selected command and wait bounds; it cannot execute
a new command, invoke a task-machine transition or reset the deadline. It reads
the original authenticated outbox rather than reopening an execution session.
Original atomic mail receipts are observed without another send. Cancellation
may release capacity only after all dispatched effects are known, or the
authenticated journal proves no dispatch occurred.

Measured conversation binding threads one optional SDK-work observer through the
original coordinator refresh, bounds and conditional append/recovery engine.
These counters describe actual SDK calls and exposed source/encoded bytes, not
provider-internal OS IO. The executing agent comes from the current sealed scratch
allocation, including read-only provenance. Only proofless roots may use their
verified original declaration agent, matching the configured host. Child fork
audiences come from that protected allocation; roots require a verified original
canonical conversation-bind proof retained on the same task owner, never a
tenant/task-UUID projection. An overdue measured fresh preflight rejects before hidden cancellation IO;
it does not claim durable cancellation, stopped effects or released capacity.
Budgeted owner append preparation admits canonical fence codec growth, hashing and
copies through the original SDK-work observer before allocation. Ordinary mail
send exposes no caller work budget: it preflights the entire typed canonical
envelope against the Stream record limit before encode/hash, then uses the
existing provider append gate. This finite record bound is not an SDK-work receipt
or a claim about opaque provider allocations.

`AgentHarness::children` asks the retained task owner for a bounded page of direct children, independent of conversation forks and task dependencies. The coordinator authenticates the parent, exposes only same-owner children in stable slot order, and requires its revision on continuation so concurrent admissions cannot silently skip a child. A caller from another owner cannot declare a child under that parent merely by knowing its ID.

`Bindings::local()` starts without authority or implicit providers. `HarnessBuilder::bindings` installs its scope, registries, provider boundaries, and concurrency together. An imported binding scope can only be narrowed; configure local root grants on `HarnessBuilder` before building, or construct the desired scope in the binding set before importing it. Later builder calls can register more tasks and tools.

`HarnessBuilder::workspaces` binds a parent-authenticated `ProjectWorkspaceProvider` independently of content and model providers. The neutral contract returns an opaque inspected `ProjectJoinPlan`, exact source/expected-target generations, and typed applied, stale, conflict, fenced, and no-change outcomes. Fork, inspection, and application each require the signed parent scope; inspection also takes the current parent reducer and proves that the child fork was published, even when the workspace binding predates publication. The plan is not a bearer write grant. Merge provider retry identity is derived from the single Harness operation ID. Attenuated views do not inherit parent fork or workspace control. The Filesystem adapter retains its concrete `JoinPlan` and expected-generation CAS internally.

`HarnessBuilder::task` and `HarnessBuilder::tool` register individual pinned definitions; `tasks` and `tools` bind complete registries. A stock or replacement turn executor requires an owner-controlled execution journal; `HarnessBundle::run` executes typed turn input through that pinned journal. A custom live `AgentLoop` instead runs through `HarnessBundle::run_agent` as an ordinary typed task, with no claim of crash recovery. For a ready-to-run ephemeral preset, `filesystem::LocalHarness::new(model, provider).await` (feature `filesystem`) creates the local journal, agent-private volume, conversation, and stock model loop, then offers `run(prompt)` over the same typed path. `HarnessBundle::runtime` exposes typed local spawning, stable-ID durable admission, scoped task/tool contexts, explicit cancellation, and ordered or completion-based task observation. `AgentHarness::scoped` narrows grants and limits; `scoped_model` and `scoped_context` select child model and context bindings. `TaskContext::build_context` and `model_events` use those bindings with bounded context/events and granted tools. They deliberately refuse hidden provider I/O in a resumable task; that path still needs recorded effect/reconciliation boundaries. `Bindings` is the lower-level composition contract for applications supplying their own tool registry, durable host, and interaction router. A dropped task handle does not cancel its work; `TaskGroup::cancel` does, including descendants. Child tasks use independently bounded groups so recursive fork-join still runs with a one-permit parent group. Durable context reconstruction requires the host to authenticate the task and return its pinned, narrowed scope.

`HarnessBuilder::policy` binds one versioned `ToolPolicy` for stock and typed calls. Tool grants remain mandatory even when policy allows an invocation. An approval decision opens a journaled interaction bound to the exact task, tool revision, arguments, and operation; decline, expiry, cancellation, denial, and unresolved status never dispatch the tool. Terminal non-approvals return a typed `InteractionRejection` and distinct v2 wire error codes; uncertainty retains the operation ID for reconciliation. A durable `CoordinatorTaskHost` must bind the same policy identity with `with_policy`, and admission pins that identity so a changed implementation cannot silently authorize a replay. Direct `StockExecutor::new` starts with no tool grants; standalone callers must explicitly supply a scope through `with_tool_authority`.

The stock model request advertises only tools with an exact `tool:call:<name>` grant, and dispatch checks that grant again. A stock `HarnessBuilder` also requires `model:generate`; passing a model provider alone does not authorize inference.

Live `TaskContext::scoped_policy` adds a pinned child policy without replacing its ancestor's decision. Descendant tasks and groups inherit the added layer; each layer must allow the call, and each requested approval is independently bound to that layer's exact action. Durable tasks reject late scoped policy overrides because their owner host would otherwise replay under a policy that was never admitted.

`TaskDefinition::resumable` takes its input and output JSON Schemas at construction alongside the pinned machine implementation. Both schemas are part of the durable definition digest; a catch-all `{}` or `true` schema is invalid, including after a later `.schemas(...)` call. Live Rust tasks may use their exact Rust types without an explicit JSON Schema.

`HarnessBuilder::resumable_tool` registers a version-pinned `ResumableMachine` behind a typed tool definition. An admitted durable `TaskContext::open_resumable_tool` binds the exact task, call, scope, definition, input, and machine identity to an owner journal before the first transition. `ResumableToolSession::step` commits a validated checkpoint and its ref-only command outbox atomically; a completion value must satisfy the pinned output schema before commit. `MemoryWorkflowJournal` supplies the local implementation, while `FilesystemWorkflowJournal` persists admission and transitions as private files with ref-only Stream entries. An unknown command effect must be reconciled by its stable operation ID, never blindly re-executed.

After a restart, `AgentHarness::attach(task_id, &definition)` asks the durable host for the identity recorded at admission and accepts only the same registered resumable definition. A caller cannot reconstruct `RuntimeTask<Output>` from an ID and a chosen Rust output type alone; successful results are still checked against the pinned output schema.

Durable observation distinguishes ordinary pending work (`None`) from a terminal `Indeterminate` outcome. `RuntimeTask::result` waits for a terminal observation before join, race, first-success, or quorum counts that task as complete. Hosts may replace the bounded-poll wait with provider completion notifications. Long-lived Stream coordinators refresh foreign commits before observation and reconcile a repeated operation against the committed intent.

Task and tool registries can retain several revisions under one logical name. Use `name@version` for exact task lookup; unqualified task lookup works only when unique. `ToolRegistry::select_model_version` explicitly chooses the one revision advertised to a model when several are retained; durable tool calls still resolve their own pinned revision. Ambiguity fails before stock model dispatch rather than silently choosing the newest registration.

Assemble each tool explicitly from `Tool { definition, executor, projection }` and register it with `ToolRegistry::register` or `HarnessBuilder::tool`. The selected definition pins its revision and independent argument, canonical result and projection schemas. A bundle can contain a single tool without implementing other coding operations. Fork and merge remain parent-controlled workspace operations.

Every admitted `ToolInvocation` and `ToolContext` carries a runtime-owned `operation_id` independently of the provider-owned `call_id`. The stock loop derives this identity from the turn operation, model step, and call ID, so retries reconcile the same effect while a reused provider call ID in another turn cannot alias it. Durable dispatch rejects a context bound to another operation or task. Host executors must use `operation_id` for side-effect deduplication and recovery.

`DurableToolRunner` accepts only an authenticated, admitted `ToolContext`; its former contextless dispatch entrypoint is removed. It checks the exact task/call identity, effective grant and policy, and owner-scoped executor authorization on both dispatch and replay. Its pinned request digest includes the effective artifact and model-projection byte limits, and completed refs are rechecked against those limits on recovery.

Model context is a separate explicit selection over canonical conversation history. Projection resolves only selected refs under an authorized provider and applies bounded text/image/file policies; changing model providers does not rewrite the conversation. `CompositeContentVerifier` routes manifest and member reads across exact registered owner volumes, including distinct agents on one provider, preserving one configurable admission limit. Forks capture exact project generations, create fresh agent-private volumes, retain a bounded direct-parent conversation pointer in a child-owned file, and leave session-shared resources behind explicit grants. The seed pins the last inherited message sequence; grants and published attachment manifests are checked against that prefix, so omitted history is not implicitly shared. The parent can attach additional reader agents to a fork and delegate exact-version, owner-mediated read capabilities for published references it controls across private, project, and shared volumes. Manifest bytes and each listed member need independent read authority: original ownership, a selected project/shared-volume read grant, or an exact-file grant. Child-owned inherited-context files also produce exact-file read capabilities for the child and attached readers. A foreign owner's private file cannot be delegated by the parent. Attached read grants carry no private-volume write authority; the original owner remains the writer. The child conversation remains bound to its one owning agent. The parent conversation controls fork selection and project promotion; a child reports changes but cannot merge them into its parent. Only project state is eligible for inspected, conflict-aware merge.

Use `HistoryReader` for pinned cold pages, indexed original operations and message ranges rather than retaining an unlimited reducer tail. `StreamAggregate::open_measured` uses the same authenticated checkpoint/suffix engine; `open_at_revision_measured` selects the exact original checkpoint and cut without replaying later history. SDK-boundary receipts count each actual tail/read/write invocation and raw returned record bytes before decoding, including malformed/oversized prefixes. Metadata with no exposed encoded size contributes zero encoded bytes. Append-record counters mean confirmed SDK acknowledgments, including idempotent acknowledgments of an original record, **not physical new appends**; lost-ack inspection is a read, not an observed append acknowledgment.

`HistoryReader::effect_events_measured` returns only the selected effect's original, owner-attested transitions at a pinned cut, or issuer-authenticated absence. Its byte allowance covers the complete cut certificate, radix proof, immutable locators and canonical records together. It reuses the ordinary bounded effect reader; it neither hydrates the full conversation nor grants effect execution authority.

Archived Full forks require an issuer-authenticated original-cut witness once the parent has retired history; Summary captures the parent's logical head, not the surviving cache tail. `verify_published_child_measured` checks the original canonical parent publication and child binding for either policy without a new publication lease or rebinding. ROOT binding uses the original admitted task owner, protected scope issuer, exact bind operation/intent and atomic owner/conversation fence; only a verified canonical binding can authorize its cached audience. Children instead use their sealed creation authority and canonical causal binding.

`TaskContext::group` provides typed `spawn_many`, `map`, `join`, `as_completed`, `race`, `first_success`, `quorum`, and `ordered_reduce` for local recursive work. `map` admits every input before awaiting results and returns `RuntimeMap::Incomplete` with one admission or terminal outcome per input if any member fails. It preserves partial successes and uncertainty rather than presenting an incomplete batch as successful. For durable descendants, `group_with_id` plus `spawn_batch(definition, Batch::new(id, inputs))` first retains an immutable ref-only request manifest, then admits stable indexed child operations. `reconcile_batch_id(definition, id)` loads that retained request after process loss without reconstructing its input vector or submitting missing work; `spawn_batch` with the same request fills only confirmed-absent slots. A group can select `with_batch_policy(BatchGroupPolicy::CancelOnFailure)` only when its spawner provides owner-retained batch cancellation; the pinned policy triggers that owner action on the first definitive failure. `cancel_batch_id` makes the same owner request explicitly. The owner records one cancellation declaration before requesting accepted members, rejects later unadmitted members, and returns per-slot request status without claiming a terminal outcome. `cancel_local` affects only live process-local members. `AdmissionBatch::join` returns a `BatchOutcome` that preserves rejection and admission uncertainty separately from terminal task outcomes, reports uncertain cancellation explicitly, and marks unresolved batches incomplete. Process-local `spawn` remains unavailable inside a durable parent.

`HarnessBuilder::artifacts` binds an independent immutable artifact provider using the same owner-authenticated, digest-verified file contract as `content`. `TaskContext::stage_artifact` and `read_artifact` keep artifact publication and resolution separate from conversation/workspace files; a file ref alone still grants no read or write authority.

`HarnessBuilder::execution` and `Bindings::execution` select a qualified durable route with one exact spawner/state pair. Admission retains the provider identity, immutable task build, optional sandbox, and readiness commitment outside ref-only history; the state owner must attest that exact record. `AgentHarness::scoped_execution` selects another provider only for newly admitted work under narrowed authority, while already admitted tasks retain their route. A live `TaskContext` can select a scoped execution route for descendants; a resumable task must record such a transition as a durable effect. Live closures are rejected on an execution route: a remote task must be a registered resumable build admitted with a stable operation ID. The stock local durable host rejects remote placements rather than silently executing them locally. See the Machines adapter for ready-image qualification and explicit access preflight.

`StreamAggregate::prepare_fork` requests an exact parent-revision capture through a `ForkPreparer`. The immutable `ForkPreparation` in that request fixes both child volume identities and the bounded inherited prefix before any provider work; the report cannot substitute another allocation. `reconcile_fork` supplies the entire original request, so an operation ID alone cannot retrieve a differently selected fork. `spawn_from_report` publishes a fully captured report under parent authority before binding the distinct child conversation. Repeating the same report reconciles either lost acknowledgement without minting another child. Providers must still implement the selected resource captures; this is not a cross-provider atomic snapshot unless the report carries a provider-attested boundary.

`HarnessBuilder::fork_preparer` also exposes that capture/reconciliation boundary through `HarnessBundle`, `AgentHarness`, and live `TaskContext`. The binding pins one exact parent conversation revision and requires `fork:publish` in the effective scope; `at_parent_snapshot` creates a new immutable view after the parent advances without retargeting admitted tasks. Reconciliation retains the exact full request. It does not publish or bind a child: that remains the parent aggregate's authenticated operation. A durable task must record fork capture as an effect instead of dispatching it as hidden I/O.

`ForkCaptureProvider` is the replaceable boundary for non-Filesystem selections. The Filesystem preparer journals the parent request and each selected provider attempt/result. After a started attempt loses its reply, it calls `reconcile` under the same operation identity and never blindly repeats an effect; an unobserved result stays indeterminate. The Machines adapter captures forkable checkpoints, and the Objects adapter captures exact version-pinned artifacts without loading their bodies. Publication still requires the matching `ForkSeedVerifier` for every selected provider.

Model projection preserves the full ordered attachment list in canonical storage and uses the caller-selected attachment allowance. A caller or provider adapter may explicitly select a narrower projection; omitted references receive an explicit count marker without mutating history. There is no fixed 1,022-reference policy ceiling.

Use the `filesystem` module (feature `filesystem`) for mutable volumes, forks, project merge, and execution journals, or the `objects` module (feature `objects`) for immutable owner-bound content and attachment manifests. Both supply the same provider-neutral `ContentBindings`; each is an optional feature, so Harness core pulls neither provider in by default. The `machines` feature adds the Machines checkpoint and execution adapter, and `grpc` adds the tonic server for the wire API. For file-only providers, `ContentForkVerifier` proves exact file residency; `CompositeForkVerifier` reads every selected manifest from its owner and checks all cross-provider members and grants before fork publication. Resource captures still require a provider-specific revision verifier. The [custom executor example](https://github.com/acyclic-labs/sdk/blob/main/rust/crates/harness/examples/custom_executor.rs) shows replacement composition. The wire contract is `acyclic.harness.v2`; v1 histories are not read or migrated.

Stock model dispatch now takes `PreparedModelRequest`, exposing read-only
`request()`, `bytes()` and `manifest()` views. Admission validates the complete
ordered exchange against pinned model/tool bindings, optional caller-selected output budgets
and independent input limits. The execution journal retains the exact request
artifact and rechecks it before replay or reconciliation. TypeScript providers
receive the same Rust-admitted bytes as `serializedInput`.

For model-visible fork inheritance, bind an owner-authenticated reader with
`StockExecutor::with_inherited_prefix`. After the fork-planning tool completes,
`ParentProjectController::stage_completed_model_prefix` freezes its retained
request and completed exchange into a parent-owned immutable segment. Publish
the returned reference through the authoritative fork before activating children.
Subsequent segments retain only the added messages and the pinned direct parent;
children append their explicit local context after the reconstructed prefix.
The TypeScript builder's `inheritedModelPrefix` uses its owner WASM core, signed
scope and captured exact-reference byte map to invoke the same Rust constructor.
Ancestor files require explicit owner approvals; a parent cannot delegate an
unrelated owner's private file. See the [contract and verification map](docs/model-request-properties.md)
for assumptions, negative controls and the boundary with agent composition.

Native preparation requires a host-preallocated `NativePreparationRoot` with explicit
witnessed root, operation directory, target and backup identities. Prepare, inspect
and cleanup reuse the materializer journal and its exact qualified source intent;
recovery does not adopt a PID or fabricate source authority. Caller grants must still
be fresh when used. Root replacement is rejected and unrelated user data is preserved.
Authenticated history effect queries use
`HistoryReader.effect(cursor, effect, HistoryReadLimits)`, with qualified identities
and finite read limits. Native process receipts expose queryable publication
diagnostics, destination/completed state and measured work; a stopped invocation
retains cleanup outcome and its original effect identity rather than launching again.
These adapters are not a sandbox and do not promise cross-volume atomic publication.
OS cooperation is a liveness assumption: capture, observation and cleanup bounds are
not a hard wall-clock guarantee for an unresponsive operating system.
Bounded vector-slot collections and sorted native-health registries centralize
bounded state in the canonical owners, not a second execution engine.

### Durable mailbox steering and subtree cancellation

The retained task topology supplies the default parent↔direct-child mail route;
it never grants file access. Both admissions must authorize the exact pinned
payload and its size. Other routes require the sender's `mail:send` and the
recipient's `mail:read`. `CoordinatorTaskHost::send_owned` and WASM `send` return
`MailSendAck` with the exact sender, recipient, operation and payload; a lost
reply can be recovered by the same fenced send. Changing the recipient or file
under that identity is rejected.

`FilesystemTaskRuntime::task_context(task, fence)` binds `TaskContext::send`
and `inbox` to that same retained mailbox owner; stock workers and tool/model
contexts receive it automatically. Unbound contexts cannot fall back to an
unfenced state-provider send, and an attenuated view cannot reuse the original
mailbox token under changed grants or file limits.

`filesystem::MailboxModelMachine` receives one item through a sole
`MailReceiveTaskCommand`, then commits that item and its one-based cursor in the
workflow checkpoint before emitting the pinned model command. Its input has
`receive` and `model` WorkflowCommands; the model payload sets
`ModelTaskCommand::mailbox_operation` to the receive operation. Stock dispatch
checks the original receive cursor and immutable inbox receipt, then adds the
file using bounded-full projection before the **new** model admission. Restart
uses that checkpoint, not the latest inbox tail; an already admitted model
request is never edited. Register the machine in the ordinary task and machine
registries. A receive command cannot share an outbox with another command.

`TaskCancelCommand` and Rust/WASM `cancel_child_owned`/`cancelChild` request a
direct child's complete subtree through the existing recursive scheduler
intent. They require the parent's retained lease and `operation:cancel`, and
bind the cancellation operation to the exact child across retries and restart.
A successful request does not prove provider stop or release an uncertain
effect's reservation; reconciliation still uses the original effect journals.
