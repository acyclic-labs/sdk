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

`json_value_bounded_trusted(value, JsonBounds, charge)` returns the SAME canonical encoder's actual owned `Value`, without a tree-to-bytes-to-tree roundtrip or unchecked `to_value` prelude. Returned custody covers actual String/Vec capacities and logical map-entry payload, not the stack root header; the concrete owner separately accounts its declared root object. Already-freed temporary claims are not transferred, and failure drops nodes before unconditional release. Direct tree construction emits **zero `Work.bytes`**: its escaped-string/scalar counting writers still enforce the logical encoded-shape bound, but their actual sizing chunks use `Inspect`, not owned-wire encoding or source/backend-read counters. Custom serde internals and opaque allocator/node/RSS effects remain trusted. `json_value_bounded_native_trusted(value, bounds, code, charge)` additionally preserves actual serde sources in `NativeEncode`; SDK static bounds and callback interruptions remain inline. The consumer moves its original preclaimed diagnostic guard into that error, never formats or admits new error memory after denial.

The SDK-private `owned_logical_bytes(&Value)` reuses that capacity audit for an already funded, finite-depth resident tree. It excludes the root `Value` header and opaque map nodes; it performs no admission, cancellation, backend IO, copying or allocation. Concrete consumers account their root header separately and retain the original guard through ownership transfer.

SDK-private native canonical and ordinary byte companions use those SAME original encoder/two-pass engines with an error-carrier flag, preserving exact wire bytes. Their SDK bounds are inline, their genuine serde errors retain `NativeEncode` source/custody, and ordinary/legacy helper signatures and transport remain unchanged. No second encoder or native serialization fallback is installed.

SDK-private `canonical_json_bytes_identity_native_trusted(&Value, code, charge)` preserves the original identity policy exactly (`usize::MAX` bytes/items, `u32::MAX` depth), using the SAME encoder without the finite entry points' depth clamp. It accepts only an already paid trusted resident `Value`; native migration payload budgets and finite admission of first-party untrusted wire remain the caller's obligations. Legacy unlimited identity recursion retains its original call-stack risk: finite work custody does not promise a universal stack or RSS bound. This companion neither reinterprets private-looking map keys nor establishes a new parser or generic custom-serializer proof.

Callback interruptions and closed SDK bounds use inline private serializer errors, retaining the first owned Root cause without allocating a fallback message after denial. Actual SDK reserve rejection moves its typed `TryReserveError`. Standard serde writer/parser `ErrorImpl` boxes remain opaque, not observed allocator operations. `sdk_json_diagnostic_bytes()` is a **closed SDK-generated diagnostic** quota derived from serde_json 1.0.151/Rust 1.98.1 static labels, bounded line/column rendering, overlapping formatter buffers and known carrier ABI; it does not bound custom serializer messages or arbitrary typed unknown-field errors. Serde's nested error conversion strips an existing line/column suffix into structured positions rather than accumulating suffixes. Concrete consumers preclaim their applicable diagnostic quota and preserve that original custody through the error's lifetime; native callback interruption itself requires no diagnostic pre-allocation.

`json_value_decode_owned_bytes(JsonValueShape)` factors the **closed serde_json::Value** parser policy as checked arithmetic over an SDK-issued sealed shape, never a caller-manufactured byte/item assertion. The original lexer streams escaped private RawValue sources through its SAME allocation-free grammar before selected Value parsers can grow, accounting hidden semantic nodes, active Raw payload/scratch/shrink overlap, items and zero-based stack depth. `JsonValueSchema` enumerates actual closed SDK Value field paths and late-tag gates; unrelated ordinary string maps keep their original semantics, and the private key is not blanket-rejected. Unknown generic serde reachability receives a conservative byte-derived Raw heap allowance, **not** a semantic-stack proof. No second source scan, alternate JSON parser or cloned tree is used. Actual typed envelopes add their own audited Content/typed/Box/diagnostic overlap and hold real original quota before serde; no allocator/RSS or arbitrary `DeserializeOwned` theorem is asserted.

For selected Value decoders, `maximum_depth` bounds cumulative zero-based logical JSON ancestry across simultaneously live RawValue parsers, including lexical ancestors of a selected field. A nested serde parser resets its own recursion counter, but its parent visitors are still live; the admission counter does not reset. For example, `{"$serde_json::private::RawValue":"[0]"}` requires depth two even though each individual parser has local depth one. Native selected-Value callers using the default finite ceiling therefore apply it to this aggregate ancestry, not each parser or only the resulting tree. This deliberately conservative boundary is not an exact Rust-stack byte guarantee; generic unknown-schema and literal-clone paths do not acquire selected-Value guarantees.

`JsonCodecCharge` distinguishes byte-producer work, actual node/sizing/virtual inspection, explicit copies, original lexical input consumption, before-growth logical retention and after-free release. `Retain` changes logical quota with **zero allocator operations**. Positive `AllocationOperation { bytes }` immediately precedes a genuine SDK String/Vec reservation; its bytes are the full requested capacity, not a growth delta or opaque allocator footprint. Zero/no-growth reservations and opaque map/serde internals invent no operations. `CopyAdmission` checks the original ledger without spending or creating a receipt; `Copy` spends immediately before an infallible already-reserved copy, with no intervening SDK allocation/failure. If the copy allowance is lost after reservation, the real allocation attempt remains spent, copied bytes stay zero, and capacity drops before release. `Release` is unconditional cleanup after physical drop, including cancellation/fencing; spent work/highwater and the original primary failure are preserved. Returned output and separate schema/adoption guards stay on their original custody until last use. Borrowed input/credentials remain caller-owned; lexical/virtual inspection is not another provider read. Custom serde code executed before callbacks remains trusted, not sandboxed.

`json_from_slice_bounded_trusted(bytes, bounds, maximum_owned_bytes, charge)` preserves original generic lexical bounds and ordinary serde transport. Its `Admission { maximum_owned_bytes, value_shape }` is a conservative **audited concrete-schema** preflight plus parser scratch, not an allocation receipt or generic heap/semantic-stack guarantee. Native closed consumers select their actual Value paths; type owners establish finite fields/replay layers and account retained capacities separately. Legacy unbounded canonical convenience uses the SAME encoder without imposing finite-producer policy.

Native finite typed consumers use `json_from_slice_bounded_native_trusted(bytes, bounds, maximum_owned_bytes, code, value_schema, charge)` (filesystem, non-WASM). It uses the SAME original lexical pass and serde decoder, moving actual serde errors into `NativeDecode { code, source, custody: None }` without `to_string`. `code` retains request `Invalid` versus persisted-record `Storage`; lexical/bounds errors are inline static `NativeBoundary Invalid`. Move the original preclaimed diagnostic allocation into error custody without late cancellation/resizing, and retain successful payload custody through last use. SDK-private borrowed-Value decoding inspects the paid resident tree before genuine `T::deserialize(&Value)`; only audited closed DTOs with equivalent borrowed/owned visitor acceptance may use it. Genuine consuming `from_value` callers keep their original converter and actual map iterator order. Their sealed owned-inspection/additional-quota helper excludes MOVED resident strings, so the original input guard covers those strings while separately admitted NEW Content/typed/Raw growth overlaps conversion. Slice, consuming and borrowed origins are distinct and wrong-origin quota use fails statically. None of these private arithmetic/data helpers grants authority or proves arbitrary serde safe.

SDK-private finite seed companions reuse the SAME decoder in native and portable/WASM modes, with an explicit actual Value schema. Closed manifest seeds preserve their member maximum **before typed next-element decoding/growth**. An original SDK failure remains in its owned sidecar and takes precedence over a static serde marker; no SDK formatting or new SDK error buffer is allowed after denial. Serde's private marker/ErrorImpl allocation remains foreign opaque, not a zero-malloc claim. SDK-private native sizing likewise uses the existing counting writer/lexer without creating a Value or wire buffer: sizing bytes are `Inspect`, never backend reads or owned encoding. Its actual closed source must already have finite resident bounds before serde; an existing Value is inspected with `None` first so a sizing check cannot reinterpret a private Raw key. Neither callback boundary promises a CPU deadline inside opaque custom serde code.

Native host JSON reads reuse that audited preparation `Value` decoder on the original native ledger. Its admitted decoded allowance transfers into the existing history custodian before the value leaves; lexical consumption remains application-source bytes, not encoded output or another backend read. An observer without that guarded decoder rejects before deserialization rather than falling back to unchecked parsing.
Custody absorption borrows the incoming guard. If transfer is rejected, the original guard stays in the typed error until the decoded value or content bytes have physically dropped; rejection never releases a still-live payload's allowance.

`NativeFilesystem` retains the original SDK filesystem error and its existing custody, without an intermediate diagnostic string. `Limits::validate_with_native_work` uses the same numeric predicate as ordinary validation, but checks the original cancellation/allowance and returns an allocation-free native denial.

After possible MCP HTTP dispatch, `NativeMcpHttp` retains the original operation and provider cause; `NativeMcpIncomplete` retains the original operation, EOF decoder cause and any paid diagnostic custody. Both classify as `Indeterminate`, propagate before additional provider work, and never authorize resending the operation. Response-phase lexical failures retain their existing typed cause and custody with contextual uncertainty classification; the actual dispatch owner retains the original attempt identity for reconciliation.

The native-only observer can borrow its exact original ledger and adopt a returned guard from that same ledger. This conveys accounting identity only: scopes, owners, fences and grants still require the original authorization before any native producer call. Ordinary observers expose no native ledger.
Native bounded-reader construction, pinning and pages share that ledger and return values before their custody guards. Canonical output is staged from original source records and charged only when it escapes. A scoped native state loan returns retained deltas to the existing cache owner's last guard before that owner can drop, including when the caller is cancelled.

Content callbacks use one funded future carrier on the original native ledger: the concrete frame is admitted before its builder or box, and the frame guard drops after the future. Native verification returns no owned payload; native reads transfer the original byte-vector guard before returning bytes. Ordinary observed callbacks likewise admit their concrete frame before boxing and release that frame after the await has physically dropped it. Tracked publication paths admit the SDK string payload while the source String remains live; staging consumes the original canonical Vec and releases its captured capacity only after all request and acknowledgment views have dropped.

The append counter records SDK-confirmed append-record acknowledgments, not physical new records. An idempotent append acknowledgment counts; finding the original record after a lost acknowledgment does not manufacture an observed append acknowledgment. Native terminal SDK failures retain the original typed source; the current closed Stream error enum has no dynamic diagnostic payload.

Observed scope clones preserve the original signed fields, and effect attestations use the unchanged canonical signed tuple and keyed hash. Known string copies and original hash inputs are admitted on the same observer; no replacement signature format or extra serialization manufactures work.
Canonical event reads preflight the complete closed Protobuf carrier, then audit the **concrete `EventPayload` graph** before its existing serde decoder runs. Its logical quota includes the four reachable tagged-enum `Content` buffering layers, initial/geometric Vec slots sized from actual declared element types, typed map-entry payloads, heap-owned root boxes, the custom interaction-outcome `Value`/`from_value` overlap, decoded string data, parser scratch and source-bounded diagnostic interpolation. File-reference paths are strings; native paths/process bodies are not part of this schema. The audit anchors serde 1.0.229's actual hidden `Content` type, so a dependency upgrade requires source review. This caller-owned logical quota/highwater reservation spans decode and error formatting, then is replaced by observed retained payload custody without double counting. Guards release after owned buffers are dropped, including cancellation failures. Canonical re-encoding is the original wire validation, never an extra encoding just to manufacture receipt work. This is not an arbitrary serde guarantee and invents no allocator attempts, opaque node footprint or global RSS bound.

Event and Snapshot admissions issue their selected sealed Value shape in that same original source pass. Hidden Raw Value quota extends the existing normal accounting exactly once; ordinary non-Raw quota policy remains unchanged. Snapshot event payloads and retained interaction outcomes use per-container gates, not a global tag or key heuristic.

Checkpoint decoding uses the same lexer and original typed serde decoder with a separate **closed `Snapshot` schema** admission. It reuses the core snapshot clone inventory for collection/map-entry slots and the event audit for tagged buffers, strings, transient `Value`, diagnostics and per-event boxes. The quota additionally covers `ConversationState`'s omitted UUID/position/digest caches and its original per-message canonical history-hash tree/output overlap. It preserves the caller's finite multi-chunk checkpoint byte allowance rather than imposing a single-event cap. One `Admission` must create real logical RAII custody before decoding; failures release after payloads drop, and success adopts actual retained capacities without double counting through snapshot/restore ownership. This admission neither authenticates a snapshot nor replaces authoritative event-anchor/MAC checks. Cancellation is cooperative at codec callbacks; the audited synchronous serde converter is not an arbitrary user-code sandbox. Opaque allocator/node overhead and process RSS remain outside the bound.

The same optional observer covers original coordinator replay, declaration, append/retry recovery and lease release. Scheduler reads admit all seventeen concrete event variants before the original single Serde parse; protobuf, raw JSON and actual decoded-event custody overlap on one ledger. A measured quiescent release uses the original coordinator, execution and protected workflow tails in one atomic commit, including the current pending/suspended release witness rather than an initial workflow tail.

Cold native readonly ownership first authenticates the original task admission and protected preparation record, then selects that record's exact original fence. The private reservation proof authenticates the original declaration scope and canonical coordinator source, requiring one matching full admission and Started origin. It grants no current dispatch/write authority. With no persisted start revision, this is an explicitly finite-quota cold prefix scan to a captured SDK tail—not constant-history lookup—and quota exhaustion or ambiguous origins reject rather than adopting a newer fence or repairing history.

Fenced commit producers construct a private `PreparedCommitRequest` on that original observer. Known condition, mutation and nested-record Vec growth is admitted before allocation; owned request payloads drop before their quota guard. The same commit/recovery engine consumes its borrowed custody without a release/reacquire gap or double retention, and retains a generated retry-key payload through acknowledgment-loss inspection. This witness conveys storage custody, never publication authority.

Checkpoint reads likewise admit the closed `Snapshot` schema, including concrete conversation caches and canonical-digest scratch. One original Serde parse moves a private conversation wire carrier; the original reconstruction then observes its actual message walks, canonical encoder and digest inputs on the same ledger. Its future cache/scratch allowance is released before actual cache construction, whose retained Vec capacities and logical map payloads transfer into snapshot custody. No extra re-encoding manufactures work, and no provider/allocator/RSS footprint is inferred.

Versioned extension events stage JSON in an owner-controlled content provider and append only a verified `FileRef` plus pinned schema, implementation digest, and fork policy. Stream admission checks digest, length, JSON syntax, and the registered schema before publication; replay requires the exact registered implementation and never embeds extension payload bytes. A parent may inherit an extension revision only under `inherit`, capture a distinct provider-owned child revision only under `reset`, and cannot select it under `reject`.

`SchemaRegistry::register_with_dependencies` pins exact extension-version edges before activation. Its deterministic `activation_order(selected)` resolves only the chosen roots and transitive dependencies, rejecting missing versions, version conflicts, and cycles without recursive traversal; installation may be out of order, but an incomplete selection cannot be activated. `register_configured` additionally pins a JSON configuration schema. An agent aggregate accepts `ConfigureExtension` only at a pending/waiting boundary, after provider admission has verified the staged JSON bytes, digest, length, and schema. `SelectExtensions` then requires configuration for every selected version that declares it and records the exact configuration refs alongside its dependency-ordered selection. `Reducer::extension_admission` captures the committed activation event and exact selected configuration refs; scoped tasks cannot retarget that snapshot, and the durable host stores it in its ref-only admission file for recovery. A later configuration or selection does not rewrite earlier admissions. An extension state version switch requires `MigrateExtensionState` from the exact latest retained event; a plain append cannot bypass it. `StreamAggregate::migrate_extension_state` authenticates the source and exact destination write grant before reading, executes a pure, exact-digest `NativeExtensionMigrations` implementation over verified prior bytes, stages the target JSON through an owner-bound volume (which may differ from the inherited source volume), and publishes only its ref. Direct provider admission independently re-executes and compares the migration before Stream CAS, rejecting absent or substituted implementations. The host lifecycle is complete at composition level: `ExtensionRuntime` pins exact executable leases, links native tasks/tools and provider slots through `NativeExtensionBundle`, gates new admissions after `disable`, and retains existing leases for reconciliation.

Effects likewise pin request, successful result, and JSON Schema bytes by `FileRef`. Planning validates the staged schema before admission; resolution re-reads that exact schema and result under the owning provider grant before publication. Stream events retain only refs, operation identities, and digests.

Interactions have one ref-only aggregate history for open requests and versioned outcomes. Prompt/schema and answer JSON are staged as private files and validated by the content provider before admission. Presentation through `InteractionRouter` is separate from the owner-mediated `InteractionResolver`; `HarnessBuilder::interaction_resolver` binds the latter. `AgentHarness::inspect_interaction`, `resolve_answer`, and `resolve_approval` use an authenticated responder scope, exact version, and stable operation identity, returning a typed `ResolutionReceipt` including whether the operation replayed. The Filesystem/Stream adapter validates the signed responder grant before staging bytes, checks the response against the admitted request, and reconstructs the original aggregate CAS revision on a lost-ack retry. The Stream never contains prompt or answer bodies. The execution journal can retain its own effect/wait boundary without copying those bodies.

Scoped tool installation asks a `ToolApprovalVerifier` to read a resolved journal approval for the exact installation operation and tool-definition digest. A caller-constructed approval object is not accepted.

Durable scheduler declarations pin initial process state by `FileRef`, not inline JSON. Opening the Stream-backed coordinator requires an authority-scoped content verifier; it checks declared state and successful result bytes on replay and before new publication. New operations must enter through `declare_operation` with a signed, matching owner scope and `operation:declare`; generic event submission cannot publish a declaration or an orchestration decision. Successful worker outcomes are JSON `FileRef`s. Joins, quorums, and reducers stage their result through an owner-bound `SchedulerPayloadStore` before publishing a ref-only event. Malformed or missing bytes leave no new event and prevent reopening an affected history, while an exact in-process retry reconciles by idempotency key. Placement is bounded exact-match worker label metadata, enforced when leasing, and durable inbox items carry `FileRef` payloads.

The coordinator task host's `*_with_native_work` lifecycle methods use the
caller's original finite ledger for authenticated admission, pull/recovery,
start, current cancellation, passive suspension/wake, retained workflow input,
outcome and settlement. Scalar cancellation reads do not clone operation state.
Returned DTOs carry `NativeAllocation` custody through their last consumer;
drop the DTO before its guard. Native failures propagate before deadline clocks
or cancellation/retry work. Metrics cover visible SDK calls and original record
bytes, not opaque provider allocations or physical I/O. Quiescent suspension
uses the existing execution, scheduler and pending-publication tail CAS.

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
Native consumers share one SDK-issued `NativeTaskContext` for commands,
root-provider reads, and the original-owner content verifier. Sharing consumes
the actual context and its original metadata allocation receipt; an ordinary
context clone or unrelated allocation cannot establish that receipt. Content
callbacks reauthorize the original READ owner and the same sealed context
before and after reading exact bytes. Returned native byte buffers retain
their original allocation guard until the last consumer drops the bytes.
Shared-slot accounting admits the known SDK value and its allocation attempt;
it does not measure allocator overhead, RSS, or private standard-library Arc
backing retained by weak references.
`ToolContext::new_with_native_work` retains that same shared context and
observes its owned call-ID string on the original ledger. Ordinary tool-context
clones preserve the context and policy but invalidate native tool-metadata
funding; allocation membership never replaces original invocation admission.
Native MCP preflight additionally requires the SDK's original current-dispatch
purpose and borrowed task/fence binding. Metadata-only and READ/query contexts
cannot authorize transport dispatch. The preflight performs no provider IO or
allocation and does not replace invocation admission or remote-namespace grants.
Reconciliation preflight checks the same original native metadata and borrowed
owner binding without promoting reconciliation purpose to current dispatch.
Its genuine SDK producer supplies that owner binding after settlement checks;
READ/query contexts have no mailbox. Approved effect authority remains a
separate check.
The SDK stock worker may retain its original per-command journal interaction
router in tool-local metadata without replacing the shared task or its policy.
This override is not a public custom-executor setter; its producer and policy
pipeline must independently verify the original task, owner and fence.
`with_native_scope` monotonically attenuates tool-local grants, content limits
and run bounds before copying. `effective_scope` is the grant/limit facade;
`task()` and `native_task()` still identify the same original issued context.
Derived scope metadata retains original created-resource/extension provenance;
caller scope metadata supplies restrictions, never a replacement issuer or proof.
Stock configuration preflight checks against the current tool-local effective
scope, not merely the wider original task scope, before any candidate allocation.
`with_native_stock_scope` uses stock grants/run restrictions with its independently
configured execution limits, preserving ordinary stock scoping when those limits
differ from the policy scope's limits. Both producers use the same attenuation engine.

`FilesystemForkPreparer::project_creation_operation` exposes the exact derived
Filesystem creation identity for binding an already-created child to its native
overlay. It does not authorize creation or start another fork.

An optional explicit child result volume uses the same original fork, protected
allocation claims and admission proofs. Its mandatory `Result` role creates an
empty, unmounted private volume with READ/WRITE custody; it is never inferred
from private-volume order and never replaces the unique ordinary `Scratch`
writer. `stage_artifact` selects this result binding when present.
`child_scratch_creation_proof` instead retains only the original scratch identity,
sharing the existing provider handle without copying a lease. Measured and
original-ledger native settlement share the same task, agent, exact allocation,
retained writer and current-fenced WRITE checks. Recheck before every namespace
mutation; a READ-only historical owner cannot authorize cleanup, and a native
physical leaf receipt remains separately required.

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

Trusted native bootstrap uses `AuthorityIssuer::verifier_with_native_work`,
`root_with_native_work`, or `root_for_agent_with_native_work` on its original
ledger. These reuse the configured issuer and canonical MAC; they neither create
authority nor infer an agent or capability. The returned metadata custody must
outlive every retained verifier/scope consumer. SDK String/vector growth is
observed before allocation; opaque BTree allocator overhead is not an RSS claim.

Native durable-host callbacks borrow the original lease fence and return a funded
`NativeFutureBundle`. Admission and terminal observations also return custody for
their owned metadata. Custom hosts reject unsupported native callbacks before
allocating a future or starting SDK IO; a native execution preflight does not
authenticate a task or authorize a dispatch.
Native cancellation likewise retains the ordinary retry key and signed owner
authorization on the same observed coordinator engine. Its acknowledgement
requests cancellation; it does not assert process exit or namespace cleanup.

Native model claims use that same current-owner gate, retained admission and
session ceilings, canonical lease digest and original keyed coordinator append.
An uncertain acknowledgement remains the same attempt; reconciliation does not
allocate a new lease. Execution appends and quiescent suspension likewise use
the original optional observer and guarded journal CAS, not a second runtime.

Input READ authorization uses the same file, whole-volume and directory-prefix
capability recipes with or without the SDK-work observer. The observed route
admits canonical digest work and actual capability-key copies before execution;
it does not allocate temporary directory-prefix strings or grant WRITE.

`Capabilities::new_with_native_work` takes borrowed names (including chained
policy/journal iterators), examines duplicates without copying them, and retains
known String slots plus actual UTF-8 capacity on the original ledger.
`RuntimeScope::new_with_native_work` consumes that paid set without cloning it;
its private custody follows the grants, while the returned guard covers the
scope's logical Self slot without claiming a heap allocation. Native run-limit,
session-limit, and limit validation keep ordinary wire codes and authority
attenuation, with cancellation and static denial text. Drop each returned value
before its guard.
`RuntimeScope::clone_with_native_work` and `narrow_with_native_work` retain
concrete proof/extension vector slots and buffers before copying, then reconcile
actual private field capacities. Narrowing consumes already-paid child grants;
immutable configured registries and authenticated created access share existing
Arcs without minting authority or a created-resource seal. Ordinary `Clone`
preserves policy but does not propagate native metadata origin.
Borrowed native narrowing/run-limit validators apply those same attenuation
checks without cloning a context; metadata custody and extension pinning are not
issuer authentication.

Native scheduler validation borrows the current projection for every original
event variant, including cancellation acknowledgement through
`Completed(Outcome::Cancelled)`. Ordinary and native application share that
validator and mutation body; preflight does not clone a shadow scheduler.
Cached declaration, reservation, checkpoint, workflow-input and outcome
metadata retain their original per-field allocation custody after query IO
drops. Replacing a field drops its old payload before releasing that custody.
Capacity snapshots and work leases clone only the metadata they return;
logical SDK tuple/String/Vec ownership excludes opaque BTree allocator nodes.
Session limits use the same scalar validator for ordinary and native reads;
root lookup walks borrowed parent links with observed, cycle-bounded lookups,
without allocating a lineage set or formatting a native missing-task denial.

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
`NativeProcessProvider::new_with_native_work` uses the ordinary binding engine with
the prepared view's original ledger: request/schema reads, closed DTO decoding,
schema validation, approvals and provider-owned Arc payloads retain that custody.
Its native entry borrows the plan; the admitted future copies closed metadata
through the existing measured String/FileRef producer, never a caller-side clone.
Legacy views and different ledger families are rejected before binding I/O;
ledger identity is membership, not a replacement for current owner or grants.
`NativeProcessRequest::approval_digest_with_native_work` uses the same canonical
approval tuple and hash recipe with original-ledger encoding and hash admission.
`into_arc_with_native_work` keeps registry ownership in the provider's last guard.
`EffectProvider::dispatch_with_native_work` admits the actual launch future before
construction and threads the caller's original work ledger through authenticated
history, protected receipt encoding/appends, and the original process/view engine.
Receipt paths use the original-meter intrinsic `StreamPath` constructor; append
aliases that guarded path instead of allocating a second String-to-Arc copy.
Operation-owned verifier and scope data move with
`NativeProcessAuthority::authority_custody`, which drops after its capabilities.
Sharing the authority retains that original claim even when a bound effect or
provider reference retires; a pre-existing ordinary service authority owns no
operation claim. Custody is accounting, not a grant or approval.
The supervisor retains that ledger, original cancellation and cached payload custody
after caller or registry/provider drop. The shared `NativeFutureBundle` drops its
physical future before releasing the original frame guard; no second boxed adapter
or replacement allowance is constructed.
Successful dispatch and `reconcile_with_native_work` move the complete effect payload
with its existing allocation guard; reconciliation never creates a view or launches
again. A denial after the launch receipt leaves that same attempt indeterminate.
Counters describe observed SDK operations and logical owned payload custody, not
provider-internal allocations, command/runtime heap internals, or process RSS.
The shared boxed process engine admits concrete stdout, stderr and input buffers
before spawning, polls the original held view cancellation and authenticated owner,
and retains an unresolved original process tree for warm settlement.
Actual mounted-source namespace scopes remain with that tree through terminal
process and stdio quiescence, including caller-future drop; materialized-only views
open no kernel mount scope. Namespace accounting does not classify callbacks by PID
or thread and does not confer task, path, or volume authority.
Cold receipts never adopt process control. Every finite publication failure retains
the original typed cause and cleanup ownership, completed destinations and staged
diagnostic in a preadmitted error context; formatting never replaces that cause.
Unfunded observation stays pending or uncertain rather than claiming a recorded result.
`NativeProcessResult::decode_with_native_work` reads pinned result bytes through
the same closed-record codec and retains the actual stdout, stderr and diagnostic
capacities on the original ledger. Its source-byte custody remains with the caller;
`mcp_response_with_native_work` returns the decoded protocol response with its guard.
`NativeProcessRequest::decode_with_native_work` uses that same decoder with a closed
profile selecting only `mcp_stdio.params` as arbitrary JSON. It retains the prepaid
opaque decode reservation for its path/map/resource graph, not allocator or RSS
estimates; the caller separately retains the pinned source bytes.
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
