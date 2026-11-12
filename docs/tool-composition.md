# Tool composition: slice D

This record describes the current D source and its qualification obligations.
Source implementation, executed evidence, and verified main landing are distinct.

## Independent tools and retained definitions

Consumers assemble `Tool { definition, executor, projection }` through
`ToolRegistry::register` and `HarnessBuilder::tool`. There is no universal
`CodingToolHost` factory. Selecting a read tool does not require execution,
PTY, LSP, or browser adapters.

`ToolRegistry::remove_from_model` hides a logical tool from future catalogs while
retaining its immutable revisions for admitted replay. Registering a new revision
does not restore a hidden tool. Restoration requires explicit version selection;
ambiguous visible revisions and conflicting registrations are rejected.

`ToolDefinition::output_schema` validates the canonical `ToolResult::value`.
Mandatory `projection_schema` validates the complete `ToolResultContent`
envelope independently. Both schemas are compiled and included in the admitted
definition digest. Dispatch, reconciliation, durable execution, completed-prefix
reconstruction, and retained replay enforce their respective contracts. Canonical
results and model projections retain separate artifacts and the same call identity.
A call the harness refuses (unknown or ungranted tool, schema-invalid arguments, or a
pinned policy `Deny`) or whose admitted execution fails definitively completes with
the harness-authored `ToolResultContent::Error { message }` instead, outside the
tool's projection contract: the model reads why and the turn continues. A recorded
failure carries a `ToolFailureCause` (a stable `ToolErrorClass` plus at most 256
bytes of the error text with control characters removed); the raw error stays in
the `warn` log. Error results in supplied or retained history are admitted by their
shape and paired call identity. Every paired call's binding and argument schema
are checked: a refused call admits only an error, and a bound, valid call admits
either an error or a result meeting its projection schema. Errors carry no
authenticated evidence that a tool ran or that
the harness recorded the failure. A tool's own authority check
(`ToolExecutor::authorize`) still stops the turn, as does a declined approval. Only
an effect left uncertain ends the turn as indeterminate.
Changing the projector or its schema requires new admission; no implicit schema
alias or old-data migration is installed.

## Native JavaScript composition

The stable native catalog includes `code { code }`, backed by embedded QuickJS
on every provider protocol. Native tool declarations remain fixed when dynamic
capabilities change. Ordered `tools.changed` context notices after the active
input contain the current granted tools and their original schemas. Concatenate
each body's `json` string at its UTF-8 byte `offset` until `complete` is true to
reconstruct the JSON array. Fragments fit the admitted per-part render allowance,
including escaping and framing, even when one schema exceeds a part. The batch
retains its position as later steps append calls/results, preserving that prefix.
New turns use the latest bound registry; each program binds a fresh table from
that same snapshot. The turn's admitted tools remain pinned for receipt replay.
Native schemas remain in `tools.bound` history metadata for validating earlier
exchanges, including a parent's calls inherited by a worker whose capabilities
differ. Providers omit these records from model input: current native schemas
are already in the declaration header. Dynamic schemas remain in model-visible
`tools.changed` notices. Successor tables revoke only their own bindings for
later calls without changing the shared prefix.
The admitted request's explicit `compose_tools` setting controls this projection;
stock execution sets it from the bound JavaScript executor. A custom tool named
`code` retains ordinary declarations; its history table does not enable
JavaScript composition.
Requests saved without the composition setting require fresh admission under
the current request schema. Their original bytes and digests remain immutable.
Use it for multi-step reads, filters and loops, for example:

```javascript
const files = await tools.read({ path: "README.md" });
export default files
```

Each currently granted catalog tool is an async function in the frozen `tools`
object, indexed by its exact name. Its `input_schema` is the original JSON schema.
When the host binds a file-program source, other names resolve lazily as described
below; they do not carry an eager `input_schema` property.
Calls use ordinary stock settlement, including schema validation, authority,
approval, hooks, receipts and reconciliation. Refusals throw
`Error("tool execution failed: <reason>")`. Approvals suspend the parent turn;
resumption reruns deterministic JavaScript against the same inner call receipts.
Programs should derive call order and arguments only from their code and tool
results. Ambient time and randomness are absent.

### Hook effect ownership and recovery

`hooks::effects::run_once` requires the task's original execution journal.
Before observing a durable start or outcome, it acquires child-effect ownership
through that journal's existing filesystem operation registry. Another live
attempt at the same effect returns a conflict. A later execution of the same
agent shares this exclusion; changing the enclosing turn does not create a new
effect identity. The journal authenticates its original execution independently
of the hook's event identity, including session-start events shared by turns.

The owner publishes the start marker before running a command, and retains its
claim until the outcome is published. Reopening a completed effect returns its
original outcome. Reopening an unfinished start records the hook's interrupted
outcome and does not run the command again. An arbitrary external process effect
cannot be atomically committed with a journal outcome: interruption may leave
its external result unknown. The interrupted outcome preserves that uncertainty
rather than repeating the effect or claiming successful execution. A missing
original journal grants no execution ownership.

### File-authored tools

An agent authors a tool in its home volume, at
`volume:///home/tools/<name>/program.ts`. The directory name must be one valid
component label. The module's default export is plain data with this versioned
shape; unknown fields are rejected:

```typescript
export default {
  version: 1,
  name: 'echo',
  description: 'Return the supplied JSON',
  inputSchema: {
    type: 'object',
    properties: {text: {type: 'string'}},
    required: ['text'],
    additionalProperties: false,
  },
  entry: {sh: 'main.sh', ps1: 'main.ps1'},
};
```

Each entry is a relative, normalized path inside that program directory, ending
in the corresponding extension. Entries contain UTF-8 source. Supply `sh` for
POSIX targets and `ps1` for Windows; a missing native form refuses the call.
The PowerShell executable comes from the shared native shell selector. Static
manifest imports use the existing pure settings evaluator, with no I/O, clock,
randomness or tool access during evaluation.

Discover names with the ordinary paged directory query, for example
`for await (const entry of list('volume:///home/tools'))`. Read a selected
manifest to discover its schema, then call `await tools.echo({text: 'hello'})`
inside `code`. Listing names does not evaluate manifests, load descriptions or
schemas, or scan sibling programs. Fixed catalog tools take precedence over an
authored tool with the same name. Authored tools add no native model declarations
and do not rewrite the context epoch's cached prefix.
JavaScript's implicit `then` and `toJSON` property probes do not resolve lazy
programs. These properties require an explicit catalog declaration; awaiting or
serializing the namespace otherwise performs no host call.

One invocation reads the selected manifest, its static imports and every declared
entry from one immutable home generation. Its revision covers those observations,
including both native forms. Creating or editing a program therefore affects a
later fresh call; it cannot replace an already selected call. Children discover
programs through their own existing home-volume fork, so later parent edits do
not change a child's inherited cut. No program catalog is copied into a child.

Module reads share the selecting call's original preparation ledger, including
imports. Evaluator cache entries retain their original allocation ownership;
receiving callers fund result copies even on a cache hit. An allocation refusal
keeps its typed operational cause and cannot become a cached semantic failure.
The selected manifest and native script retain their allocation credit through
binding and dispatch by the ordinary tool adapter.

The script receives the validated arguments as UTF-8 JSON on stdin. The source
and executable argv remain separate from this data; quotes, shell syntax and NUL
characters inside JSON string values do not become executable source. The result
is the ordinary shell result, including its exit code and captured output. For
example, `main.sh` may contain `cat`, and `main.ps1` may contain
`[Console]::Out.Write([Console]::In.ReadToEnd())`.

Writing a program grants no execution authority. Calling it requires the host's
canonical `shell` capability and any configured tool ceiling, as well as the
ordinary schema, target, scope and approval checks. The shell retains its
confinement, output capture, hooks and settlement contracts; the authored name
does not bypass a shell-policy denial. A malformed selected manifest or invalid
input refuses that call without evaluating an unrelated sibling.

Before dispatch, the private once-publication retains the canonical invocation
and its original revision separately. Authorization receives the canonical
identity `shell`, including when the model uses a native declaration profile.
Reopening the executor reuses this selection without rereading program files.
Changing a call's authored name or arguments while reusing its operation identity
is a conflict. Ordinary shell receipts and reconciliation govern effects; a
selection record alone is not evidence that the process ran or completed.

Programs are ES modules with top-level `await`. The model receives the awaited
default export, or `{ error }` for an uncaught
exception. The built-in `acyclic` module provides `read`, `write`, `remove`
and lazy directory `list` through the same granted tools. Each returned file
retains one immutable selection; `text`, `json` and `lines` read its retained
text. Authoring declarations live at `sys/types/index.d.ts`; their configuration
records come from the kernel’s declaration producer. `Promise.all` submits independent calls in one program; bounded host
credit feeds them to ordinary settlement. Each settled call publishes a
`code.call` JSON log under the turn's existing `tools/.results/` directory before
its promise resolves. Follow that directory to stream tool name, success/failure
and input/output JSON byte counts, associated with the parent call and ordinal.
`console.log(...values)` publishes a `code.log` record there, with a JSON `values`
array, the parent call ID and a separate log ordinal. Await its promise to observe
publication before continuing. Unawaited logs, including logs created while
encoding the result or formatting an exception, retire before the program returns.
Replay reuses each immutable log's identity; logging does not change inner tool
call identities. Log values follow JSON encoding, including its errors for cycles
and BigInt. Console lines also accompany the model-visible result as
`{ value, logs }` while they fit its render budget; `logs_omitted` reports
lines whose presentation does not fit. Their complete records remain in files.
Each result uses the caller's admitted per-part render budget;
oversized final values return `{ error: "JavaScript result exceeds admitted output" }`.
Input, history and other results retain their own per-part allowances. The whole request passes
ordinary verified token admission, without an additional aggregate JSON limit.
Intermediate tool results remain inside the interpreter and ordinary journal
artifacts; they are not added to model history.

The interpreter resolves the built-in `acyclic` module. It exposes filesystem,
network and environment access through granted tools alone. Credentials and tool contexts stay in the stock dispatcher. QuickJS
allocations use the original native memory ledger. A 1 GiB engine memory ceiling
and a billion interrupt checks protect against runaway scripts; neither bounds
product counts. Cancellation follows the parent's event and interrupts executing
bytecode, including infinite loops. Outstanding tool calls settle through their
ordinary cancellation contracts before the composition retires.
CPU polls share the native blocking pool; waiting on a tool releases the thread.
If a program still awaits a promise after all JavaScript jobs and bound tool calls
retire, it returns an error; settled call logs remain in files. With no other host event
sources, that promise has no remaining producer; no timeout or periodic check is
needed to detect it.

## Portable file operations

`tool::files::{read_file,write_file,edit_file}` individually assemble owner-bound
adapters at revision `portable-5`. They require the original `ToolContext`, exact
call/operation identity, effective task bounds, public paths, and appropriate
read/publication authority before effects or replay. Plain context-free execution
returns an explicit unsupported outcome. Capability strings and copied references
cannot manufacture the original publisher's authority.

The reader returns exact bounded UTF-8. The writer uses `TaskContext::stage_file_once`.
Exact editing reads an immutable source, calls `tool::edit::exact_replace`, and
publishes through `TaskContext::stage_file_at`. The matcher rejects empty, missing,
and ambiguous needles, including overlapping occurrences, and checks input/output
bounds before output allocation. Unchanged UTF-8 and line endings remain exact.

`FilesystemHost::edit_text` reuses that matcher and `FilesystemHost::put_content_at`.
The existing atomic publication receipt pins the expected generation and complete
output identity. Exact retries return retained results without overwriting a later
user edit. Changed retry bytes/preconditions and stale fresh operations conflict.
Unsupported generation-aware publishers reject before ordinary staging. There is
no separate receipt ledger or merge engine.

`tool::files::patch_file(work,hunks)` selects optional revision `portable-patch-4`.
Its pure `tool::patch::apply_update` accepts bounded V4A update-file fragments:
ordered `@@` hunks, exact context/add/delete lines, optional exact anchors and EOF.
It rejects missing/ambiguous context, unknown syntax and out-of-order changes.
Source/diff/output bytes, comparisons and hunk count are finite. Unchanged bytes
retain their line endings; additions use the first source style, otherwise LF.
This single-file variant uses the same generation-aware publication path and does
not implement fuzzy matching, live-head rebasing or a complete multi-file protocol.

## Bounded text and replaceable projections

`tool::text_files::read_file_range(ReadOptions,maximum_result_bytes,ProjectionMode)`
selects an exact UTF-8 interval from one pinned source.
`search_file(SearchOptions,maximum_result_bytes,ProjectionMode)` performs bounded
case-sensitive literal search, including overlaps. Consumers compose multiple
sources using existing task primitives. The revision is `portable-text-1`.

Preflight checks the original read grant, public path, source/result ceilings,
range/query limits and retained-position storage. The authenticated reader verifies
bytes; pure helpers enforce UTF-8 boundaries and actual comparison work. Exhaustion
is an error. Canonical results retain source identity and explicit omission counts;
result bounds reject rather than silently change requested semantics. Configuration,
result ceiling and projection mode participate in the definition schema digest.

`ProjectionMode::Full` emits a typed JSON envelope. `Reference` emits a summary and
original immutable file reference, identifying details omitted from the projection
and distinguishing them from omissions in the canonical result. It grants no read
authority. `ReadProjection`, `SearchProjection`, `ReadFileProjection`, and
`FileResultProjection` are replaceable through the ordinary tool components:

```rust,ignore
use acyclic_harness::tool::{files, schema::ProjectionMode};
use std::sync::Arc;
let mut tool = files::write_file()?;
let projection = files::FileResultProjection(ProjectionMode::Reference);
tool.definition.projection_schema = projection.schema()?;
tool.projection = Arc::new(projection);
tool.definition.validate()?;
```

Native argument/result types generate complete draft-2020-12 root schemas through
`tool::schema::{input,output,json_projection}`. Nested definitions resolve from the
complete envelope root. Schemars is pinned and locked at 1.2.2 because schemas enter
immutable admission digests. Tsify supplies TypeScript file-tool shapes from Rust;
custom deserializers, task limits and authenticated grants remain authoritative.

## Native multimodal contract

Ordered model/tool-result parts retain immutable `FileRef` identity and explicit
projection policy. Stored MIME is separate from common image/audio/video/document
intent. Finite byte/work/duration/frame/page ceilings are explicit. Optional native
configuration claims pin the original admission event, extension schema/version,
immutable JSON content and linked implementation digest. Claims alone grant no
authority. The complete request is checked before any media/options are read.

Unsupported adapters/options fail explicitly. Production native model adapters are
deferred; the fixture codec must capture actual bodies, intent, option ordering and
values at the adapter boundary. Reference-envelope equality is insufficient.
Preparation/replay uses existing admitted effects and receipts. Context retention
and compaction integrate through B; browser/runtime consumption integrates through I.

## Verification obligations

| Invariant | Authored coverage | Evidence still required |
|---|---|---|
| Exact matching is bounded and unambiguous | Unicode/line-ending/overlap controls and bounded exhaustive matcher enumeration | Final-source execution; bounded enumeration is not unrestricted proof |
| Publication preserves generations and retry identity | `portable_text_tools`, `portable_file_replay`: wrong authority, stale writes, changed retries, disk reopen and retained recovery | Combined durable admission, cancellation/fault and platform runs |
| Bounded variants preserve canonical/projection separation | `portable_text_variants`, generated schema fixtures, independent schema rejection and catalog replay fixtures | Fresh producer, consumer and installed-artifact runs |
| Native media/options retain original authority and semantic identity | `native_media_boundary`: actual fixture captures, finite-work checks, corrupt/missing media, lost response, retained receipt and reopen scenarios | Final native/WASM/browser execution with actual dependencies |
| TypeScript cannot authorize claims through file reads | Inventory ordering/revision preservation and rejection before a custom provider runs | Fresh WASM/declarations and complete package tests |

Historical experiments and earlier platform passes are preserved in their source
snapshots and ignored qualification receipts. They do not qualify the current
combined source. No build slot or coordination permission approval is outstanding.

## Current integration and simplification

The portable-tool consumer now integrates PR331's qualified main landing
f0a72b86199872412fa74873d1bf16e1c8efa753. Default stock turns use ordinary
task admission with their original operation and TaskContext; the existing
LocalHarness default-file regression is the required consumer check.
Custom executors retain their separate entry point and do not acquire a stock
task implicitly. Durable task execution continues to use its original host,
journal and context, rather than a parallel admission mechanism.

The source removes the capture-based CodingToolHost factory and local
staging/reader wrappers. Portable tools reuse the original task's reader,
publisher, generation check and publication receipt. Exact editing shares one
pure matcher; patching adds a bounded parser over the same publication path.
Borrowed JSON decoding is shared by file/text tools and projection validation.
Rust produces file-tool types and schemas, avoiding duplicate TypeScript shapes.

Authorization at retained replay and at provider IO serves distinct boundaries:
replay must check the original call before journal reads; a direct provider entry
must validate its own authority. Canonical results and replaceable projections
remain separate because the durable result must survive changes in model rendering.
These are necessary boundaries, not duplicate execution paths.

PR319's native contract and PR325's lease correction have independently verified
main landings. The portable/default-tools PR324 passed its exact-source affected checks and
required SDK Qualification, then landed at 1e481d413 with all 34 owned blobs verified.
Full D browser/context/compaction integration remains dependent on qualified I/B
producer contracts. Historical source snapshots and failed qualification logs are
preserved in ignored evidence; earlier passes do not qualify this combined source.

## Explicit conversion consumer

`rust/crates/harness/examples/explicit-image-conversion.rs` assembles an ordinary
`Tool` that explicitly transforms the IMG1 fixture pixels with a selected mask.
This is a test codec example; production conversion/model adapters remain deferred.
The native media policy itself performs no conversion. The tool definition pins
its codec revision, typed source/options/result schemas, and 4096-byte/work bound.
The canonical result retains the original source, verified derived ref and mask;
the replaceable projection sends only the derived file through ordinary native
`Parts`.

The executor reuses portable source/destination authorization and the original
TaskContext reader/publisher. `stage_file_once` binds publication to the admitted
conversion operation. Pure bounded recomputation during reconciliation reuses that
receipt; no new effect ID, attachment engine or conversion journal is introduced.
The existing native boundary scenario composes this public example with a real
admitted task and tool journal, rejects changed options on replay, retains identical
source/derived refs over composition reopen, and captures the actual derived bytes
at the model boundary after a lost model response. Memory-backed reopen remains
narrower than OS process restart or default fork/compaction; those and ordinary
browser runtime consumption still require the qualified I/B producer contracts.
