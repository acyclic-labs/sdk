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
Changing the projector or its schema requires new admission; no implicit schema
alias or old-data migration is installed.

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

## Current source checkpoint (2026-10-09)

The full D branch now combines its portable file tools, bounded text variants,
canonical/projection split, original native option admission, and current
TypeScript consumers with actual main `f80ad5d25c`. Earlier development receipts
above describe their named source snapshots; they do not qualify this combined
source. PR304's dispatch fix landed independently at `98c272e8d2`.

The source simplification audit removed the capture-based `CodingToolHost`
factory and local staging/reader wrappers. Portable tools use the existing
owner-bound `TaskContext` reader, publisher, generation check, and publication
receipt. Exact editing reuses the pure matcher; patching adds only its bounded
parser/matcher and uses the same publication path. Borrowed JSON deserialization
is shared by file/text tools and projection validation. Selected context and
model dispatch share the TypeScript model-content conversion. File-tool types
and schemas are produced from Rust rather than maintained in parallel.

Authorization at durable replay and again at provider IO remains deliberate:
replay must not bypass original authority, and direct provider entry points must
validate their own boundary. Canonical results and independently selected model
projections remain separate artifacts. Neither an option claim nor a read grant
proves original extension admission. Rust verifies the original admission,
registered schema and linked implementation before reading media/options. The
local TypeScript composition has no such registry/runtime, so its Rust-produced
`modelContentInventory` causes claimed native options to fail before hydration
or custom-provider generation. Plain native inputs remain subject to the
selected provider's explicit capability checks; production native adapters are
deferred.

A frozen Linux `--lib --features filesystem --no-run` compile of source
`36f33fea4012bf8b1525ee39002d133dd3b7858a` eliminated the earlier duplicate fixture
and import errors, but failed on prerequisites absent from actual main:
`contract::validate_json_byte_bound`, borrowed `ModelContent::parts`, and
`ModelProvider::{context_capacity,count_tokens}` with their context result types.
Related borrowed-content type diagnostics remain unresolved until that producer
lands. The full browser qualification also needs the runtime owner's browser
feature. This is compiler diagnostic evidence, not executed semantic tests or a
platform pass. The schema generator's resolved dependencies are now locked.

Fresh generated WASM/declarations, combined native/browser/adversarial tests,
installed-artifact checks, the final simplification review, required exact-head
CI and an owned main merge with verified landing remain open. No full D goal
completion is claimed.
