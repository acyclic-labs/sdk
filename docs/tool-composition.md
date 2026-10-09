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
TypeScript consumers with actual main `e219fd244f33`, including PR314's
producer merge `4bba8f3b0dc5` and D's PR319 native producer landing. Earlier development receipts
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

PR314 released the bounded JSON writer, borrowed model parts and provider context
capacity/token-count contracts. Frozen full-D source `725597de5bfe` compiled on
Linux: 288 library tests passed and three failed. Two failures were stale
canonical fixture/assertion expectations, now corrected in the owned native
producer. The third exposed a production default execution gap: `LocalHarness`
runs the stock executor without the admitted original `TaskContext` required by
the portable tools. I owns the shared authenticated context/admission producer;
D will adapt this consumer after that qualified core lands. The existing default
path and its test remain required. A fabricated context or a new operation per
retry would violate the original admission and is not a remedy.

All seven frozen-725597 portable/native integration tests passed separately,
including disk reopen, actual admitted tasks, native body capture, corrupt/missing
media and lost-response reconciliation. A fresh Windows WASM build of that source
also passed. These results apply to their named snapshot; later source and
nullable/readonly declaration fixes still require fresh qualification. The local
TypeScript affected suite reached 114 passes with one stale positive prefix-schema
mismatch, which has since been corrected. This is not a final package pass.

The browser runtime feature remains dependent on I's qualified main landing.
B's full cold-open, compaction and Summary integration also remains open. The
schema generator's resolved dependencies are locked.

The owned continuation regenerated full-D Harness WASM/declarations from Rust,
including nullable native configuration and readonly projected parts, and built
the actual Objects WASM dependency. The file-tool fixture is produced by the
native executable and repository renderer. Current Harness/Pi package builds,
both type-contract suites, and all 269 source tests pass. Production snapshot
`454479f33` passes all 13 named portable/native/replay integration tests. The
focused tool/schema suite passed 23 tests on production-equivalent `2e3d3d3b6`;
the following change only consumes an owned definition in the fixture example.
Strict full-D library/test/example lint passed on frozen `e7abd6346`. A fresh
installed package also passed all 51 tests, native consumer types and package
metadata/export checks. These are qualified subsets, not a pass
for the default LocalHarness context path or the complete goal.

PR319 head `0cd892ed834acbefdeac5834e46478d5814bc553` independently passed
required SDK Qualification run 37963332851, Linux 326 tests and strict lint,
Windows 304 tests, WASM build/strict lint, 267 source tests and type checks,
and 51 fresh installed-package tests plus consumer types/exports. After the
user completed ivar authentication, the same frozen source passed all 326 Mac
arm64 tests and strict lint. PR319 merged directly to main at
`e219fd244f330be36bb275fd748924c9f5849ac4`; a fresh fetch verified all 48 owned
file blobs match the qualified head with zero mismatches.

The full D branch integrates that actual main landing via `12a131b2c333`.
Squash-history conflicts were resolved while preserving portable original-context
execution, exact envelope byte bounds and stronger replay controls. Frozen
post-integration source matched all 1638 archive files. Both boundary tests,
all 13 portable/native/replay tests and strict library/test/example lint passed.
The default LocalHarness path still requires the qualified shared original
TaskContext producer and D consumer adaptation; these subset passes do not
substitute for that integration.

The default portable consumer adaptation, browser and full context/compaction
integration, combined final platform/adversarial and installed-artifact checks,
final simplification review, required exact-head CI and owned main landing
remain open. No full D goal completion is claimed.

An independent replay audit found that bounded range/search tools accepted
scope-only authorization, while fresh execution required the original admitted
call context. Production fix `00b41f142` shares one original-context preflight
between execution and retained replay; scope-only authorization now rejects.
The real admitted/reopened regression verifies wrong operation/call rejection
before journal IO and unchanged canonical results with one execution. Frozen
`a611d1545` matched all 1638 archive files and passed three text units, both
portable replay/text-variant integration tests and strict library/test/example
lint. Its complete library suite reports 291 passes and one failure: the
existing default LocalHarness original-context consumer gap. This failure is
preserved and remains a required integration gate.

The replay fix also passed a fresh production WASM rebuild. Following test-only
and documentation commits, source `e0bc68d76` passed both package builds/type
contracts, all 269 Harness/Pi source tests and 51 fresh installed-package tests,
consumer types and metadata/export validation. The local workspace dependencies
were restored with the unchanged frozen lockfile; no manifests or lockfiles
changed. These artifact passes do not waive the default-context failure above.

### Qualified B foundation integration (PR323)

D now integrates actual main `93853ede4b7a7ac0ac6c8c81b82da342a2b5e317`,
which contains B's qualified indexed history, admitted context selection,
checkpoints, explicit fork policies and bounded four-record Summary foundation.
Both independent memory-storage test blocks are retained; the combined WASM
surface is regenerated from actual Rust rather than selecting one declaration
side of the merge.

This foundation does not supply the original admitted `TaskContext` to
`LocalHarness.run -> MemoryHarnessStorage.run_conversation -> HarnessBundle.run`.
That public default path still calls the executor without original task context;
its portable file-tool regression remains required and must not be replaced by
the separate durable-task adapter. I's original-context/browser/resource producer
handoffs and B's bounded default cold hydration/lifetime work remain open.
PR324's earlier required SDK run `37975596996` failed at this exact default-path
gate (1,485 passed / one failed before fail-fast); its TypeScript and policy lanes
passed. Final combined qualification and a fresh exact-head required SDK success
are still required before D merges into main.
