# Tool composition: slice D

This working record starts from main `6aaed7e3a49a609c8a62790356ce94588d2e44f5`.
It records implemented primitives and remaining acceptance obligations separately.
It is not a final qualification receipt.

`ToolRegistry::remove_from_model` removes a logical tool from future catalogs
while retaining immutable revisions. Explicit version lookup still works for
admitted requests. Registering another revision does not restore a removed tool;
the consumer explicitly selects a revision to restore it. Duplicate registrations
and ambiguous visible revisions retain their existing rejection behavior.

`tool::edit::exact_replace` is a portable pure transformation. It rejects empty,
missing and ambiguous needles, including overlapping occurrences; preserves exact
UTF-8 and line endings outside the replacement; and checks input and output byte
bounds before allocating its output. It supplies no authority and performs no
publication. The owning adapter must read a pinned generation and commit through
the existing Filesystem transaction at that generation.

`FilesystemHost::edit_text` now does that publication through
`FilesystemHost::put_content_at`. Both generation-checked editing and ordinary
uploads share the existing atomic content/metadata/retry publication path. The
receipt records the expected generation alongside the complete file descriptor;
metadata remains the file descriptor used by pinned discovery. No old receipt
format is accepted as an alternative. Exact retries return the retained result
without modifying a later user edit. The public edit rejects internal storage
and checks the separate source read and destination write grants.
Transformation and generation-checked publication reject zero or `u64::MAX`
allowances; editing validates that bound before reading source bytes.

## Integration cuts still required

`ToolDefinition::output_schema` describes the canonical executor value.
Mandatory `projection_schema` now independently pins the projected JSON value.
Stock execution, completed tool-prefix reconstruction, durable tool execution
and the paired model-request validator use their respective schemas. Both
schemas are compiled and included in the exact definition digest. Canonical
results remain in the existing result artifact; projection changes require a
new admission. Added source tests exercise differing shapes, required field,
schema compilation and changed digest; independent output/projection failures
on dispatch and terminal replay; and wrong-schema retained prefix payloads with
valid canonical encoding and digests. These tests have not been executed on
the recovered source. Generated bindings and installed consumers remain open.

Tools now assemble independently through `Tool { definition, executor, projection }`,
`ToolRegistry::register` and `HarnessBuilder::tool`. The all-vocabulary
`CodingToolHost`/`coding_tools` host and its object-only schema restriction are
removed. Selecting a read tool does not require a shell, PTY, LSP or browser
implementation. Definitions retain their exact names, revisions and compiled
input, canonical output and projection schemas. The updated builder fixture
uses one explicit string-valued tool and preserves original binding checks.

Portable defaults select the owner-bound read/write/exact-edit adapters.
Bounded search, read options and patch editing remain open. Optional execution
consumes the landed PR5 provider and its existing approval, selected multi-volume
view, publication and recovery path; no second execution journal is justified.

## Approved native multimodal scope

The coordinator confirmed this implementation scope on 2026-10-08. The shape
below still requires concrete consumer review and qualification; approval is
not a readiness claim.

Extend the existing ordered `ModelContent::Parts` and verified immutable
`FileRef` path. Keep stored MIME/encoding separate from requested modality intent
and provider options. Typed common intents and versioned schema-validated custom
options must be pinned before admission; unsupported capabilities/options return
an explicit outcome unless the consumer installed an explicit fallback. Do not
substitute OCR, transcription, captions or silently dropped media.

Tool projections select canonical result artifacts into ordered native parts
while retaining the existing tool call identity and pairing. Bytes/digests,
selection, option schema/revision, adapter revision and finite byte/duration/frame
and work limits must be validated. A native representation does not authorize
reading its reference. Preparation/uploads use existing admitted effects and
receipts. Live URLs and streams need immutable capture before admission.

Qualify captured native/WASM mock adapter inputs, including byte bodies and exact
option ordering/values at the actual adapter boundary. SDK reference-envelope
equality alone is insufficient wire evidence. Production model adapters remain
deferred; mock captures establish only the exercised typed adapter contract.
Context retention and compaction integration belong to B; runtime/browser
consumption belongs to I. Their seams require coordination before shared edits.

## Verification ledger

| Invariant | Production mechanism | Assumption | Verification obligation |
|---|---|---|---|
| Exact edit is unambiguous and bounded | Pure UTF-8 exact matcher, checked output size | Finite caller bound; generation-CAS publication remains separate | Overlap/Unicode/line-ending controls and exhaustive binary strings through six characters |
| Exact edit preserves concurrent user edits and retry identity | Shared atomic content publication with expected generation in receipt | Existing Filesystem provider commit and retention guarantees | `portable_text_tools`: memory consumer checks wrong-volume writer, stale precondition, changed retry output/precondition and pinned readback; persistent consumer drops all provider/host handles and reopens the disk store, checks unchanged user head, retained result and invalid-bound rejection |
| Removing a tool retains replay contracts | Existing version map; explicit optional catalog selection | Admitted runtime pins exact definitions | Removal/restoration/new-hidden-version unit scenario; durable journal source scenario rebuilds after catalog removal and requires retained replay with unchanged provider counters; recovered-source tests unexecuted |
| Canonical result differs from projection | Existing result/projection artifacts and independently pinned schemas | Exact definition admitted before effects; generated consumer join still open | Dispatch, terminal replay and retained-prefix source tests added but unexecuted; no qualification claim |
| Native media retains semantic identity | Existing ordered parts and content verification | Validated options/capability adapter still needed | Native/WASM actual mock captures unresolved |

Windows focused tests, platform checks, real Filesystem effects/faults, fresh
generated/installed artifact checks, final simplification audit, owned PR merge
and actual-main verification remain required. The finite matcher enumeration is
a bounded production-function check, not unrestricted proof or a Filesystem
stale-write/recovery test.

Historical Windows worktree checkpoint: `cargo test -p acyclic-harness --features
filesystem-local --test portable_text_tools --lib --locked` passed 241 library
tests and both portable-edit consumers, with zero failures or ignored tests.
Formatting, diff whitespace and current filesystem-local strict lint passed
(`cargo clippy -p acyclic-harness --features filesystem-local --lib --test
portable_text_tools --locked -- -D warnings`). These results cover
memory and a real disk-store reopen, not persistent-provider fault injection,
WASM/browser, fresh-installed-artifact, final-source or landing receipts.

Runtime integration still requires I coordination. The existing `TaskContext`
content writer binds host-authenticated authority separately from narrowed
`RuntimeScope` capability strings. The portable edit executor must use that
owner-authenticated route and its pinned generation rather than treat the
presence of a copied capability string as a new signed write grant.

After I's exact reader review and the coordinator's source-only grant,
`ContentPublisher::stage_at`, the Filesystem implementation and
`TaskContext::stage_file_at` are wired through the shared publication helper.
The generic publisher rejects unsupported generation checks without calling
ordinary staging. The memory wrapper delegates and retains the exact result.
`tool::files` exposes independently assembled read, write and exact-edit tools
using owner-bound task context, exact call identity and pinned-source retry.
Missing context is explicit `Unsupported`. The former memory/default adapters
and stock loop still need their agreed context-consumer replacement; the new
module alone is not a qualified portable-default path. A source regression
counts ordinary writes and requires zero on unsupported generation checks.
These changes are formatted but have not been compiled or tested.

The scoped portable-tool consumer source test now exercises the public edit
executor through real signed Filesystem publisher/reader bindings, preserved
source/result reads, identical retained retry after a user edit, fresh stale
operation rejection, missing read/write/writer authority, a foreign writer with
zero ordinary writes, changed retry bytes and missing context. Its task-state
fixture supplies only the scope-resume boundary; it does not qualify original
task admission, stock default dispatch or persistent provider fault handling.
The existing durable journal test also reconstructs the runner after catalog
removal and requires replay of the exact admitted revision without provider work.
Both added scenarios remain unexecuted until an owned build slot is granted.

Direct user approval on 2026-10-08 resolved the coordination permission gate.
The original checkout was empty and absent from Git's worktree registrations.
Snapshot `235f8c46389a42359b9c72709b4c0175dfc16493`, parent
`6aaed7e3a49a609c8a62790356ce94588d2e44f5`, preserved exactly the six source
files in this checkpoint. Recovery restored the original checkout and applied
the snapshot to the original branch without committing or discarding it. The
snapshot reference remains intact. App attachment failed because the original
path was no longer recognized as managed. Ignored build artifacts were not
recovered. The test counts above are historical, not current-source receipts.

Concrete independent projection contract proposed to I, B and the coordinator:
mandatory `ToolDefinition::projection_schema`, compiled alongside input and
canonical output schemas and included in the existing exact definition digest.
`output_schema` validates only `ToolResult::value`; `projection_schema` validates
the projected JSON value. Existing identity projections declare the same schema
explicitly. There is no default, alias or old-data migration. Keep the existing
`ToolCompleted` canonical-result and projection references and call identity.
Validate both contracts at stock dispatch, reconciliation and retained replay;
durable tool execution must also validate the projection before publication
and on retained replay. Shared runtime readers and generated bindings require
the agreed source join. No PR, completed feature, full platform qualification or
merge is claimed.
