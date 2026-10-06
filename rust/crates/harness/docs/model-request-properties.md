# Model request and immutable prefix contracts

This change extends the existing stock execution journal, conversation fork
controller and content verifier. It adds no scheduler or provider adapter.
Tests are implementation evidence, not an unrestricted correctness proof.

| Invariant | Mechanism | Assumptions | Evidence |
| --- | --- | --- | --- |
| A provider receives one admitted request and the exact canonical bytes that identify it. | Private `PreparedModelRequest` fields; native providers accept only that value; WASM constructs and decodes the same bytes. | Rust type safety, matching generated WASM, trusted provider encoding. | Shared Unicode/NUL/CRLF/tool-pair fixture and native/TypeScript provider byte assertions. |
| Model, tool revisions and schemas, roles, messages and ordered tool exchanges determine identity. | Versioned manifest with whole-request, binding and per-message BLAKE3 digests; shared schema and pairing validation. | Collision resistance, canonical serializer and schema validator correctness. | Negative controls for roles, interrupted or unpaired exchanges, arguments/results, duplicate definitions, revisions and bounds. |
| Input and output have separate explicit bounds. | 64 MiB serialized input ceiling, existing content/message/event limits, positive output-token ceiling. | Providers honor output ceilings. Wire counts are not heap guarantees. | Aggregate input and zero/missing output-ceiling tests; existing stream admission tests. |
| Replay validates the retained request before cached completion or reconciliation. | `ModelStarted.request` pins a private artifact; descriptor, canonical bytes, admission and request digest are checked. | Trusted linearizable journal and retained immutable versions. | Removed/corrupt artifacts cause failure without redispatch; configuration and prefix changes reject retries. |
| A completed fork exchange is frozen before prefix publication and child activation. | `completed_tool_prefix` reads the retained request and completed model/tool journal; `stage_completed_model_prefix` stages only after that succeeds. | Trusted journal and controller; composition publishes the returned reference before activating children. | Missing completion stages no prefix or generation change; extracted bytes equal the actual next provider request. |
| Children share an immutable prefix, retaining only their local suffix and direct parent reference. | `ModelPrefix::select`; parent-owned deterministic staging; fork admission binds exact reference grants. | Authenticated controller and owner-approved exact read grants. | Real LocalStream/Filesystem depth-three and sibling forks, identical shared ref reuse, pinned scratch and attachment refs, later parent write, provider bytes and reopened storage. |
| Traversal cannot silently substitute a different parent history. | Iterative direct-parent reads; descriptor/canonical/version/binding/count/cycle checks; an incremental digest of ordered canonical-message digests binds every segment's full prefix. | Trusted verifier authenticates grants and residency; BLAKE3 collision resistance. | Canonical, granted but mismatched parent with the same shape/binding is rejected; corrupt, missing and ungranted references fail closed. |
| Child history descriptors do not copy parent message lists. | Format-2 `InheritedConversationPrefix` carries parent revision, selected sequence, ordered history digest and bounded reader identity. | Authoritative parent reducer remains available for admission. | Existing publication/retry, isolation, recursive-fork and project-only merge tests. |
| Native and WASM enforce the same inheritance rules. | WASM owner core authenticates signed scopes over captured generation-pinned bytes, then calls native `inherit`; TypeScript only selects this constructor and converts generated facade names. | Content host captures actual resident bytes; signed scope is owner-issued. | WASM depth-three/two-child provider assertions, missing attachment grant rejection and stale-module checks. |

The ordered prefix digest uses BLAKE3 with domain
`harness:model-prefix:ordered:v1`, followed by the fixed-width canonical-message
BLAKE3 digests in order. Each message is processed once during reconstruction;
ancestor message lists are not repeatedly serialized or copied into new segments.

`ModelPrefix` is model-visible context. The small conversation inheritance
pointer binds canonical parent history separately. A reference is never a read
grant. The generic fork controller does not delegate arbitrary ancestor-owned
files: composition must arrange explicit approvals from their owners. The durable
integration fixture supplies those exact approvals, without ancestor volume
access. The filesystem reader permits exact reads only in the already public
inheritance namespace; execution and interaction internals retain their owning
volume requirement.

Notification, task, identity and workspace fields are explicit local input in the
integration fixture, with real private scratch and attachment references. Their
production environment assembly and ordinary-task activation belong to the
separate agent composition slice. The prefix constructor introduces no retrieval,
summarization, truncation, sibling selection or credential discovery. Provider
options and explicit caller content remain caller-owned inputs.

The default `acyclic.read_file` contract advances to revision 2: raw and projected
outputs both satisfy its pinned string schema, without a redundant file echo.

## Verification status

Final source-bound native, WASM, Linux and macOS verification is still in progress.
Earlier source snapshots are not evidence for later edits. The request-only
snapshot based on `105728988b2b1831f2a0d596fc744e3b2d4eee5e`, archive SHA-256
`5a8ad8f395d2d310e2c93588790b751ffcf5dd9d4481b73aebc216bb8c42e3e8`, passed
191 Harness unit and 17 integration tests on Linux and macOS. Its macOS Codex
consumer passed 43 tests, with three real-Codex qualification tests ignored.
Its default parallel Linux consumer had an `ETXTBSY` fixture launch failure;
isolated and serial passes do not erase that failure.

The new durable provider/fork test and the 12 focused WASM model tests passed at
intermediate checkpoints. A later concurrent full TypeScript run timed out in
an existing large-attachment test; that failure is retained pending an uncontended
rerun. No real-Codex or production-provider qualification is claimed.

Completion requires current-main source verification, all required green gates,
one focused draft PR, and verification of the actual merged main commit. The
user has authorized administrative merge after those gates; review is not a
required gate.
