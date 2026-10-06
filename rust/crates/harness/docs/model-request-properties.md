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
The TypeScript default loop preserves exact admitted tool values; values above
the explicit render limit fail closed rather than becoming schema-incompatible
omission objects. Before inherited dispatch, fresh local file references are
authenticated and read through the bound content host into a per-request capture.
This capture does not mutate the inherited prefix snapshot. Regression tests cover
the exact tool-result byte boundary, new pinned versions after harness construction,
missing signed grants and corrupt local bytes before provider dispatch.

## Verification records

Passing snapshots are identified below; they do not qualify later source edits.
The final-head CI receipts, additional platform results and actual merged main
commit are recorded in [PR #251](https://github.com/acyclic-labs/sdk/pull/251).
Completion requires every required gate to pass against current main and
verification of the landed commit.

| Source snapshot | Passing checks |
| --- | --- |
| `b178db478fcfca9bbd12467c970486aa824d7060`, based on `1fe8b86685910437dd1ec350bc4ab4088aacbdc8`; archive SHA-256 `0dfd488145a9e8936be432eeb3b64b8d1095f0c31d38eef1ffe25fe22b875a4a` | Windows: warnings-denied all-targets filesystem-local lint, 193 Harness unit tests, 17 integration tests and doc-test invocation. WASM: warnings-denied target lint and fresh build. TypeScript: 226 tests, 1,106 assertions, package and type-test checks. macOS: the same Harness test counts and lint; 43 mock Codex consumer tests and a repeated default-parallel 13-test executor suite. |
| `aa0915627593d95e9c70801a8b18f6f21766f7f5`; archive SHA-256 `2118d88509d494314b5a003a7c0ea0a8c4dd9ff7c8fe4de608d7b0cb1de4aaad` | Shared fixtures moved into the published crate, with unchanged bytes and production behavior. Windows: 193 Harness unit and 17 integration tests. TypeScript: all 226 tests. macOS: warnings-denied lint, 193 Harness unit and 17 integration tests, 43 mock Codex consumer tests and the repeated default-parallel executor suite. |

Failures remain part of the record. The earlier request-only Linux snapshot had
an `ETXTBSY` fixture launch failure; isolated and serial passes did not qualify
that parallel run. The installer-child repair subsequently passed full and
repeated parallel consumer runs. An intermediate concurrent TypeScript run timed
out in the existing large-attachment test; uncontended full reruns passed without
changing its timeout. The first `b178db478f` Linux lint compilation crashed inside
a dependency with the compiler cache wrapper; the uncached lint rerun passed.
The `b178db478f` Linux CI package test exposed missing external fixture files,
which prompted the crate-local fixture move. Later CI exposed a kernel-cache
test expecting cached attributes immediately after a Linux read invalidated
atime; its correction refreshes those attributes once and still requires zero
requests across repeated unchanged stats, without sleeps or retries.

No real-Codex or production-provider qualification is claimed. Three existing
real-Codex qualification tests remain ignored in the mock-consumer suites.
