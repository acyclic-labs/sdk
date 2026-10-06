# Model request contracts and verification map

This slice extends the existing task/execution/conversation journal family.
It introduces no runtime, scheduler, provider adapter or verification framework.
Tests are implementation evidence, not unrestricted correctness proofs.

| Invariant | Production mechanism | Assumptions | Verification and evidence |
| --- | --- | --- | --- |
| Every native dispatch carries one immutable admitted request and its canonical bytes. | `PreparedModelRequest::prepare`; private fields; `ModelProvider::generate` accepts only the prepared value. | Rust type safety; trusted providers honor the values and any provider-specific encoding. | Constructor roundtrip and provider-boundary assertions in stock executor tests. No machine-checked semantic proof. |
| Ordered roles, content, tool IDs, model/tool revisions and schemas determine request identity. | Canonical bytes; whole-request, binding and ordered-message BLAKE3 digests. | Collision resistance; exact serializer contract. Digests authenticate content only when bound to an authoritative journal/reference. | Unicode/control-character roundtrip, schema revision and mutation regressions. |
| Each historical tool result matches one preceding assistant call and satisfies the pinned result schema; call arguments satisfy the pinned input schema. | Shared request admission, tool registry schema validator and call-identity validator. | Pinned schemas describe model-visible projections; schema validation library correctness. | Negative controls for unpaired/reordered exchanges, wrong roles, invalid arguments/results and duplicate definitions. |
| Admission requires a positive output ceiling; serialized input has a 64 MiB protocol ceiling; content parts and output retain independent bounds. | Constructor plus existing `Limits`; stock/live dispatch supplies a token ceiling. | Providers honor token ceilings. Wire counts are not heap guarantees. | Aggregate input ceiling and missing/zero ceiling regressions; existing stream event/output limits. |
| Durable replay validates the retained exact request before reconciliation or cached completion. | `ModelStarted.request` is a private pinned artifact; `load_json` verifies descriptor/canonical encoding; reconstruction validates and compares bytes. | Trusted linearizable journal, authenticated content reader and immutable version retention. | Stock replay test reopens a journal, corrupts and removes the retained request, and checks that neither model nor tool redispatches. This is mock storage restart evidence. |
| Stage-added file references are authority/residency checked before dispatch. | Stock journal `verify_input_file`; live task grant and content verifier. | Bound verifiers enforce exact generation grants and descriptors. | Filesystem/native attachment gates remain required. Constructor alone does not establish residency. |
| Native and WASM use the same request admission and encoding. | WASM `prepareModelRequest` delegates to the Rust constructor; TypeScript decodes admitted bytes into its generated facade and supplies those bytes to providers. | Generated module matches source; thin facade preserves canonical identities. | Shared Unicode/tool-pair byte fixture in native/WASM tests; actual stock and TypeScript provider-boundary checks. Broader fork-input equivalence remains required. |

## Remaining owned requirements

The existing `InheritedConversationPrefix` copies a ref-only message list into
each child's private volume. That is not the required shared immutable model
prefix. It also does not bind a completed fork-tool exchange to provider bytes.
The final implementation must replace those copies with authenticated,
generation-pinned shared references and iterative direct-parent traversal.

The depth-three/multiple-child provider-boundary test must include Unicode,
attachments, tool pairing, inherited scratch, fresh child scratch, later parent
mutation and restart. No existing recursive Filesystem test proves that model
input contract. Notification/task/identity/workspace append order must be
verified against exact serialized inputs, with no hidden retrieval, summaries,
sibling history, UI metadata or inherited credentials.

Request admission can be developed independently on current main. Coding-agent
activation uses ordinary tasks and belongs to the separately owned agent
composition slice; no unmerged implementation may be copied or cherry-picked.
The goal stays incomplete until all owned integration and validation gates and
the focused draft PR are complete.

## Current verification checkpoint

Native library tests: 178 passed. Filesystem-local unit tests: 191 passed;
all 17 integration tests passed, including 1,024 recursive forks. These fork
tests verify the existing Filesystem contracts, not the pending shared model
prefix contract. Documentation tests pass using the matching installed
`rustdoc` after the configured toolchain could not locate its own executable.
TypeScript Harness: 225 tests passed; Objects/Harness type checking passed.
Shared constructor WASM generation and native lint passed. Linux-only
`harness-codex` consumer validation remains an external platform gate.

The default `acyclic.read_file` tool is revision 2: its raw and projected
outputs both follow the pinned string schema. This removes the redundant
file echo and keeps schema validation at both dispatch and replay boundaries.

## Shared-prefix foundation in progress

`ModelPrefix::select` retains only local added messages and a pinned direct
parent reference. `PreparedModelRequest::inherit` authenticates each referenced
version, checks canonical bytes, bindings and message counts, walks iteratively,
and validates the full inherited exchange before appending explicit local input.
Inherited attachment references pass through the same verifier.

The mock reader regression exercises depth three and two children, Unicode,
a completed tool pair, attachment grants, changed bindings, missing grants,
corrupt retained bytes, lower count limits and byte equality after reopening
the mock store. Its scratch/notification/task/identity/workspace strings are
synthetic explicit messages; this does not test actual environment composition
or a real provider invocation. Publication integration, replacing the copied
conversation prefix, WASM parity and durable Filesystem restart tests for this
new contract remain pending. The earlier checkpoint test counts do not include
this later foundation.

`StockExecutor::with_inherited_prefix` now binds the exact prefix reference
to the existing execution identity and prepends the authenticated inherited
messages after local context assembly. A provider-boundary regression checks
exact bytes and replay without dispatch, and rejects removing the bound prefix
from a retry. This does not yet publish the prefix as part of a completed fork.

## Platform checkpoint evidence

The earlier request-construction source snapshot (base
`105728988b2b1831f2a0d596fc744e3b2d4eee5e`, archive SHA-256
`5a8ad8f395d2d310e2c93588790b751ffcf5dd9d4481b73aebc216bb8c42e3e8`)
was actually executed using Rust 1.98.1 in isolated Linux WSL and macOS
`ivar` directories. Both Harness lanes passed 191 unit and 17 integration
tests. The macOS Codex consumer lane passed 43 tests with three real-Codex
qualification tests ignored. Linux initially failed one temporary-executable
launch with `ETXTBSY`; that test passed in isolation and the complete consumer
suite passed with one test thread (43 passed, three ignored). The default
parallel Linux consumer run remains a recorded failure; a serial rerun does
not erase it. No real-Codex qualification or production-provider test is claimed.

The later prefix and stock-loop integration have 180 native library tests
passing with native lint. They were added after the platform snapshot and
require fresh platform/WASM evidence after final integration. Owned work stays
active through review, required CI repairs and human-authorized merge; delivery
requires verifying the landed commit on main.
