# SDK contract goal evidence

Status: active; local transport work is incomplete and PR 223 is unmerged.

## Superseding Objects direction

On 2026-09-30 the human user instructed the Integrator chat to change the SDK
Objects API for eventual consistency and horizontal scaling, removing public
versions, forks, and operations that require cross-object coordination. That
user message was inspected directly. This replaces the original Objects
compatibility/capture requirement; other family requirements remain in force.
The uncommitted versioned Objects HTTP draft was removed.

Subsequent inspected human replies require S3 bucket/key addressing, eventual
reads and lexicographic continuation listing, multipart, ranges, and opaque
ETag single-key conditional writes. Public versions, bucket snapshots/forks,
and captured listings are removed from the target. FS keeps its own semantics
and stores content-addressed bytes; managed FS deployment belongs on Actors.
Stream multi-path Commit remains, with bounded participants and source-shard
fork placement by default. Durable inputs use private retained generations
and idempotent selection receipts rather than public Object history.

Objects v1 shipped in `acyclic-v0.1.5`; the redesign needs an explicit breaking
protocol/package transition and must not reuse removed field numbers/names.
The current canonical definitions still describe the old Objects model and
are not the intended final state.

The human also authorized coherent verified slices to land before waiting on
dependencies. Merging the current Actor/Worker/Stream transport slice does not
complete the remaining SDK goal or establish live qualification.

PR: https://github.com/acyclic-labs/sdk/pull/223

Canonical RPC inventory: `compatibility/public-rpc-matrix.json`, generated from
the seven service descriptors by `scripts/generate-public-rpc-matrix.mjs`.
The inventory contains 42 RPCs. Boolean transport fields indicate implemented
surfaces; they do not establish live service acceptance.

## Local evidence on 2026-09-30

- `bun run test:contracts`: passed. Node and Bun each invoke all 42 RPCs
  against a local authenticated TLS HTTP/2 server, exercising client/server
  streaming, Actor invocation headers, alias expected revision, and multi-path
  Commit serialization.
- The same runner invokes all 15 Actors/Workers HTTP operations in Node, Bun,
  and Rust, and all 15 Rust gRPC operations. Rust HTTP authentication rejection
  decodes the canonical Actor capability-denied code; oversized HTTP responses
  are rejected before decoding.
- `cargo test -p acyclic-actors -p acyclic-workers --locked`: passed, including
  alias creation versus positive-revision replacement validation.
- `cargo clippy -p acyclic-actors -p acyclic-workers --all-targets --offline --
  -D warnings`: passed.
- `cargo test --workspace --all-features --locked` passed locally on Windows,
  including 1,119 filesystem unit tests and crash-atomicity cases. Native mount
  tests marked ignored still require their explicit qualification lanes.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
  passed; the finalized Workers change also passed its focused tests/Clippy.
- Actor/Worker descriptors and generated bindings were regenerated; current
  digests are recorded in `compatibility/manifest.json`. Objects v1 and Stream
  v2 schema/descriptor digests remain unchanged.
- `bun scripts/check-generated.mjs`, `bun scripts/check-metadata.mjs`,
  and `bunx buf lint`: passed.
- Full `bun run test`: 404 package tests, 12 hosted filesystem tests, and the
  transport runner passed. Installed tarball smoke passed all ten packages in
  Node/Bun with strict declarations after isolating its Bun install cache.
- `GrpcStreamProvider` implements the existing TypeScript provider interface
  using Rust request validation and unary response projection. Its local TLS
  server delegates actual operations to Rust MemoryStream through WASM.
  Node/Bun checks cover replay, cancellable follow, forks, atomic multi-path
  Commit, conflict without mutation, retry replay/rebinding, error mapping,
  response cursor integrity, and configured message bounds.
- Workers v1 now accepts logical bucket/key `ObjectRef` inputs, reserving the
  removed version tag/name. `JobResult` returns bounded exact bytes instead of
  an Object version pointer. The Cloud Integrator confirmed this shape before
  first public merge. Native and Node/Bun tests exercise these fields through
  both transports; private retention/durable acceptance remain Cloud work.

## Pinned intermediate handoff

Signed commit `87648c845b99a93a6a94ccc943b33fd1328708e5` was pushed to PR 223
for the Integrator's Workers dependency. This commit is an intermediate contract
handoff, not goal completion. The final Objects consumer rewrite remains pending.

## Remaining SDK work

- Rust HTTP Objects provider that preserves the revised hosted
  JSON envelopes and implement existing provider traits.
- TypeScript gRPC adapter for the revised Objects provider interface.
- Complete Rust Objects/Stream client-to-server transport coverage, including
  streaming payloads and canonical error responses.
- Full generated/compatibility/package/repository qualification, local Windows
  qualification, and fixes for any observed failures.
- Both initial review threads are resolved. Update the existing PR after local
  verification and merge the coherent slice once required checks pass.

## CI diagnosis and merge dependency

Run 36631259983 failed in planning: sequential qualification-marker cache restores
exhausted the plan job's ten-minute timeout. The Windows job failed while waiting
for that plan; it never checked out or tested SDK code. The next run's plan
passed in 18 seconds, so no workflow change is justified by that timeout alone.

Run 36649445498's Linux lane failed release archive validation: Actors and
Workers changelog titles/version headings did not match the archive contract.
The exact validator was run locally, metadata was fixed, and the validator is
now included in installed tarball smoke for every published TypeScript package.

The Integrator chat owns canonical Workers service/API integration and explicitly
needs the Workers v1 descriptors/client exports to build and qualify both
transports against a stable SDK version. This is the named merge dependency.
Merge only after stable contract handoff and required checks pass.

## Cloud dependencies

The Integrator owns Workers server integration. Data hosts own local bringup of
Streams, Objects, Actors, and origins. SDK local fixtures do not establish staging
or production acceptance. Cloud owners must implement the canonical server RPCs
  and HTTP envelopes, account authentication, admission, authority/attempt fencing,
and their service behavior. Stream v2 requires atomic multi-path Commit and
indefinite retained history, with no public deletion, truncation, or rewind.

Infisical retrieval and IAM mapping remain with Foundations. SDK clients consume
caller-supplied endpoint/token/CA and do not own secret references. Package
publication and live acceptance are separate goals.

## Native Stream HTTP checkpoint (2026-09-30)

`acyclic_stream::http::HttpStream` now implements every Stream provider operation
with the existing hosted JSON projection, sensitive bearer headers, bounded
responses, caller CA trust, and cancellable polling follow. Its request encoder
is shared with WASM and preserves Commit deadlineUnixMillis as a decimal string.
Successful Commit and committed inspection additionally fetch `commits/read`
to construct the complete Rust envelope; Cloud credentials must permit that
route. This is a service capability dependency, not live acceptance evidence.

Local checks: Rust Stream all-feature tests (58 passed, one scaling benchmark
ignored), strict Stream Clippy, HTTP-only feature compilation, all four-family
contract fixtures, and full native HTTP public conformance passed. HTTP checks
also cover two-path atomic append Commit/replay/inspection, caller deadline
rejection without mutation, authentication and bounded response rejection.

The provider-facing `request` module exposes the existing canonical validation,
commit normalization and digest implementation for replicated Cloud providers.
It does not implement replication or replace their atomic authority.

Qualification run 36651776885 Linux failed the live FUSE cache test at fuse.rs:5416
(one unexpected getattr). Local WSL with /dev/fuse passed that exact test 30
consecutive times and passed all 25 ignored native-mount tests together. The CI
failure has not been reproduced, and no filesystem implementation was changed.

Retention audit: memory/local currently forget retry identities and envelopes
at 24 hours of their retention clocks while preserving records. An old identity
can execute again. Integrator received the exact behavior and a proposal for
permanent compact identity/digest/terminal receipts with admission before
capacity; result compaction and caller deadlines are separate. No guessed
retention change has been made pending service-contract agreement.
Generated bindings/digests check and installed TypeScript tarball smoke passed.
A new review finding about TS Commit fork-cut errors was fixed: gRPC now uses
the existing memory/HTTP invalid_argument mapping, with Node/Bun regression checks.
Branch protection also requires one approving review. The human has authorized
an admin merge override for that approval policy after coherent changes are
verified. Required qualification and the consolidated SDK finish line still
govern merge.

## Objects v2 consumer migration draft — 2026-09-30

The human's revised Objects direction supersedes the original v1 compatibility
assumption for this family: logical S3-style bucket/key operations, eventual
GET/HEAD/LIST, atomic replacement and IfAbsent/IfMatch, automatic reclamation,
and no public versions, history, snapshots or forks. The replacement wire package
is acyclic.objects.v2; published v1 field numbers are not reinterpreted. Active
v1 paths are temporarily present while FS/Harness/WASM consumers are rewritten.
They must be removed before the consolidated PR is considered complete. The
breaking package transition must be documented before release; Git preserves
the historical published contract, rather than an active legacy service.

The local draft implements all 13 v2 operations in bounded MemoryObjects and
GrpcObjects. TLS fixture coverage exercises every RPC, multi-frame PUT/GET/part
uploads, private CA trust, bearer authentication, typed conditional failures,
bounded reads, malformed successful responses and terminal stream errors.
Additional tests cover competing conditional replacements, retry replay without
resurrection, quota rollback, range selection, live listing cursor behavior,
query binding, multipart failure preserving staged parts, ordered receipts,
and incremental body hashing independent of frame boundaries/retry keys.

The Objects Cloud owner approved the SDK-owned HTTP binding: POST routes under
/v2/objects, descriptor-based Protobuf JSON unary messages, bounded header-first
NDJSON upload/download frames. The exact inventory and status/error/framing
rules are in docs/objects-v2-http.md and v2::HTTP_ROUTES. GetObjectResponse has a
terminal ErrorDetail frame for semantic failure after HTTP headers start. This
binding and S3 use the same authority; native ETags are opaque, not checksums.

Provider-facing v2::request exposes all mutation validation/digest helpers,
including incremental PUT/part hashing and ordered multipart selection. The
memory provider uses these same helpers. Descriptor JSON codec tests preserve
uint64 above 2^53 and binary bytes exactly and reject unknown/trailing JSON.

Local checks for the draft: 14 focused v2 tests pass through the native HTTP and
TLS gRPC fixtures. The complete Objects suite passes 83 tests plus one doctest
(one local latency benchmark ignored). Strict Objects Clippy passes for all
targets/all features and no default features; the Objects WASM bridge passes
strict wasm32 Clippy. Formatting and diff checks pass.
Earlier draft descriptor SHA256 (superseded by the completion-frame change below):
`eebe96b8b1df909e9f047f6a0fdcf3d80ba682dec1710e0b81a74638dc3dda4b`.
Consumer migration, active v1 removal, package transition and final matrix/
compatibility qualification remain pending; none is marked complete by these
draft checks. Cloud's durable bucket registry, metadata/time, listing index,
multipart sessions, public replay receipts and multi-block manifests are exact
service dependencies reported by its owner; local fixtures are not live acceptance.

FS retirement audit: distributed.rs StreamAuthorityStore::retire_authority
appends a stable retirement marker on its separate Stream path and preserves the
authority's committed records. Workspace access/claim release and underlying
permanent Stream history remain separate; content reclamation follows the
human's pending explicit workspace-delete decision.

### Linux installed-package qualification correction

Run 36653616935 at 68c5d503 passed Windows, web, all alternate Linux targets,
macOS, policy and all host qualification tests. The SDK Linux lane failed at
the installed TypeScript consumer: its old hard-coded overrides omitted Actors
and Workers and attempted to fetch those unpublished packages from npm (404).
The required aggregate therefore failed. This is separate from the earlier
FUSE cache-test failure, which passed on this run.

Reproduced the missing-override consumer locally with packed packages: Bun
failed with those exact two npm 404s. The corrected shell-script consumer uses
the release inventory and each package's own version to override every local
archive, an isolated install cache, and both Bun/Node export checks. Executed
the script's actual embedded generator and smoke source: installation, both
runtimes and bash syntax pass. The complete source-clean packaging shell gate
was not rerun against this dirty Objects migration draft.

Signed correction c4a220ad9fc515acf93c4a30bd69b25c92424241 was pushed to PR223.
Objects v2 remains a local migration draft and is not included in that commit.

### Objects v2 TypeScript/WASM transport evidence

The local migration draft now contains MemoryObjectsV2, HttpObjectsV2 and
GrpcObjectsV2, plus complete generated Node/Bun gRPC clients. Public request and
response fields come from generated Protobuf types. Rust WASM performs binary
request validation, response metadata/listing/range validation, and descriptor
JSON encoding/decoding. HTTP route message types come from Rust HTTP_ROUTES.
Memory capacity rejects values outside wasm32 allocation bounds before the ABI
can wrap them. Buffered reads require an explicit decoded allocation bound.

The shared 13-operation lifecycle passes in both Node and Bun through HTTP and
TLS gRPC, backed by the Rust WASM reference provider. It includes 135k uploads
and downloads split into 64KiB frames, arbitrary HTTP response chunk boundaries,
conditional replacement races, mutation replay without resurrection, multipart
creation/upload/list/complete/abort, suffix ranges, canonical errors and bearer
authentication. Four Node/Bun tests also exercise malformed metadata/ranges,
truncated/unterminated/duplicate frames, wrong media types, terminal semantic
errors, request preflight and wire/allocation limits.

Local TLS qualification reproduced Bun Windows prematurely closing a large
compressed GET. Disabling optional response compression passes that same test
in Node and Bun. The gRPC clients own a closable HTTP/2 session and a 30s default
request timeout. Buffered GET discards an allocation-rejected response while
draining within its wire bound, preserving subsequent calls; hard wire overflow
cancels the request. Public streamed uploads and browser runtime qualification
still need completion beyond these buffered lifecycle checks.

Commands: node scripts/build-objects-wasm.mjs; bun x tsc -b
typescript/packages/objects; node --test
typescript/packages/objects/test/v2-conformance.mjs; bun test
./typescript/packages/objects/test/v2-conformance.mjs; node
typescript/packages/objects/test/v2-grpc-conformance.mjs; cargo test -p
acyclic-objects --all-features; cargo clippy -p acyclic-objects --all-targets
--all-features -- -D warnings; cargo clippy -p acyclic-objects-wasm --target
wasm32-unknown-unknown --all-targets -- -D warnings.

The new source clients are not final package exports while the active v1
consumers are being migrated. Browser runtime qualification, package exports,
breaking version transition and the final RPC matrix remain required. Local
fixtures do not prove Cloud deployment acceptance or package publication.

### Objects v2 streamed download and cancellation qualification

Native GrpcObjects and HttpObjects now expose get_stream returning a validated
metadata header and bounded decoded Bytes chunks. Buffered provider GET collects
that same stream. The native fixtures verify three chunks for a 135000-byte
representation, successful reads after dropping a partially consumed stream,
decoded allocation rejection, exact EOF length and terminal semantic errors.
All 14 focused v2 tests and strict native all-feature Clippy pass after this change.

The TypeScript TLS fixture now also rejects an over-wire-bound download, then
uses another client successfully, in Node and Bun. Investigation found Connect's
response iterator intentionally omits return(), so early loop exit could leave
its 30s deadline timer alive. GrpcObjectsV2 now supplies a per-call AbortController
and observes the cancelled iterator to release that timer. Hard wire overflow
cancels this request rather than other calls sharing the connection. The complete
Node/Bun fixture, including this rejection, finishes in approximately two seconds.
Server write-ECANCELED diagnostics during this negative case are expected evidence
of cancellation, not a swallowed client error.

### Objects v2 streamed upload completion qualification

Native GrpcObjects and HttpObjects now expose put_stream and upload_part_stream.
They split caller chunks into bounded 64KiB frames, propagate source failures,
and cancel the request when its future is dropped. Buffered uploads use these
same paths. The native cancellation fixture reproduced partial publication when
gRPC cancellation arrived as clean request EOF. The unmerged v2 schema now
requires a final complete=true frame (oneof field 3), followed by EOF, for both
PUT and upload-part. Missing/false/duplicate completion or a subsequent body
frame must not publish. The public request::UploadFraming helper enforces this
discipline after a validated header. Cloud accepted this native gateway gate;
its public gateway remains unimplemented. S3 body completion is separate.

The updated descriptor SHA-256 is
f5a0791f472ec500f3aa8a0a22f10b84426062ee0697b0fbed9457815c51894d.
All 84 Objects tests and one doctest pass. Strict Clippy passes with all features
and targets, native HTTP alone, and wasm32 JSON alone. Node and Bun pass the four
v2 WASM/HTTP tests and the TLS gRPC 13-operation fixture after regeneration.
Generated bindings and descriptor rebuilds are in sync; git diff --check passes.
Native fixtures cover failed source publication, failed replacement preserving
the old object, pending-source cancellation, retry after failure, empty chunks,
large chunk reframing, and failed part replacement preserving its prior receipt.
Explicit framing tests reject missing/false/duplicate completion and later bodies.

This remains an uncommitted local migration draft. Public package exports,
consumer migration, browser runtime qualification and Cloud acceptance remain
pending.

### Stream provider preparation helpers

The public preparation module now provides append, fork and coordinated commit
outcome/envelope preparation from pre-commit existence/tails and accepted commit
ID/time. Coordinated observations explicitly distinguish missing input from an
absent stream, normalize canonical participant ordering, return exact conflicts,
check fork source authority, and preserve source pre-commit cuts when the same
commit appends to that source. The memory provider shares conditions, fork cuts,
authority and record/envelope construction with these helpers. It prepares every
coordinated record before applying any mutation. Exclusive-tail overflow fails
before publication. Retry lookup, durable admission, capacity, authorization and
atomic publication remain provider responsibilities.

All 61 Stream tests pass (one optional scaling benchmark ignored); strict
all-feature/all-target Clippy and wasm32 WASM Clippy pass. Tests compare standalone
append/fork and multi-path preparation envelopes with the reference provider and
cover missing observations, exact conflicts and unrepresentable exclusive tails.
This does not resolve the pending durable retry lifetime/routing decision.

### Objects v2 TypeScript streamed uploads

GrpcObjectsV2 now exposes putStream and uploadPartStream with AsyncIterable byte
sources and optional AbortSignal cancellation. Buffered uploads use those paths.
The client validates the snapshotted header and accumulated source length through
Rust WASM, splits chunks into 64KiB frames with backpressure, emits completion only
after successful source EOF, preserves canonical source errors, and validates the
receipt against the decoded byte count. A pending caller iterator cannot delay RPC
cancellation; iterator cleanup is requested without waiting for caller-owned work.
The Node/Bun TLS fixture passes large/empty chunks, exact round-trip bytes, failed
replacement preserving the old object, pending-source cancellation leaving no
object, and failed part replacement preserving the old receipt. TypeScript build
and git diff --check pass. This does not qualify browser execution or finish the
v1 filesystem/Harness/local-storage consumer migration.

The qualified Objects v2 schema, generated bindings, reference provider, Rust
gRPC/HTTP clients, WASM validator/codec and TypeScript source clients are now being
recorded as one reviewable intermediate commit in PR223. Earlier notes calling
these files uncommitted describe their state at those checkpoints. This commit
does not finish the migration: active v1 consumers and package exports remain,
the compatibility manifest/matrix still require their final transition, and no
released package version or Cloud deployment is established by this source pin.

### Harness Objects consumer migration

ObjectContentStore now consumes Objects v2 and logical bucket names. Harness file
revisions carry a canonical descriptor/display-name digest, including SHA-256 and
length, and resolve immutable content keys published with IfAbsent. Artifact refs
have no Objects version field. Artifact capture checks bounded bytes and metadata
against the content identity; it cannot accept a silently overwritten key. Owner
scope, attached-reader grants, route isolation and manifest member checks remain.
The adapter performs no public history/snapshot/version operation.

All 196 Harness unit tests and 17 integration tests pass, including local reopen,
recursive fork, residency, execution journal and workflow checks. Strict all-feature
all-target Clippy passes. Migration tests verify old references survive new content
at the same logical path and overwritten content fails file/artifact verification.
Presence and hash verification do not establish service retention: Cloud must adopt
private retain claims under durable acceptance intents before publishing accepted
refs. Filesystem/local provider migration and final package transition remain.
