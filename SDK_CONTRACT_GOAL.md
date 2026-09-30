# SDK contract goal evidence

Status: active; local transport work is incomplete and PR 223 is unmerged.

## Inference idle KV contract slice (2026-09-30)

Inference owner confirmed the human's renewal choice: change timeout from prior
verified actual Run use, without advancing last-use or resetting the window.
Before first use, initial verified KV pin time supplies a distinct baseline.
The additive v1 schema has `IdleKvPolicy`, `IdleKvRetention`, mutually exclusive
legacy/idle Retain and Renew fields, `WarmView.idle_kv`, and discovered
`ModelCapability.idle_kv_profiles`. Existing message tags and opaque Context,
WarmContext and Run identities remain. No capacity/throughput/latency guarantee.
Only verified actual Run reuse of the pinned revision/descendant prefix advances
last-use; fork/edit/admission/inspect/recovery do not. Expired/released pins cannot
resurrect through renewal/replay. Service lifecycle enforcement is still a Cloud
dependency; SDK validation does not establish actual reuse or deployment.

Local evidence: Rust host/contract 13 tests; reflection 4 tests; TypeScript 17
tests and type checks; Node canonical WASM validation; strict native Clippy;
Buf lint; Inference package dry pack all passed. Tests cover exclusive policies,
checked deadlines/overflow, paired actual-use timestamp/Run ID, source policy
binding, stable caller retry identity, handle recovery, HTTP auth/encoding and
renewal baseline. HTTP fixture acceptance is local, not Cloud acceptance.
Inference descriptor SHA-256:
`0b0806c6876fd028c9cd588d0f7c6b7557643adda193b1f095c9ce710aec16ee`.

The broad `bun run check:generated` failed at filesystem N-API declaration
generation with Windows `EBUSY` reconciliation metadata; its earlier generated
bindings check passed. This full gate remains open and must be resolved locally.
Inference-specific generated checks are tracked separately for this slice.

Follow-up: reproduced the same filesystem reconciliation `EBUSY` by invoking
NAPI-RS directly with Node. Updated `scripts/filesystem-napi-types.mjs` to build
fresh Rust macro declarations and render them with NAPI-RS's public
`generateTypeDef` API. The check compares the exact bytes to the committed file,
rejects missing output, and does not publish unrelated native binaries/loaders.
Fresh Windows declarations matched without changing the committed bindings.
`CARGO_BUILD_TARGET=x86_64-pc-windows-msvc bun run check:generated` then passed
the full local generated-file gate. Strict WASM Clippy also passed with default
host features disabled. The prior full-gate failure is resolved locally.

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
Objects v2 definitions and source clients are implemented. The compatibility
manifest, package exports, and filesystem/local consumers still expose the old
model and require the final breaking transition.

The human also authorized coherent verified slices to land before waiting on
dependencies. Merging the current Actor/Worker/Stream transport slice does not
complete the remaining SDK goal or establish live qualification.

PR: https://github.com/acyclic-labs/sdk/pull/223

Canonical RPC inventory: `compatibility/public-rpc-matrix.json`, generated from
the ten service descriptors by `scripts/generate-public-rpc-matrix.mjs`.
The current inventory contains 38 target RPCs plus 17 Objects v1 RPCs pending
removal, covering all 55 RPCs still present in source. Boolean transport fields
indicate implemented source surfaces; `typescriptPackageExported` distinguishes
package exposure. These fields do not establish browser or live acceptance.
The optional `complete` mode fails on remaining legacy RPCs or missing target
transport/package exposure; it is a surface check, not the goal completion audit.

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

- Complete filesystem/local storage and simulation consumers' Objects v2
  migration, preserving filesystem content addressing, retention and measured
  physical operation counts; then remove active v1 source/generated paths.
- Finish the breaking Objects package exports, compatibility manifest, metadata
  and installed consumer transition. The v2 source clients already implement
  Rust gRPC/HTTP and TypeScript Node/Bun gRPC/HTTP, but root exports still use v1.
- Qualify browser HTTP execution and complete the remaining canonical Inference
  idle retention and Actor/Worker logical network policy contract changes.
- Audit and fold unique filesystem-accounting/Stream-acknowledgement work from
  the earlier open PRs before final qualification.
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

### Local filesystem merge history publication

The managed-agents Linux lifecycle gate reproduced the reported second-child
promotion failure at SDK e6fcc9eabf911e431f4ff8680485e9b198c388c0: it returned
Indeterminate with storage failure/object missing instead of the typed join
conflict. A smaller SDK regression also failed on Windows after reopening local
storage between each edit and promotion. The first merge created a normalized
source generation root as a history parent, but its publication flushed only the
candidate namespace closure. The new parent remained in private staging and was
lost when the engine closed.

Merge-history joins now flush that normalized parent before the authority
publication. Its referenced namespace and ancestors come from the published
source. The regression passes on Windows and WSL/Linux and confirms the second
join returns Conflicted while preserving the first child's accepted content.
All 93 Windows workspace/lazy-workspace tests pass. This fixes local durability;
the managed-agents gate still needs qualification at the updated source pin.

### Stream retry identities never expire into reexecution

MemoryStream and LocalStream now keep complete retry outcomes and immutable
envelopes indefinitely within the existing capacity bounds. Replay precedes
capacity admission, while a fresh identity fails closed at capacity. The old
24-hour minimum constant remains for source compatibility; time does not release
an admitted identity. The existing local journal/snapshot format is unchanged,
and earlier snapshot deadline metadata cannot trigger eviction. Outcomes already
discarded by an older snapshot cannot be reconstructed by this change.

All 63 Stream unit tests pass (one optional scaling benchmark ignored), including
full gRPC conformance, retained replay after a year/u64 clock exhaustion,
cross-path changed-digest rejection, local close/reopen, and restoration of a
snapshot carrying an elapsed legacy receipt deadline. Strict native all-feature
all-target Clippy, WASM Clippy and documentation checks pass. The canonical
Stream WASM artifact was rebuilt; Node and Bun gRPC replay/follow/fork/multi-path
Commit/authentication/error/bounds conformance passes against that artifact.
Cloud account-wide retry routing and durable admission remain service-owner work;
these local provider results do not establish live service acceptance.

### RPC matrix reconciled with the Objects v2 transition

The generated inventory now includes every RPC still present: 38 target operations
and 17 legacy Objects v1 operations awaiting removal. Objects v2 Rust HTTP source
is represented, and all 13 v2 TypeScript operations correctly report missing
canonical package exports. Matrix freshness passes. Its `complete` mode fails on
those 30 rows as expected, making the current surface gaps reviewable without
claiming final acceptance. Objects v2 descriptor SHA-256 remains
f5a0791f472ec500f3aa8a0a22f10b84426062ee0697b0fbed9457815c51894d.

The complete `bun run test:contracts` runner passes at this source: Node/Bun
invoke all earlier 42 RPCs and all 13 Objects v2 RPCs; Rust invokes all 15
Actor/Worker operations through gRPC and HTTP and full Stream HTTP conformance.
Objects v2 HTTP lifecycle/framing/error/bounds tests pass in Node and Bun.
This is local runtime evidence; browser execution, final package transition,
consumer migration, repository qualification and merge remain required.

### Harness logical Objects migration and reusable core metadata validators

The TypeScript Harness content adapter now consumes Objects v2 generated requests
and the public `./v2` package entry. `./v2/http` and `./v2/grpc` expose the existing
transport implementations; the gRPC options no longer depend on v1 types. These
are source subpaths in the unmerged branch, not a published package transition.
Canonical root exports and filesystem consumers still require migration.

A regression demonstrated that a fresh upload identity selecting already-present
content could later select another path, because its failed IfAbsent PUT had no
successful mutation receipt. Both Rust and TypeScript now publish and verify an
immutable logical upload intent binding path, descriptor and display name before
content publication. Each retry rereads the intent and selected content; neither
a mutation receipt nor an upload intent establishes durable service retention.
Write-only grants can stage, while public reads still require a read grant.

Objects v2 `response` is available without grpc/json/http features and exposes
typed timestamp, bucket, object-info, download-header, listing and multipart-page
validators for durable service metadata. A core-only external test verifies this
public API. Single PUT/part limits are not incorrectly applied to metadata for
completed multipart objects; download selections still enforce caller bounds.
The Objects v2 wire descriptor is unchanged:
f5a0791f472ec500f3aa8a0a22f10b84426062ee0697b0fbed9457815c51894d.

Local qualification: 84 all-feature Objects Rust tests, one external metadata test
and one doctest pass (one optional latency receipt ignored); core-only external
test and strict native all-target/WASM Clippy pass. Focused Rust Harness Objects
tests and strict native Harness Clippy pass. TypeScript build and Harness type
contracts pass; all 223 Harness tests pass with 1084 assertions. Node and Bun
workspace package-entry smoke tests pass. Installed tarballs and deployed Cloud
acceptance are not established by these checks.

The integrator relayed the user's expanded goal scope: canonical Rust source on
main, published distributions usable by exact Cloud pins, affected consumer local
tests, and actual deployed service API conformance are required before completion.
Service owners implement deployments; SDK owns canonical contracts and managed
filesystem semantics. The goal remains active, including these coordinated gates.

### Real browser HTTP qualification

`bun run test:contracts:browser` drives a private headless Chrome over local HTTPS
with trust limited to the fixture's ephemeral SPKI. It exercises package browser
entries using native fetch, generated messages and the shared Rust/WASM rules.
All 13 Objects lifecycle operations, 15 Actor/Worker operations and 10 Stream
HTTP routes are observed. Objects uploads/downloads cross 64 KiB frame boundaries;
Actor subscription cursors/checkpoints, Worker pinned versus alias resolution,
Stream cancellable follow, atomic multi-path Commit/replay/conflicts, per-family
authentication and response bounds are covered. Token creation is a transport
fixture response; it does not establish Cloud token issuance or grant enforcement.

The first Chrome run reproduced Illegal invocation from all four HTTP clients'
default fetch receiver. Default native fetch is now bound to the browser global;
injected fetch callbacks remain supported. Browser qualification also reproduced
an absent Stream retry observation projecting null despite the public undefined
type. The Rust/WASM response projector now emits undefined consistently for memory
and HTTP. The canonical WASM artifact was rebuilt and a focused regression added.

Chrome qualification passes, all 45 Stream TypeScript tests pass (223 assertions),
the full Node/Bun/Rust contract runner and full generated-file checks pass; strict Stream WASM Clippy passes
with no default native transport features. These are local SDK transport results;
canonical Objects root/export migration, filesystem durable provider migration,
full repository/consumer qualification, merge/publication and deployed service
acceptance remain required. The goal is active.

### Review qualification and response bounds

The Linux qualification failure at 7196ce28 was reproduced locally by
`bun scripts/check-metadata.mjs`: the package check required every descriptor
version to occupy the single `./proto` export. Objects v2 now has an explicit
`./v2/proto` export, and metadata validation checks each generated version's
actual protobuf export. The local metadata check passes.

Browser qualification now builds Objects and Stream WASM before compiling or
launching Chrome. The command passes with all 38 fixture routes observed.
N-API build metadata explicitly tracks the force-build environment value;
two successive declaration checks pass with fresh Rust macro output.

Objects v2 HTTP downloads decode bounded NDJSON frames incrementally, validate
selection size at the header, and apply caller body limits before retaining
frames. The parser uses a fixed 128 KiB line buffer. Node and Bun tests prove
oversized headers cancel the stream without pulling the body; existing lifecycle,
range, malformed framing and error tests pass. Chrome qualification also passes.

Legacy Inference retain/renew responses must preserve legacy retention mode.
Rust host checks and the shared Rust/WASM validator reject substituted idle-KV
views, while recovered inspect remains mode-neutral. All 14 Rust tests, strict
native Clippy, 18 TypeScript tests and type checks pass; WASM was rebuilt.

Unresolved Stream HTTP response atomicity and legacy local receipt-capacity
review findings still require investigation before merge. Billing meter units
and native signed proof/custody semantics are an exact dependency being
coordinated with Inference and Billing; uint64 counters alone do not establish
pricing semantics or physical residency. The Goal remains active.

The full generated-file check passes after these changes. Inference also reports
that Cloud mounts canonical tonic services but lacks the canonical SDK JSON
routes/watch NDJSON. A descriptor-backed bounded native protobuf-JSON route
codec in acyclic-inference is an SDK dependency; Cloud will dispatch its decoded
messages into the same service handlers. No HTTP deployment acceptance is claimed.

### Stream review fixes and durable recovery

Both Rust and TypeScript HTTP reads validate the monotonic tail before reading,
preventing append-after-empty-response cursor reclassification. Successful HTTP
Commit responses now carry the admitted immutable envelope alongside existing
compact fields; Rust uses it without a follow-up read. Local native HTTP tests
prove commit-only authorization succeeds and replays while commits/read is denied.
Published compact-only servers retain their documented read-access requirement.
Data confirms its canonical service will emit full outcomes without that legacy
server path and validate read cursors in a coherent authority view.

`http_response::encode` exposes the same Rust response projection to native
service consumers and WASM, without requiring transport features. Core-only
external tests cover atomic envelope, exact uint64 timestamps, output bounds,
malformed nested IDs and absent observations. Protobuf descriptors are unchanged.

A legacy snapshot at receipt/envelope capacity followed by an intact post-expiry
journal command reproduces Capacity under ordinary old recovery admission.
Recovery now allows a finite inventory of surviving snapshot outcomes plus bounded
journal commands, then restores live limits. The regression preserves both
outcomes/history, refuses fresh admission at capacity, and reopens after snapshot
compaction. No receipt eviction, public deletion or history removal is added.

All 64 Stream Rust unit tests and two external tests pass (one optional scaling
benchmark ignored); strict native all-target and WASM Clippy pass. All 45 Stream
TypeScript tests pass, and the full contract runner and Chrome qualification pass.
Remaining canonical Objects/FS migration, repository/package qualification,
merge/publication and coordinated service dependencies keep the Goal active.

The next Linux failure was reproduced with `bun x buf format -d --exit-code`:
Actors, Workers and Objects v2 had single-line messages outside canonical Buf
format. Formatting, binding regeneration, metadata and full generated checks
pass locally. Comparing FileDescriptorSet after dropping sourceCodeInfo proves
all three wire descriptors unchanged; raw source-location digests changed.
Actors descriptor is 70720491f34232b4b7e424a17f8383ad5a69b1018460e8fff7a62600fb6ec16c;
Workers is 851b6cd37b8cb4baa6d3a111efdad655b89936b2e1057ecb74e62825715bd7d8;
Objects v2 is 21cb9f4893ce487716645e2814ffc680b9b6db8e0f23a9ea6867851100861d6b.
Installed tarball smoke passes for all ten workspace packages (nine release
packages and one workspace-only package), with Bun/Node imports and strict
declarations. A Buf git-ref comparison could not clone the local partial Git
object store; direct canonical descriptor comparison provides semantic evidence.

### Native Inference HTTP codec handoff

The optional `acyclic-inference/http-codec` feature now exposes descriptor-derived
inventory for all 14 customer RPC routes, bounded protobuf JSON request decoding
and bounded response/event encoding. It builds without `host` on native Windows
and WASM. No service route table or wire shape is maintained in parallel. The
codec rejects unknown request fields, trailing JSON, malformed wire messages and
wire/JSON ceilings; uint64 decimal strings, base64 bytes and enum names round-trip.
Output serialization uses a capped writer, including escaped-string expansion.
Watch events remain individual JSON values for the service to frame as NDJSON.

Qualification: all 14 native Inference unit tests and three external codec tests
pass; core-only codec tests and strict native all-target/WASM Clippy pass. Existing
protobuf descriptors and published package versions are unchanged. The codec is
serialization only: Cloud still owns authentication, semantic admission,
caller-bound checks, stream lifecycle and actual canonical HTTP mounting.
The Inference owner's negative warm-admission/cancellation receipt question and
Billing logical usage units/provenance remain separate exact contract dependencies.

### Objects v2 native batch migration prerequisite

Objects v2 now has an explicit native composition capability, `NativeBatchObjects`,
with no default endpoint loop and no added public RPC. The memory provider executes
ordered put/get batches under one authority lock, using the same canonical request,
response, precondition, quota and retry logic as individual operations. Failed
items leave both publication and retry receipts unchanged; reads retain independent
bounds and errors. This capability is needed before migrating the filesystem's
batch accounting. The existing filesystem adapter and durable local Objects
backend still use v1; this change does not establish their migration.

All 85 native Objects unit tests pass, including complete v2 TLS gRPC and HTTP
conformance. Strict native all-target and core WASM Clippy pass. The batch
regression covers mixed ordered successes/failures, quota rollback, exact receipt
replay, bounded reads and absent failed publications. No protobuf changes.

PR223 head 260df90f failed Linux qualification job 109819924978 in the live FUSE
cache test at fuse.rs:5416 (unexpected getattr after revalidation). Exact local
live test passed once and 60 consecutive repeats; the default-feature native
mount group also passed all 25 tests. The CI failure is not reproduced or fixed
by these results. Full workspace all-feature Linux qualification remains to be
revalidated before updating or merging the PR. No managed-agents changes exist
in the local diff or the PR diff against current main.

### First Objects v2 filesystem consumer migration

The WASM memory filesystem now composes the canonical v2 memory provider through
`LogicalObjectStore`, retaining filesystem digest identity, immutable IfAbsent
publication, configured object/memory bounds and the existing capacity error.
Native batches use the explicit capability rather than remote endpoint loops.
Collected bytes cannot be answered by a stale mutation receipt. Reads verify
content; contains charges actual fetched/hashed bytes, and batch read budgets
admit known work before calling the provider. The adapter owns no durable state.

All three external filesystem v2 tests pass, covering ordered/deduplicated
batches, backend accounting, pre-admission bounds/cancellation, substituted
content and republishing collected bytes. Commands: `cargo test -p acyclic-fs
--locked --target x86_64-pc-windows-msvc --test logical_objects`, matching strict
Clippy, and strict WASM Clippy for acyclic-fs-wasm. The FS package was rebuilt;
Node and Bun memory composition, type checks and an isolated installed tarball
consumer pass. Chrome browser-smoke now explicitly runs memory workspace
composition, alongside IndexedDB/OPFS and the existing multi-window checks.
The complete `bun run check:generated` check also passes after this rebuild.
Native filesystem memory/simulation and the durable local Objects engine still
use v1; SDK root/default clients also remain a migration dependency.

Full local Linux workspace/all-feature native mount qualification passed all
25 selected native mount tests using `cargo test --workspace --all-features
--locked --lib --target x86_64-unknown-linux-gnu -- --ignored --test-threads=1
native_mount::` in WSL Ubuntu with /dev/fuse. This does not explain or fix the
required CI Linux failure. Windows/macOS and policy passed at PR head 260df90f;
the aggregate SDK gate remains failed because of Linux.

Inference owner commit 12b0a9417 consumes SDK 260df90f with host/http-codec and
reports 17 local service tests (real Qwen/CUDA), strict Clippy and canonical HTTP
routes/watch through the existing authority handlers. Evaluations remain
explicitly unsupported. Owner commit c7c0f2e57 also proves the actual TypeScript
InferenceClient/HttpInferenceTransport and shipped WASM validator against the
local service, including same-Run watch disconnect/resume without cancellation.
The fixture rewrites its HTTPS origin to loopback HTTP; public TLS and package
distribution are not established. Local Inference artifact hashes are
dist/index.js 73b73b2343e319fb6e37427e711d2e2145219ba05ed387f7be94df886897af16
and WASM 5f89e8e4512b26db9f77a0384b3cddd713d8ccadf8411cb87d8b1b75bb1cfbb9;
Inference source remains 260df90f (d78acc0 changes only Objects batching/evidence).
Negative failed warm-admission/cancellation receipts remain an exact contract gap.

Billing PR1323 merged c67f0a04528340228c263cad9872b0a8c56c73d5, reporting canonical
decimal bounds, provenance and native signed fixture evidence. SDK logical meter
units/pricing are still unagreed. No schema interpretation or physical cache-time
meter is inferred. Data/integrator own the Streams-first native integration.
