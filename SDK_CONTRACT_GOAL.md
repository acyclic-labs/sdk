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
Branch protection also requires one approving review; all code findings are fixed
but this approval and required qualification still govern merge.
