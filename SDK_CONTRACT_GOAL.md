# SDK contract goal evidence

Status: active; local transport work is incomplete and PR 223 is unmerged.

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
- Actor/Worker descriptors and generated bindings were regenerated; current
  digests are recorded in `compatibility/manifest.json`. Objects v1 and Stream
  v2 schema/descriptor digests remain unchanged.
- `bun scripts/check-generated.mjs`, `bun scripts/check-metadata.mjs`,
  and `bunx buf lint`: passed.

## Remaining SDK work

- Rust HTTP Objects and Streams providers that preserve the established hosted
  JSON envelopes and implement existing provider traits.
- TypeScript gRPC adapters for existing Objects/Stream provider interfaces.
- Stronger semantic fixtures for subscriptions/checkpoints, pinned versus alias
  invocation, streaming payloads, and bounded gRPC/error responses.
- Full generated/compatibility/package/repository qualification, local Windows
  qualification, and fixes for any observed failures.
- Resolve both review threads after verifying the concrete Actor headers and
  Worker alias CAS changes. Update the existing PR after local verification.

## CI diagnosis and merge dependency

Run 36631259983 failed in planning: sequential qualification-marker cache restores
exhausted the plan job's ten-minute timeout. The Windows job failed while waiting
for that plan; it never checked out or tested SDK code. Planning cache work needs
a bounded fallback before the next qualification push.

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
