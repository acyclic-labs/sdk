# Rust-owned TypeScript generation prototype

Status: Actors and Workers prototype, 2026-10-03

## Scope

`rust/crates/sdk-typescript` is an independent Cargo workspace. Actors now
consume the descriptor emitted by the typed Rust model in
`acyclic-sdk-contract-wire::actors_descriptor()` and its routes from
`acyclic_actors::HTTP_ROUTES`. A typed Workers model is present in the
worktree, but its module is not yet exported through the wire crate, so Workers
remains explicitly marked `bootstrap-runtime-descriptor` and consumes
`acyclic_workers::FILE_DESCRIPTOR_SET` until that integration gate is complete.
It does not read TypeScript,
copy an existing facade, modify the root Cargo workspace, or replace the
shipping packages yet.

The prototype writes three review artifacts under its own `prototype/`
directory:

- `manifest.json`, with package/service identities, every route/RPC pair,
  request and response message names, stream directions, protobuf JSON
  encoding, bearer-auth policy, bounded-response policy, and descriptor
  descriptor SHA-256 values, and source-content SHA-256 provenance;
- `actors-metadata.ts` and `workers-metadata.ts`, generated readonly method
  tables; and
- thin `createActorsClient`/`createWorkersClient` facades that accept a
  transport-neutral invoker. The invoker remains target plumbing, so the
  existing Node, Bun, browser, private-auth, endpoint, error, and response
  bound behavior can be retained while the route/method authority moves to
  Rust.

The facades import the existing generated protobuf message types and expose
concrete request and response types per RPC. The generated `*-types.test.ts`
files include compile-time negative request-shape checks and response-output
inference checks; the `*-loopback.test.ts` files exercise a real local HTTP
consumer with the generated route, bearer-auth, and JSON body metadata.

## Authority and compatibility boundary

The generator matches each Rust route operation ID to the corresponding RPC
in the Rust descriptor. A missing method, duplicate route, or malformed
descriptor fails generation. Each output records the descriptor digest,
source-content digest, source kind/artifact, and the RPC identity, including
its full service path.
Actors therefore no longer use the runtime-generated descriptor as generation
input. Workers' descriptor is explicitly a bootstrap input and must not be
treated as the final source-of-truth milestone. The descriptor remains the
compatibility oracle for field numbers, JSON names, optional presence, enum
values, oneofs, and streaming direction until the structured Rust contract
model is complete for every family.

Actors and Workers' Rust HTTP clients currently establish the behavior policy
represented in the prototype: HTTPS or loopback HTTP, bearer authentication,
canonical protobuf JSON, and cumulative bounded response reads. The policy is
encoded in generated metadata so a later target runtime does not infer it from
handwritten TypeScript. Before this generator becomes a product dependency,
those policy values should be exported as typed constants by each family
crate, alongside route tables, and the prototype should consume those
constants rather than carrying the temporary bootstrap policy locally.

The existing `scripts/generate-runtime-routes.mjs` remains in place during the
parity phase. Its output is still checked by the repository's current
generation path. This prototype must first produce byte-for-byte equivalent
route tables and pass the public RPC matrix before that script is retired.

## Commands and drift behavior

Run the standalone tests and generator from the SDK worktree:

```text
cargo test --manifest-path rust/crates/sdk-typescript/Cargo.toml --locked --offline
cargo run --manifest-path rust/crates/sdk-typescript/Cargo.toml --locked --offline -- write
cargo run --manifest-path rust/crates/sdk-typescript/Cargo.toml --locked --offline -- check
```

The command defaults to
`rust/crates/sdk-typescript/prototype`. A different output directory can be
passed as the final argument. `check` fails if any generated artifact is
missing or differs. `SDK_SOURCE_REVISION` can be supplied by a release or
preview job; the default `working-tree` keeps local output deterministic.

The current tests prove that all eight Actors routes and all seven Workers
routes match a descriptor RPC and that repeated generation is deterministic.
They do not yet prove transport parity, because the generated invoker is
deliberately transport-neutral.

## Migration gates

1. Move auth, endpoint, response-limit, and error-policy constants into the
   Rust family contract exports. Keep private credential handling in the
   shared target runtime; never generate credentials or log them.
2. Add a route parity check against the existing TypeScript `routes.ts` files
   and `compatibility/public-rpc-matrix.json`. The check must compare route
   paths, operation IDs, RPC identities, method directions, and package
   exports.
3. Bind generated method metadata to the existing Actors and Workers HTTP and
   gRPC runtimes. Run the Node, Bun, browser, and service conformance suites
   before removing any handwritten method dispatch.
4. Replace only duplicated method and route declarations first. Keep the
   shared runtime's HTTPS/loopback checks, bearer header construction,
   cumulative body bounds, malformed UTF-8 handling, canonical error decoding,
   cancellation, and retry/idempotency policy until generated metadata and
   differential tests cover them.
5. Extend the model to Stream, Objects, Inference, Machines, Filesystem, and
   Harness only after it can represent their stream frames, recovery,
   cancellation, identity, capability, and archived-protocol constraints.

## Known limits

This is a remote unary-method prototype. It does not generate protobuf message
implementations, gRPC transport code, OpenAPI, stream framing, validation
logic, browser persistence, or high-level language ergonomics. Those features
need separate Rust metadata and conformance gates. In particular, Objects
upload/download, Stream follow/read, Inference watch, Filesystem hosted
operations, and Harness replay cannot be reduced to a route table without
losing backpressure, exact byte accounting, terminal evidence, cancellation,
or recovery semantics.

The intended replacement is therefore incremental: Rust-generated metadata
first, a shared target transport runtime second, and deletion of duplicated
TypeScript declarations only after drift and black-box conformance prove that
the public behavior is unchanged.
