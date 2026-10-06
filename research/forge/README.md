# Forge bounded prototype

This directory records a bounded run of Cloudflare Forge against the
Rust-derived Actors OpenAPI projection. The upstream checkout and generated
JSON are under ignored `target/` paths; they are deliberately not vendored.

## Pinned inputs

- Cloudflare Forge: `cloudflare/forge` at
  `86cb1ef3047abc7441d96c894e8cd35e826fa8e5` (the `main` revision resolved
  on 2026-10-03), package `@cloudflare/forge` `0.1.0`.
- Forge runtime: Node `v24.4.1`, pnpm `10.13.1`; the checkout declares Node
  `>=22` and pnpm `10.27.0`.
- Rust projection: `rust/crates/sdk-openapi-prototype`, generated from the
  Actors descriptor and Rust-owned route table into
  `rust/crates/sdk-openapi-prototype/target/actors.openapi.json`.

## Commands and results

The Forge package dependencies were installed only inside the ignored
upstream checkout:

```text
pnpm install --filter @cloudflare/forge... --ignore-scripts --frozen-lockfile
pnpm --filter @cloudflare/forge test
```

The pinned Forge resolver test suite passed: 36 tests, 0 failures. The Rust
projection passed its 3 unit tests and produced an approximately 25 KiB
OpenAPI 3.0.3 document with eight Actors routes.

The resolver was then run directly against that generated document. It
resolved `createActor`, `inspectActor`, and `invokeActor` to their POST paths,
request-body references, and `200`/`default` responses. A second in-memory
run supplied temporary Forge command metadata and descriptions and built one
`actors` command with all eight methods. This proves the useful resolver and
metadata layer without treating Forge as the contract author.

The first metadata run intentionally omitted operation descriptions and
failed with Forge's validation errors for all eight methods. Adding temporary
descriptions made the run pass. This is an actionable gap: Rust comments or
Rust-owned documentation metadata must be projected before Forge's docs
pipeline can consume the output without a separately authored overlay.

The official Fern path was not executed. Docker Desktop's client is present,
but `docker info` cannot connect to the Linux daemon (`dockerDesktopLinuxEngine`
is unavailable). Forge's `cloudflare-fern-config` script requires a running
Docker daemon and downloads generator images, so no SDK package or website
artifact is claimed from that path.

## Reproduction

From the SDK worktree, run the Rust projection first:

```text
cargo test --manifest-path rust/crates/sdk-openapi-prototype/Cargo.toml
cargo run --manifest-path rust/crates/sdk-openapi-prototype/Cargo.toml -- rust/crates/sdk-openapi-prototype/target/actors.openapi.json
```

Then install the pinned Forge checkout under `target/forge-upstream` and run
its package tests. The one-line resolver probes used in this run are captured
below; they intentionally read the generated file and do not modify tracked
source:

```text
cd target/forge-upstream
node --import tsx --input-type=module -e "import { readFileSync } from 'node:fs'; import { populateOperationMap, resolveOperation } from './packages/forge/index.ts'; const d=JSON.parse(readFileSync('../../rust/crates/sdk-openapi-prototype/target/actors.openapi.json','utf8')); populateOperationMap(d); for (const id of ['createActor','inspectActor','invokeActor']) { const op=resolveOperation(id); console.log(JSON.stringify({id,path:op?.path,method:op?.method,requestBodyRef:op?.requestBodyRef,responses:Object.keys(op?.responses??{})})); }"
```

For Forge's command/docs metadata validation, add temporary
`x-fern-sdk-group-name`, `x-fern-sdk-method-name`, and `description` fields in
memory, then call `initFromOpenApi(document)` and inspect `methodMap()`.
Descriptions are required by the validator; this is why the initial probe
fails before the Rust projection owns that metadata.

The dependency-free preflight is checked in as `research/forge/probe.py`. It
is deliberately a comparison harness rather than product tooling: it reports
operation-description coverage, unresolved schema references, response
presence, protobuf RPC identities, provenance, and counts for protobuf
JSON/presence/oneof/enum extensions.

```text
python research/forge/probe.py rust/crates/sdk-openapi-prototype/target/actors.openapi.json --strict
python -m unittest discover -s research/forge -p 'test_probe.py' -v
```

The current Actors projection exits non-zero in strict mode because its eight
operations have no descriptions. It still reports all eight RPC identities,
44 local schema references, and the expected protobuf extension counts. The
unit tests cover a valid document, empty descriptions, a bad `$ref`, and a
missing RPC identity. This captures a useful Forge precondition without
installing or making the TypeScript package part of the product core.

The Rust contract model now contains method documentation metadata, but the
OpenAPI emitter has not projected it into operation `description` fields yet;
the strict failure is therefore a precise handoff to the OpenAPI/docs owner.

## Boundary

Forge currently consumes OpenAPI. The Actors projection remains derived from
Rust-owned protobuf descriptors and routes. Forge can be a downstream HTTP
SDK/docs surface after the projection gains Rust-owned descriptions,
availability, status mappings, and examples. It cannot replace protobuf or
gRPC generation for stream framing, cancellation, recovery, oneof/presence,
or wire compatibility.
