# Workers Rust contract model

The Workers v1 model is implemented in
[`rust/crates/sdk-contract-wire/src/workers.rs`](../../rust/crates/sdk-contract-wire/src/workers.rs).
It is a structured Rust input for descriptor, OpenAPI, SDK, and documentation
exporters. The checked-in `proto/workers/v1/workers.proto` remains a one-time
migration and golden compatibility oracle; normal generation must not parse it.

## Contract inventory

The model is `WORKERS: ContractSpec` with:

- file identity `workers/v1/workers.proto`, package `acyclic.workers.v1`,
  proto3 syntax, and Go package
  `github.com/acyclic-labs/sdk/go/gen/workers/v1;workersv1`;
- 24 messages: version/deployment publication, object-backed payloads, job
  admission/observation/cancellation, invocation, and typed errors;
- `JobState` values 0 through 5 and `ErrorCode` values 0 through 10 with the
  exact protobuf names and numbers;
- seven unary RPCs on `WorkersService`, each with explicit input/output
  message names and streaming flags;
- seven hosted HTTP routes in `WORKERS_ROUTES`, linked to those RPCs with
  method, path, operation ID, request/response types, and Rust-owned prose:
  publish, deployment selection, job submit/inspect/cancel, and version or
  deployment invocation;
- reserved field number/name identities for `ObjectRef` (`3`, `version_id`)
  and `JobResult` (`2`, `object_version`);
- explicit proto JSON field names, field numbers, cardinality, scalar/message/
  enum types, and presence metadata.

The two real oneofs are `Payload.source` (`inline_bytes` or `object`) and
`JobTarget.target` (`deployment_alias` or `version_sha256`). The optional
scalar fields retain synthetic oneofs and explicit presence:

| Field | Tag | Presence marker |
| --- | ---: | --- |
| `SelectDeploymentRequest.expected_revision` | 3 | `_expected_revision` |
| `InvokeResponse.resolved_revision` | 5 | `_resolved_revision` |

The model exports documentation tables for service, message, enum, method,
and field prose. Exporters should use those tables when adding rustdoc,
protobuf comments, OpenAPI operation/schema descriptions, package README
content, and website API reference pages. This keeps the prose beside the
wire identity and leaves the designated `lib.rs` owner free to add the module
export and shared documentation dispatch without overwriting concurrent work.

## Compatibility checks

The first integration should compare `workers_descriptor()` with a descriptor
compiled once from `proto/workers/v1/workers.proto`, checking:

1. file name, package, syntax, dependency list, and Go package option;
2. message and enum sets, field tags, JSON names, scalar types, cardinality,
   reserved ranges/names, oneof membership, synthetic presence, and enum
   numbers;
3. service method names, input/output identities, and streaming flags.

The source file must not become a generation dependency after this oracle
comparison. Changes to a published Workers contract should update the Rust
model first, regenerate all descriptors and derived surfaces, and run drift
checks against archived protocol fixtures.

## Export handoff

The model intentionally does not edit `src/lib.rs`. The designated exporter
owner should add `pub mod workers;` and re-export `WORKERS`,
`WORKERS_SERVICE`, `WORKERS_ROUTES`, and `workers_descriptor`/`workers_proto`
as appropriate.
The shared `ContractSpec::render_proto` currently supplies generic fallback
comments for names it does not know; the exporter owner should route the
`WORKERS_*_DOCS` tables into the common documentation emitter so generated
Workers comments use the Rust-owned prose.

The protobuf descriptor remains transport-neutral, while `WORKERS_ROUTES` is
the explicit hosted HTTP projection. Durable job retry, cancellation,
idempotency, alias compare-and-swap, and invocation resolution are contract
semantics documented beside the fields and RPCs; a remote SDK facade must
preserve them rather than infer behavior from an OpenAPI projection. The two
parameterized paths retain their wire identities: `sha256hex` is derived from
the immutable version digest and `alias` is validated before substitution.
