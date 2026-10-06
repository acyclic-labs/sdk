# Inference v1 Rust contract model

Inference v1 is authored by the Rust model in
[`rust/crates/sdk-contract-wire/src/inference.rs`](../../rust/crates/sdk-contract-wire/src/inference.rs).
The archived protobuf and runtime descriptor are compatibility evidence only;
normal generation starts from `INFERENCE` and does not parse the active `.proto`
file.

## Wire surface

`INFERENCE` emits `inference/v1/inference.proto` in package
`inference.customer.v1`, with the validation-options dependency and Go package
`github.com/acyclic-labs/sdk/go/gen/inference/v1;inferencev1`.

The model contains 56 messages, 6 enums, and five services with 14 RPCs:

- `ModelsService.List` advertises model capabilities and retention profiles;
- `ContextsService` creates, inspects, and mutates immutable canonical context
  content;
- `WarmContextsService` retains, inspects, renews, and releases customer warm
  context commitments;
- `RunsService` generates, inspects, watches, and cancels recoverable logical
  runs; and
- `EvaluationsService` admits and inspects content-addressed evaluation work.

`RunsService.Watch` is server streaming. The Rust route table contains 14
explicit `POST` projections under `/v1/inference/`; route entries retain the
fully qualified RPC, request, response, operation path, and operation prose.
The paths are transport projections and do not replace the gRPC service
identity.

The model preserves protobuf presence and choice semantics. Synthetic oneofs
remain attached to optional fields such as `IdleKvRetention.last_run_id`,
`RenewWarmRequest.idle_timeout_ms`, `GenerateRunRequest.seed`, and
`RunView.result`. Real oneofs encode context actions, provenance origins, run
events, and evaluation results. `ExactRational.numerator` is signed zig-zag
(`sint64`) and must remain distinct from ordinary `int64` in descriptor output.

## Validation options

`INFERENCE_OPTIONS` records the 138 field, oneof, enum-value, and method
assignments from the validation contract. `inference_raw_options(subject)`
converts one subject’s assignments through the shared typed option registry;
the helper rejects unknown identities, duplicate extensions, and scalar-width
mistakes. Method `http_path` options and field constraints therefore remain
Rust-owned metadata and are not inferred from route spelling.

The executable option check is:

```powershell
cargo test --manifest-path rust/crates/sdk-contract-wire/Cargo.toml inference_custom_options_are_retained_and_typed
```

The compatibility and route checks are:

```powershell
cargo test --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --test inference_model
```

The golden test removes compiler source locations from the immutable runtime
descriptor in memory before comparison. It never changes the fixture and never
reads the active protobuf to fill a missing model field.

## Availability semantics

The wire contract exposes logical content, verified retention, recoverable
generation, and exact evaluation artifacts. Placement, workers, allocation,
migration, cache cleanup, and grader execution details remain private. A
client must preserve request identities, content digests, optional presence,
oneof choices, and terminal run events when projecting this contract to another
SDK or HTTP surface.

The designated shared-core owner must route the Inference documentation tables
through `ContractSpec::render_proto`, add `sint64` to `FieldType`, and emit the
validation extension bytes plus dependency descriptors. Those are shared-core
integration tasks; this model owns the contract data and compatibility tests.
