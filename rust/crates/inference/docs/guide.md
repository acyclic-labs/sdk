# Inference Rust guide

`acyclic-inference` is the customer-side Rust SDK for immutable Context
revisions, recoverable generation Runs, streamed Run events, evaluations, and
explicit warm-retention commitments. The crate owns typed request builders,
wire validation, and the optional descriptor-derived HTTP JSON codec. It does
not claim that a model, endpoint, billing account, or hosted service is
available.

## Package and source qualification

The workspace package is `acyclic-inference` version `0.2.0`. The source
checkout is the authority for this guide. Qualify a checkout with the exact
revision, lockfile, and feature set that produced the guide:

```text
git rev-parse HEAD
cargo metadata --locked --no-deps --format-version 1 --manifest-path rust/crates/inference/Cargo.toml
cargo check --locked --all-targets --manifest-path rust/crates/inference/Cargo.toml
cargo test --locked -p acyclic-inference
cargo test --locked -p acyclic-inference --features http-codec --test http_codec
```

The default feature is `host`; `http-codec` is independent and can be enabled
without the host transport. For a local workspace consumer, use a path
dependency so the source revision is explicit:

```toml
[dependencies]
acyclic-inference = { path = "../inference", version = "=0.2.0", default-features = false, features = ["http-codec"] }
```

The TOML block is a dependency declaration for a caller and is not an executable Rust fence. The source-owned `examples/inference-capability-discovery.rs` is compile-checked with the package, but its execution requires `INFERENCE_ENDPOINT`, `INFERENCE_API_KEY`, and `INFERENCE_CA_PEM` for a real authenticated customer service; local qualification intentionally does not invoke it.

The package declaration is owned by `rust/crates/inference/Cargo.toml` and
pins version `0.2.0`; the workspace requires Rust `1.98`. Its default build
reads the package-local `inference_model_descriptor_docs.bin` generation
overlay, while `inference_model_descriptor.bin` remains the canonical model
descriptor and `inference_descriptor.bin` remains the archived runtime
handshake fixture. The overlay supplies Rust API comments to generated source;
it does not change wire fields, options, or handshake bytes.

The workspace metadata permits publication, but these source checks do not
prove that a matching registry artifact or hosted endpoint exists. A release
receipt must bind the final source revision, package version, lockfile, feature
set, and artifact digest before changing the dependency to a registry
instruction.

## Capability, error, and service policy

`Inference::models` is the capability discovery surface. It returns the
service-provided `ModelCapability` records, including execution profiles,
context/output bounds, features, retention profiles, and idle-KV profiles.
Those values are response evidence for a caller's selected service; they do
not establish that a named model or endpoint is available before a successful
authenticated call.

The public host error surface is `inference::Error`: `Invalid` identifies a
locally rejected request or response shape, `Transport` identifies channel or
TLS setup failure, and `Observation` preserves a failed gRPC observation.
Run, context, warm-retention, and evaluation handles retain their caller
identities so recovery can inspect the admitted object after an uncertain
result. The current public model does not emit a shared `OperationPolicy`
message; operation behavior is expressed by these typed builders, identities,
and recovery methods.

The model contains five gRPC services and 14 RPC methods. The optional
`http_codec::routes` inventory is a local descriptor projection of those
methods; it does not mean an HTTP endpoint is mounted. `Inference::connect` selects the default tonic gRPC transport over authenticated HTTPS/TLS. The `http-codec` feature is an optional local adapter projection and does not override the remote client transport. The native Rust client currently qualifies only gRPC, so `Inference::connect` has no alternate native transport override; browser HTTP/JSON selection is a separate runtime policy. Deployment,
authentication authority, billing, and service availability remain outside
the crate's source qualification.

## Build a bounded local contract check

The following example uses only the public HTTP codec and descriptor-derived
route inventory. It does not connect to a service or imply that the routes are
mounted remotely.

Compile this exact fence with the path declaration above and a locked
workspace or package lock before executing it. The receipt for a packaged
consumer should retain the archive, extracted `Cargo.lock`, fence bytes, and
the command output together.

```rust
use acyclic_inference::http_codec::routes;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let routes = routes()?;
    assert_eq!(routes.len(), 14);
    assert!(routes.iter().any(|route| route.path == "runs/watch"));
    assert!(routes
        .iter()
        .all(|route| !route.method.is_client_streaming()));
    Ok(())
}
```

Capability discovery has a source-owned companion at `examples/inference-capability-discovery.rs`. It calls `Inference::models` and prints the service response as evidence for that authenticated endpoint; it does not claim a named model or deployment exists.

The codec uses paths relative to `/v1/inference/`, enforces the crate's JSON
and protobuf byte ceilings, rejects unknown fields and trailing JSON, and
encodes one value per `runs/watch` event for an adapter's NDJSON framing. It
does not perform authentication, semantic admission, caller-bound identity
checks, or stream lifecycle checks; the service adapter remains responsible
for those checks.

## Context revisions and recoverable Runs

`Inference::context(model)` creates a `CreateContext` builder. Add text with
`instructions` or a generated `wire::Item`, call `create`, and retain the
returned `Context`. A Context handle identifies an immutable revision. Its
`fork`, `edit`, `append`, `truncate`, `compact`, `release`, and `transfer`
builders admit new revisions and return receipts; they do not mutate a prior
revision in place. `Context::generate` admits a recoverable `Run`. Save the
run identity from `GenerateRun::id` before waiting on output so an interrupted
caller can use `Inference::recover_run`.

`Run::inspect` reads the latest validated view. `Run::watch(from_sequence)`
consumes ordered events, and `Run::cancel` requests cancellation. A watch
consumer must retain sequence order and require a terminal event. The public
`WatchRunState` and `watch_run_start_state_wire` helpers provide the same
bounded cursor and terminal checks for a non-host adapter.

## Warm retention and evaluations

`Context::retain` creates a separate warm commitment. `Retention::warm_until`
and `Retention::idle_kv` represent distinct policies. A `WarmContext` can be
inspected, renewed, or released through its own builders. The idle KV policy
is an additive source contract: its opaque profile comes from
`ModelCapability.idle_kv_profiles`; it provides no capacity, throughput, or
latency guarantee. Only verified reuse by an actual Run advances the idle-use
record. Fork, edit, admission, inspect, and recovery do not reset that
baseline.

`Inference::evaluation` accepts an `EvaluationSpec`, while
`CreateEvaluation::send` admits an immutable evaluation and
`Evaluation::inspect` reads it. The crate enforces candidate, case, metric,
digest, rational, and result bounds through `validate_customer_wire` and the
evaluation limits exported from the crate. Grader execution and model access
remain service-owned.

## Legacy topic coverage and availability boundary

The website ledger contains `/docs/inference` plus a dynamic slug route backed
by 19 legacy data pages. The Rust source maps those topics to concrete APIs:

| Legacy topic family | Rust authority |
| --- | --- |
| overview and quickstart | `README.md`, `Inference::connect`, `Inference::context` |
| immutable context revisions | `Context`, `ContextMutation`, `wire::ContextView` |
| runs, recovery, and streaming | `GenerateRun`, `Run`, `RunEvents`, `WatchRunState` |
| warm retention | `Retention`, `RetainWarm`, `WarmContext` |
| evaluations | `wire::EvaluationSpec`, `CreateEvaluation`, `Evaluation` |
| HTTP and JSON transport | `http_codec::routes`, `decode_http_request`, `encode_http_response` |

This table preserves discovery and topic ownership; it does not mark the
legacy website pages as migrated. `Inference::connect` validates the supplied
HTTPS endpoint and caller CA material, but transport construction is not
evidence of deployment availability. Keep model access, authentication,
billing, and service qualification separate from local Rust validation and
route inventory.

