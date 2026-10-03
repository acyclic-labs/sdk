# Inference Rust guide

`acyclic-inference` is the customer-side Rust SDK for immutable Context
revisions, recoverable generation Runs, streamed Run events, evaluations, and
explicit warm-retention commitments. The crate owns typed request builders,
wire validation, and the descriptor-derived HTTP JSON codec.

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

The command block is the maintainer qualification matrix. The final command
checks the HTTP adapter profile. The supported caller profile enables the
native host client by default, so a consumer uses one dependency declaration:

```toml
[dependencies]
acyclic-inference = { path = "../inference", version = "=0.2.0" }
```

The TOML block is a dependency declaration for a caller and is not an
executable Rust fence. The source-owned
`examples/inference-capability-discovery.rs` is compile-checked with the
package. To query a provider, supply `INFERENCE_ENDPOINT`,
`INFERENCE_API_KEY`, and `INFERENCE_CA_PEM`; a caller uses `Inference::connect`
and the typed client methods through the default dependency declaration.

The package declaration is owned by `rust/crates/inference/Cargo.toml` and
pins version `0.2.0`; the workspace requires Rust `1.98`. Its default build
reads the package-local `inference_model_descriptor_docs.bin` generation
overlay, while `inference_model_descriptor.bin` remains the canonical model
descriptor and `inference_descriptor.bin` remains the archived runtime
handshake fixture. The overlay supplies Rust API comments to generated source;
it does not change wire fields, options, or handshake bytes.

## Capability, error, and service policy

`Inference::models` is the capability discovery surface. It returns the
service-provided `ModelCapability` records, including execution profiles,
context/output bounds, features, retention profiles, and idle-KV profiles.
Those values describe the execution profiles, bounds, features, retention
policies, and idle-KV policies supplied by the selected service.

The public host error surface is `inference::Error`: `Invalid` identifies a
locally rejected request or response shape, `Transport` identifies channel or
TLS setup failure, and `Observation` preserves a failed gRPC observation.
Run, context, warm-retention, and evaluation handles retain their caller
identities so recovery can inspect the admitted object after an uncertain
result. The current public model does not emit a shared `OperationPolicy`
message; operation behavior is expressed by these typed builders, identities,
and recovery methods.

The model contains five gRPC services and 14 RPC methods. The
`http_codec::routes` inventory lists the descriptor-derived HTTP paths and
methods. `Inference::connect` selects the default tonic gRPC transport over
authenticated HTTPS/TLS. The HTTP adapter applies the JSON mapping, while the
provider supplies authentication authority, billing, and deployment policy.
Browser HTTP/JSON selection is a separate runtime policy.

## Build a bounded local contract check

The following example constructs the public HTTP codec and descriptor-derived
route inventory for an adapter.

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

Capability discovery has a source-owned companion at
`examples/inference-capability-discovery.rs`. It calls `Inference::models` and
prints the model and policy records returned by the authenticated provider.

The codec uses paths relative to `/v1/inference/`, enforces the crate's JSON
and protobuf byte ceilings, rejects unknown fields and trailing JSON, and
encodes one value per `runs/watch` event for an adapter's NDJSON framing. It
performs JSON and protobuf validation; the service adapter adds authentication,
semantic admission, caller-bound identity checks, and stream lifecycle checks.

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
`ModelCapability.idle_kv_profiles`. The provider applies the profile's capacity,
throughput, and latency policy. Only verified reuse by an actual Run advances
the idle-use record. Fork, edit, admission, inspect, and recovery do not reset
that baseline.

`Inference::evaluation` accepts an `EvaluationSpec`, while
`CreateEvaluation::send` admits an immutable evaluation and
`Evaluation::inspect` reads it. The crate enforces candidate, case, metric,
digest, rational, and result bounds through `validate_customer_wire` and the
evaluation limits exported from the crate. The configured provider performs
grader execution and model access.

## Legacy topic coverage

The website ledger contains `/docs/inference` plus a dynamic slug route backed
by 19 legacy data pages. The Rust source maps those topics to concrete APIs:

| Legacy topic family | Rust authority |
| --- | --- |
| `overview` | `README.md`, `Inference::connect`, `Inference::context` |
| `quickstart` | `README.md`, `Inference::connect`, `examples/inference-capability-discovery.rs` |
| `contexts` | `Context`, `ContextMutation`, `wire::ContextView` |
| `editing` | `Context::edit`, `ContextMutation`, `wire::Edit` |
| `forks` | `Context::fork`, `ContextMutation`, `wire::ForkContextRequest` |
| `generation` | `Context::generate`, `GenerateRun`, `RunEvents` |
| `operations` | Typed operation builders, operation identities, `Inference::recover_run` |
| `retention` | `Retention`, `RetainWarm`, `WarmContext` |
| `kv` | `WarmView::idle_kv`, `wire::IdleKvProfile` |
| `models` | `Inference::models`, `wire::ModelCapability` |
| `reasoning` | `wire::ModelCapability.execution_profile`, model feature records |
| `anthropic` | `Inference::models`, provider model identifiers and capability records |
| `openai` | `Inference::models`, provider model identifiers and capability records |
| `billing` | Model capability records and provider policy returned with discovery |
| `security` | `Inference::connect`, trusted CA input, `validate_customer_wire` |
| `structured-output` | Generated `wire` messages and `validate_customer_wire` |
| `tools` | Typed request builders, model feature records, and wire validation |
| `transfer` | `Context::transfer`, `ContextMutation`, model profile identity |
| `reference` | Generated `wire` modules, `http_codec::routes`, `decode_http_request`, `encode_http_response` |

This table maps each legacy topic to its Rust authority. `Inference::connect`
validates the supplied HTTPS endpoint and caller CA material, while the selected
provider supplies model access, authentication, billing, and service policy.

