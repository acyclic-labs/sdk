# acyclic-inference

Customer-side Inference SDK for immutable Context revisions, recoverable Runs, streamed events, and explicit warm retention.

Install the package:

```sh
cargo add acyclic-inference
```

Connect with `client::Client::connect(endpoint, token)`. The Rust facade selects
authenticated gRPC on native targets and HTTP/JSON in browsers, verifies the
Rust-owned handshake before application calls, and keeps the typed operation
surface identical across targets. Use `connect_with_ca` only when a native
provider uses a private CA. Create or attach a Context, then use the typed
operation builders to fork, edit, generate, and retain.

## Idle KV retention

Use `context.retain(Retention::idle_kv(profile, idle_timeout_ms))` and
`warm.renew_idle(idle_timeout_ms)`. Discover opaque policy profiles from
`ModelCapability.idle_kv_profiles`.

Verified Run reuse of the pinned revision or descendant prefix advances
last-use. Fork, edit, admission, inspect, and recovery preserve that baseline.
Renewal changes the timeout from the last verified-use baseline, or the initial
verified pin time before first use. Released or expired pins require a new
retain identity. Clone the admission or renewal builder to retry the same
operation; inspect reports the current lifecycle. `WarmView.idle_kv` separates
initial pin time from optional actual-use time and its authoritative Run identity.

## HTTP service adapters

The `http_codec` module supplies `routes`, `decode_http_request`, and
`encode_http_response`. Routes are descriptor-derived paths relative to
`/v1/inference/`; request and response JSON use the standard protobuf mapping.
The codec bounds wire and JSON bytes, rejects unknown request fields and trailing
JSON, and emits one JSON value per `runs/watch` event for NDJSON framing.
Service adapters handle authentication, semantic admission, caller-bound
validation, and stream lifecycle checks.

The [Rust guide](docs/guide.md) maps the topics to these APIs.
`examples/inference-capability-discovery.rs` demonstrates `client::Client::list`;
provide the endpoint and credential to run it.

See the [Rust API](https://docs.rs/acyclic-inference/latest/acyclic_inference/),
[repository example](https://github.com/acyclic-labs/sdk/blob/main/README.md),
and [customer protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/inference).
