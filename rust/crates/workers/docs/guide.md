# Workers v1 Rust guide

`acyclic-workers` owns the Workers v1 validation rules, wire descriptor, and
the [JavaScript module contract](/rust/crates/workers/REFERENCE.md#acyclic_workers-module_typescript_contract). A published module is identified by the exact
SHA-256 of its bytes. A durable job resolves a deployment alias or exact digest
at acceptance and retains that selected version for retries.

## Validate an immutable publication

[`validate_publish`](/rust/crates/workers/REFERENCE.md#acyclic_workers-validate_publish) binds the request to the exact JavaScript bytes. A changed byte
fails with `ContractError::DigestMismatch`; a module larger than the one MiB
bound fails with `ContractError::LimitExceeded`.

```rust
use acyclic_workers::{validate_publish, wire};
use sha2::{Digest, Sha256};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let module = br#"export default { fetch() { return new Response('ok') } }"#;
    let request = wire::PublishVersionRequest {
        javascript_module: module.to_vec(),
        expected_sha256: Sha256::digest(module).to_vec(),
        idempotency_key: "publish-example-v1".into(),
    };
    validate_publish(&request)?;
    Ok(())
}
```

[`validate_select`](/rust/crates/workers/REFERENCE.md#acyclic_workers-validate_select) accepts an absent expected revision for creating an alias and
requires a positive current revision when changing one. Each accepted
selection advances the revision. `validate_submit` checks the target, input
payload, retry count, output budget, and resource limits. Object inputs name a
bucket and key; acceptance retains the selected bytes privately so replacement
of the key cannot change a retry's input. `validate_result` bounds the exact
job body by the accepted output budget.

## Module and service contract

An ES module may export `default.fetch(request)` for ordinary HTTP invocation,
`default.run(input, { jobId, attempt, signal })` for durable jobs, or both.
`attempt` starts at one. Cancellation signals the handler but cannot undo
external effects. The Rust-owned `MODULE_TYPESCRIPT_CONTRACT` constant is the
source for generated TypeScript declarations.

`acyclic_workers::connect(endpoint, token)` selects the verified native gRPC
transport when available and the verified HTTP contract otherwise. The
`grpc::connect` module is native-only. `connect_with_ca_certificate` pins a
private CA for the native connection; browser builds use browser trust and
reject caller-provided CA bytes. The HTTP client exposes these seven operations:

* `v1/workers/versions/publish`
* `v1/workers/deployments/select`
* `v1/workers/jobs/submit`
* `v1/workers/jobs/inspect`
* `v1/workers/jobs/cancel`
* `v1/workers/versions/{sha256hex}/invoke`
* `v1/workers/deployments/{alias}/invoke`

The authoritative route list is [`acyclic_workers::HTTP_ROUTES`](/rust/crates/workers/REFERENCE.md#acyclic_workers-http_routes). HTTP mutations
are not automatically retried. A successful HTTP invocation is ambiguous from
the caller's point of view; durable job acceptance is the separate operation
that creates a retained retry contract.

<!-- acyclic-guide-scenario: workers-publish-roundtrip -->
```rust
// capability: supported
use acyclic_workers::{validate_publish, wire};
use sha2::{Digest, Sha256};

let module = "export default { fetch() { return new Response('ok') } }";
let request = wire::PublishVersionRequest { javascript_module: module.as_bytes().to_vec(), expected_sha256: Sha256::digest(module.as_bytes()).to_vec(), idempotency_key: "publish-example-v1".into() };
validate_publish(&request)?;
```
