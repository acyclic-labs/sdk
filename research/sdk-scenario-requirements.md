# Rust scenario requirements

The registry in `rust/crates/sdk-generation/src/scenarios.rs` is the source of
truth. Each row below records the source closure and execution requirement for
the registered scenario; generated TypeScript consumers may only project fields
that are present in the executable Rust output.

| Scenario | Rust source | Mode | Required input | Consumer output |
| --- | --- | --- | --- | --- |
| `actors/transport-conformance-unary` | `rust/crates/actors/examples/transport-conformance.rs` | endpoint | endpoint, HTTP endpoint, CA certificate, bearer token on stdin | transport receipt; no local TypeScript projection |
| `actors/typescript-consumer` | `rust/crates/actors/examples/actors-typescript-consumer.rs` | local | canonical Rust `CreateActorRequest` values and validation result | generated Actors protobuf request encoder |
| `stream/http-conformance-streaming` | `rust/crates/stream/examples/http-conformance.rs` | endpoint | declared stream qualification service | transport receipt; no local TypeScript projection |
| `stream/typescript-consumer` | `rust/crates/stream/examples/stream-typescript-consumer.rs` | local | canonical Rust memory append/read values and retry identity | generated Stream memory consumer executed against local WASM |
| `filesystem/embedded-workspace` | `rust/crates/filesystem/examples/embedded_workspace.rs` | local | self-contained Rust workspace fixture | local Rust behavior receipt |
| `machines/typescript-consumer` | `rust/crates/machines/examples/machines-typescript-consumer.rs` | local | canonical Rust create request and page-size value | generated Machines provider consumer |

Every source closure also includes the owning package manifest and workspace
lockfile. Endpoint scenarios remain receipt-only until their declared service
is available; they must not be projected from hand-authored TypeScript data.
