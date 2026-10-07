# Published snippet ownership audit

This checkout's public practical examples are package-owned README and test
flows. The executable registry now binds the Rust-owned flows to the generated
consumer projections listed below.

| Family | Current public flow | Rust executable owner | Generated consumer |
| --- | --- | --- | --- |
| Workers | `typescript/packages/workers/README.md`, HTTP/grpc client tests | `rust/crates/workers/examples/workers-typescript-consumer.rs` | `workers/typescript-consumer` |
| Objects | `typescript/packages/objects/README.md`, v2 lifecycle/conformance tests | `rust/crates/objects/examples/objects-typescript-consumer.rs` | `objects/typescript-consumer` |
| Inference | `typescript/packages/inference/README.md`, client and idle-kv tests | `rust/crates/inference/examples/inference-typescript-consumer.rs` | `inference/typescript-consumer` (contract defaults; no local inference service) |
| Actors | generated actor request consumer plus authenticated transport fixture | `rust/crates/actors/examples/actors-typescript-consumer.rs`; `transport-conformance.rs` | `actors/typescript-consumer` (request-only public protobuf schemas; no fabricated endpoint call) |
| Stream | generated memory append/read consumer | `rust/crates/stream/examples/stream-typescript-consumer.rs` | `stream/typescript-consumer` |
| Machines | generated create/list consumer | `rust/crates/machines/examples/machines-typescript-consumer.rs` | `machines/typescript-consumer` |
| Harness | seven low-level policy examples | `rust/crates/harness/examples/*.rs` | Rust receipt only; no matching public package operation, so no hand-authored TS mirror |
| Pi | `typescript/packages/pi/README.md` and adapter tests | shared file descriptor/policy validation comes from Rust-owned Harness WASM; Pi wire projection remains intentionally TS-owned | no separate Rust Pi projection; adapter is qualified by its TS type tests |
| Plugin | `plugin/README.md` CLI/install/service flows | `plugin/src/main.rs` (`plugin/cli-help`) and its integration fixtures | no TS model consumer; host CLI behavior stays Rust-owned |
| Native runtime | `rust/crates/native-runtime/README.md` and Rust backend tests | `rust/crates/native-runtime/examples/native-runtime-positional-io.rs` (`native-runtime/positional-io`) plus `src` | no public TS facade; low-level host I/O is executable Rust-only and is not rendered as a language snippet |

The registry and projection receipts are in `generated/execution-catalog.json`
and `generated/projection-catalog.json`; package qualification details are in
`qualification.json`. Exact archive hashes and Rust/source/output bindings for
all six generated consumers are in `package-archive-receipts.json`.

## Registry coverage

The 19 source-closed entries are: `actors/transport-conformance-unary`,
`actors/typescript-consumer`, `stream/http-conformance-streaming`,
`stream/typescript-consumer`, `filesystem/embedded-workspace`,
`machines/typescript-consumer`, `workers/module-contract`,
`workers/typescript-consumer`, `objects/typescript-consumer`,
`inference/typescript-consumer`, `plugin/cli-help`,
`native-runtime/positional-io`, and the seven `harness/*` policy examples.
The two endpoint entries are source-validated and compile-qualified, then
executed automatically by the same Rust generation gate against task-local
authenticated fixtures; their fixture commands, script hashes, outputs, and
no-production-service scope are recorded in `endpoint-qualification.json`.
`generated/execution-catalog.json` contains all 19 passed executions (17 local
and 2 endpoint). All six package consumer projections
pass strict TypeScript compilation and Bun runtime qualification against
extracted 0.2.0 package archives. Only those six entries have TypeScript
projections.

Pi is not an absent authority: Harness WASM owns the shared file descriptor,
length, digest, and projection-policy checks used by the adapter. Pi’s model
wire projection and event mapping are deliberately TypeScript-owned, as stated
by `typescript/packages/pi/src/index.ts`, so adding a Rust Pi scenario would
duplicate that semantic layer.
