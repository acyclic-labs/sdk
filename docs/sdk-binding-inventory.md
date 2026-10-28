# SDK transport and binding inventory

The Actors service declaration in [`contract_definitions.rs`](../rust/crates/actors/src/contract_definitions.rs) owns RPC names and request/response types. Its descriptor is exported as `FILE_DESCRIPTOR_SET`; the Actors metadata producer consumes that descriptor for TypeScript. The Rust semantic client admits each request before transport and admits each response before returning it. Native and browser bridges retain that path.

| Producer or transport | Callers | Payload and retained admission |
|---|---|---|
| Actors semantic client | Rust callers, Actors NAPI and WASM | Generated protobuf bytes; nominal and operation-specific admission; lossless status/detail metadata |
| Actors HTTP client | Rust HTTP callers | Canonical Protobuf JSON through `acyclic-protobuf-json`; original bounded complete-body read and typed service errors |
| Inference HTTP codec | HTTP route adapters | Canonical descriptors and shared Protobuf JSON; JSON and wire ceilings before allocation/publication, with the original bounded response writer |
| Stream gRPC client | Rust and Stream NAPI | Canonical Stream wire codec and session revision tracking; finite read admission retains the complete `ReadRequest` |
| Stream NAPI finite cursor | TypeScript native read iterator | One yielded record per pull; terminal errors remain errors; close releases the cursor and credential lifetime |
| Stream HTTP codec | TypeScript HTTP transport and hosted HTTP adapters | Existing custom request/options shapes and decimal/base64 fields; canonical cursor validation belongs to the provider |
| Machines WASM HTTP codec | Machines browser callers | Existing tagged JSON, including `$bigint`, maps and byte arrays; this protocol differs from Protobuf JSON |

The Stream and Machines HTTP formats require an explicit protocol composition before substituting the Protobuf JSON helper. Sharing a JSON implementation cannot change their accepted shapes, enum handling, integer precision, error classification or size checks.

All twelve Actors operations retain their original Rust, HTTP, NAPI and WASM entries: DeployNamespace, DeployWorker, InvokeWorker, DeleteWorker, UpdateNamespace, InspectActor, AddSubscription, RemoveSubscription, ResumeSubscription, CheckpointActor, InvokeActor and DeleteAll. The NAPI and WASM templates share wrapper behavior within each bridge. Their method/type tables match the canonical service declaration; they are still explicit ABI projections rather than a second service definition. Native producer output and WASM declarations preserve all twelve exported names. These source and producer checks establish neither RPC runtime parity nor performance equivalence.

Existing controls retain the behavioral boundaries:

- `every_nonzero_uint64_keeps_its_decimal_string_and_exact_wire`, `bytes_use_base64_and_each_refusal_stage_stays_distinct` and `caller_writer_keeps_its_ceiling_and_wire_refuses_before_output` cover the shared JSON codec's scalar and failure stages.
- Inference's `every_declared_rpc_has_a_native_json_boundary`, `watch_cursor_and_events_preserve_uint64_bytes_and_enums` and `malformed_unknown_and_oversized_inputs_fail_closed` retain route, enum, byte, integer and ceiling coverage.
- Actors' credential controls retain expiry before credential work and cancellation between a ready credential and transport sending. Raw status/detail regressions and unknown-enum admission remain separate from wrapper generation.
- Stream's shared `verify_read_open_failures` retains canonical invalid-cursor admission. Cursor/drop and terminal-error controls retain early exit and prefix-followed-by-error behavior.

The native Stream tail adapter still needs a complete remote `TailResponse` projection: accepted, durable-through and applied fields must survive one validated unary call with the original revision floor. Reconstructing a response from the scalar tail or issuing a second observation does not preserve that contract.

The legacy timer scheduling callbacks and JavaScript record encoder are retired. Logical timer identities and deadlines belong to the artifact checkpoint, with one generation-fenced actor alarm for the earliest pending deadline. The Rust timer codec, its remaining layout/diagnostic consumers, generator and independent proofs remain until their atomic retirement. See [the alarm contract](actors-alarm-migration.md).

Whole-operation deadline and cancellation adoption must retain each protocol's settlement rules. A stopped wait does not cancel a committed mutation; a lost response retains Unknown and its original identity. Typed nonretryable refusals cause zero retries. See [the caller-context contract and measurement plan](client-operation-context.md) for the remaining transport parity and performance controls.
