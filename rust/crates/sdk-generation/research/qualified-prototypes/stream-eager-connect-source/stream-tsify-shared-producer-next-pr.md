# Stream shared-producer follow-up research

Status: research only. No production checkout was edited by this note.

## Provenance and scope

The frozen qualification source is `Q:/sdk/work/stream-main-port` at exact commit `ed2f7c5e0a9e588e6b7559ca1325f0b1d09caf0f`. Its Stream source closure remains the qualified `sha256:f1cd7e8afe02172fd8dfeb340c5632f1a6e0684ccbbb79c50914eb27a5dcf72d`; this research does not change that checkout, its artifact, or its receipts. This note describes a separate, future PR.

The next PR should move shared Stream response/error authority toward executable Rust and the existing `tsify` pipeline, following the Actors/Machines shared-producer shape. It should not add a second hand-authored protocol model or introduce `ts-rs` into a repository that already has a working `tsify`/wasm-bindgen generation path. `rust/crates/stream/Cargo.toml` already gates `tsify` under the `wasm` feature; no new generator dependency is needed.

## Existing shared-producer evidence

- `typescript/packages/machines/src/simulator.ts` imports generated Rust DTOs and uses the small `asPublic<Generated, Public extends Generated>` cast. The generated value crosses unchanged; the cast preserves ergonomic branded/public names while TypeScript assignment catches Rust shape drift.
- `rust/crates/machines-wasm/src/public.rs` is the producer: `Serialize + Tsify` DTOs, explicit bigint/byte/optional-field annotations, and conversion functions. This is the architecture Stream should copy.
- `rust/crates/stream/src/wasm.rs` already owns protobuf decode, scalar conversion, response oneofs, and HTTP projection, but its public functions still advertise `unknown` in `acyclic_stream_wasm.d.ts`. That is the boundary to make concrete.
- `typescript/packages/stream/src/types.ts` is currently 140 lines. Its `PublicWire` aliases and result unions are useful ergonomic wrappers, but the structural protocol fields are duplicated from generated protobuf and from Rust's existing projections.
- Current exact checkout line counts are: `memory.ts` 167, `native.ts` 279, `grpc.ts` 153, `contract.ts` 269, and `client.ts` 281. The transport adapters and untrusted-response checks remain necessary orchestration; they are not candidates for wholesale deletion.

## Dependency-complete next PR: canonical Rust error aliases

Make alias projection a core Rust operation and have both memory and native consume it.

1. Add a small core helper beside `StreamError` in `rust/crates/stream/src/lib.rs` (or a focused `error_mapping.rs` module):

   `public_stream_error_code(raw: &str, operation: &str) -> Option<&'static str>`.

   It accepts canonical wire codes and already-public aliases, maps `capacity` to `capacity_exhausted`, `already_exists` to `destination_exists`, maps `not_found` to `commit_not_found` only for `read_commit` and otherwise to `stream_not_found`, and maps `prefix_not_retained` to `invalid_argument` only for `commit`. It returns `None` for unknown values and for `commit_not_found` on a non-`read_commit` operation. Add table-driven Rust tests for every base code, each alias, and the operation-sensitive cases.

2. Use this helper in `rust/crates/stream/src/wasm.rs`. Keep `publicHttpErrorCode` as the stable JS export for hosted routes, but make it call the core helper. Add a small `publicMemoryErrorCode(raw, operation)` wasm export for the memory adapter. The existing `is_stream_error_code` remains the base-code validator; the new helper is the public projection authority.

3. Change `rust/crates/stream-napi/src/lib.rs::stream_error` to receive the operation at the N-API operation boundary and emit the already-public `code`. Keep `connect_error` and malformed protobuf decoding separate because they are configuration/transport and boundary errors, not `StreamError` aliases. Pass the operation at the existing result-producing call sites (`append`, `fork`, `commit`, `read_commit`, `read`, `follow`, `children`, `children_page`, `tail`, and idempotency/token operations). This is roughly 10-15 call-site edits plus the one mapper; it is dependency-complete because native metadata is then canonical before TypeScript sees it.

4. In `memory.ts`, replace the local alias chain in `streamError` with the generated `publicMemoryErrorCode(rawCode, operation)` result. Retain the raw-code validation and passthrough behavior for unknown/non-Stream errors. In `native.ts`, delete the local alias chain and the 24-entry `knownCodes` set; native metadata is already canonical Rust output, so only validate the metadata shape and use its public code. The gRPC mapper remains transport-specific because it maps tonic status classes and reason metadata, but its alias cases should be covered by the same parity table.

5. Add focused tests for memory and native parity: `capacity` => `capacity_exhausted`; `already_exists` => `destination_exists`; `not_found` on `read_commit` => `commit_not_found`; `not_found` elsewhere => `stream_not_found`; commit `prefix_not_retained` => `invalid_argument`; non-commit prefix retention remains `prefix_not_retained`; unknown codes remain unavailable/unchanged according to the existing adapter contract. Test the Rust core table and the generated WASM/N-API paths, not just the TypeScript helper.

This phase removes approximately 15-25 authored TypeScript lines (the duplicated native alias/known-code block and memory alias block), while adding one Rust authority used by both memory and native. The exact deletion should be reported from the implementation diff; do not count generated files as authored reduction.

## Dependency-complete next PR: Rust-generated public DTOs

After the alias phase is green, make the Rust WASM producer return concrete public DTOs. Add a `public` module in `rust/crates/stream` or keep the DTO definitions in `wasm.rs`, using the existing `tsify` feature and the Machines annotations:

- `RecordOut`, `AppendOut`, `ForkReceiptOut`, `ChildrenPageOut`;
- `CommittedMutationOut`, `CommittedEnvelopeOut`, `CommitConflictOut`, `CommitOut`;
- `IdempotencyObservationOut`, `TokenGrantOut`, and `AccessTokenOut`;
- response unions keyed by the same operation tags currently asserted in `types.ts`.

Use `#[tsify(large_number_types_as_bigints)]`, `#[tsify(type = "Uint8Array")]` for opaque bytes, and explicit `undefined`/`null` annotations where the JS contract requires them. Each DTO must be constructed from the canonical `wire_codec` conversion, so no fields are reimplemented in TypeScript. Change `projectMemoryResponse` and the HTTP projection entry points to return concrete `Tsify::JsType` values; regenerate `acyclic_stream_wasm.d.ts` and import those types from `memory.ts`, `http.ts`, and the shared type facade.

The TypeScript facade should follow the Machines `asPublic` pattern. Keep only JS-specific and ergonomic material:

- branded `CommitId`/`IdempotencyKey` constructors and path comparison;
- `Stream`, `StreamProvider`, `ProviderCommitRequest`, `CommitCondition`, and `CommitMutation` because they contain JS objects, `AbortSignal`, or runtime provider handles;
- `StreamError` plus provider/transport-only failures;
- thin `asPublic` casts where a generated DTO is structurally assignable to a branded public type.

Retain `contract.ts` request adapters, validation, cloning, byte detachment, cursor lifecycle, sequence/limit checks, and native/gRPC response guards. These are boundary/orchestration responsibilities, not duplicated shared protocol fields.

Expected authored reduction, measured against the exact frozen checkout: delete roughly 55-75 structural alias lines from `types.ts` (the `PublicWire` result/response shapes and duplicated tagged-union field declarations), and 20-35 response decode/project lines from `memory.ts` once concrete generated return types are available. The retained `types.ts` facade should be around 60-85 lines, subject to the final ergonomic API. Do not claim removal of `contract.ts` or transport checks.

## Validation gates for the implementation PR

- Rust unit tests for the public error-code table and DTO conversions.
- Regenerated WASM declarations checked into the package and verified by the package TypeScript build.
- Existing Stream 55-test suite, TypeScript/planner tests, browser/native selector tests, and the memory/native/gRPC parity cases above.
- A generated-source check that fails if the committed WASM declaration does not match the Rust producer.
- Native package qualification must continue to assert the loaded `.node` hash against the staged artifact; this research does not change the frozen ed2 receipts.

## Non-goals

Do not add `ts-rs` as a parallel generator, hand-build a second RPC/protocol stack, delete transport-specific validation, or make `types.ts` disappear wholesale. Do not alter the frozen `ed2f7c5e` source or package while prototyping this plan.

## Existing alias audit

The earlier Greptile claim that native `capacity` remains unmapped is stale/invalid: `native.ts` already maps it to `capacity_exhausted` at the qualified source. The real remaining parity defect is `memory.ts`: its local `streamError` maps `not_found`, `already_exists`, and commit `prefix_not_retained`, but leaves `capacity` raw. That is the concrete first regression the canonical Rust helper must cover.
