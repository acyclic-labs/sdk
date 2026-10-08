# Changelog

All notable changes to Acyclic are recorded here. Every Rust crate, npm package,
and coding-agent integration in this repository shares one version and one
release commit.

## 0.2.0 - Unreleased

- Move the public Objects SDK, Stream contract and native Filesystem Objects composition to the sole v1 contract. Remove historical clients, readers and compatibility artifacts. Local roots with obsolete format discriminators fail closed without upgrade or overwrite.
- Complete the public Actors v1, Workers v1 and Stream v1 transport surfaces under the unified breaking candidate version. Publication and live Cloud acceptance require separate qualification.
- Objects downloads and Stream reads carry codec frames. A `GetObjectResponse` body is a `Body { codec, data, decoded_length }`, and a Stream `ReadResponse` (Read and Follow) is `{ codec, data, decoded_length }` whose data decodes to a `RecordBatch`. `codec` is `CODEC_ZSTD = 1` or `CODEC_NONE = 2`; zero is invalid; every client checks the declared length before decompressing (Objects: 64 KiB for a plain frame, 8 MiB for a compressed one, which is one whole stored block, and never more than the bytes still selected; Streams: the command limit), requires the exact decoded length and refuses an unknown codec. Rust decodes with `ruzstd`; TypeScript decodes through the same Rust code in WASM. SDK reference providers send `CODEC_NONE`. An Objects HTTP NDJSON line may now be up to 12 MiB, so one compressed 8 MiB frame fits.
- `LocalStream` group-commits its journal: mutations apply and write frames in one order under a short lock, then share one device flush outside it, so writers on independent paths and readers no longer queue behind every fsync. Acknowledgement and read results still wait for durability, except for state written under `deferring_durability`, which reads may return before it is flushed, as before; the journal format is unchanged.
- Local Objects commits cost what they change, not the store's size: a commit finds the objects it added, replaced or removed by walking only the subtrees the persistent state does not share with the committed one, instead of revisiting every stored object. Compaction moves the bodies its journal carries into as few segments as the segment bounds allow, under one directory synchronization, instead of a segment and two synchronizations per body. The journal and segment formats are unchanged.
- Harness wire adapters report malformed client requests (command, resume, observe, cancel, handshake) as invalid input instead of a retryable storage failure. Cancelling a terminal operation no longer advances its revision, and a cancelled operation waiting on children now closes. `validate_cancel_request` returns a named `CancelControlRequest`.
- WASM bindings throw named `Error` objects carrying a `code`: Harness the numeric wire code, Objects its numeric code, Machines a string code such as `not-found`, and Inference `invalid` or `unavailable` on the new `InferenceProtocolError.code`. The Machines not-found message now reads `not found: â€¦`.
- TypeScript HTTP clients refuse redirects, and every Bearer token must be non-blank, at most 12 KiB (the platform's maximum bearer, `MAX_BEARER_TOKEN_BYTES` in every client) and free of CR, LF and NUL. `HttpSseWireTransport` validates its endpoint, and it and `WebSocketWireTransport` take an optional `maximumMessageBytes` (default 8 MiB) that bounds bodies and each message in UTF-8 bytes. Machines rejects `afterSequence`, `startUnixMs` and `endUnixMs` values that are not safe non-negative integers.
- Actors idempotency keys are limited to 1â€“256 bytes. Family clients treat every loopback address as local and mark credentials sensitive.
- Filesystem materialization rollback includes an edit applied before its progress record became durable, decoded multi-root publications are checked with the coordinator's journal rules, and listing cursors longer than the volume's name bound are rejected.
- Retire the duplicate `acyclic-inference-contract` and `acyclic-stream-wasm` crates; `acyclic_inference::DESCRIPTOR` is available without the host feature. Replace the unmaintained `tsify-next` with `tsify`.
- Add `acyclic-harness-codex` (unpublished): an `Executor` that runs a turn as one pinned Codex CLI 0.155.1 process. Model calls go through a metered local Responses proxy, and the consumer's granted tools are served over MCP. It is qualified end to end against the real binary.

## 0.1.1 - 2026-09-24

### Added

- A unified 0.1.1 release of the Rust and TypeScript SDK families and the
  `acyclic` coding-agent plugin, with one qualified source commit and
  platform-specific binaries.
- Native-view and projection improvements for source-backed workspaces, plus
  real-mount qualification across Linux FUSE, macOS NFS, and Windows ProjFS.

### Changed

- Package documentation now describes each family separately, including its
  provider and host guarantees.
- Release publishing uses exact qualified artifacts and short-lived trusted
  publisher credentials. Existing immutable 0.1.0 Cargo archives are retained;
  changed source is published under 0.1.1.

## 0.1.0 - 2026-09-21 (partial Cargo bootstrap)

### Added

- Published the first 0.1.0 Cargo archives for `acyclic-inference`,
  `acyclic-machines`, and `acyclic-native-runtime`. The remaining SDK packages
  and the binary release were not published at 0.1.0.

See [`release/README.md`](release/README.md) for the release and trusted-publisher
runbook.
