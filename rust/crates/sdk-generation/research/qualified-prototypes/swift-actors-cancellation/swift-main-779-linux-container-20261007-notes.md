# Linux Swift container qualification — 2026-10-07

Receipt: `swift-main-779-linux-container-20261007-receipt.json` (SHA-256 `02DD65B531F07D079F60F7020BD07AF47E4FB401A5AD9AC43DA17EBE1BCC6A96`).

The maintained official Swift container was pulled and verified:

- Image: `docker.io/library/swift:6.4.0-jammy`
- Digest and image ID: `sha256:11bb836ebbc05dbe59b28ffa6cb6ca6b452375d7a65b322c5b6c83f44d52f8c0`
- Image size: `1,350,095,773` bytes
- Swift: `6.4 (swift-6.4-RELEASE)`
- Target: `x86_64-unknown-linux-gnu`

The package used the nominal generated artifacts (`acyclic_actors.swift` SHA-256 `699903753C4E4C378E6D05F36917F7BD51D797A9AB0F9D30033F24324DA35140`; UniFFI Swift SHA-256 `CC419003CCB5A1BA006B2298C52A37FB3955299C41573AB6B3C9255C4D496855`) and Rust library SHA-256 `755B758CF1C128C0764B08C5E419989E0EE096C4CAA7E17FF94C56CFA244A231`.

Boundary run output:

`typed ActorId rejected empty input`

`typed CodeSha256 rejected short input`

`typed PositiveU64 rejected zero`

`PASS c8 source-pinned Swift all19 roots, cursor 0/2^63/u64MAX, PositiveU64.max, presence, immutable records, CurrentHead marker, all eight operations`

Cancellation run output:

`baseline started=0 aborted=0 active=0`

`active-before-cancel started=1 aborted=0 active=1`

`task-cancel-outcome=error=CancellationError()`

`final started=1 aborted=1 active=0`

`PASS generated Swift Task.cancel -> Rust future cancel; cancellation argument=nil; no manual handle`

The negative package failed compilation as intended with errors proving `ActorLimits.handlerTimeoutMillis` is a `let` constant and `ActorId(rawValue:)` is unavailable. Task-owned Linux fixture processes were stopped after testing and verified absent.

The official CDN tarball remained unavailable, but the official Swift 6.4.0 Jammy image provided the same maintained toolchain family without changing the producer or generated artifact cohort. The earlier pending Linux receipt remains unchanged.
