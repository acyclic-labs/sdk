# Filesystem topic guide

This guide is the Rust source counterpart for the existing Filesystem topic
routes. The package is `acyclic-fs` at the workspace version (`0.2.0`).

| Existing route topic | Rust-owned section | Source anchor |
| --- | --- | --- |
| `quickstart` | Quickstart | [`examples/embedded_workspace.rs`](../examples/embedded_workspace.rs) |
| `workspaces`, `consistency`, `durability` | Workspaces; consistency and durability | [`model.rs`](../src/model.rs), [`facade.rs`](../src/facade.rs) |
| `branches-merge` | Branches and merge | [`merge_driver.rs`](../src/merge_driver.rs) |
| `files` | Files and content | [`facade.rs`](../src/facade.rs) |
| `mounts` | Mounts | [`mount.rs`](../src/mount.rs) |
| `local-hosted` | Local and hosted deployments | [`hosted.rs`](../src/hosted.rs) |
| `sources`, `s3` | Sources and S3 | [`source.rs`](../src/source.rs), [`s3.rs`](../src/s3.rs) |
| `reference` | Reference and verification | [`lib.rs`](../src/lib.rs), [`wire_service.rs`](../src/wire_service.rs) |

## Workspaces and generations ([model.rs](../src/model.rs), [facade.rs](../src/facade.rs))

`VolumeConfig::portable`, `Lifecycle`, `GenerationSelector`, and `CheckoutMode`
are the core workspace model. A `Volume` owns a generation history and a
`Checkout` provides access to one selected generation. Use
`GenerationSelector::Head` for the moving head or a pinned selector when a
stable read is required. The [quickstart](quickstart.md) shows the complete
local setup and uses `MountedView` to route two checkouts.

## Branches and merge ([merge_driver.rs](../src/merge_driver.rs), [facade.rs](../src/facade.rs))

Forks and merge preparation are explicit in the facade. The public
`MergePreparation`, `GenerationDiff`, `MergeConflict`, `MergePreparation`, and
`CheckoutCommitOutcome` types carry the result of a merge attempt; a caller
must inspect the outcome before publishing. `merge_driver` supplies
`MergeDriverRegistry`, `MergePlan`, and `resolve_merge_plan` for path-aware
conflict handling. No merge is implied by creating a checkout or by mounting a
second volume.

## Consistency and durability ([model.rs](../src/model.rs), [core_state.rs](../src/core_state.rs))

`CheckoutMode` combines `AccessMode`, `ConsistencyMode`, and `MutationMode`.
`TrackingSafe` consistency and `PrivateOverlay` mutations are the safe defaults
used by the executable example. `Lifecycle::Durable` and
`Lifecycle::Ephemeral` select volume lifecycle behavior; provider-specific
durability controls remain in the local storage options. Native builds also expose
deferred authority flushing through `LocalFs::flush_deferred_authority`; the
default profile selects this backend without a consumer feature flag. Cargo omits
native-only dependencies for `wasm32`.

## Files and content ([facade.rs](../src/facade.rs), [kernel](../src/kernel/mod.rs))

The facade models file identity and content separately. `FileDescription`,
`ResolvedFile`, `PinnedReader`, `ContentStager`, `StagedContent`, and
`ResolvedFileRangeReadRequest` are the public pieces for describing, staging,
reading, and range-reading content. Directory pagination uses
`DirectoryPageRequest` and `DirectoryRecordPage`; it is tied to the selected
generation rather than the moving head.

## Mounts ([mount.rs](../src/mount.rs))

`MountedView::builder().mount(path, checkout).build()` creates a validated
path-to-checkout routing table. `resolve`, `route_mut`, `snapshot`, and
`validate_rename` make routing explicit. Mounts route paths; they do not copy
content or widen a checkout's authority. `MountError` reports overlapping or
invalid mount paths.

## Local and hosted deployments ([facade.rs](../src/facade.rs), [hosted.rs](../src/hosted.rs))

`Fs::local(LocalOptions)` is the embedded native backend in the default native
profile. `HostedFs`, `HostedFsOptions`, `HostedWorkspace`, and
`HostedTransaction` are the native hosted client path. The hosted client is
not a `wasm32` surface. See [service availability](service-availability.md)
for the complete feature and target matrix.

## Sources and S3 ([source.rs](../src/source.rs), [s3.rs](../src/s3.rs))

The `source` module contains the source lifecycle (`SourceMode`, `SourceState`,
`SourceOptions`, and `ReconcileOutcome`) and is enabled only where its native
watching prerequisites are available. The `s3` module provides S3 workspace
types and bounded list cursors. S3 HTTP integration is feature-gated by
`s3-http`; documentation should label it unavailable when that feature is not
compiled. It does not turn the embedded backend into an S3 service.

## Reference and verification ([lib.rs](../src/lib.rs), [wire_service.rs](../src/wire_service.rs))

The stable public reference is the crate's rustdoc surface and the exported
wire descriptor in `FILE_DESCRIPTOR_SET`. The descriptor identifies
`acyclic.filesystem.v2` and the shared `acyclic.protocol.v1` handshake. Use
`cargo check --example embedded_workspace` to qualify the
local guide example against the current package revision.
