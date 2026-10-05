# Changelog

All notable changes to Acyclic are recorded here. Every Rust crate, npm package,
and coding-agent integration in this repository shares one version and one
release commit.

## 0.2.0 - Unreleased

- Move the public Objects SDK and native Filesystem composition to canonical v2, retiring active v1 clients and engine. Keep published v1 wire history immutable. Existing v1 local roots fail closed without upgrade or overwrite.
- Complete the public Actors v1, Workers v1 and Stream v2 transport surfaces under the unified breaking candidate version. Publication and live Cloud acceptance require separate qualification.
- `LocalStream` group-commits its journal: mutations apply and write frames in one order under a short lock, then share one device flush outside it, so writers on independent paths and readers no longer queue behind every fsync. Acknowledgement and read results still wait for durability; the journal format is unchanged.
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
